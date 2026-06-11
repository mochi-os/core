// Mochi server: self-service account closure.
//
// A user closes their own account by calling mochi.user.close() (step-up
// re-authenticated by the calling app, mirroring export). Closure is a soft
// delete: the account's status flips to "closing", a purge timestamp is set
// `account_closing_days` in the future, and every session is revoked so the
// account immediately looks gone. During the grace window the user can
// re-authenticate and reach the reactivation interstitial, which calls
// /_/auth/close/cancel to restore the account. Once the purge timestamp
// passes, closure_manager (leader-gated) hard-deletes the account via
// user_delete, which broadcasts the network tombstone.
//
// Administrators cannot close their own account: a self-closed sole admin
// would strand the server. They must hand off the role (or be closed by
// another admin) first.
package main

import (
	"fmt"
	"html"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// UserPurge is the payload of a "user/purge" replication op — a deliberate,
// signed "the account is gone everywhere" instruction (close / admin full
// delete), distinct from a replicated row delete (which the apply path no-ops
// for safety). Signed by one of the user's identity entities; the receiver
// re-checks its own closing/purge state before acting. AccountGone is always
// true now — the leave form (one host deleting another's copy) was the
// forgeable strip primitive and is replaced by self-asserted membership.
type UserPurge struct {
	User        string
	AccountGone bool
}

// replication_emit_user_purge propagates an authoritative account-gone purge of
// `user` to every replica that holds the account — recipients(user) = the
// per-user host set ∪ all server pairs. Each recipient applies it via
// replication_user_purge_event, which re-checks local state before deleting.
// Must be called while the user's identity entities still exist (the op is
// signed by one of them).
func replication_emit_user_purge(user string) {
	peers := recipients(user)
	if len(peers) == 0 {
		return
	}
	replication_send_user_purge(user, peers)
}

// replication_send_user_purge signs an account-gone UserPurge with one of the
// user's identity entities and sends it to each peer. Only the account-gone
// form exists: the leave form (one host removing another's copy) was the
// forgeable strip primitive and is replaced by self-asserted membership — a
// host removes only its OWN copy, via replication_membership_depart.
func replication_send_user_purge(user string, peers []string) {
	if len(peers) == 0 {
		return
	}
	udb := db_open("db/users.db")
	row, err := udb.row("select id from entities where user=? order by id limit 1", user)
	if err != nil || row == nil {
		warn("user/purge emit: no signing entity for user %q: %v", user, err)
		return
	}
	from, _ := row["id"].(string)
	if from == "" {
		return
	}
	up := &UserPurge{User: user, AccountGone: true}
	for _, peer := range peers {
		m := message(from, from, "replication", "user/purge")
		m.add(up)
		m.send_peer(peer)
	}
}

// replication_user_purge_event applies a received user/purge. The framework has
// already verified the signature against e.from; we additionally require e.from
// to be one of the named user's own identities, so a foreign entity can't
// authorise the deletion. Idempotent (purging an absent user is a no-op).
func replication_user_purge_event(e *Event) {
	var up UserPurge
	if !e.segment(&up) {
		info("user/purge dropping: cannot decode payload")
		return
	}
	if up.User == "" {
		return
	}

	db := db_open("db/users.db")
	row, _ := db.row("select status, purge from users where uid=?", up.User)
	if row == nil {
		return // already gone — idempotent no-op
	}

	// Signer must be one of this user's identities.
	if ok, _ := db.exists("select 1 from entities where user=? and id=?", up.User, e.from); !ok {
		audit_signature_failed(e.from, "user/purge signer is not an identity of "+up.User)
		return
	}

	if !up.AccountGone {
		return // only the account-gone form exists; leave is self-asserted now
	}

	// Ordering / reactivation-race safety: purge only if THIS host's copy
	// is still closing with the deadline elapsed. A reactivated (active)
	// account drops the op; a lagging replica re-derives it later only if
	// the account is genuinely still due.
	status, _ := row["status"].(string)
	purge, _ := row["purge"].(int64)
	if status != "closing" || purge <= 0 || purge > now() {
		return
	}
	if _, err := user_purge_local(up.User, true); err != nil {
		info("user/purge: delete failed for %q: %v", up.User, err)
		return
	}
	audit_user_deleted(up.User, up.User)
}

// account_closing_days is the grace period, in days, between a self-service
// closure and the hard purge. Operator-tunable via [account] closing in the
// config; defaults to 30 (the de-facto deactivation window users expect).
// Floored at 1 so a misconfiguration can't purge instantly.
func account_closing_days() int {
	days := ini_int("account", "closing", 30)
	if days < 1 {
		days = 1
	}
	return days
}

// api_user_close is mochi.user.close(): the caller marks their OWN account
// for deletion after the grace period. Returns the purge timestamp (unix
// seconds). Step-up re-authentication is enforced by the calling app via
// mochi.user.session.reauthenticate before this runs, mirroring export.
func api_user_close(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user, _ := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if user.administrator() {
		return sl_error(fn, "administrators cannot close their own account")
	}

	ip, language := "", ""
	if action, ok := t.Local("action").(*Action); ok && action.web != nil {
		ip = rate_limit_client_ip(action.web)
		language = request_language(action.web, user)
	}

	purge, err := user_close(user, language)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	audit_account_closed(user.Username, ip)
	return sl.MakeInt64(purge), nil
}

// user_close performs the soft delete for an active account: flip status to
// "closing", set the purge timestamp, revoke all sessions, and email the
// user a cancellation notice. Returns the purge timestamp. Errors if the
// account is not currently active (re-closing is a no-op error).
func user_close(user *User, language string) (int64, error) {
	db := db_open("db/users.db")

	row, _ := db.row("select status from users where uid=?", user.UID)
	if row == nil {
		return 0, fmt.Errorf("user not found")
	}
	status, _ := row["status"].(string)
	if status != "active" {
		return 0, fmt.Errorf("account is not active")
	}

	purge := now() + int64(account_closing_days())*86400
	db.exec("update users set status='closing', purge=? where uid=? and status='active'", purge, user.UID)
	replication_emit_users_users_set(user.UID, map[string]string{
		"status": "closing",
		"purge":  fmt.Sprintf("%d", purge),
	})

	// Soft delete: drop every active session so the account looks gone
	// immediately. The user re-authenticates to reach the reactivation
	// interstitial during the grace window.
	sessions_revoke_all(user.UID)

	email_account_closing(user, user.Username, purge, language)
	return purge, nil
}

// web_auth_close_cancel handles POST /_/auth/close/cancel: a user who has
// re-authenticated during the grace window cancels the pending closure and
// reactivates their account. The fresh session minted by the login they just
// completed identifies them — only the holder of the account's login factors
// can reach this, never a stale cookie (closure revoked them all).
func web_auth_close_cancel(c *gin.Context) {
	u := web_auth(c)
	if u == nil {
		respond_error(c, http.StatusUnauthorized, "authentication_required", "errors.authentication_required", nil)
		return
	}
	if u.Status != "closing" {
		respond_error(c, http.StatusBadRequest, "account_not_closing", "errors.account_not_closing", nil)
		return
	}

	db := db_open("db/users.db")
	db.exec("update users set status='active', purge=0 where uid=? and status='closing'", u.UID)
	replication_emit_users_users_set(u.UID, map[string]string{"status": "active", "purge": "0"})

	audit_account_reactivated(u.Username, rate_limit_client_ip(c))
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// user_purge returns the purge timestamp for a user (0 if not closing). Used
// by /_/identity so the reactivation interstitial can show the deletion date.
func user_purge(uid string) int64 {
	db := db_open("db/users.db")
	row, _ := db.row("select purge from users where uid=?", uid)
	if row == nil {
		return 0
	}
	purge, _ := row["purge"].(int64)
	return purge
}

// closure_manager hard-deletes accounts whose grace period has elapsed. Runs
// shortly after startup — a purge that came due while the server was down
// should not wait an hour for the first tick, but the P2P layer needs a
// moment to connect so the user/purge farewell can reach the other
// replicas — then hourly; a coarse tick is fine for a multi-day timer.
func closure_manager() {
	time.Sleep(time.Minute)
	closure_run_due(now())
	for range time.Tick(time.Hour) {
		closure_run_due(now())
	}
}

// closure_run_due purges every account whose purge timestamp has passed.
//
// Deliberately NOT leader-gated. Account data is per-host state: each replica
// holds its own copy and must delete its own, so every host that sees the
// account as due runs user_delete locally — that is how a multi-host account
// converges to deleted everywhere. (The closing status and purge timestamp
// replicate via the close path, so every host independently reaches this
// point once the deadline passes.) Leader-gating would let exactly one host
// delete its copy and strand the rest, because user deletion does not
// propagate as a row op (the apply path no-ops incoming user deletes for
// safety).
//
// The one cross-host side effect — the signed directory/delete tombstone each
// entity broadcasts — is idempotent on receivers, so the handful of redundant
// broadcasts from several hosts purging around the same time are harmless.
func closure_run_due(t int64) {
	db := db_open("db/users.db")
	rows, err := db.rows("select uid from users where status='closing' and purge>0 and purge<=?", t)
	if err != nil {
		return
	}
	for _, row := range rows {
		uid, _ := row["uid"].(string)
		if uid == "" {
			continue
		}
		// Propagate the authoritative purge to every replica (host-set ∪
		// pairs) BEFORE deleting locally — the op is signed by one of the
		// user's entities, which the local delete is about to remove. Each
		// recipient re-checks its own closing/purge state before acting, and
		// the op is idempotent, so it's safe for several hosts to emit it and
		// for it to reach a host that has already purged.
		replication_emit_user_purge(uid)
		if _, err := user_delete(uid); err != nil {
			info("Account closure purge failed for %q: %v", uid, err)
			continue
		}
		audit_user_deleted(uid, uid)
	}
}

// email_account_closing tells the user their account is scheduled for
// deletion. Localised to the user's language via the core label resolver.
// Deduped per (address, purge) so two replicas processing the same closure
// don't email twice.
//
// The email deliberately contains NO link or action button. A "your account
// is scheduled for deletion — click here to cancel" message is a prime
// phishing template; including a real cancel link would train users to click
// such links. The body instead tells them to sign in to their account
// themselves (the reactivation page is reached through normal login).
func email_account_closing(user *User, to string, purge int64, language string) {
	event_id := fmt.Sprintf("closing:%d", purge)
	if user != nil && email_already_delivered(user, to, event_id) {
		return
	}

	date := time.Unix(purge, 0).UTC().Format("2006-01-02")
	args := map[string]any{"date": date}

	subject := resolve_core_label(language, "email.account_closing.subject", nil)
	heading := resolve_core_label(language, "email.account_closing.heading", nil)
	body := resolve_core_label(language, "email.account_closing.body", args)

	text := body + "\n"
	html_body := `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; background-color: #f4f4f5; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="min-height: 100vh;">
    <tr>
      <td align="center" style="padding: 40px 20px;">
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width: 440px; background-color: #ffffff; border-radius: 12px; box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);">
          <tr>
            <td style="padding: 40px; text-align: center;">
              <h1 style="margin: 0 0 16px 0; font-size: 24px; font-weight: 600; color: #18181b;">` + html.EscapeString(heading) + `</h1>
              <p style="margin: 0; font-size: 15px; color: #52525b; line-height: 1.5;">` + html.EscapeString(body) + `</p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>`

	email_send_multipart(to, subject, text, html_body)
	if user != nil {
		email_mark_delivered(user, to, event_id)
	}
}
