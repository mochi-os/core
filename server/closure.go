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
// hourly — a coarse tick is fine for a multi-day timer.
func closure_manager() {
	for range time.Tick(time.Hour) {
		closure_run_due(now())
	}
}

// closure_run_due purges every account whose purge timestamp has passed. Each
// purge is leader-gated with strict=true: user_delete broadcasts an
// irreversible network tombstone, so only the strict-majority leader for the
// account may fire it — a partition-isolated minority must not double-delete.
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
		if !replication_leader_claim("closure", uid, true) {
			continue
		}
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
