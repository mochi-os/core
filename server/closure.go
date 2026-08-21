// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: self-service account closure.
//
// mochi.user.close() soft-deletes: status flips to "closing", a purge timestamp
// is set `account_closing_days` ahead, and every session is revoked. During the
// grace window the user re-authenticates and /_/auth/close/cancel restores the
// account; after it, closure_manager hard-deletes via user_delete, which
// broadcasts the network tombstone.
//
// Administrators cannot close their own account - a self-closed sole admin
// would strand the server.
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

// api_user_close is mochi.user.close(): the caller marks their OWN account for
// deletion after the grace period, returning the purge timestamp. Step-up
// re-authentication is enforced by the calling app before this runs.
func api_user_close(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/close"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := principal_caller(t)
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

	// Drop every active session so the account looks gone immediately; the user
	// re-authenticates to reach the reactivation interstitial.
	sessions_revoke_all(user.UID)

	email_account_closing(user, user.Username, purge, language)
	return purge, nil
}

// web_auth_close_cancel handles POST /_/auth/close/cancel: a user who has
// re-authenticated during the grace window reactivates their account. Closure
// revoked every session, so only a fresh login can reach this.
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
// shortly after startup so a purge due while the server was down does not wait
// an hour, but late enough for P2P to carry the farewell messages, then hourly.
func closure_manager() {
	time.Sleep(time.Minute)
	closure_run_due(now())
	for range time.Tick(time.Hour) {
		closure_run_due(now())
	}
}

// closure_run_due purges every account whose purge timestamp has passed. The
// one cross-host effect is each entity's directory tombstone, idempotent on
// receivers.
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
		// user_delete broadcasts each entity's directory tombstone before
		// removing this host's copy: the tombstone is signed by the entity,
		// whose key the delete is about to destroy, so the order matters.
		if _, err := user_delete(uid); err != nil {
			info("Account closure purge failed for %q: %v", uid, err)
			continue
		}
		audit_user_deleted(uid, uid)
	}
}

// email_account_closing tells the user their account is scheduled for deletion,
// deduped per (address, purge). The body deliberately carries no link or action
// button - a "click here to cancel" deletion notice is a phishing template, so
// it tells the user to sign in themselves.
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
