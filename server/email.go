// Mochi server: Email
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"html"
	"net/mail"
	"strings"

	gm "github.com/wneessen/go-mail"
)

// email_tls_policy returns the go-mail TLS policy implied by the [email] tls
// config flag. Default is opportunistic STARTTLS with full verification;
// `tls = false` switches to plain SMTP, useful when relaying through a
// loopback / LAN postfix whose cert isn't in any public CA chain.
func email_tls_policy() gm.TLSPolicy {
	if email_tls {
		return gm.TLSOpportunistic
	}
	return gm.NoTLS
}

// email_send sends a plain text email.
// email_send_dedup is email_send with a per-user (address, event_id)
// dedup gate. When event_id is non-empty and the user already received
// an email for the same (address, event_id) within the TTL window, the
// send is suppressed — two replicas independently invoking the same
// logical notification produce one email per recipient address instead
// of two. Cross-replica dedup is local-only at this layer (same shape
// as webpush_delivered); the small concurrent-emit race is documented
// as acceptable for the user-facing duplicate-email impact.
func email_send_dedup(u *User, event_id, to, subject, body string) {
	if event_id != "" && u != nil && email_already_delivered(u, to, event_id) {
		debug("email dedup: address=%q event_id=%q already delivered", to, event_id)
		return
	}
	email_send(to, subject, body)
	if event_id != "" && u != nil {
		email_mark_delivered(u, to, event_id)
	}
}

// email_dedup_db opens the per-user notifications DB (shared with
// webpush_delivered) and lazily creates the email_delivered table.
func email_dedup_db(u *User) *DB {
	return db_user(u, "notifications")
}

// email_already_delivered consults the per-user dedup table. Returns
// true when an earlier call already recorded a delivery to this
// (address, event_id) inside the TTL window.
func email_already_delivered(u *User, address, event_id string) bool {
	db := email_dedup_db(u)
	exists, _ := db.exists("select 1 from email_delivered where address=? and event_id=? and ts > ?", address, event_id, now()-email_dedup_ttl)
	return exists
}

// email_mark_delivered records (address, event_id) and opportunistically
// prunes stale rows.
func email_mark_delivered(u *User, address, event_id string) {
	ts := now()
	db := email_dedup_db(u)
	db.exec("insert or ignore into email_delivered (address, event_id, ts) values (?, ?, ?)", address, event_id, ts)
	db.exec("delete from email_delivered where ts < ?", ts-email_dedup_ttl)
}

// Dedup TTL — same 24h window as webpush_dedup so the two backends'
// rolloff is uniform from the user's point of view.
const email_dedup_ttl int64 = 24 * 3600

// email_deliverable reports whether an address could ever receive mail. It
// returns false for the RFC 2606 / RFC 6761 reserved domains — mail to them is
// guaranteed to bounce, so attempting it only spams the admin with failures.
func email_deliverable(address string) bool {
	at := strings.LastIndex(address, "@")
	if at < 0 {
		return false
	}
	domain := strings.ToLower(strings.TrimSpace(address[at+1:]))
	if domain == "" {
		return false
	}
	for _, d := range []string{"example.com", "example.net", "example.org"} {
		if domain == d || strings.HasSuffix(domain, "."+d) {
			return false
		}
	}
	for _, tld := range []string{".test", ".example", ".invalid", ".localhost"} {
		if strings.HasSuffix(domain, tld) {
			return false
		}
	}
	return true
}

func email_send(to string, subject string, body string) {
	// Never attempt delivery to a reserved / non-deliverable domain (RFC 2606
	// + 6761): example.com/.net/.org and the .test/.example/.invalid/.localhost
	// TLDs can never receive mail, so a send only produces a bounce back to the
	// admin. Test harnesses sign up throwaway @example.com users; this stops
	// their verification codes from bouncing, on every instance, while real
	// addresses (including the admin error-mail recipient) are unaffected.
	if !email_deliverable(to) {
		debug("Email suppressed to reserved/undeliverable address %q", to)
		return
	}
	m := gm.NewMsg()

	from := setting_get("email_from", "mochi-server@localhost")
	err := m.From(from)
	if err != nil {
		info("Email failed to set from address %q: %v", from, err)
		return
	}
	err = m.To(to)
	if err != nil {
		info("Email failed to set to address %q: %v", to, err)
		return
	}
	m.Subject(subject)
	m.SetBodyString(gm.TypeTextPlain, body)

	c, err := gm.NewClient(email_host, gm.WithPort(email_port), gm.WithTLSPolicy(email_tls_policy()))
	if err != nil {
		info("Email failed to create mail client: %v", err)
		return
	}
	err = c.DialAndSend(m)
	if err != nil {
		info("Email failed to send message: %v", err)
		return
	}
}

// email_send_html sends an HTML email.
func email_send_html(to string, subject string, html string) {
	if !email_deliverable(to) {
		debug("Email suppressed to reserved/undeliverable address %q", to)
		return
	}
	m := gm.NewMsg()

	from := setting_get("email_from", "mochi-server@localhost")
	err := m.From(from)
	if err != nil {
		info("Email failed to set from address %q: %v", from, err)
		return
	}
	err = m.To(to)
	if err != nil {
		info("Email failed to set to address %q: %v", to, err)
		return
	}
	m.Subject(subject)
	m.SetBodyString(gm.TypeTextHTML, html)

	c, err := gm.NewClient(email_host, gm.WithPort(email_port), gm.WithTLSPolicy(email_tls_policy()))
	if err != nil {
		info("Email failed to create mail client: %v", err)
		return
	}
	err = c.DialAndSend(m)
	if err != nil {
		info("Email failed to send message: %v", err)
		return
	}
}

// email_login_code sends a styled HTML email with a login code, localised to
// the given language (BCP 47 tag) via the core label resolver's fallback
// chain. When `user` is non-nil, the send is deduped per (address, code)
// so two replicas independently generating the same login round don't
// produce two emails for the same browser session. Each issued code is
// distinct so the dedup never blocks a legitimate later code.
func email_login_code(user *User, to string, code string, language string) {
	if user != nil && email_already_delivered(user, to, "login:"+code) {
		debug("email_login_code dedup: address=%q already delivered", to)
		return
	}

	subject := resolve_core_label(language, "email.login_code.subject", nil)
	heading := resolve_core_label(language, "email.login_code.heading", nil)
	tagline := resolve_core_label(language, "email.login_code.tagline", nil)
	expiry := resolve_core_label(language, "email.login_code.expiry", nil)
	ignore := resolve_core_label(language, "email.login_code.ignore", nil)

	// Plain-text part: the same instruction, code, and notice as the HTML, with
	// the code on its own line. Reuses the tagline/expiry/ignore labels (also
	// used by the HTML body) so there's a single translated source for each.
	text := tagline + ":\n\n" + code + "\n\n" + expiry + ". " + ignore + "\n"
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
            <td style="padding: 40px 40px 32px 40px; text-align: center;">
              <h1 style="margin: 0 0 8px 0; font-size: 24px; font-weight: 600; color: #18181b;">` + html.EscapeString(heading) + `</h1>
              <p style="margin: 0; font-size: 15px; color: #71717a;">` + html.EscapeString(tagline) + `</p>
            </td>
          </tr>
          <tr>
            <td style="padding: 0 40px;">
              <div style="background-color: #f4f4f5; border-radius: 8px; padding: 24px; text-align: center;">
                <span style="font-family: 'SF Mono', Monaco, 'Cascadia Code', monospace; font-size: 32px; font-weight: 600; letter-spacing: 4px; color: #18181b;">` + html.EscapeString(code) + `</span>
              </div>
            </td>
          </tr>
          <tr>
            <td style="padding: 32px 40px 40px 40px; text-align: center;">
              <p style="margin: 0; font-size: 14px; color: #a1a1aa;">` + html.EscapeString(expiry+". "+ignore) + `</p>
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
		email_mark_delivered(user, to, "login:"+code)
	}
}

// email_send_multipart sends an email with both plain text and HTML parts.
func email_send_multipart(to string, subject string, text string, html string) {
	if !email_deliverable(to) {
		debug("Email suppressed to reserved/undeliverable address %q", to)
		return
	}
	m := gm.NewMsg()

	from := setting_get("email_from", "mochi-server@localhost")
	err := m.From(from)
	if err != nil {
		info("Email failed to set from address %q: %v", from, err)
		return
	}
	err = m.To(to)
	if err != nil {
		info("Email failed to set to address %q: %v", to, err)
		return
	}
	m.Subject(subject)
	m.SetBodyString(gm.TypeTextPlain, text)
	m.AddAlternativeString(gm.TypeTextHTML, html)

	c, err := gm.NewClient(email_host, gm.WithPort(email_port), gm.WithTLSPolicy(email_tls_policy()))
	if err != nil {
		info("Email failed to create mail client: %v", err)
		return
	}
	err = c.DialAndSend(m)
	if err != nil {
		info("Email failed to send message: %v", err)
		return
	}
}

func email_valid(address string) bool {
	_, err := mail.ParseAddress(address)
	if err != nil {
		return false
	}
	return true
}
