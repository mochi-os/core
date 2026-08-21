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

// email_identifier returns a Message-ID value built from the sender's domain.
// The library would derive it from the machine's hostname, often not fully
// qualified, and receiving filters penalise a non-FQDN right-hand side.
func email_identifier(from string) string {
	domain := "localhost"
	if address, err := mail.ParseAddress(from); err == nil {
		if at := strings.LastIndex(address.Address, "@"); at >= 0 {
			domain = address.Address[at+1:]
		}
	}
	return uid() + "@" + domain
}

// email_send_dedup is email_send with a per-user (address, event_id) dedup
// gate: a notification emitted more than once produces one email per recipient
// address within the TTL window. The concurrent-emit race costs a duplicate
// email.
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

// email_send sends a plain text email.
func email_send(to string, subject string, body string) {
	// Never attempt delivery to a reserved domain (RFC 2606 / 6761): example.com
	// and the .test/.example/.invalid/.localhost TLDs can never receive mail, so a
	// send only produces a bounce to the admin. Test harnesses sign up such users.
	if !email_deliverable(to) {
		debug("Email suppressed to reserved/undeliverable address %q", to)
		return
	}
	m := gm.NewMsg()

	from := setting_effective("email_from")
	err := m.From(from)
	if err != nil {
		info("Email failed to set from address %q: %v", from, err)
		return
	}
	m.SetMessageIDWithValue(email_identifier(from))
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

	from := setting_effective("email_from")
	err := m.From(from)
	if err != nil {
		info("Email failed to set from address %q: %v", from, err)
		return
	}
	m.SetMessageIDWithValue(email_identifier(from))
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

// email_login_code sends a styled HTML login code, localised via the core label
// fallback chain. With a non-nil user it dedups per (address, code); codes are
// distinct, so a later legitimate code is never blocked.
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

	from := setting_effective("email_from")
	err := m.From(from)
	if err != nil {
		info("Email failed to set from address %q: %v", from, err)
		return
	}
	m.SetMessageIDWithValue(email_identifier(from))
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

// email_address reduces a header value to the bare lowercase mailbox, so "Alice
// <a@b.com>", " a@b.com " and "A@B.com" are one key; empty when it does not
// parse. Anything keyed on an address - a rate limiter above all - must key on
// this.
func email_address(value string) string {
	parsed, err := mail.ParseAddress(value)
	if err != nil {
		return ""
	}
	return strings.ToLower(parsed.Address)
}
