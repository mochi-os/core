// Mochi server: Email
// Copyright Alistair Cunningham 2024-2025

package main

import (
	gm "github.com/wneessen/go-mail"
	"net/mail"
)

// email_send sends a plain text email.
func email_send(to string, subject string, body string) {
	m := gm.NewMsg()

	from := setting_get("email_from", "mochi-server@localhost")
	err := m.From(from)
	if err != nil {
		warn("Email failed to set from address %q: %v", from, err)
		return
	}
	err = m.To(to)
	if err != nil {
		warn("Email failed to set to address %q: %v", to, err)
		return
	}
	m.Subject(subject)
	m.SetBodyString(gm.TypeTextPlain, body)

	c, err := gm.NewClient(email_host, gm.WithPort(email_port), gm.WithTLSPolicy(gm.TLSOpportunistic))
	if err != nil {
		warn("Email failed to create mail client: %v", err)
		return
	}
	err = c.DialAndSend(m)
	if err != nil {
		warn("Email failed to send message: %v", err)
		return
	}
}

// email_send_html sends an HTML email.
func email_send_html(to string, subject string, html string) {
	m := gm.NewMsg()

	from := setting_get("email_from", "mochi-server@localhost")
	err := m.From(from)
	if err != nil {
		warn("Email failed to set from address %q: %v", from, err)
		return
	}
	err = m.To(to)
	if err != nil {
		warn("Email failed to set to address %q: %v", to, err)
		return
	}
	m.Subject(subject)
	m.SetBodyString(gm.TypeTextHTML, html)

	c, err := gm.NewClient(email_host, gm.WithPort(email_port), gm.WithTLSPolicy(gm.TLSOpportunistic))
	if err != nil {
		warn("Email failed to create mail client: %v", err)
		return
	}
	err = c.DialAndSend(m)
	if err != nil {
		warn("Email failed to send message: %v", err)
		return
	}
}

// email_login_code sends a styled HTML email with a login code.
func email_login_code(to string, code string) {
	subject := "Your Mochi login code"
	html := `<!DOCTYPE html>
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
              <h1 style="margin: 0 0 8px 0; font-size: 24px; font-weight: 600; color: #18181b;">Login Code</h1>
              <p style="margin: 0; font-size: 15px; color: #71717a;">Enter this code in your browser to sign in</p>
            </td>
          </tr>
          <tr>
            <td style="padding: 0 40px;">
              <div style="background-color: #f4f4f5; border-radius: 8px; padding: 24px; text-align: center;">
                <span style="font-family: 'SF Mono', Monaco, 'Cascadia Code', monospace; font-size: 32px; font-weight: 600; letter-spacing: 4px; color: #18181b;">` + code + `</span>
              </div>
            </td>
          </tr>
          <tr>
            <td style="padding: 32px 40px 40px 40px; text-align: center;">
              <p style="margin: 0; font-size: 14px; color: #a1a1aa;">This code expires in 1 hour</p>
            </td>
          </tr>
        </table>
        <p style="margin: 24px 0 0 0; font-size: 13px; color: #a1a1aa;">If you didn't request this code, you can safely ignore this email.</p>
      </td>
    </tr>
  </table>
</body>
</html>`
	email_send_html(to, subject, html)
}

// email_send_multipart sends an email with both plain text and HTML parts.
func email_send_multipart(to string, subject string, text string, html string) {
	m := gm.NewMsg()

	from := setting_get("email_from", "mochi-server@localhost")
	err := m.From(from)
	if err != nil {
		warn("Email failed to set from address %q: %v", from, err)
		return
	}
	err = m.To(to)
	if err != nil {
		warn("Email failed to set to address %q: %v", to, err)
		return
	}
	m.Subject(subject)
	m.SetBodyString(gm.TypeTextPlain, text)
	m.AddAlternativeString(gm.TypeTextHTML, html)

	c, err := gm.NewClient(email_host, gm.WithPort(email_port), gm.WithTLSPolicy(gm.TLSOpportunistic))
	if err != nil {
		warn("Email failed to create mail client: %v", err)
		return
	}
	err = c.DialAndSend(m)
	if err != nil {
		warn("Email failed to send message: %v", err)
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
