// Mochi server: Email
// Copyright Alistair Cunningham 2024

package main

import (
	gm "github.com/wneessen/go-mail"
	"net/mail"
)

func email_send(to string, subject string, body string) {
	m := gm.NewMsg()

	err := m.From(email_from)
	if err != nil {
		log_warn("Email failed to set from address '%s': %v", email_from, err)
		return
	}
	err = m.To(to)
	if err != nil {
		log_warn("Email failed to set to address '%s': %v", to, err)
		return
	}
	m.Subject(subject)
	m.SetBodyString(gm.TypeTextPlain, body)

	c, err := gm.NewClient(email_host, gm.WithPort(email_port), gm.WithTLSPolicy(gm.TLSOpportunistic))
	if err != nil {
		log_warn("Email failed to create mail client: %v", err)
		return
	}
	err = c.DialAndSend(m)
	if err != nil {
		log_warn("Email failed to send message: %v", err)
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
