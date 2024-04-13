// Comms server: Email
// Copyright Alistair Cunningham 2024

package main

import (
	"fmt"
	"net/mail"
	"net/smtp"
)

func email_send(to string, subject string, body string) {
	log_debug("Sending email, to='%s', subject='%s', body='%s'", to, subject, body)
	message := fmt.Sprintf("From: comms-server@localhost\r\nTo: %s\r\nSubject: %s\r\n\r\n%s", to, subject, body)

	err := smtp.SendMail("127.0.0.1:25", nil, "comms-server@localhost", []string{to}, []byte(message))
	check(err)
	log_debug("Email sent")
}

func email_valid(address string) bool {
	_, err := mail.ParseAddress(address)
	if err != nil {
		return false
	}
	return true
}
