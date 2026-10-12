// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: XMPP client
//
// A linked XMPP account, signed in with the user's address and password. The
// protocol has no token to keep, so the password is kept, and the manager
// holds one connection per linked account for as long as it is linked: the
// server is found by its SRV records, the stream secured by TLS, the account
// authenticated by SCRAM where the server offers it and PLAIN otherwise, and
// each stanza that arrives handed to the chat functions through
// linked_inbound. Conversations and rooms are chat's, built on this in its
// own phases.

package main

import (
	"context"
	"crypto/tls"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"time"

	"mellium.im/sasl"
	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/dial"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

// xmpp_connect_timeout bounds finding the server and negotiating the stream.
const xmpp_connect_timeout = 30 * time.Second

// xmpp_dial opens the connection to an address's server: the SRV records
// decide the host and whether TLS is immediate, and the dialer's control hook
// refuses a private address as every outbound request does. A variable so
// tests can hand over a connection of their own.
var xmpp_dial = func(ctx context.Context, address jid.JID) (net.Conn, error) {
	dialer := dial.Dialer{
		Dialer: net.Dialer{
			Timeout: 10 * time.Second,
			Control: func(network string, address string, _ syscall.RawConn) error {
				return url_address_allowed(address)
			},
		},
		TLSConfig: &tls.Config{ServerName: address.Domainpart()},
	}
	return dialer.Dial(ctx, "tcp", address)
}

// xmpp_address reads the address a user typed as a bare JID.
func xmpp_address(text string) (jid.JID, error) {
	text = strings.TrimPrefix(strings.TrimSpace(text), "xmpp:")
	address, err := jid.Parse(text)
	if err != nil || address.Localpart() == "" || address.Domainpart() == "" {
		return jid.JID{}, fmt.Errorf("invalid address")
	}
	return address.Bare(), nil
}

// xmpp_session opens an authenticated session for an account.
func xmpp_session(ctx context.Context, address jid.JID, password string) (*xmpp.Session, error) {
	ctx, cancel := context.WithTimeout(ctx, xmpp_connect_timeout)
	defer cancel()
	conn, err := xmpp_dial(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("the server could not be reached: %v", err)
	}
	session, err := xmpp.NewClientSession(ctx, address, conn,
		xmpp.StartTLS(&tls.Config{ServerName: address.Domainpart()}),
		xmpp.SASL("", password, sasl.ScramSha256Plus, sasl.ScramSha1Plus, sasl.ScramSha256, sasl.ScramSha1, sasl.Plain),
		xmpp.BindResource(),
	)
	if err != nil {
		conn.Close()
		if xmpp_refused(err) {
			return nil, linked_signed_out
		}
		return nil, fmt.Errorf("the session could not be started: %v", err)
	}
	return session, nil
}

// xmpp_refused reports whether a negotiation error is the server refusing
// the credential.
func xmpp_refused(err error) bool {
	text := err.Error()
	for _, condition := range []string{"not-authorized", "credentials-expired", "account-disabled", "invalid-authzid"} {
		if strings.Contains(text, condition) {
			return true
		}
	}
	return false
}

// xmpp_close ends a session and its connection, giving the stream five
// seconds to close cleanly.
func xmpp_close(session *xmpp.Session) {
	session.SetCloseDeadline(time.Now().Add(5 * time.Second))
	session.Close()
	if conn := session.Conn(); conn != nil {
		conn.Close()
	}
}

// xmpp_link signs in once with the fields of the form - the address and the
// password - and answers the account and what to store, the password among it.
func xmpp_link(ctx context.Context, user *User, fields map[string]string) (*linked_profile, map[string]any, error) {
	address, err := xmpp_address(fields["address"])
	if err != nil {
		return nil, nil, err
	}
	if err := host_allowed(user, "", address.Domainpart()); err != nil {
		return nil, nil, err
	}
	session, err := xmpp_session(ctx, address, fields["password"])
	if err != nil {
		if err == linked_signed_out {
			return nil, nil, fmt.Errorf("the server did not accept the sign-in")
		}
		return nil, nil, err
	}
	go xmpp_close(session)
	bare := address.String()
	return &linked_profile{
			ID:      "xmpp:" + bare,
			Address: bare,
			Name:    address.Localpart(),
			URL:     "xmpp:" + bare,
			Server:  address.Domainpart(),
		}, map[string]any{
			"address":  bare,
			"password": fields["password"],
		}, nil
}

// xmpp_verify signs in once to check the credential.
func xmpp_verify(ctx context.Context, l *linked) error {
	address, err := xmpp_address(l.get("address"))
	if err != nil {
		return err
	}
	session, err := xmpp_session(ctx, address, l.get("password"))
	if err != nil {
		return err
	}
	go xmpp_close(session)
	return nil
}

// xmpp_run is the account's connection: a session is opened, presence sent,
// and every stanza handed on until the stream ends. It returns when the
// connection fails, to be run again, or with linked_signed_out when the
// server refuses the credential.
func xmpp_run(ctx context.Context, l *linked, s *linked_session) error {
	address, err := xmpp_address(l.get("address"))
	if err != nil {
		return linked_signed_out
	}
	session, err := xmpp_session(ctx, address, l.get("password"))
	if err != nil {
		return err
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			xmpp_close(session)
		case <-done:
		}
	}()
	if err := session.Send(ctx, stanza.Presence{Type: stanza.AvailablePresence}.Wrap(nil)); err != nil {
		xmpp_close(session)
		return err
	}
	s.connected.Store(true)
	s.reason.Store("")
	err = session.Serve(xmpp.HandlerFunc(func(t xmlstream.TokenReadEncoder, start *xml.StartElement) error {
		xmpp_receive(l, t, start)
		return nil
	}))
	xmpp_close(session)
	if ctx.Err() != nil {
		return nil
	}
	if err == nil || err == io.EOF {
		return fmt.Errorf("the server closed the stream")
	}
	return err
}

// xmpp_receive hands a stanza to linked_inbound: a message with its sender,
// type, id and body, or a presence with its sender and type. The handler's
// reader holds the stanza's children and its end element, so the start element
// is put back for the decoder.
func xmpp_receive(l *linked, t xmlstream.TokenReadEncoder, start *xml.StartElement) {
	d := xml.NewTokenDecoder(xmlstream.MultiReader(xmlstream.Token(*start), t))
	switch start.Name.Local {
	case "message":
		var message struct {
			stanza.Message
			Body    string `xml:"body"`
			Subject string `xml:"subject"`
		}
		if err := d.Decode(&message); err != nil && err != io.EOF {
			return
		}
		linked_inbound(l.user, l.id, "xmpp", map[string]any{
			"kind":    "message",
			"from":    message.From.String(),
			"to":      message.To.String(),
			"type":    string(message.Type),
			"id":      message.ID,
			"body":    message.Body,
			"subject": message.Subject,
		})
	case "presence":
		var presence struct {
			stanza.Presence
			Show   string `xml:"show"`
			Status string `xml:"status"`
		}
		if err := d.Decode(&presence); err != nil && err != io.EOF {
			return
		}
		linked_inbound(l.user, l.id, "xmpp", map[string]any{
			"kind":   "presence",
			"from":   presence.From.String(),
			"type":   string(presence.Type),
			"show":   presence.Show,
			"status": presence.Status,
		})
	}
}
