// Mochi server: HTTPS certificate selection tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"golang.org/x/crypto/acme"
)

// TestManualCertificateIsServed pins the regression fixed in cfea87d8.
//
// The HTTPS listener used to be started through autotls, which overwrote the
// tls.Config's GetCertificate with the bare autocert manager's — silently
// discarding domains_get_certificate and with it every manually installed
// certificate. Wildcards can only ever be manual, since ACME issues none over
// TLS-ALPN-01 or HTTP-01, so those domains failed to serve entirely.
//
// This drives a real TLS handshake against the configuration the server
// actually uses, so a future change that drops GetCertificate again fails here
// instead of in production.
func TestManualCertificateIsServed(t *testing.T) {
	original := domains_certs
	t.Cleanup(func() { domains_certs = original })

	exact := test_certificate(t, "manual.example")
	wildcard := test_certificate(t, "*.wild.example")
	domains_certs = map[string]*tls.Certificate{
		"manual.example": exact,
		"*.wild.example": wildcard,
	}

	config := web_tls_config()

	// TLS-ALPN-01 validation depends on acme-tls/1 staying advertised: autocert
	// answers those challenges through GetCertificate, which is only reached
	// for a domain with no manual certificate.
	found := false
	for _, protocol := range config.NextProtos {
		if protocol == acme.ALPNProto {
			found = true
		}
	}
	if !found {
		t.Errorf("NextProtos %v does not advertise %q, so TLS-ALPN-01 validation cannot work", config.NextProtos, acme.ALPNProto)
	}

	listener, err := tls.Listen("tcp", "127.0.0.1:0", config)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// Complete the handshake, then drop it; the client only inspects
			// which certificate was chosen.
			go func() {
				if tls_conn, ok := conn.(*tls.Conn); ok {
					_ = tls_conn.Handshake()
				}
				conn.Close()
			}()
		}
	}()

	tests := []struct {
		name   string
		server string
		want   string
	}{
		{"exact host", "manual.example", "manual.example"},
		{"subdomain covered by a wildcard", "anything.wild.example", "*.wild.example"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn, err := tls.Dial("tcp", listener.Addr().String(), &tls.Config{
				ServerName:         test.server,
				InsecureSkipVerify: true, // self-signed by design // nolint
			})
			if err != nil {
				t.Fatalf("handshake for %q failed, so no certificate was served: %v", test.server, err)
			}
			defer conn.Close()
			served := conn.ConnectionState().PeerCertificates
			if len(served) == 0 {
				t.Fatalf("no certificate served for %q", test.server)
			}
			if got := served[0].Subject.CommonName; got != test.want {
				t.Errorf("served certificate for %q is %q, want %q", test.server, got, test.want)
			}
		})
	}

	// A host with neither a manual certificate nor a domains entry must be
	// refused rather than served something arbitrary.
	if _, err := tls.Dial("tcp", listener.Addr().String(), &tls.Config{
		ServerName:         "unknown.example",
		InsecureSkipVerify: true, // nolint
	}); err == nil {
		t.Error("an unknown host was served a certificate")
	}
}

// test_certificate builds a self-signed certificate for one name. A manual
// certificate's provenance is irrelevant here — what matters is whether the
// listener selects it.
func test_certificate(t *testing.T, name string) *tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if ip := net.ParseIP(name); ip != nil {
		template.IPAddresses = []net.IP{ip}
	} else {
		template.DNSNames = []string{name}
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
