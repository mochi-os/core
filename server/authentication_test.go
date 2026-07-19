// Mochi server: Authentication unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
)

// Create a JWT using a specific HMAC secret (test helper)
func jwt_create_with_secret(user_id string, secret []byte) (string, error) {
	claims := mochi_claims{
		User: user_id,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Unix(now()+jwt_expiry, 0)),
			IssuedAt:  jwt.NewNumericDate(time.Unix(now(), 0)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(secret)
	if err != nil {
		return "", err
	}
	return signed, nil
}

// Test jwt_create_with_secret function
func TestJwtCreateWithSecret(t *testing.T) {
	secret := []byte("test-secret-key-12345678901234567890")
	user_id := "u42"

	token, err := jwt_create_with_secret(user_id, secret)
	if err != nil {
		t.Fatalf("jwt_create_with_secret failed: %v", err)
	}

	// Token should be a valid JWT format (3 parts separated by dots)
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Errorf("jwt_create_with_secret produced invalid token format: expected 3 parts, got %d", len(parts))
	}

	// Parse and verify the token
	claims := &mochi_claims{}
	parsed, err := jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
		return secret, nil
	})
	if err != nil {
		t.Fatalf("Failed to parse token: %v", err)
	}

	if !parsed.Valid {
		t.Error("Token is not valid")
	}

	if claims.User != user_id {
		t.Errorf("Token user = %q, want %q", claims.User, user_id)
	}
}

// Test jwt_create_with_secret with different user IDs
func TestJwtCreateWithSecretUserIds(t *testing.T) {
	secret := []byte("test-secret-key-12345678901234567890")

	user_ids := []string{"u0", "u1", "u100", "u999999"}
	for _, user_id := range user_ids {
		token, err := jwt_create_with_secret(user_id, secret)
		if err != nil {
			t.Errorf("jwt_create_with_secret(%q) failed: %v", user_id, err)
			continue
		}

		claims := &mochi_claims{}
		_, err = jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
			return secret, nil
		})
		if err != nil {
			t.Errorf("Failed to parse token for user %q: %v", user_id, err)
			continue
		}

		if claims.User != user_id {
			t.Errorf("Token user = %q, want %q", claims.User, user_id)
		}
	}
}

// Test that different secrets produce different tokens
func TestJwtCreateWithSecretDifferentSecrets(t *testing.T) {
	secret1 := []byte("secret-one-12345678901234567890123")
	secret2 := []byte("secret-two-12345678901234567890123")
	user_id := "u42"

	token1, err := jwt_create_with_secret(user_id, secret1)
	if err != nil {
		t.Fatalf("jwt_create_with_secret with secret1 failed: %v", err)
	}

	token2, err := jwt_create_with_secret(user_id, secret2)
	if err != nil {
		t.Fatalf("jwt_create_with_secret with secret2 failed: %v", err)
	}

	// Tokens should be different due to different secrets
	if token1 == token2 {
		t.Error("Tokens with different secrets should be different")
	}

	// Token1 should verify with secret1 but not secret2
	claims := &mochi_claims{}
	_, err = jwt.ParseWithClaims(token1, claims, func(token *jwt.Token) (interface{}, error) {
		return secret2, nil
	})
	if err == nil {
		t.Error("Token1 should not verify with secret2")
	}
}

// Test token expiration claims
func TestJwtCreateWithSecretExpiry(t *testing.T) {
	secret := []byte("test-secret-key-12345678901234567890")
	user_id := "u42"

	token, err := jwt_create_with_secret(user_id, secret)
	if err != nil {
		t.Fatalf("jwt_create_with_secret failed: %v", err)
	}

	claims := &mochi_claims{}
	_, err = jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
		return secret, nil
	})
	if err != nil {
		t.Fatalf("Failed to parse token: %v", err)
	}

	// Check that expiration is set
	if claims.ExpiresAt == nil {
		t.Error("Token should have ExpiresAt claim")
	}

	// Check that issued at is set
	if claims.IssuedAt == nil {
		t.Error("Token should have IssuedAt claim")
	}

	// ExpiresAt should be after IssuedAt
	if claims.ExpiresAt.Time.Before(claims.IssuedAt.Time) {
		t.Error("ExpiresAt should be after IssuedAt")
	}
}

// Benchmark jwt_create_with_secret
func BenchmarkJwtCreateWithSecret(b *testing.B) {
	secret := []byte("benchmark-secret-key-1234567890123")
	user_id := "u42"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jwt_create_with_secret(user_id, secret)
	}
}

// TestRedirectLocal verifies the OAuth post-login redirect target is confined
// to a same-site relative path — external / protocol-relative / backslash
// targets must be rejected (open-redirect defence).
func TestRedirectLocal(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		// allowed same-site paths
		{"/", "/"},
		{"/login/identity", "/login/identity"},
		{"/settings/?tab=oauth", "/settings/?tab=oauth"},
		{"/feeds/abc/-/posts", "/feeds/abc/-/posts"},
		// rejected -> "" (caller applies its own default)
		{"", ""},
		{"https://evil.example/x", ""},
		{"//evil.example/x", ""},
		{"/\\evil.example/x", ""},
		{"http:/evil", ""},
		{"javascript:alert(1)", ""},
		{"evil.example", ""},
	}
	for _, tc := range cases {
		if got := redirect_local(tc.in); got != tc.want {
			t.Errorf("redirect_local(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
