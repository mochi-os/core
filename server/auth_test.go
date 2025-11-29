// Mochi server: Authentication unit tests
// Copyright Alistair Cunningham 2025

package main

import (
	"strings"
	"testing"

	jwt "github.com/golang-jwt/jwt/v5"
)

// Test jwt_create_with_secret function
func TestJwtCreateWithSecret(t *testing.T) {
	secret := []byte("test-secret-key-12345678901234567890")
	userId := 42

	token, err := jwt_create_with_secret(userId, secret)
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

	if claims.User != userId {
		t.Errorf("Token user = %d, want %d", claims.User, userId)
	}
}

// Test jwt_create_with_secret with different user IDs
func TestJwtCreateWithSecretUserIds(t *testing.T) {
	secret := []byte("test-secret-key-12345678901234567890")

	userIds := []int{0, 1, 100, 999999, -1}
	for _, userId := range userIds {
		token, err := jwt_create_with_secret(userId, secret)
		if err != nil {
			t.Errorf("jwt_create_with_secret(%d) failed: %v", userId, err)
			continue
		}

		claims := &mochi_claims{}
		_, err = jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
			return secret, nil
		})
		if err != nil {
			t.Errorf("Failed to parse token for user %d: %v", userId, err)
			continue
		}

		if claims.User != userId {
			t.Errorf("Token user = %d, want %d", claims.User, userId)
		}
	}
}

// Test that different secrets produce different tokens
func TestJwtCreateWithSecretDifferentSecrets(t *testing.T) {
	secret1 := []byte("secret-one-12345678901234567890123")
	secret2 := []byte("secret-two-12345678901234567890123")
	userId := 42

	token1, err := jwt_create_with_secret(userId, secret1)
	if err != nil {
		t.Fatalf("jwt_create_with_secret with secret1 failed: %v", err)
	}

	token2, err := jwt_create_with_secret(userId, secret2)
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
	userId := 42

	token, err := jwt_create_with_secret(userId, secret)
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
	userId := 42

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jwt_create_with_secret(userId, secret)
	}
}
