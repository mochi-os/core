// Mochi server: Apps unit tests
// Copyright Alistair Cunningham 2025

package main

import (
	"testing"
)

// Test AppVersion.user_allowed() with no requirements
func TestUserAllowedNoRequirements(t *testing.T) {
	av := &AppVersion{}

	// No requirements - anyone is allowed
	if !av.user_allowed(nil) {
		t.Error("user_allowed should return true for nil user when no requirements")
	}

	user := &User{ID: 1, Username: "user@example.com", Role: "user"}
	if !av.user_allowed(user) {
		t.Error("user_allowed should return true for regular user when no requirements")
	}

	admin := &User{ID: 2, Username: "admin@example.com", Role: "administrator"}
	if !av.user_allowed(admin) {
		t.Error("user_allowed should return true for admin when no requirements")
	}
}

// Test AppVersion.user_allowed() with administrator role requirement
func TestUserAllowedAdminRequired(t *testing.T) {
	av := &AppVersion{}
	av.Require.Role = "administrator"

	// Nil user should be denied
	if av.user_allowed(nil) {
		t.Error("user_allowed should return false for nil user when admin required")
	}

	// Regular user should be denied
	user := &User{ID: 1, Username: "user@example.com", Role: "user"}
	if av.user_allowed(user) {
		t.Error("user_allowed should return false for regular user when admin required")
	}

	// Admin should be allowed
	admin := &User{ID: 2, Username: "admin@example.com", Role: "administrator"}
	if !av.user_allowed(admin) {
		t.Error("user_allowed should return true for admin when admin required")
	}
}

// Test AppVersion.user_allowed() with user role requirement
func TestUserAllowedUserRequired(t *testing.T) {
	av := &AppVersion{}
	av.Require.Role = "user"

	// Nil user should be denied
	if av.user_allowed(nil) {
		t.Error("user_allowed should return false for nil user when user role required")
	}

	// Regular user should be allowed
	user := &User{ID: 1, Username: "user@example.com", Role: "user"}
	if !av.user_allowed(user) {
		t.Error("user_allowed should return true for regular user when user role required")
	}

	// Admin should be denied (exact role match required)
	admin := &User{ID: 2, Username: "admin@example.com", Role: "administrator"}
	if av.user_allowed(admin) {
		t.Error("user_allowed should return false for admin when user role required (exact match)")
	}
}
