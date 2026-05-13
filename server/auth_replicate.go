// Mochi server: POST /_/auth/replicate — signup-with-replicate-from
// Copyright Alistair Cunningham 2026
//
// The "Advanced disclosure" path on the signup form. The user fills in
// their local email (B-local username), the source peer-id, and the
// source-side username they want to replicate from. This handler:
//
//   1. Validates email locally (well-formed, not already taken on B).
//   2. P2P-looks up the source username on the source peer via
//      replication/user-lookup; gets the canonical uid.
//   3. Refuses if the resolved uid is already on this server (the
//      replication-to-self rule — covers prior per-user opt-in and
//      whole-server pair coverage).
//   4. Creates a `pending-replication` placeholder user row with the
//      A-side uid and the B-local email as the local username.
//   5. Emits the `replication/link-request` op to the source peer,
//      kicking off the alice-approves-in-her-settings flow.
//   6. Creates a session for the placeholder and sets the session
//      cookie, so the user lands on B logged-in-but-waiting.
//
// The frontend (#60 frontend half) consumes this endpoint from the
// signup form's Advanced disclosure. It also needs to render a waiting
// banner for `pending-replication` users and re-check status until A
// approves and the placeholder flips to `active`.
//
// All other auth-flow concerns (email verification, MFA, identity
// creation) are deferred to the post-approval state — at that point
// the user logs in with their A-side credentials, which have been
// replicated by the link-approved op.

package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// admin_replica_resolve_user is a package-level function variable so
// tests can stub the synchronous P2P lookup out.
var admin_replica_resolve_user = replication_user_lookup

// admin_replica_emit_link_request is a package-level function variable
// so tests can stub the link-request emit out.
var admin_replica_emit_link_request = replication_emit_link_request

// web_auth_replicate is POST /_/auth/replicate.
// Body: {"email": "<B-local-username>", "source": "<source-peer-id>", "source_username": "<source-side-username>"}
func web_auth_replicate(c *gin.Context) {
	var input struct {
		Email          string `json:"email"`
		Source         string `json:"source"`
		SourceUsername string `json:"source_username"`
	}
	if err := c.ShouldBindJSON(&input); err != nil {
		respond_error(c, http.StatusBadRequest, "invalid_request", "errors.invalid_request", nil)
		return
	}
	if input.Email == "" || input.Source == "" || input.SourceUsername == "" {
		respond_error(c, http.StatusBadRequest, "invalid_request", "errors.invalid_request", nil)
		return
	}
	if !email_valid(input.Email) {
		respond_error(c, http.StatusBadRequest, "invalid_email", "errors.invalid_email", nil)
		return
	}
	if !setting_signup_enabled() {
		respond_error(c, http.StatusForbidden, "signup_disabled", "errors.signup_disabled", nil)
		return
	}

	// Email must be free locally (the local users_username unique
	// constraint would catch this on insert anyway, but a friendly
	// form-level error is better UX).
	udb := db_open("db/users.db")
	if exists, _ := udb.exists("select 1 from users where username=?", input.Email); exists {
		respond_error(c, http.StatusConflict, "username_taken", "errors.username_taken", nil)
		return
	}

	// P2P lookup the source user. Source peer is treated as a routing
	// destination; we don't validate it against any allow-list because
	// the operator's signup policy (setting_signup_enabled) is the
	// gate on whether replication-from-anywhere is allowed at all.
	sourceUID, exists, err := admin_replica_resolve_user(input.Source, input.SourceUsername)
	if err != nil {
		respond_error(c, http.StatusBadGateway, "source_unreachable", "errors.source_unreachable", nil)
		return
	}
	if !exists || sourceUID == "" {
		respond_error(c, http.StatusNotFound, "source_user_not_found", "errors.source_user_not_found", nil)
		return
	}

	// Replication-to-self check: if the source uid is already a user
	// on this server (via a prior per-user opt-in or whole-server pair
	// coverage), refuse with a friendly message.
	if alreadyHere, _ := udb.exists("select 1 from users where uid=?", sourceUID); alreadyHere {
		respond_error(c, http.StatusConflict, "already_replicated", "errors.already_replicated", nil)
		return
	}

	// Create the placeholder. Uid is the A-side canonical uid so the
	// eventual link-approved keys-transfer flips the same row to
	// active. Username is the B-local email per the server-local
	// username rule.
	udb.exec(
		"insert into users (uid, username, status) values (?, ?, 'pending-replication')",
		sourceUID, input.Email)

	// Emit the link-request to the source peer. Source's Settings →
	// Replication page will pick it up; alice approves there.
	admin_replica_emit_link_request(input.Source, input.SourceUsername, "", sourceUID)

	// Session for the placeholder. The frontend renders a waiting
	// banner when it sees status='pending-replication' on the user
	// object; once the placeholder flips to active (link-approved
	// arrives), the user has a working account.
	session := login_create(sourceUID, c.ClientIP(), c.Request.UserAgent())
	web_cookie_set(c, "session", session)

	c.JSON(http.StatusOK, gin.H{
		"status":      "pending",
		"uid":         sourceUID,
		"source":      input.Source,
		"source_user": input.SourceUsername,
	})
}
