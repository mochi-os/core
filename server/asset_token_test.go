// Mochi server: the token a page puts in an image or attachment URL.
//
// The app token authorizes every action its app has and lives as long as the
// session, which is a year. A URL is copied, pasted, screenshotted and kept, so
// the one that travels in a URL is minted apart: read-only, and good for half
// an hour.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
	"time"
)

// asset_token_session stands up a session both token kinds can be signed under.
func asset_token_session(t *testing.T) {
	t.Helper()
	create_web_test_env(t)
	db := db_open("db/sessions.db")
	db.exec("create table if not exists sessions (user text not null, code text not null, secret text not null default '', expires integer not null, created integer not null default 0, accessed integer not null default 0, address text not null default '', agent text not null default '', primary key (user, code))")
	db.exec("insert into sessions (user, code, secret, expires, created, address, agent) values ('u1', 'asset-session', 'asset-secret-12345678901234567890', ?, ?, '127.0.0.1', 'test')", now()+86400, now())
}

func TestAssetTokenIsShortLivedAndMarked(t *testing.T) {
	asset_token_session(t)

	asset := auth_create_asset_token("u1", "asset-session", "feeds")
	app := auth_create_app_token("u1", "asset-session", "feeds")
	if asset == "" || app == "" {
		t.Fatal("could not mint the tokens")
	}

	claims, err := jwt_verify_claims(asset)
	if err != nil {
		t.Fatalf("verify asset token: %v", err)
	}
	if claims.User != "u1" || claims.App != "feeds" {
		t.Errorf("asset token names user %q app %q, want u1 / feeds", claims.User, claims.App)
	}
	if claims.Purpose != token_purpose_asset {
		t.Errorf("asset token purpose %q, want %q - the purpose is what makes it read-only at the gate", claims.Purpose, token_purpose_asset)
	}

	// Half an hour, not the app token's year: a copied URL is worth little for
	// long. Compared rather than pinned to a number, so the two cannot silently
	// converge.
	life := time.Until(claims.ExpiresAt.Time)
	if life > time.Duration(jwt_expiry_asset)*time.Second || life < time.Duration(jwt_expiry_asset-60)*time.Second {
		t.Errorf("asset token life %v, want about %ds", life, jwt_expiry_asset)
	}
	ordinary, err := jwt_verify_claims(app)
	if err != nil {
		t.Fatalf("verify app token: %v", err)
	}
	if ordinary.Purpose != "" {
		t.Errorf("app token purpose %q, want empty", ordinary.Purpose)
	}
	if !ordinary.ExpiresAt.After(claims.ExpiresAt.Add(24 * time.Hour)) {
		t.Error("the app token must outlive the asset token by far; they have converged")
	}
}

// The socket is a subscription to everything an app sends this user, which is
// not what a URL-borne credential buys.
func TestAssetTokenCannotOpenASocket(t *testing.T) {
	// The websocket fixture, which stands up the users and sessions the
	// handshake path resolves against.
	app := websocket_test_credentials(t)
	_, tagged, ok := websocket_authenticate(
		websocket_test_handshake(t, "", "?key=notifications&token="+app, "", ""))
	if !ok || tagged != "app-entity-1" {
		t.Fatalf("control: an app token must open the socket, got app %q ok %v", tagged, ok)
	}

	asset := auth_create_asset_token("u1", "login1", "app-entity-1")
	_, tagged, ok = websocket_authenticate(
		websocket_test_handshake(t, "", "?key=notifications&token="+asset, "", ""))
	if ok || tagged != "" {
		t.Errorf("an asset token opened a socket: app %q token_auth %v", tagged, ok)
	}
	_, tagged, ok = websocket_authenticate(
		websocket_test_handshake(t, "", "?key=notifications", "", websocket_protocol_token+asset))
	if ok || tagged != "" {
		t.Errorf("an asset token opened a socket through the subprotocol: app %q token_auth %v", tagged, ok)
	}
}
