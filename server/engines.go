// Mochi server: Protocol engines
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// An engine is a protocol core speaks on the apps' behalf: the DAV engine
// today, ActivityPub, the AT Protocol, Matrix and XMPP as they are built. Core
// carries the transport and the identity; an app supplies the meaning through
// the functions it declares in its manifest, which the engine calls as the
// server (app_call_as_server). Each engine answers fixed paths that the
// protocol's clients and servers expect, so those paths are reserved from
// apps, and a request to one is routed here before any app or entity is
// resolved.

package main

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// engine is one protocol engine's registration: the request paths it answers
// and the handler that answers them. handler reports whether it answered; a
// false lets the request fall through to ordinary routing, which no engine
// registered so far needs.
type engine struct {
	name     string
	prefixes []string
	// setting names the network whose system setting switches the engine off
	// (network_enabled); empty for an engine that is always on, as DAV is.
	setting string
	// body caps a request body on the engine's paths, in bytes. Zero keeps
	// the general cap (web_body_maximum); an engine whose clients upload
	// media or large documents on some route raises it for its paths.
	body    int64
	handler func(c *gin.Context) bool
}

// engines are the registered engines, in registration order. Registration
// happens once, at startup, before the listener is up, so reads need no lock.
var engines []*engine

// engine_segments are the first path segments core reserves for engines: the
// paths Mastodon, Lemmy, Bluesky and Matrix clients and servers expect at fixed
// places, plus the account and object paths (decision 40 of the federation
// plan, "/~<id>" and "/@<handle>", which begin with a character no app path
// may). An app may not claim any of them as its path, so the reservation holds
// before an engine exists to answer; a request to a reserved segment with no
// engine behind it takes the ordinary route and ends as it always did.
var engine_segments = []string{".well-known", "api", "oauth", "pictrs", "xrpc", "_matrix"}

// engine_register adds an engine. A prefix is matched whole: "/xrpc/" answers
// everything under it, "/.well-known/carddav" that one path and nothing else
// beside it, and "/~" or "/@" every path beginning with the sigil.
func engine_register(e *engine) {
	engines = append(engines, e)
}

// engine_path_reserved reports whether an app path is one engines reserve.
// Paths are compared whole, so "api" is refused and "apiary" is not.
func engine_path_reserved(path string) bool {
	for _, s := range engine_segments {
		if path == s {
			return true
		}
	}
	return false
}

// engine_prefix_matches reports whether a request path falls under a
// registered prefix. A prefix ending in "/" or in a sigil takes any path that
// begins with it; any other prefix takes the path itself or paths below it.
func engine_prefix_matches(path string, prefix string) bool {
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	if len(path) == len(prefix) {
		return true
	}
	last := prefix[len(prefix)-1]
	if last == '/' || last == '~' || last == '@' {
		return true
	}
	return path[len(prefix)] == '/'
}

// engine_for returns the engine answering a request path, taking the longest
// matching prefix, or nil.
func engine_for(path string) *engine {
	var found *engine
	length := -1
	for _, e := range engines {
		for _, prefix := range e.prefixes {
			if len(prefix) > length && engine_prefix_matches(path, prefix) {
				found = e
				length = len(prefix)
			}
		}
	}
	return found
}

// engine_route hands a request to its engine, before any app, entity or domain
// route is consulted, and reports whether the engine answered it.
func engine_route(c *gin.Context) bool {
	e := engine_for(c.Request.URL.Path)
	if e == nil {
		return false
	}
	// A network the owner has switched off, or one still behind the development
	// switch, answers nothing: its paths are not found, as they were before the
	// engine existed.
	if e.setting != "" && !network_enabled(e.setting) {
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		return true
	}
	return e.handler(c)
}

// engine_body_maximum returns the body cap an engine sets for a request path,
// or zero when no engine claims it or the engine keeps the general cap.
func engine_body_maximum(path string) int64 {
	if e := engine_for(path); e != nil {
		return e.body
	}
	return 0
}

// engines_start registers the engines core ships. Called once from web_start,
// before the listener accepts requests.
func engines_start() {
	engine_register(dav_engine())
	engine_register(bluesky_client_engine())
}
