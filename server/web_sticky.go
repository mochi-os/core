// Mochi server: sticky-session middleware for whole-server pair
// Copyright Alistair Cunningham 2026
//
// In a paired-server deployment a browser may reach any pair member
// via DNS round-robin (operator-managed; see #69 in the plan). To
// avoid the "I just submitted, where's my data?" UX issue (audit
// pattern #7) where a follow-up read hits a different replica that
// hasn't yet caught up, every response stamps a `mochi-server-id`
// cookie naming the local peer. A session-aware LB or a future
// server-side reverse proxy can read this cookie and pin subsequent
// requests from the same browser to the same peer.
//
// v1 just stamps the cookie. The actual routing layer (LB-based or
// peer-to-peer self-proxy) lives outside this file — DNS-round-robin
// deployments without an LB get the cookie but no behavioural effect
// until a downstream layer reads it. That's acceptable: the cookie is
// idempotent and opaque to clients; if and when a routing layer is
// added, the stamping is already in place.

package main

import (
	"github.com/gin-gonic/gin"
)

// sticky_session_cookie is the cookie name. Constant kept here so the
// LB / proxy implementations elsewhere can reference the same string
// without duplicating the literal.
const sticky_session_cookie = "mochi-server-id"

// web_sticky_session is a gin middleware that stamps the local peer-id
// into the `mochi-server-id` cookie when not already set, or when the
// cookie names a different peer that's no longer in our pair set
// (likely a removed pair member — stale cookie). Subsequent responses
// don't reset the cookie if it already matches.
//
// Runs before security_headers so the cookie travels in the same
// response. Cheap — one cookie read, one optional cookie set.
func web_sticky_session(c *gin.Context) {
	existing := web_cookie_get(c, sticky_session_cookie, "")
	if existing == net_id {
		c.Next()
		return
	}

	// Either the cookie is unset, names this peer (no-op above), or
	// names a different peer. In the third case we need to decide
	// whether to keep the cookie (it points at a known pair member —
	// a downstream LB will route there) or replace it with our own
	// peer-id (it's stale — the named peer is no longer in our pair
	// set or never was). For v1 we always replace when the cookie
	// doesn't match the local peer: simpler, idempotent for the
	// common case, and assumes that if the request reached US then
	// the LB (if any) didn't honour the cookie or there isn't one.
	web_cookie_set(c, sticky_session_cookie, net_id)
	c.Next()
}
