// Mochi server: stamps the serving host's peer id into a response cookie
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Every response stamps a `mochi-server-id` cookie naming the peer that served
// it. Nothing in the server reads it back: it is a marker for a downstream
// routing layer, and for an operator asking which host answered a request.

package main

import (
	"github.com/gin-gonic/gin"
)

// sticky_session_cookie is the cookie name.
const sticky_session_cookie = "mochi-server-id"

// web_sticky_session is a gin middleware that stamps the local peer id into the
// `mochi-server-id` cookie whenever the cookie is unset or names a different
// peer. A cookie that already matches is left alone.
//
// Runs before security_headers so the cookie travels in the same response.
// Cheap — one cookie read, one optional cookie set.
func web_sticky_session(c *gin.Context) {
	existing := web_cookie_get(c, sticky_session_cookie, "")
	if existing == net_id {
		c.Next()
		return
	}

	// Skip cross-site requests. The sandboxed app iframe has an opaque origin,
	// so its sub-resource (avatar/thumbnail/photo) and API requests are
	// cross-site; the browser rejects a SameSite=Lax Set-Cookie in that context
	// (and never sends the cookie back on such requests), so stamping is futile
	// — it just floods the console with "cookie rejected … cross-site" warnings
	// and can't pin anything. Sticky pinning only works for, and only matters
	// for, same-site top-level navigations, which still get stamped below.
	if c.GetHeader("Sec-Fetch-Site") == "cross-site" {
		c.Next()
		return
	}

	// The cookie is unset or names a different peer. Replace it either way: the
	// request reached this host, so this host is what the cookie should name.
	web_cookie_set(c, sticky_session_cookie, net_id)
	c.Next()
}
