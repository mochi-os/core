// Mochi server: resource-route response guard
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// shell_wrap_candidate exempts attachment and git URLs from the shell so they
// reach the browser as top-level responses — Chrome's PDF viewer needs a real
// origin, which the shell's sandboxed iframe does not have.
//
// That exemption is decided from the raw URL path, and the URL space belongs to
// the apps: an app declares its own action paths, the validator permits "/"
// freely, and "git" is an ordinary app's route (apps/repositories/app.json).
// So an app can name an action so that it matches the exemption and have its
// own HTML served top-level, same-origin and cookie-bearing, which is where
// POST /_/token mints a JWT for every installed app. allow-popups-to-escape-
// sandbox means it can reach that URL from inside the shell without the user
// doing anything.
//
// The guard here fixes the consequence rather than the classification. What
// makes an unwrapped response dangerous is that it is an executable document in
// the app's origin; a PDF or an image is not. So a resource-route response that
// arrives as HTML (or SVG, which carries script) gets a sandbox CSP, giving it
// the opaque origin and no-scripts treatment the iframe would have applied.
// Genuine resources are untouched and the PDF viewer keeps working.
//
// This is deliberately independent of Sec-Fetch-Dest: a stripped or forged
// header cannot evade it, because the check is on what is being served.

package main

import (
	"strings"

	"github.com/gin-gonic/gin"
)

// shell_resource_policy is the sandbox with no allow-* tokens: opaque origin,
// no script execution, no form submission. Matches what the shell's iframe
// withholds, which is what the exemption skipped.
const shell_resource_policy = "sandbox"

// shell_resource_path reports whether a path takes the resource exemption.
// shell_wrap_candidate calls this to decide what skips the shell, and the guard
// below calls it to decide what needs sandboxing if it comes back as a
// document - one definition, so the exemption and its mitigation cannot drift
// apart.
func shell_resource_path(path string) bool {
	return strings.Contains(path, "/-/attachments/") || strings.Contains(path, "/git/")
}

// shell_resource_executable reports whether a content type creates a document
// that can run script in the response's own origin. text/html is the payload an
// escaped app wants; SVG carries script too, and is why content_type_inline
// already refuses to serve it inline.
func shell_resource_executable(content_type string) bool {
	switch content_type_base(content_type) {
	case "text/html", "application/xhtml+xml", "image/svg+xml":
		return true
	}
	return false
}

// web_resource_guard wraps the writer for requests on a resource-exempt path so
// the response's own content type decides whether it needs sandboxing.
func web_resource_guard(c *gin.Context) {
	if !shell_resource_path(c.Request.URL.Path) {
		c.Next()
		return
	}
	c.Writer = &resource_writer{ResponseWriter: c.Writer}
	c.Next()
}

// resource_writer applies the policy when the header block is actually sent.
//
// Deliberately NOT on WriteHeader: gin's WriteHeader only records the status,
// and c.Status() calls it before the renderer runs. c.Redirect goes
// c.Status(302) -> http.Redirect, and http.Redirect sets Content-Type after
// that, so applying on WriteHeader latched on an empty content type and the
// redirect went out unsandboxed. The send happens in WriteHeaderNow, which gin
// also reaches through the first Write, so those are the hooks.
type resource_writer struct {
	gin.ResponseWriter
	applied bool
}

func (w *resource_writer) apply() {
	if w.applied {
		return
	}
	w.applied = true
	if shell_resource_executable(w.Header().Get("Content-Type")) {
		w.Header().Set("Content-Security-Policy", shell_resource_policy)
	}
}

func (w *resource_writer) WriteHeader(code int) {
	w.ResponseWriter.WriteHeader(code)
}

func (w *resource_writer) WriteHeaderNow() {
	w.apply()
	w.ResponseWriter.WriteHeaderNow()
}

func (w *resource_writer) Write(data []byte) (int, error) {
	w.apply()
	return w.ResponseWriter.Write(data)
}

func (w *resource_writer) WriteString(s string) (int, error) {
	w.apply()
	return w.ResponseWriter.WriteString(s)
}
