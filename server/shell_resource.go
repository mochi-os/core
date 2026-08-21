// Mochi server: resource-route response guard
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// The shell exempts attachment and git URLs so they reach the browser as
// top-level responses, but apps author their own paths and can name an action
// to match. The guard fixes the consequence: a resource-route response arriving
// as an executable document (HTML or SVG) gets a sandbox CSP, whatever the
// headers say.

package main

import (
	"strings"

	"github.com/gin-gonic/gin"
)

// shell_resource_policy is the sandbox with no allow-* tokens: opaque origin,
// no script execution, no form submission. Matches what the shell's iframe
// withholds, which is what the exemption skipped.
const shell_resource_policy = "sandbox"

// shell_resource_path reports whether a path takes the resource exemption. Used
// by both shell_wrap_candidate and the guard below, so the exemption and its
// mitigation cannot drift apart.
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

// resource_writer applies the policy when the header block is actually sent -
// WriteHeaderNow and the first Write. NOT on WriteHeader: gin only records the
// status there, and c.Redirect sets Content-Type after c.Status(302).
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
