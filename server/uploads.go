// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: XMPP file uploads
//
// The upload service (XEP-0363) of each domain this server answers for on
// XMPP (the federation plan, phase 26): a hosted account's client asks the
// server for a slot, puts the file to the address the slot names with the
// token it gives, and the file is kept with its user's files, fetched by
// anyone from the same address. A slot is used once, within five minutes. The
// files chat sends through a hosted account, and those a relaying member's
// server sends a relayed group's room, are kept the same way and sent as
// their address, as any XMPP client sends a file.

package main

import (
	"crypto/subtle"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"mellium.im/xmpp/jid"
)

const xmpp_ns_upload = "urn:xmpp:http:upload:0"

// How long a slot may wait to be used.
var xmpp_slot_lifetime = 5 * time.Minute

// The types a file is shown as in a browser; anything else is saved.
var xmpp_upload_inline = []string{"image/png", "image/jpeg", "image/gif", "image/webp", "image/avif"}

// xmpp_upload_request answers a hosted account's client asking for a slot on
// its own domain: a name to put the file under, the token to put it with,
// and the address it is fetched from; refused where the file is larger than
// the server keeps.
func xmpp_upload_request(s *xmpp_stanza) *xmpp_stanza {
	from, err := jid.Parse(s.get("from"))
	if err != nil {
		return xmpp_failure(s, "bad-request", "modify", "")
	}
	a := xmpp_account_of(from.Bare())
	domain := strings.ToLower(s.get("to"))
	if a == nil || !strings.EqualFold(a.address.Domainpart(), domain) {
		return xmpp_failure(s, "not-allowed", "cancel", "")
	}
	var wrapper struct {
		Request *struct {
			Name string `xml:"filename,attr"`
			Size string `xml:"size,attr"`
			Type string `xml:"content-type,attr"`
		} `xml:"urn:xmpp:http:upload:0 request"`
	}
	if s.decode(&wrapper) != nil || wrapper.Request == nil {
		return xmpp_failure(s, "bad-request", "modify", "")
	}
	request := *wrapper.Request
	size, err := strconv.Atoi(request.Size)
	name := xmpp_upload_name(request.Name)
	if err != nil || size <= 0 || name == "" {
		return xmpp_failure(s, "bad-request", "modify", "")
	}
	if size > media_maximum {
		return xmpp_failure(s, "not-acceptable", "modify", `<file-too-large xmlns="`+xmpp_ns_upload+`"><max-file-size>`+strconv.Itoa(media_maximum)+`</max-file-size></file-too-large>`)
	}
	kind := strings.TrimSpace(request.Type)
	if kind == "" {
		kind = "application/octet-stream"
	}
	id := uid()
	token := random_alphanumeric(32)
	xmpp_database().exec("insert into uploads (id, user, account, domain, name, type, size, token, created, stored) values (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)", id, a.user.UID, a.id, domain, name, kind, size, token, now())
	address := xmpp_upload_address(domain, id, name)
	return xmpp_result(s, `<slot xmlns="`+xmpp_ns_upload+`"><put url="`+xmpp_escape(address)+`"><header name="Authorization">Bearer `+token+`</header></put><get url="`+xmpp_escape(address)+`"/></slot>`)
}

// xmpp_upload_name is a file's name as a slot keeps it: its last part, or ""
// where it has none.
func xmpp_upload_name(name string) string {
	name = filepath.Base(strings.ReplaceAll(strings.TrimSpace(name), "\\", "/"))
	if name == "." || name == "/" || name == ".." || len(name) > 255 || strings.ContainsAny(name, "\x00\r\n") {
		return ""
	}
	return name
}

// xmpp_upload_address is where a file is put and fetched.
func xmpp_upload_address(domain string, id string, name string) string {
	return "https://" + domain + "/xmpp/upload/" + id + "/" + url.PathEscape(name)
}

// xmpp_upload_store keeps a file an app sends through XMPP for a user, on a
// domain, as a slot used at once, and answers {url, name, type, size}.
func xmpp_upload_store(user *User, account string, domain string, data []byte, kind string, name string) (map[string]any, error) {
	name = xmpp_upload_name(name)
	if name == "" {
		name = "file"
	}
	if kind == "" {
		kind = "application/octet-stream"
	}
	id := uid()
	path := media_path(user, id)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return nil, err
	}
	xmpp_database().exec("insert into uploads (id, user, account, domain, name, type, size, token, created, stored) values (?, ?, ?, ?, ?, ?, ?, '', ?, 1)", id, user.UID, account, strings.ToLower(domain), name, kind, len(data), now())
	return map[string]any{"url": xmpp_upload_address(domain, id, name), "name": name, "type": kind, "size": len(data)}, nil
}

// xmpp_upload_serve answers the upload service's address: a file put to its
// slot with the slot's token, or fetched, by anyone, once kept.
func xmpp_upload_serve(c *gin.Context) {
	c.Header("Access-Control-Allow-Origin", "*")
	rest := strings.TrimPrefix(c.Request.URL.Path, "/xmpp/upload/")
	id, _, _ := strings.Cut(rest, "/")
	if c.Request.Method == http.MethodOptions {
		c.Header("Access-Control-Allow-Methods", "GET, HEAD, PUT, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Authorization, Content-Type")
		c.Header("Access-Control-Max-Age", "86400")
		c.AbortWithStatus(http.StatusNoContent)
		return
	}
	row, _ := xmpp_database().row("select * from uploads where id=?", id)
	if row == nil || !strings.EqualFold(row_string(row, "domain"), activitypub_host(c)) {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	user := user_by_uid(row_string(row, "user"))
	if user == nil {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	switch c.Request.Method {
	case http.MethodPut:
		xmpp_upload_put(c, user, row)
	case http.MethodGet, http.MethodHead:
		if row_int(row, "stored") != 1 {
			c.AbortWithStatus(http.StatusNotFound)
			return
		}
		data, err := os.ReadFile(media_path(user, id))
		if err != nil {
			c.AbortWithStatus(http.StatusNotFound)
			return
		}
		kind := row_string(row, "type")
		disposition := "attachment"
		for _, inline := range xmpp_upload_inline {
			if strings.EqualFold(strings.TrimSpace(strings.Split(kind, ";")[0]), inline) {
				disposition = "inline"
			}
		}
		c.Header("Content-Disposition", disposition+`; filename*=UTF-8''`+url.PathEscape(row_string(row, "name")))
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("Content-Security-Policy", "sandbox; default-src 'none'")
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		if c.Request.Method == http.MethodHead {
			c.Header("Content-Type", kind)
			c.Header("Content-Length", strconv.Itoa(len(data)))
			c.Status(http.StatusOK)
			return
		}
		c.Data(http.StatusOK, kind, data)
	default:
		c.AbortWithStatus(http.StatusMethodNotAllowed)
	}
}

// xmpp_upload_put keeps a file put to its slot: with the slot's token, within
// its lifetime, once, and of the size the slot was asked for.
func xmpp_upload_put(c *gin.Context, user *User, row map[string]any) {
	token := row_string(row, "token")
	given := strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")
	if row_int(row, "stored") == 1 || token == "" || subtle.ConstantTimeCompare([]byte(given), []byte(token)) != 1 {
		c.AbortWithStatus(http.StatusForbidden)
		return
	}
	if now()-row_int(row, "created") > int64(xmpp_slot_lifetime/time.Second) {
		c.AbortWithStatus(http.StatusGone)
		return
	}
	size := row_int(row, "size")
	if c.Request.ContentLength >= 0 && c.Request.ContentLength != size {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	data, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, size+1))
	if err != nil || int64(len(data)) != size {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	id := row_string(row, "id")
	db := xmpp_database()
	// Only one put takes the slot, and the file is there before it is
	// fetched.
	xmpp_uploads_lock.Lock()
	defer xmpp_uploads_lock.Unlock()
	if stored, _ := db.exists("select 1 from uploads where id=? and stored=1", id); stored {
		c.AbortWithStatus(http.StatusConflict)
		return
	}
	path := media_path(user, id)
	err = os.MkdirAll(filepath.Dir(path), 0o700)
	if err == nil {
		err = os.WriteFile(path, data, 0o600)
	}
	if err != nil {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	db.exec("update uploads set stored=1, token='' where id=?", id)
	c.Status(http.StatusCreated)
}

// xmpp_uploads_lock takes the slots one put at a time.
var xmpp_uploads_lock sync.Mutex

// xmpp_uploads_prune drops the slots never used.
func xmpp_uploads_prune() {
	xmpp_database().exec("delete from uploads where stored=0 and created<?", now()-int64(xmpp_slot_lifetime/time.Second))
}
