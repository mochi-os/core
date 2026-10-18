// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Matrix media
//
// The files the Matrix engine's accounts send and receive (the federation
// plan, phase 24). A file a hosted account sends is kept in its user's
// storage and named mxc://<domain>/<id>; another server fetches it signed,
// at /_matrix/federation/v1/media/download/<id>, as a multipart answer of
// its metadata and its bytes. A file another server's user sends is fetched
// the same way, signed as the account's domain, and kept in core's evictable
// cache, never in permanent storage: a miss fetches it again.

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

// content_id_re is the shape of a media id, ours and other servers'.
var content_id_re = regexp.MustCompile(`^[A-Za-z0-9_-]{1,255}$`)

// content_tables makes the engine's table of the files its accounts sent.
func content_tables(db *DB) {
	db.exec("create table if not exists media (id text not null primary key, domain text not null, user text not null, type text not null, name text not null default '', size integer not null default 0, created integer not null)")
}

// content_store keeps a file a hosted account sends and answers what a
// message names it by: {url, name, type, size}, the url its mxc address.
func content_store(user *User, domain string, data []byte, kind string, name string) (map[string]any, error) {
	kind = strings.ToLower(strings.TrimSpace(kind))
	if kind == "" {
		kind = "application/octet-stream"
	}
	if len(kind) > 100 || !strings.Contains(kind, "/") || strings.ContainsAny(kind, " \r\n\"") {
		return nil, fmt.Errorf("invalid type")
	}
	id := strings.ReplaceAll(uid(), "-", "")
	path := media_path(user, id)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return nil, err
	}
	federation_database().exec("insert into media (id, domain, user, type, name, size, created) values (?, ?, ?, ?, ?, ?, ?)", id, domain, user.UID, kind, name, len(data), now())
	return map[string]any{"url": "mxc://" + domain + "/" + id, "name": name, "type": kind, "size": len(data)}, nil
}

// content_serve answers another server's request for one of this server's
// files: the metadata, empty, and the bytes, as the specification's
// multipart answer. A thumbnail is answered with the file itself.
func content_serve(c *gin.Context, r *fclient.FederationRequest, id string) {
	row, _ := federation_database().row("select domain, user, type, name from media where id=?", id)
	if row == nil || !strings.EqualFold(row_string(row, "domain"), string(r.Destination())) {
		federation_error(c, http.StatusNotFound, "M_NOT_FOUND", "no such file")
		return
	}
	user := user_by_uid(row_string(row, "user"))
	if user == nil {
		federation_error(c, http.StatusNotFound, "M_NOT_FOUND", "no such file")
		return
	}
	data, err := os.ReadFile(media_path(user, id))
	if err != nil {
		federation_error(c, http.StatusNotFound, "M_NOT_FOUND", "no such file")
		return
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	metadata, _ := writer.CreatePart(textproto.MIMEHeader{"Content-Type": {"application/json"}})
	metadata.Write([]byte("{}"))
	header := textproto.MIMEHeader{"Content-Type": {row_string(row, "type")}}
	if name := row_string(row, "name"); name != "" {
		header.Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": name}))
	}
	part, _ := writer.CreatePart(header)
	part.Write(data)
	writer.Close()
	c.Header("X-Content-Type-Options", "nosniff")
	c.Data(http.StatusOK, "multipart/mixed; boundary="+writer.Boundary(), body.Bytes())
}

// content_address is where an app fetches a file another server's user
// sent, named by its mxc address: the federation path on its server, which
// mochi.account.media fetches signed for a hosted account.
func content_address(mxc string) string {
	rest, ok := strings.CutPrefix(mxc, "mxc://")
	if !ok {
		return ""
	}
	server, id, found := strings.Cut(rest, "/")
	if !found || server == "" || !content_id_re.MatchString(id) {
		return ""
	}
	return "https://" + server + "/_matrix/federation/v1/media/download/" + id
}

// content_cached is where a fetched file is kept in the cache, and the file
// beside it holding its type.
func content_cached(user *User, server string, id string) string {
	return filepath.Join(cache_dir, "apps", user.UID, "matrix", strings.ReplaceAll(server, ":", "_"), id)
}

// content_fetch fetches a file another server's user sent, as a hosted
// account, from the cache where it is there: its status, type and bytes.
func content_fetch(ctx context.Context, h *matrix_hosted, address string) (int, string, []byte, error) {
	u, err := url.Parse(address)
	if err != nil {
		return 0, "", nil, err
	}
	id, ok := strings.CutPrefix(u.Path, "/_matrix/federation/v1/media/download/")
	if !ok || !content_id_re.MatchString(id) || u.Host == "" {
		return 0, "", nil, fmt.Errorf("not a Matrix file")
	}
	server := u.Host
	path := content_cached(h.user, server, id)
	if data, err := os.ReadFile(path); err == nil {
		kind, _ := os.ReadFile(path + ".type")
		touched := time.Now()
		os.Chtimes(path, touched, touched)
		return 200, string(kind), data, nil
	}
	mine, err := spec.NewUserID(h.address, true)
	if err != nil {
		return 0, "", nil, err
	}
	var kind string
	var data []byte
	if federation_local(spec.ServerName(server)) {
		row, _ := federation_database().row("select user, type from media where id=? and domain=?", id, server)
		owner := user_by_uid(row_string(row, "user"))
		if row == nil || owner == nil {
			return 404, "", nil, nil
		}
		if data, err = os.ReadFile(media_path(owner, id)); err != nil {
			return 404, "", nil, nil
		}
		kind = row_string(row, "type")
	} else {
		response, err := federation_client().DownloadMedia(ctx, mine.Domain(), spec.ServerName(server), id)
		if err != nil {
			return 0, "", nil, err
		}
		defer response.Body.Close()
		if response.StatusCode != 200 {
			return response.StatusCode, "", nil, nil
		}
		kind, data, err = content_read(ctx, h.user, response)
		if err != nil {
			return 0, "", nil, err
		}
	}
	if _, err := cache_write_file(path, bytes.NewReader(data)); err == nil {
		os.WriteFile(path+".type", []byte(kind), 0o644)
	}
	return 200, kind, data, nil
}

// content_read reads a federation download's answer: the file, or the
// address it redirects to, from the multipart answer's second part.
func content_read(ctx context.Context, user *User, response *http.Response) (string, []byte, error) {
	media, parameters, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(media, "multipart/") {
		data, err := read_limited(response.Body, int64(media_maximum))
		return response.Header.Get("Content-Type"), data, err
	}
	reader := multipart.NewReader(response.Body, parameters["boundary"])
	if _, err := reader.NextPart(); err != nil {
		return "", nil, err
	}
	part, err := reader.NextPart()
	if err != nil {
		return "", nil, err
	}
	if location := part.Header.Get("Location"); location != "" {
		request, err := http.NewRequestWithContext(ctx, "GET", location, nil)
		if err != nil {
			return "", nil, err
		}
		redirected, err := network_request(ctx, user, "matrix", request, 3)
		if err != nil {
			return "", nil, err
		}
		defer redirected.Body.Close()
		if redirected.StatusCode != 200 {
			return "", nil, fmt.Errorf("the file's address answered %d", redirected.StatusCode)
		}
		data, err := read_limited(redirected.Body, int64(media_maximum))
		return redirected.Header.Get("Content-Type"), data, err
	}
	data, err := read_limited(io.Reader(part), int64(media_maximum))
	return part.Header.Get("Content-Type"), data, err
}
