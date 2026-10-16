// Mochi server: the files a hosted fediverse account's objects carry
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// An image or other file an app uploads through a hosted fediverse account
// (mochi.account.upload) is kept under the user's storage and served at
// /~<account>/media/<id>, the address the object lists it by - a Page's link
// or an attached Document - so every server that takes the object can fetch
// it. The files go with the account.

package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
)

// media_path is where an account's file is kept.
func media_path(user *User, id string) string {
	return filepath.Join(user_storage_dir(user), "media", id)
}

// media_store keeps a file for a hosted fediverse account and answers the
// reference an object names it by: {id, url, data}, data being the Document
// the object may list.
func media_store(user *User, row map[string]any, data []byte, kind string, name string, description string) (map[string]any, error) {
	account, _ := row["id"].(string)
	actor := account_actor(user, account)
	if actor == "" {
		return nil, fmt.Errorf("account has no address")
	}
	kind = strings.ToLower(strings.TrimSpace(kind))
	if kind == "" || len(kind) > 100 || !strings.Contains(kind, "/") || strings.ContainsAny(kind, " \r\n\"") {
		return nil, fmt.Errorf("invalid type")
	}
	id := uid()
	path := media_path(user, id)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return nil, err
	}
	db_user(user, "user").exec("insert into media (id, account, type, name, description, size, created) values (?, ?, ?, ?, ?, ?, ?)", id, account, kind, name, description, len(data), now())
	url := actor + "/media/" + id
	document := map[string]any{"type": media_document_type(kind), "mediaType": kind, "url": url}
	if description != "" {
		document["name"] = description
	}
	return map[string]any{"id": id, "url": url, "data": document}, nil
}

// media_document_type is the ActivityStreams type of a file by its media type.
func media_document_type(kind string) string {
	switch {
	case strings.HasPrefix(kind, "image/"):
		return "Image"
	case strings.HasPrefix(kind, "video/"):
		return "Video"
	case strings.HasPrefix(kind, "audio/"):
		return "Audio"
	}
	return "Document"
}

// media_serve answers /~<account>/media/<id> with the file's bytes.
func media_serve(c *gin.Context, user *User, account string, id string) {
	if id == "" || strings.ContainsAny(id, "/\\.") {
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		return
	}
	row, _ := db_user(user, "user").row("select type from media where id=? and account=?", id, account)
	if row == nil {
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		return
	}
	data, err := os.ReadFile(media_path(user, id))
	if err != nil {
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		return
	}
	c.Header("X-Content-Type-Options", "nosniff")
	c.Header("Cache-Control", "public, max-age=86400")
	c.Data(http.StatusOK, row_string(row, "type"), data)
}

// media_remove drops an account's files with the account.
func media_remove(user *User, account string) {
	db := db_user(user, "user")
	rows, _ := db.rows("select id from media where account=?", account)
	for _, row := range rows {
		os.Remove(media_path(user, row_string(row, "id")))
	}
	db.exec("delete from media where account=?", account)
}
