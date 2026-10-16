// Mochi server: communities on the fediverse
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// A community (the federation plan, phase 20) is a hosted fediverse account
// of kind group, made by the app that owns it on a community name the user
// holds in the handle registry. Its document is a Group that names the app's
// page, its moderators as the collection it is attributed to, and whether
// only they may post; its followers are the remote subscribers, and the app
// announces through it what its members publish from their own accounts.

package main

import (
	"html"
	"time"
)

// account_group says whether a hosted account row is a group.
func account_group(row map[string]any) bool {
	if row == nil {
		return false
	}
	kind, _ := account_data(row)["kind"].(string)
	return kind == "group"
}

// activitypub_group builds a community's document.
func activitypub_group(user *User, a *actor, row map[string]any) map[string]any {
	id := account_url(a.Domain, a.ID)
	data := account_data(row)
	name, _ := row["label"].(string)
	if name == "" {
		name = a.Handle
	}
	restricted, _ := data["restricted"].(bool)
	sensitive, _ := data["sensitive"].(bool)
	page, _ := data["page"].(string)
	if page == "" {
		page = "https://" + a.Domain + "/@" + a.Handle
	}
	document := map[string]any{
		"@context":                  activitypub_context,
		"id":                        id,
		"type":                      "Group",
		"preferredUsername":         a.Handle,
		"name":                      name,
		"url":                       page,
		"inbox":                     id + "/inbox",
		"outbox":                    id + "/outbox",
		"followers":                 id + "/followers",
		"following":                 id + "/following",
		"featured":                  id + "/featured",
		"attributedTo":              id + "/moderators",
		"endpoints":                 map[string]any{"sharedInbox": "https://" + a.Domain + "/~/inbox"},
		"manuallyApprovesFollowers": false,
		"discoverable":              true,
		"indexable":                 true,
		"postingRestrictedToMods":   restricted,
		"sensitive":                 sensitive,
		"published":                 time.Unix(a.Created, 0).UTC().Format(time.RFC3339),
	}
	if key := keypair_get(user, a.ID, key_signing); key != nil {
		document["publicKey"] = map[string]any{"id": id + "#main-key", "owner": id, "publicKeyPem": key.Public}
	}
	if summary, _ := data["summary"].(string); summary != "" {
		document["summary"] = "<p>" + html.EscapeString(summary) + "</p>"
		document["source"] = map[string]any{"content": summary, "mediaType": "text/markdown"}
	}
	if icon, _ := data["icon"].(string); icon != "" {
		document["icon"] = map[string]any{"type": "Image", "url": icon}
	}
	if image, _ := data["image"].(string); image != "" {
		document["image"] = map[string]any{"type": "Image", "url": image}
	}
	return document
}
