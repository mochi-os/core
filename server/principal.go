// Mochi server: who a Starlark call is for. Four separate questions, one
// accessor each:
//
//   - CALLER  - who made this request. nil is a valid answer (anonymous).
// Authorize against this.
//   - STORAGE - whose databases, files and cache this call operates on.
//   - OWNER   - who owns the entity the request addressed.
//   - APP     - which app the call runs as; half of every permission grant.
//
// Conflating caller and storage is the ambient-ownership bug class: "anonymous"
// must not be expressed as "the caller is the owner". Read through these
// accessors rather than the locals, or the decisions drift.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"

	sl "go.starlark.net/starlark"
)

// principal_caller returns the authenticated requester, or nil when nobody
// authenticated. Callers that treat nil as a refusal should say so explicitly;
// callers that legitimately answer an anonymous request should not be reading
// the caller at all, they should be reading the storage account.
func principal_caller(t *sl.Thread) *User {
	user, _ := t.Local("user").(*User)
	return user
}

// principal_owner returns the owner of the addressed entity, or nil where the
// request addressed no entity.
func principal_owner(t *sl.Thread) *User {
	owner, _ := t.Local("owner").(*User)
	return owner
}

// principal_app returns the app the call runs as, or nil when the thread has
// none. Use it rather than a bare t.Local("app").(*App): an unset local is a
// nil interface and does not assert to a concrete type, so that form panics -
// and the locals are unset for the whole of module load.
func principal_app(t *sl.Thread) *App {
	app, _ := t.Local("app").(*App)
	return app
}

// principal_storage returns the account whose data this call operates on. A
// dispatcher that knows the answer sets the storage local (OpenGraph does,
// since it renders one account's entity for any viewer); otherwise the fallback
// below decides.
func principal_storage(t *sl.Thread) (*User, error) {
	if storage, ok := t.Local("storage").(*User); ok && storage != nil {
		return storage, nil
	}

	owner, _ := t.Local("owner").(*User)
	user, _ := t.Local("user").(*User)

	var routing bool
	if action := t.Local("action"); action != nil {
		if a, ok := action.(*Action); ok && a.domain != nil && a.domain.route != nil {
			routing = a.domain.route.context != ""
		}
	}

	if user == nil {
		if owner == nil {
			return nil, fmt.Errorf("no user context available")
		}
		return owner, nil
	}
	if routing {
		if owner == nil {
			return nil, fmt.Errorf("no owner context for domain routing")
		}
		return owner, nil
	}
	return user, nil
}
