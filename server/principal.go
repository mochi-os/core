// Mochi server: who a Starlark call is for.
//
// Three separate questions were being answered by one thread local. They are
// not the same question and they do not always have the same answer:
//
//   - CALLER  - who made this request. nil is a real, valid answer: an
//     anonymous crawler, a public webhook, an unauthenticated
//     P2P frame. Authorize against this.
//   - STORAGE - whose databases, attachments, files and cache this call
//     operates on. Never nil on a call that reads anything.
//   - OWNER   - who owns the entity the request addressed. A fact about the
//     object, not about the request.
//   - APP     - which app the call runs as. Permission grants are (user, app)
//     pairs, so this is half of every authorization decision.
//
// Conflating the first two is the ambient-ownership bug class: the only way
// to reach one account's data was to claim the requester WAS that account, so
// "anonymous" got expressed as "the caller is the owner" - a statement that is
// false, and that every app then has to remember to see through. Four
// OpenGraph handlers carry a comment warning about it; check-ambient-ownership.py
// exists to catch the Starlark half.
//
// Read through these accessors rather than the locals. A gate that reads
// t.Local("user") itself is making an independent decision about which of the
// three questions it is asking, and those decisions drift.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

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
// none.
//
// A bare t.Local("app").(*App) panics in that case rather than yielding nil:
// an unset local is a nil INTERFACE, and a nil interface does not assert to a
// concrete type. So every `if app == nil` guard written below such an
// assertion was unreachable on the one input it was defending against. The
// locals are unset for the whole of module load - the interpreter executes an
// app's .star files before any entry point has set them - so a module-level
// `BASE = mochi.app.url()` reached a builtin with no app and took out the
// interpreter rather than getting the error the builtin had ready.
func principal_app(t *sl.Thread) *App {
	app, _ := t.Local("app").(*App)
	return app
}

// principal_storage returns the account whose data this call operates on.
//
// A dispatcher that knows the answer states it, by setting the storage local.
// OpenGraph does, because it renders one account's entity for any viewer.
// Everywhere else the caller and the storage account are the same person, and
// the fallback below decides.
//
// The domain-routing arm should move out to web_action, the only dispatcher
// where routing exists - a resolver shared by every dispatch path has no
// business knowing about HTTP. It stays for now because moving it is not
// behaviour-neutral: threads built directly, as the tests and any future
// non-web dispatcher build them, would stop seeing the rule. That is its own
// change with its own evidence, not a rider on this one.
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
