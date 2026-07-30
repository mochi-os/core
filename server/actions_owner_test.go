// Mochi server: a.owner, a.entity and a.routing.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"

	sl "go.starlark.net/starlark"
)

func action_owner(t *testing.T, a *Action) bool {
	t.Helper()
	value, err := a.Attr("owner")
	if err != nil {
		t.Fatalf("a.owner returned %v", err)
	}
	b, ok := value.(sl.Bool)
	if !ok {
		t.Fatalf("a.owner is %T, want bool - a guard written as `if a.owner` must never see anything else", value)
	}
	return bool(b)
}

// TestActionOwner is the field that retires the ownership derivation six apps
// were doing through mochi.entity.get. The cases that matter are the ones where
// the authenticated caller and the entity's owner differ.
func TestActionOwner(t *testing.T) {
	owner := &User{UID: "owneruser"}
	visitor := &User{UID: "visitoruser"}
	entity := &Entity{ID: "1entity", Class: "person", Name: "Owner person", User: owner.UID}

	cases := []struct {
		name    string
		user    *User
		entity  *Entity
		routing string
		want    bool
	}{
		{"owner on a plain route", owner, entity, routing_path, true},
		{"owner on a domain route", owner, entity, routing_domain, true},
		{"another logged-in caller, plain route", visitor, entity, routing_path, false},
		// The escalation. db_user_for_thread returns the owner here, so anything
		// resolving through it reports the visitor as the owner.
		{"another logged-in caller, DOMAIN route", visitor, entity, routing_domain, false},
		{"anonymous", nil, entity, routing_path, false},
		{"class-level action, no entity", owner, nil, routing_class, false},
		{"anonymous, class-level", nil, nil, routing_class, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a := &Action{user: c.user, owner: owner, entity: c.entity, routing: c.routing}
			if got := action_owner(t, a); got != c.want {
				t.Errorf("a.owner = %v, want %v", got, c.want)
			}
		})
	}
}

// TestActionEntity checks the entity is exposed for entity-routed actions and is
// None for class-level ones, so an app can tell "no entity" from "an entity I
// know nothing about".
func TestActionEntity(t *testing.T) {
	entity := &Entity{ID: "1entity", Class: "feed", Name: "A feed", User: "someone"}

	a := &Action{user: &User{UID: "someone"}, entity: entity, routing: routing_path}
	value, err := a.Attr("entity")
	if err != nil {
		t.Fatalf("a.entity returned %v", err)
	}
	// A dict, not an object: `class` is a reserved word in Starlark, so
	// a.entity.class would be a parse error and the field unreachable.
	d, ok := value.(*sl.Dict)
	if !ok {
		t.Fatalf("a.entity is %T, want *starlark.Dict", value)
	}
	for field, want := range map[string]string{"id": "1entity", "class": "feed", "name": "A feed"} {
		got, found, err := d.Get(sl.String(field))
		if err != nil || !found {
			t.Fatalf(`a.entity["%s"] missing (err %v)`, field, err)
		}
		if string(got.(sl.String)) != want {
			t.Errorf(`a.entity["%s"] = %q, want %q`, field, got, want)
		}
	}

	class_level := &Action{user: &User{UID: "someone"}, routing: routing_class}
	value, err = class_level.Attr("entity")
	if err != nil {
		t.Fatalf("a.entity returned %v", err)
	}
	if value != sl.None {
		t.Errorf("a.entity on a class-level action = %v, want None", value)
	}
}

// TestActionRouting pins each mode. The values are a contract with the apps, so
// a rename here is a breaking change and should fail loudly.
func TestActionRouting(t *testing.T) {
	for _, mode := range []string{routing_class, routing_path, routing_direct, routing_domain, routing_hosted} {
		a := &Action{routing: mode}
		value, err := a.Attr("routing")
		if err != nil {
			t.Fatalf("a.routing returned %v", err)
		}
		if string(value.(sl.String)) != mode {
			t.Errorf("a.routing = %q, want %q", value, mode)
		}
	}

	expected := map[string]string{
		routing_class:  "class",
		routing_path:   "path",
		routing_direct: "direct",
		routing_domain: "domain",
		routing_hosted: "hosted",
	}
	for constant, literal := range expected {
		if constant != literal {
			t.Errorf("routing constant = %q, want %q - apps compare against the literal", constant, literal)
		}
	}
}

// TestActionAttrNamesCoversNewFields guards the listing used for introspection
// and error messages; a field absent from AttrNames is invisible to tooling even
// though Attr answers for it.
func TestActionAttrNamesCoversNewFields(t *testing.T) {
	names := map[string]bool{}
	for _, n := range (&Action{}).AttrNames() {
		names[n] = true
	}
	for _, want := range []string{"owner", "entity", "routing"} {
		if !names[want] {
			t.Errorf("AttrNames is missing %q", want)
		}
	}
}
