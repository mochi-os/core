// Mochi server: Groups for access control
// Copyright Alistair Cunningham 2025
//
// Provides app-level groups that can be used as subjects in access control.
// Groups can contain users or other groups (up to group_max_depth levels).

package main

import (
	"fmt"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"strings"
)

const (
	group_max_depth = 10
)

type Group struct {
	ID          string
	Name        string
	Description string
	Created     int64
}

type GroupMember struct {
	Parent  string `db:"parent"`
	Member  string `db:"member"`
	Type    string `db:"type"`
	Created int64  `db:"created"`
}

var api_group = sls.FromStringDict(sl.String("mochi.group"), sl.StringDict{
	"create":      sl.NewBuiltin("mochi.group.create", api_group_create),
	"get":         sl.NewBuiltin("mochi.group.get", api_group_get),
	"list":        sl.NewBuiltin("mochi.group.list", api_group_list),
	"update":      sl.NewBuiltin("mochi.group.update", api_group_update),
	"delete":      sl.NewBuiltin("mochi.group.delete", api_group_delete),
	"add":         sl.NewBuiltin("mochi.group.add", api_group_add),
	"remove":      sl.NewBuiltin("mochi.group.remove", api_group_remove),
	"members":     sl.NewBuiltin("mochi.group.members", apigroup_members),
	"memberships": sl.NewBuiltin("mochi.group.memberships", apigroup_memberships),
})

// Create group tables
func (db *DB) groups_setup() {
	db.exec("create table if not exists groups ( id text not null primary key, name text not null, description text not null default '', created integer not null)")
	db.exec("create table if not exists group_members ( parent text not null, member text not null, type text not null, created integer not null, primary key( parent, member ) )")
	db.exec("create index if not exists group_members_member on group_members( member )")
}

// Get all groups a user belongs to
func (db *DB) group_memberships(user string) []string {
	var groups []string
	seen := make(map[string]bool)

	// Check direct memberships
	current := []string{user}
	current_type := "user"

	for depth := 0; depth < group_max_depth; depth++ {
		if len(current) == 0 {
			break
		}

		// Build query for current level
		placeholders := strings.Repeat("?,", len(current))
		placeholders = placeholders[:len(placeholders)-1]

		args := make([]any, len(current)+1)
		for i, c := range current {
			args[i] = c
		}
		args[len(current)] = current_type

		query := fmt.Sprintf("select parent, member, type, created from group_members where member in (%s) and type=?", placeholders)
		var gms []GroupMember
		err := db.scans(&gms, query, args...)
		if err != nil {
			warn("Database error loading group memberships: %v", err)
			return groups
		}

		// Collect new groups for next iteration
		var next []string
		for _, gm := range gms {
			if !seen[gm.Parent] {
				seen[gm.Parent] = true
				groups = append(groups, gm.Parent)
				next = append(next, gm.Parent)
			}
		}

		// Next iteration looks for groups containing these groups
		current = next
		current_type = "group"
	}

	return groups
}

// Get members of a group
func (db *DB) group_members(group string, recursive bool) []map[string]any {
	if !recursive {
		rows, _ := db.rows("select member, type from group_members where parent=?", group)
		return rows
	}

	// Recursive: expand nested groups
	var results []map[string]any
	seen := make(map[string]bool)

	var expand func(group string, depth int)
	expand = func(group string, depth int) {
		if depth >= group_max_depth {
			return
		}

		var gms []GroupMember
		err := db.scans(&gms, "select member, type from group_members where parent=?", group)
		if err != nil {
			warn("Database error expanding group %q: %v", group, err)
			return
		}
		for _, gm := range gms {
			switch gm.Type {
			case "user":
				if !seen[gm.Member] {
					seen[gm.Member] = true
					results = append(results, map[string]any{"member": gm.Member, "type": "user"})
				}

			case "group":
				expand(gm.Member, depth+1)
			}
		}
	}

	expand(group, 0)
	return results
}

// Check for cycles when adding a group to another group
func (db *DB) group_would_cycle(group string, member_group string) bool {
	if group == member_group {
		return true
	}

	// Check if group is a member (direct or indirect) of member_group
	seen := make(map[string]bool)
	current := []string{member_group}

	for depth := 0; depth < group_max_depth; depth++ {
		if len(current) == 0 {
			break
		}

		var next []string
		for _, g := range current {
			if seen[g] {
				continue
			}
			seen[g] = true

			var gms []GroupMember
			err := db.scans(&gms, "select parent, member, type, created from group_members where member=? and type='group'", g)
			if err != nil {
				warn("Database error checking group containment: %v", err)
				return false
			}
			for _, gm := range gms {
				if gm.Parent == group {
					return true
				}
				next = append(next, gm.Parent)
			}
		}
		current = next
	}

	return false
}

// mochi.group.create(id, name, description?) -> dict: Create a new group
func api_group_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return sl_error(fn, "syntax: <id: string>, <name: string>, [description: string]")
	}

	id, ok := sl.AsString(args[0])
	if !ok || !valid(id, "id") {
		return sl_error(fn, "invalid id")
	}

	name, ok := sl.AsString(args[1])
	if !ok || name == "" {
		return sl_error(fn, "invalid name")
	}

	description := ""
	if len(args) > 2 {
		description, _ = sl.AsString(args[2])
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_user(owner, "user")
	db.exec("replace into groups (id, name, description, created) values (?, ?, ?, ?)", id, name, description, now())

	return sl_encode(map[string]any{"id": id, "name": name, "description": description}), nil
}

// mochi.group.get(id) -> dict or None: Get a group by ID
func api_group_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid id")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_user(owner, "user")
	row, err := db.row("select * from groups where id=?", id)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	if row == nil {
		return sl.None, nil
	}
	return sl_encode(row), nil
}

// mochi.group.list() -> list: List all groups
func api_group_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_user(owner, "user")
	rows, err := db.rows("select * from groups order by name")
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	return sl_encode(rows), nil
}

// mochi.group.update(id, name=..., description=...) -> None: Update a group
func api_group_update(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>, name=..., description=...")
	}

	id, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid id")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_user(owner, "user")

	for _, kw := range kwargs {
		key := string(kw[0].(sl.String))
		val, _ := sl.AsString(kw[1])

		switch key {
		case "name":
			db.exec("update groups set name=? where id=?", val, id)
		case "description":
			db.exec("update groups set description=? where id=?", val, id)
		}
	}

	return sl.None, nil
}

// mochi.group.delete(id) -> None: Delete a group and its memberships
func api_group_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid id")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_user(owner, "user")

	db.exec("delete from groups where id=?", id)
	db.exec("delete from group_members where parent=?", id)
	db.exec("delete from group_members where member=? and type='group'", id)

	return sl.None, nil
}

// mochi.group.add(group, member, type) -> None: Add a member to a group
func api_group_add(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <group: string>, <member: string>, <type: 'user' or 'group'>")
	}

	group, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid group")
	}

	member, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid member")
	}

	member_type, ok := sl.AsString(args[2])
	if !ok || (member_type != "user" && member_type != "group") {
		return sl_error(fn, "type must be 'user' or 'group'")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_user(owner, "user")

	// Check for cycles if adding a group
	if member_type == "group" {
		if db.group_would_cycle(group, member) {
			return sl_error(fn, "adding this group would create a cycle")
		}
	}

	db.exec("replace into group_members (parent, member, type, created) values (?, ?, ?, ?)", group, member, member_type, now())

	return sl.None, nil
}

// mochi.group.remove(group, member) -> None: Remove a member from a group
func api_group_remove(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <group: string>, <member: string>")
	}

	group, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid group")
	}

	member, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid member")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_user(owner, "user")
	db.exec("delete from group_members where parent=? and member=?", group, member)

	return sl.None, nil
}

// mochi.group.members(group, recursive?) -> list: Get members of a group
func apigroup_members(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <group: string>, [recursive: bool]")
	}

	group, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid group")
	}

	recursive := false
	if len(args) > 1 {
		recursive = bool(args[1].Truth())
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_user(owner, "user")
	return sl_encode(db.group_members(group, recursive)), nil
}

// mochi.group.memberships(user) -> list: Get groups a user belongs to
func apigroup_memberships(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <user: string>")
	}

	user, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid user")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_user(owner, "user")
	return sl_encode(db.group_memberships(user)), nil
}
