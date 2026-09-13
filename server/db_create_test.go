// Mochi server: the defensive table creates on the hot paths.
//
// "create table if not exists" is not free even when it does nothing: exec_e
// sends anything beginning CREATE down its schema branch, which throws away
// this handle's whole prepared-statement cache. The send and receive paths call
// these creates on every event. And when the table really is missing, the
// concurrent callers race: SQLite answers all but one "database is locked" at
// once rather than waiting, because a read that has to become a write cannot
// safely block.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"sync"
	"testing"
	"time"
)

func TestDatabaseCreateIsConcurrent(t *testing.T) {
	test_data_directory(t)
	db := db_open("db/create.db")

	// Every caller arrives at once on a database that has none of it yet,
	// which is what the broadcast paths do to a fresh app database.
	const callers = 200
	var wg sync.WaitGroup
	wg.Add(callers)
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			<-start
			db.create("widgets", "create table if not exists widgets (id text not null primary key)")
			db.create("widgets_id", "create index if not exists widgets_id on widgets(id)")
		}()
	}
	close(start)
	wg.Wait()

	if !db.holds("widgets") {
		t.Error("the table was never created")
	}
	if !db.holds("widgets_id") {
		t.Error("the index was never created")
	}
	if n := db.integer("select count(*) from sqlite_master where name='widgets'"); n != 1 {
		t.Errorf("sqlite_master holds %d rows named widgets, want 1", n)
	}
}

// The cost that shows up on every event: a create of a table that is already
// there must not go down exec_e's schema branch, which closes every cached
// statement this handle holds.
func TestDatabaseCreateKeepsThePreparedCache(t *testing.T) {
	test_data_directory(t)
	db := db_open("db/cache.db")
	ddl := "create table if not exists widgets (id text not null primary key)"
	db.create("widgets", ddl)

	// Warm the cache with a statement worth keeping.
	db.integer("select count(*) from widgets")
	if len(db.statement_cache) == 0 {
		t.Fatal("setup: nothing cached to lose")
	}

	db.create("widgets", ddl)
	if len(db.statement_cache) == 0 {
		t.Error("a create of an existing table flushed the prepared-statement cache")
	}
}

// A table something else drops must come back: create remembers nothing, it
// asks the database each time.
func TestDatabaseCreateAfterADrop(t *testing.T) {
	test_data_directory(t)
	db := db_open("db/recreate.db")

	ddl := "create table if not exists widgets (id text not null primary key)"
	db.create("widgets", ddl)
	db.exec("drop table widgets")
	if db.holds("widgets") {
		t.Fatal("setup: the table is still there after the drop")
	}
	db.create("widgets", ddl)
	if !db.holds("widgets") {
		t.Error("a dropped table was not created again")
	}
}

// The schema setups hold lock(db.path) while they call the creates, so create
// must not want that same lock: a mutex is not reentrant and the server would
// wedge on its first app database.
func TestDatabaseCreateUnderThePathLock(t *testing.T) {
	test_data_directory(t)
	db := db_open("db/pathlock.db")

	done := make(chan struct{})
	go func() {
		l := lock(db.path)
		l.Lock()
		defer l.Unlock()
		db.create("widgets", "create table if not exists widgets (id text not null primary key)")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("create deadlocked against lock(db.path)")
	}
}
