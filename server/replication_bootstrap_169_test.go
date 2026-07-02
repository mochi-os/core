// #169: the subset guard streams each table row-by-row (db_file_each) instead of
// loading it as one []map[string]any, so a multi-GB DB doesn't OOM the swap. The
// abandon hard-cap (TestBootstrapShouldAbandon) stops the never-settling
// re-transfer loop. The subset LOGIC itself is covered by TestBootstrapSwapSubset*.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestDbFileEachStreamsAndStops(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := db_open("scratch.db")
	db.exec("create table t (id integer primary key, v text)")
	for i := 0; i < 10; i++ {
		db.exec("insert into t (id, v) values (?, ?)", i, "x")
	}
	path := filepath.Join(data_dir, "scratch.db")

	// Streams every row without materialising a slice.
	n := 0
	if err := db_file_each(path, "select * from t", func(map[string]any) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Errorf("streamed %d rows, want 10", n)
	}

	// A callback error stops iteration early and propagates — this is how the
	// subset guard bails the moment it finds a target row absent from the source.
	stop := fmt.Errorf("stop")
	seen := 0
	err := db_file_each(path, "select * from t", func(map[string]any) error {
		seen++
		if seen == 3 {
			return stop
		}
		return nil
	})
	if err != stop {
		t.Errorf("early-stop error = %v, want %v", err, stop)
	}
	if seen != 3 {
		t.Errorf("stopped after %d rows, want 3", seen)
	}
}
