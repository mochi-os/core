package main

import "testing"

// #167: begin_tx_retry opens a tx on a healthy pool and, on a broken/closed pool,
// returns an error after its bounded retries rather than hanging — so exec_replicated
// / exec_app_user FAIL the write instead of degrading to a non-atomic write+emit that
// would silently not replicate.
func TestBeginTxRetry(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	// Healthy pool → a usable tx.
	tx, err := db.begin_tx_retry()
	if err != nil {
		t.Fatalf("begin_tx_retry on a healthy pool: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// Closed pool → error (after the bounded retries), the trouble signal the
	// callers convert into a failed write rather than a diverging one.
	db.internal.Close()
	if _, err := db.begin_tx_retry(); err == nil {
		t.Fatal("begin_tx_retry on a closed pool should return an error, not a tx")
	}
}
