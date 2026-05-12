// Mochi server: File replication
// Copyright Alistair Cunningham 2026

package main

import (
	"os"
	"path/filepath"
)

// file_sync_max_inline is the largest file that travels inline in a
// FileSync op. Files at or below this size replicate in a single
// round-trip; larger files defer to the chunk protocol (TBD).
//
// Chosen to fit comfortably inside the existing P2P message-size
// constraints with headroom for CBOR overhead and the wrapping
// ReplicationOp envelope. The 1MB threshold catches the vast majority
// of app-data files (avatars, attachments, configs) without forcing
// every write through the chunk protocol.
const file_sync_max_inline = 1024 * 1024

// replication_emit_file_sync packages a file write for replication.
// Called from api_file_write after a successful local write. Skipped
// silently when the user has no UID (pre-v51 row, somehow), when the
// file exceeds the inline threshold (chunk-protocol territory), or
// when there are no recipients to send to.
func replication_emit_file_sync(userUID, appID, path string, data []byte) {
	if userUID == "" || appID == "" {
		return
	}
	if int64(len(data)) > file_sync_max_inline {
		// Chunk protocol lands as a follow-up; for now larger files
		// stay local-only.
		debug("Replication file-sync skipped: %q too large (%d > %d)", path, len(data), file_sync_max_inline)
		return
	}
	payload := cbor_encode(&FileSync{
		Path: path,
		Size: int64(len(data)),
		Data: data,
	})
	replication_emit(userUID, &ReplicationOp{
		Scope:    repl_scope_app,
		User:     userUID,
		Database: appID,
		Table:    "_files",
		Kind:     "sync",
		Payload:  payload,
	})
}

// replication_file_sync_apply lands a replicated file write into the
// per-(user, app) file directory. Defers when the user isn't local yet
// or the app isn't installed; the next pending-drain or a keys-transfer
// landing the user will retry.
func replication_file_sync_apply(userUID, appID string, fs *FileSync) ApplyResult {
	if fs.Path == "" || !valid(fs.Path, "filepath") {
		info("Replication file-sync rejected: invalid path %q", fs.Path)
		return ApplyInvalid
	}

	localID := user_local_id(userUID)
	if localID == 0 {
		return ApplyDeferred
	}
	u := &User{ID: localID, UID: userUID}
	a := app_by_id(appID)
	if a == nil {
		return ApplyDeferred
	}

	base := api_file_base(u, a)
	if err := os.MkdirAll(base, 0755); err != nil {
		warn("Replication file-sync: unable to create base dir %q: %v", base, err)
		return ApplyInvalid
	}

	root, err := os.OpenRoot(base)
	if err != nil {
		warn("Replication file-sync: unable to open root %q: %v", base, err)
		return ApplyInvalid
	}
	defer root.Close()

	dir := filepath.Dir(fs.Path)
	if dir != "." && dir != "" {
		if err := root_mkdir_all(root, dir); err != nil {
			warn("Replication file-sync: unable to create dir %q: %v", dir, err)
			return ApplyInvalid
		}
	}

	f, err := root.OpenFile(fs.Path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		warn("Replication file-sync: unable to open %q: %v", fs.Path, err)
		return ApplyInvalid
	}
	defer f.Close()

	if _, err := f.Write(fs.Data); err != nil {
		warn("Replication file-sync: unable to write %q: %v", fs.Path, err)
		return ApplyInvalid
	}

	debug("Replication file-sync apply: user_uid=%q app=%q path=%q size=%d", userUID, appID, fs.Path, fs.Size)
	return ApplyApplied
}
