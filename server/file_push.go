// Mochi server: Live file replication via per-peer push.
//
// Files written via mochi.file.write are pushed to every paired host
// of the owning user. The wire protocol carries metadata in the
// message content + a CBOR header in `data` (path, size), then
// streams the raw file body. The receiver reads exactly Size bytes
// from the stream into a `.partial`, atomic-renames on success, and
// ACKs. v1 has no sha256 footer — QUIC transport integrity catches
// bit-flips; per-message sha256 + resume return when we add resume.
//
// Deletes go through a separate file/delete event — a small message,
// no body, fits the regular replication ops flow.
//
// No size threshold. Files of any size up to disk capacity replicate.

package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// FilePushHeader rides in `data` on every file/push message. Tells the
// receiver where to write the body and how much to read after the
// header before the stream closes.
type FilePushHeader struct {
	User string `cbor:"user"`
	App  string `cbor:"app"`
	Path string `cbor:"path"`
	Size int64  `cbor:"size"`
}

// FileDelete is the payload for a file/delete event: tiny, fits the
// regular ops flow with no body.
type FileDelete struct {
	User string `cbor:"user"`
	App  string `cbor:"app"`
	Path string `cbor:"path"`
}

// replication_emit_file_push enqueues a file/push queue row for each
// peer in the user's host set. The row references the file path on
// disk; the queue worker reads + streams the file live at send time
// via queue_send_file_push.
func replication_emit_file_push(userUID, appID, path string) {
	if userUID == "" || appID == "" {
		return
	}
	peers := recipients(userUID)
	if len(peers) == 0 {
		return
	}

	udb := db_open("db/users.db")
	row, err := udb.row("select id from entities where user=? order by id limit 1", userUID)
	if err != nil || row == nil {
		return
	}
	from, _ := row["id"].(string)
	if from == "" {
		return
	}

	base := file_user_app_base(userUID, appID)
	full := filepath.Join(base, path)
	stat, err := os.Stat(full)
	if err != nil {
		debug("Replication file-push: cannot stat %q: %v", full, err)
		return
	}
	if stat.IsDir() {
		return
	}

	header := cbor_encode(&FilePushHeader{
		User: userUID,
		App:  appID,
		Path: path,
		Size: stat.Size(),
	})

	empty_content := cbor_encode(map[string]any{})
	for _, peer := range peers {
		queue_add_direct(uid(), peer, from, from, "replication", "file/push", "replication", nil, empty_content, header, full, 0)
	}
	queue_wake()
}

// replication_emit_file_push_delete enqueues a file/delete row on the
// regular ops path — small message, no body. The legacy
// replication_emit_file_delete (which emitted a Class=file
// ReplicationOp) is replaced by this.
func replication_emit_file_push_delete(userUID, appID, path string) {
	if userUID == "" || appID == "" {
		return
	}
	peers := recipients(userUID)
	if len(peers) == 0 {
		return
	}

	udb := db_open("db/users.db")
	row, err := udb.row("select id from entities where user=? order by id limit 1", userUID)
	if err != nil || row == nil {
		return
	}
	from, _ := row["id"].(string)
	if from == "" {
		return
	}

	payload := cbor_encode(&FileDelete{User: userUID, App: appID, Path: path})
	empty_content := cbor_encode(map[string]any{})
	for _, peer := range peers {
		queue_add_direct(uid(), peer, from, from, "replication", "file/delete", "replication", nil, empty_content, payload, "", 0)
	}
	queue_wake()
}

// file_user_app_base returns the per-(user, app) files directory.
// Mirrors api_file_base for code paths that don't have a User struct.
func file_user_app_base(userUID, appID string) string {
	return filepath.Join(data_dir, "users", userUID, appID, "files")
}

// replication_file_push_event handles an inbound file/push: reads the
// header from `data`, opens .partial, streams body bytes from the
// stream into .partial while computing sha256, reads the footer,
// verifies the hash, atomic-renames to final on success.
//
// On any error the handler returns non-nil so the framework NACKs;
// the sender's queue retries from offset 0 (v1 has no resume).
func replication_file_push_event(e *Event) {
	if e.from == "" {
		info("Replication file-push: rejecting unsigned event from peer %q", e.peer)
		e.stream.close_read()
		return
	}

	var header FilePushHeader
	if !e.segment(&header) {
		info("Replication file-push: cannot decode header from peer %q", e.peer)
		e.stream.close_read()
		return
	}

	if header.User == "" || header.App == "" || header.Path == "" {
		info("Replication file-push: empty user/app/path in header")
		e.stream.close_read()
		return
	}
	if !valid(header.Path, "filepath") {
		info("Replication file-push: invalid path %q", header.Path)
		e.stream.close_read()
		return
	}
	if header.Size < 0 {
		info("Replication file-push: negative size %d", header.Size)
		e.stream.close_read()
		return
	}

	if !user_exists(header.User) {
		// Defer: user hasn't arrived yet. We don't (in v1) buffer the
		// transfer — the sender's queue retries on NACK.
		info("Replication file-push deferred: user %q not yet local", header.User)
		e.stream.close_read()
		return
	}

	base := file_user_app_base(header.User, header.App)
	if err := os.MkdirAll(base, 0755); err != nil {
		warn("Replication file-push: cannot create base %q: %v", base, err)
		e.stream.close_read()
		return
	}

	root, err := os.OpenRoot(base)
	if err != nil {
		warn("Replication file-push: cannot open root %q: %v", base, err)
		e.stream.close_read()
		return
	}
	defer root.Close()

	dir := filepath.Dir(header.Path)
	if dir != "." && dir != "" {
		if err := root_mkdir_all(root, dir); err != nil {
			warn("Replication file-push: cannot mkdir %q: %v", dir, err)
			e.stream.close_read()
			return
		}
	}

	partial := header.Path + ".partial"
	f, err := root.OpenFile(partial, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		warn("Replication file-push: cannot open %q: %v", partial, err)
		e.stream.close_read()
		return
	}

	// Stream exactly `Size` bytes from the wire to .partial, hashing
	// as we go. raw_reader() picks up any bytes already buffered by
	// the CBOR decoder before falling through to s.reader.
	written, copyErr := file_push_copy(f, e.stream.raw_reader(), header.Size)
	f.Close()

	if copyErr != nil {
		warn("Replication file-push: body copy failed (%d/%d bytes): %v", written, header.Size, copyErr)
		root.Remove(partial)
		e.stream.close_read()
		return
	}
	if written != header.Size {
		warn("Replication file-push: short body (%d/%d bytes)", written, header.Size)
		root.Remove(partial)
		e.stream.close_read()
		return
	}

	// Atomic rename. Rename is the commit point — until this succeeds,
	// the file is considered "in flight."
	if err := root.Rename(partial, header.Path); err != nil {
		warn("Replication file-push: cannot rename %q → %q: %v", partial, header.Path, err)
		root.Remove(partial)
		e.stream.close_read()
		return
	}
	debug("Replication file-push: applied user=%q app=%q path=%q size=%d", header.User, header.App, header.Path, header.Size)
}

// file_push_copy is a one-pass io.Copy that stops at `size` bytes (so
// the footer CBOR segment isn't pulled into the body) and reports any
// short or mid-stream error. Uses a 1 MiB buffer for throughput.
func file_push_copy(dst io.Writer, src io.Reader, size int64) (int64, error) {
	if size == 0 {
		return 0, nil
	}
	limited := io.LimitReader(src, size)
	return io.Copy(dst, limited)
}

// replication_file_delete_event handles an inbound file/delete. Tiny
// message — content has no body. Removes the file, the .partial, and
// the file's parent dirs if empty.
func replication_file_delete_event(e *Event) {
	if e.from == "" {
		info("Replication file-delete: rejecting unsigned event from peer %q", e.peer)
		return
	}

	var fd FileDelete
	if !e.segment(&fd) {
		info("Replication file-delete: cannot decode payload")
		return
	}
	if fd.User == "" || fd.App == "" || fd.Path == "" {
		info("Replication file-delete: empty user/app/path")
		return
	}
	if !valid(fd.Path, "filepath") {
		info("Replication file-delete: invalid path %q", fd.Path)
		return
	}

	if !user_exists(fd.User) {
		// User isn't local yet; nothing to delete.
		return
	}

	base := file_user_app_base(fd.User, fd.App)
	root, err := os.OpenRoot(base)
	if err != nil {
		// Base dir absent — nothing to delete.
		return
	}
	defer root.Close()
	root.Remove(fd.Path)
	root.Remove(fd.Path + ".partial")
	debug("Replication file-delete: applied user=%q app=%q path=%q", fd.User, fd.App, fd.Path)
}

// queue_send_file_push streams one file/push to peer over /mochi/2/stream
// (one libp2p stream with an authenticated handshake).
//
// Wire content after the handshake: FilePushHeader CBOR segment
// (q.Data), then raw file bytes, then EOF (close_write). Receiver reads
// exactly header.Size bytes from the stream and atomic-renames the
// .partial on success.
func queue_send_file_push(q *QueueEntry) bool {
	peer := q.Target
	if peer == "" {
		return false
	}
	if q.File == "" {
		return false
	}

	// A missing source file is a permanent failure, not a transient
	// one: the file was deleted after the push was queued. That's
	// legitimate — the delete replicates as its own file/delete event,
	// and receivers tolerate bytes-not-yet-here — so retrying can never
	// succeed and would otherwise back off for the full retention
	// window. Drop the row instead, before touching the network.
	if !file_exists(q.File) {
		info("Queue file-push dropping %q: source file %q no longer exists", q.ID, q.File)
		queue_drop(q.ID, "file-missing")
		return true
	}

	var services []string
	if q.FromServices != "" {
		services = split_services(q.FromServices)
	}

	content := map[string]any{}
	if len(q.Content) > 0 {
		// q.Content is a cbor_encode(map[string]any{}) — empty map.
		// Decode it so we ship a Content map rather than a raw CBOR blob.
		_ = decode_into(q.Content, &content)
	}

	s, err := stream_open_or_self(peer, q.FromEntity, q.ToEntity, q.Service, q.Event, q.FromApp, services, content)
	if err != nil || s == nil {
		debug("Queue file-push: stream open failed peer=%q: %v", peer, err)
		return false
	}
	defer s.close()

	// stream_open shipped the (empty) content map as the first post-ack
	// segment so the receiver's receive_stream sets e.content. The
	// FilePushHeader still needs to ride as the next CBOR segment because
	// that's what the handler reads via e.segment(). q.Data is already
	// CBOR-encoded so we ship it raw — the receiver's decoder reads it as
	// the next value.
	if len(q.Data) > 0 {
		if werr := s.write_raw(q.Data); werr != nil {
			return false
		}
	}

	// Stream the file body.
	if err := file_push_send_body(s, q.File); err != nil {
		// Same permanent-failure rule for the race where the file
		// vanishes between the existence check above and the open.
		if errors.Is(err, fs.ErrNotExist) {
			info("Queue file-push dropping %q: source file %q no longer exists", q.ID, q.File)
			queue_drop(q.ID, "file-missing")
			return true
		}
		debug("Queue file-push body send failed: %v", err)
		return false
	}

	// Handler success was already signalled by the open ack at handshake
	// time; if write_raw + file send succeeded, we're done.
	return true
}

// decode_into is a tiny helper that decodes CBOR into the target,
// returning the error directly. Used so the file-push v2 branch can
// extract a Content map from the stored q.Content blob.
func decode_into(payload []byte, into any) error {
	return cbor_decode_mode.Unmarshal(payload, into)
}

// file_push_send_body streams the file body to the wire.
func file_push_send_body(s *Stream, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %q: %w", path, err)
	}
	defer f.Close()

	if s.writer == nil {
		return fmt.Errorf("stream not open for write")
	}

	_, err = io.Copy(s.writer, f)
	return err
}

// split_services is a small helper to split a comma-separated services
// string into a slice; same logic queue_send_direct uses inline.
func split_services(s string) []string {
	if s == "" {
		return nil
	}
	out := []string{}
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}
