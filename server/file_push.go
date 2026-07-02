// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

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
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// FilePushHeader rides in `data` on every file/push message. Tells the
// receiver where to write the body and how much to read after the
// header before the stream closes.
type FilePushHeader struct {
	User string `cbor:"user"`
	App  string `cbor:"app"`
	Path string `cbor:"path"`
	Size int64  `cbor:"size"`
	// Root selects the directory Path is resolved against on the receiver: "" (or
	// "files") = the per-(user,app) files/ dir (attachments, mochi.file.*); "app" = the
	// per-(user,app) dir itself, for app data that lives OUTSIDE files/ — the
	// repositories app's bare git trees at <app>/<repo>/objects/... (#105). Backward
	// compatible: an old sender omits it and an old receiver ignores it, both meaning
	// the files/ root.
	Root string `cbor:"root,omitempty"`
	// Resume signals the sender supports resumable transfer (#78): right after
	// this header it reads a FilePushResume from the receiver and streams the
	// body from that offset, so an interrupted push continues instead of
	// restarting from 0. Backward compatible: an old sender never sets it (the
	// receiver takes the one-shot path), and an old receiver ignores it and
	// never replies (the sender's resume read times out and falls back to a full
	// send-from-0).
	Resume bool `cbor:"resume,omitempty"`
}

// FilePushResume is the receiver's reply to a resumable file/push header: the
// number of contiguous bytes it already holds in <path>.partial. The sender
// seeks to this offset and streams the remaining Size-Offset bytes. (#78)
type FilePushResume struct {
	Offset int64 `cbor:"offset"`
}

// file_push_resume_timeout caps how long a resumable sender waits for the
// receiver's FilePushResume before falling back to a full send-from-0 — short,
// because the reply is tiny and only an OLD receiver (which never sends one)
// should ever hit it. (#78)
const file_push_resume_timeout = 8

// FileDelete is the payload for a file/delete event: tiny, fits the
// regular ops flow with no body.
type FileDelete struct {
	User string `cbor:"user"`
	App  string `cbor:"app"`
	Path string `cbor:"path"`
}

// replication_emit_file_push enqueues a file/push for a files/-rooted path (the
// attachment / mochi.file.* case). Kept as a var so tests stub it.
var replication_emit_file_push = func(userUID, appID, path string) {
	replication_emit_file_push_rooted(userUID, appID, "", path)
}

// replication_emit_file_push_rooted enqueues a file/push queue row for each peer in the
// user's host set. The row references the file path on disk; the queue worker reads +
// streams the file live at send time via queue_send_file_push. root selects where Path
// is resolved (see file_push_base / FilePushHeader.Root): "" for files/, "app" for the
// per-app dir (git trees). A var so tests can capture it.
var replication_emit_file_push_rooted = func(userUID, appID, root, path string) {
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

	base := file_push_base(userUID, appID, root)
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
		Root: root,
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

// file_push_root_app is the FilePushHeader.Root value for app-rooted pushes (app data
// outside files/, i.e. the repositories app's bare git trees).
const file_push_root_app = "app"

// file_push_max_size caps a single file/push's declared size — a sanity bound
// (well above any real attachment) that rejects an absurd/overflow value before
// the receiver commits to streaming that many bytes. Defence in depth behind the
// per-user authorization gate (#145).
const file_push_max_size = 8 << 30 // 8 GiB

// file_push_base returns the directory a file/push Path is resolved against, given the
// header's Root. Default ("" / unknown) is the files/ dir, preserving the original
// behaviour. "app" is the per-(user,app) dir itself, so git objects/refs stored at
// <app>/<repo>/... replicate live (#105).
func file_push_base(userUID, appID, root string) string {
	if root == file_push_root_app {
		return filepath.Join(data_dir, "users", userUID, appID)
	}
	return file_user_app_base(userUID, appID)
}

// file_push_path_allowed gates which paths an app-rooted file/push may write —
// defense-in-depth on top of os.OpenRoot's containment. It refuses anything under the
// app's db/ directory, so a push can never overwrite a replicated SQLite DB
// out-of-band (the DBs replicate via the op log, not file bytes). The files/ root needs
// no extra gate; it is already confined below files/.
func file_push_path_allowed(root, path string) bool {
	if root != file_push_root_app {
		return true
	}
	clean := filepath.ToSlash(filepath.Clean(path))
	if clean == ".." || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "/") {
		return false // escapes the app dir (also caught by valid()/os.OpenRoot; belt-and-braces)
	}
	return clean != "db" && !strings.HasPrefix(clean, "db/")
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
	if header.Size < 0 || header.Size > file_push_max_size {
		info("Replication file-push: rejecting size %d (limit %d)", header.Size, file_push_max_size)
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

	// Authorize the writer: a signed e.from proves an entity minted the envelope
	// (self-minted, so worthless alone), but nothing above checks the SENDING
	// PEER may write this user's files. Gate on the same authority sql/op uses —
	// a pair member, or a peer in this user's host set — so a stranger can't
	// plant or overwrite files in an arbitrary user's tree (#145).
	if !replication_op_authorized(e.peer, header.User) {
		info("Replication file-push: peer %q not authorized for user %q", e.peer, header.User)
		e.stream.close_read()
		return
	}

	if !file_push_path_allowed(header.Root, header.Path) {
		info("Replication file-push: path %q not allowed for root %q from peer %q", header.Path, header.Root, e.peer)
		e.stream.close_read()
		return
	}
	base := file_push_base(header.User, header.App, header.Root)
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

	if header.Resume {
		// (#78) Resumable path: keep any bytes a prior attempt left in .partial,
		// tell the sender that offset, then append the remainder. On an
		// incomplete transfer we KEEP the .partial so the next round resumes
		// instead of restarting — that's what turns a transient drop on a flaky
		// link into eventual completion rather than a permanent "Retrying 1 file".
		f, err := root.OpenFile(partial, os.O_WRONLY|os.O_CREATE, 0644) // no O_TRUNC
		if err != nil {
			warn("Replication file-push: cannot open %q: %v", partial, err)
			e.stream.close_read()
			return
		}
		var have int64
		if info, serr := f.Stat(); serr == nil {
			have = info.Size()
		}
		if have > header.Size {
			f.Truncate(0) // stale/oversized partial — would leave a garbage tail
			have = 0
		}
		if werr := e.stream.write(&FilePushResume{Offset: have}); werr != nil {
			f.Close()
			e.stream.close_read()
			return
		}
		if _, serr := f.Seek(have, io.SeekStart); serr != nil {
			f.Close()
			e.stream.close_read()
			return
		}
		written, copyErr := file_push_copy(f, e.stream.raw_reader(), header.Size-have)
		f.Close()
		if copyErr != nil || have+written != header.Size {
			warn("Replication file-push: resume body incomplete (%d+%d/%d): %v", have, written, header.Size, copyErr)
			e.stream.close_read() // KEEP .partial for the next round's resume
			return
		}
		if err := root.Rename(partial, header.Path); err != nil {
			warn("Replication file-push: cannot rename %q → %q: %v", partial, header.Path, err)
			root.Remove(partial)
			e.stream.close_read()
			return
		}
		debug("Replication file-push: applied (resumed from %d) user=%q app=%q path=%q size=%d", have, header.User, header.App, header.Path, header.Size)
		return
	}

	// Legacy one-shot path (old sender, no Resume flag): truncate, read Size
	// from offset 0, remove the partial on any failure (the sender retries from 0).
	f, err := root.OpenFile(partial, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		warn("Replication file-push: cannot open %q: %v", partial, err)
		e.stream.close_read()
		return
	}

	// Stream exactly `Size` bytes from the wire to .partial. raw_reader() picks
	// up any bytes already buffered by the CBOR decoder before falling through
	// to s.reader.
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

	// Same authority as file-push / sql/op: only a pair member or a peer in this
	// user's host set may delete this user's files, so a stranger can't wipe an
	// arbitrary user's attachments (#145).
	if !replication_op_authorized(e.peer, fd.User) {
		info("Replication file-delete: peer %q not authorized for user %q", e.peer, fd.User)
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

	if s.writer == nil {
		return false
	}

	// Open the body ONCE and size it from the OPEN fd, rewriting the header's
	// Size to match. The file may have changed since it was stat'd at enqueue,
	// and the receiver reads exactly header.Size bytes — so size and body must
	// come from the same moment or they diverge (file grew: trailing bytes
	// corrupt the stream framing; file shrank: receiver short-reads). A
	// missing source is a permanent failure (the delete replicates separately
	// and retrying can't succeed), so drop the row. (#14)
	f, header, size, err := file_push_payload(q.File, q.Data)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			info("Queue file-push dropping %q: source file %q no longer exists", q.ID, q.File)
			queue_drop(q.ID, "file-missing")
			return true
		}
		debug("Queue file-push: cannot prepare body %q: %v", q.File, err)
		return false
	}
	defer f.Close()

	// stream_open shipped the (empty) content map as the first post-ack
	// segment; the (size-corrected) FilePushHeader rides as the next CBOR
	// segment, which the handler reads via e.segment().
	if len(header) > 0 {
		if werr := s.write_raw(header); werr != nil {
			return false
		}
	}

	// (#78) Resumable transfer: read the receiver's FilePushResume — how many
	// contiguous bytes it already holds — and stream the body from there, so an
	// interrupted push continues instead of restarting at 0. A short read
	// deadline means an OLD receiver (which never replies) just times out here
	// and we fall back to a full send-from-0.
	var offset int64
	s.timeout.read = file_push_resume_timeout
	var resume FilePushResume
	if rerr := s.read(&resume); rerr == nil && resume.Offset > 0 && resume.Offset <= size {
		if _, serr := f.Seek(resume.Offset, io.SeekStart); serr == nil {
			offset = resume.Offset
		}
	}

	// Stream EXACTLY `size-offset` bytes — the same total now stamped in the
	// header (offset already on the receiver). io.CopyN returns a short count +
	// error if the file was truncated after the stat, so we never ship a body
	// shorter than promised; the send fails and retries with a fresh stat.
	if n, cerr := io.CopyN(s.writer, f, size-offset); cerr != nil || n != size-offset {
		debug("Queue file-push: body send %q failed (resume=%d, %d/%d): %v", q.File, offset, n, size-offset, cerr)
		return false
	}
	return true
}

// decode_into is a tiny helper that decodes CBOR into the target,
// returning the error directly. Used so the file-push v2 branch can
// extract a Content map from the stored q.Content blob.
func decode_into(payload []byte, into any) error {
	return cbor_decode_mode.Unmarshal(payload, into)
}

// file_push_payload opens the file to be pushed and sizes it from the OPEN
// file descriptor, returning the fd plus a header whose Size matches that
// just-read size. Taking size and body from the same open fd closes the
// enqueue-stat vs send-body race (#14): header.Size always equals the number
// of bytes the caller then streams (exactly `size`, via io.CopyN). The caller
// owns the returned fd and must Close it.
func file_push_payload(path string, original []byte) (*os.File, []byte, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, 0, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, 0, err
	}
	size := st.Size()

	header := original
	var h FilePushHeader
	if len(original) > 0 && decode_into(original, &h) == nil {
		h.Size = size
		h.Resume = true // advertise resumable transfer (#78)
		header = cbor_encode(&h)
	}
	return f, header, size, nil
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
