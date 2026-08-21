// Mochi server: Streams
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	cbor "github.com/fxamacker/cbor/v2"
	sl "go.starlark.net/starlark"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	cbor_maximum_size     = 100 * 1024 * 1024 // 100MB maximum message size
	cbor_maximum_depth    = 32                // Max nesting depth
	cbor_maximum_pairs    = 1000              // Max map pairs
	cbor_maximum_elements = 10000             // Max array elements
	content_maximum_key   = 256               // Max content key length
	content_maximum_value = 100 * 1024 * 1024 // 100MB maximum content value length
)

var cbor_decode_mode cbor.DecMode

// Deliberately a package init(), not a call from main_serve: a nil
// cbor_decode_mode would decode with library defaults and no depth, map or
// element caps, so no path can be reachable before the limits exist.
func init() {
	cbor_decode_mode = must(cbor.DecOptions{
		MaxMapPairs:      cbor_maximum_pairs,
		MaxArrayElements: cbor_maximum_elements,
		MaxNestedLevels:  cbor_maximum_depth,
	}.DecMode())
}

type Stream struct {
	id      int64
	reader  io.ReadCloser
	writer  io.WriteCloser
	decoder *cbor.Decoder
	encoder *cbor.Encoder
	remote  string // Remote address (for incoming streams)
	timeout struct {
		read  int
		write int
	}
	// maximum_bytes overrides the cumulative LimitReader cap on the CBOR decoder
	// (zero = cbor_maximum_size). Must be set BEFORE the first read: the decoder
	// and its LimitReader are constructed lazily.
	maximum_bytes int64
	// abandoned records that a write failed against the REMOTE end rather than the
	// local source, so the dispatcher logs instead of warn-emailing. Set only
	// where the failure is known to be the remote's; never inferred from an error
	// string.
	abandoned     bool
	on_close      func() // Called once when stream is closed (e.g. release semaphore)
	on_close_once sync.Once
}

// stream_destination wraps the remote end of a copy so that a failure there
// can be told apart from a failure reading the source. io.Copy returns one
// error for both sides, so a vanished peer and an unreadable file are the same
// value to its caller; only this wrapper's own Write can record the remote's.
type stream_destination struct {
	writer io.Writer
	failed error
}

func (d *stream_destination) Write(p []byte) (int, error) {
	n, err := d.writer.Write(p)
	if err != nil {
		d.failed = err
	}
	return n, err
}

// send copies source to the remote, marking the stream abandoned if it was the
// remote that gave out. The error is returned unchanged either way: whether the
// transfer failed is the caller's business, and which end failed is the log's.
func (s *Stream) send(source io.Reader) (int64, error) {
	destination := &stream_destination{writer: s.writer}
	n, err := io.Copy(destination, source)
	if err != nil && destination.failed != nil {
		s.abandoned = true
	}
	return n, err
}

var (
	streams_lock       = &sync.Mutex{}
	stream_next  int64 = 1
)

// Create a new stream over /mochi/2/stream (authenticated handshake via claim +
// open). When the recipient entity has several known locations, each is tried
// in entity_peers_failover order until one completes the handshake.
func stream(from string, to string, service string, event string, from_app string, services []string) (*Stream, error) {
	peers := entity_peers_failover_for(from, to)
	if len(peers) == 0 {
		return nil, fmt.Errorf("stream unable to determine location of entity %q", to)
	}

	var last_error error
	for _, peer := range peers {
		s, err := stream_to_peer(peer, from, to, service, event, from_app, services)
		if err == nil {
			return s, nil
		}
		last_error = err
	}
	return nil, last_error
}

// Create a stream to a specific peer, without entity lookup; a self-target
// routes to the in-process loopback. The stream is in raw mode - the open frame
// is already shipped and acked, so the caller reads and writes bytes directly.
func stream_to_peer(peer string, from string, to string, service string, event string, from_app string, services []string) (*Stream, error) {
	s, err := stream_open_or_self(peer, from, to, service, event, from_app, services, nil)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, fmt.Errorf("stream unable to open to peer %q", peer)
	}
	return s, nil
}

// Get next stream ID
func stream_id() int64 {
	streams_lock.Lock()
	id := stream_next
	stream_next = stream_next + 1
	streams_lock.Unlock()
	return id
}

// Create a new stream from an existing reader and writer
func stream_rw(r io.ReadCloser, w io.WriteCloser) *Stream {
	return &Stream{id: stream_id(), reader: r, writer: w}
}

// stream_writer meters everything an app writes to a peer, wrapping s.writer so
// a write path added later is metered too. CloseWrite and SetWriteDeadline are
// forwarded deliberately: the write builtins type-assert on s.writer for them.
type stream_writer struct {
	inner io.WriteCloser
	app   string
}

func (w *stream_writer) Write(p []byte) (int, error) {
	if err := stream_outbound_refusal(w.app); err != nil {
		return 0, err
	}
	n, err := w.inner.Write(p)
	if n > 0 {
		stream_outbound_charge(w.app, n)
	}
	return n, err
}

func (w *stream_writer) Close() error { return w.inner.Close() }

func (w *stream_writer) CloseWrite() error {
	if cw, ok := w.inner.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return w.inner.Close()
}

func (w *stream_writer) SetWriteDeadline(t time.Time) error {
	if d, ok := w.inner.(interface{ SetWriteDeadline(time.Time) error }); ok {
		return d.SetWriteDeadline(t)
	}
	return nil
}

// meter wraps the stream's writer so an app's outbound bytes are counted. No-op
// for an unattributed stream, and never double-wraps.
func (s *Stream) meter(app string) {
	if s == nil || s.writer == nil || app == "" {
		return
	}
	if _, already := s.writer.(*stream_writer); already {
		return
	}
	s.writer = &stream_writer{inner: s.writer, app: app}
}

// Close only the write direction of a stream (if supported), otherwise close entirely
func (s *Stream) close_write() {
	if s.writer == nil {
		return
	}
	// Check if writer supports CloseWrite (libp2p streams do)
	if cw, ok := s.writer.(interface{ CloseWrite() error }); ok {
		cw.CloseWrite()
	} else {
		s.writer.Close()
	}
}

// Close only the read direction of a stream (if supported), otherwise close entirely
func (s *Stream) close_read() {
	if s.reader == nil {
		return
	}
	if cr, ok := s.reader.(interface{ CloseRead() error }); ok {
		cr.CloseRead()
	} else {
		s.reader.Close()
	}
}

// close closes both the reader and writer of the stream
func (s *Stream) close() {
	if s.reader != nil {
		s.reader.Close()
	}
	if s.writer != nil {
		s.writer.Close()
	}
	if s.on_close != nil {
		s.on_close_once.Do(s.on_close)
	}
}

// Read a CBOR encoded segment from a stream
func (s *Stream) read(v any) error {
	if s == nil || s.reader == nil {
		return fmt.Errorf("stream not open for reading")
	}

	timeout := s.timeout.read
	if timeout <= 0 {
		timeout = 30
	}

	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	if r, ok := s.reader.(interface{ SetReadDeadline(time.Time) error }); ok {
		_ = r.SetReadDeadline(deadline)
		defer r.SetReadDeadline(time.Time{})
	}

	if s.decoder == nil {
		s.decoder = cbor_decode_mode.NewDecoder(io.LimitReader(s.reader, s.cbor_limit()))
	}
	err := s.decoder.Decode(v)
	if err != nil {
		// %w so callers can errors.Is the EOF class (a far end that
		// closed without answering) apart from decode corruption.
		return fmt.Errorf("stream %d unable to read segment: %w", s.id, err)
	}

	// debug("Stream %d read segment: %+v", s.id, v)
	return nil
}

// cbor_limit returns the cumulative byte limit applied to the CBOR decoder via
// io.LimitReader. The default caps a stream's lifetime reads at 100 MB, which
// breaks bulk DB transfer; those streams set s.maximum_bytes before the first
// read.
func (s *Stream) cbor_limit() int64 {
	if s.maximum_bytes > 0 {
		return s.maximum_bytes
	}
	return int64(cbor_maximum_size)
}

// Read a content segment from a stream
func (s *Stream) read_content() (map[string]any, error) {
	content := map[string]any{}
	err := s.read(&content)
	if err != nil {
		return nil, err
	}

	// Validate key/value sizes
	for k, v := range content {
		if len(k) > content_maximum_key {
			return nil, fmt.Errorf("content key too long: %d > %d", len(k), content_maximum_key)
		}
		if str, ok := v.(string); ok {
			if len(str) > content_maximum_value {
				return nil, fmt.Errorf("content value too long: %d > %d", len(str), content_maximum_value)
			}
		}
	}

	return content, nil
}

// Get a reader for raw data after CBOR reads (includes any buffered data from decoder)
func (s *Stream) raw_reader() io.Reader {
	if s.decoder == nil {
		return s.reader
	}
	// Decoder's Buffered() returns any data read but not yet decoded
	buffered := s.decoder.Buffered()
	return io.MultiReader(buffered, s.reader)
}

// Write a CBOR encoded segment to a stream
func (s *Stream) write(v any) error {
	if s == nil || s.writer == nil {
		return fmt.Errorf("stream not open for writing")
	}
	// debug("Stream %d writing segment: %+v", s.id, v)

	timeout := s.timeout.write
	if timeout <= 0 {
		timeout = 30
	}

	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	if w, ok := s.writer.(interface{ SetWriteDeadline(time.Time) error }); ok {
		_ = w.SetWriteDeadline(deadline)
		defer w.SetWriteDeadline(time.Time{})
	}

	if s.encoder == nil {
		s.encoder = cbor.NewEncoder(s.writer)
	}
	err := s.encoder.Encode(v)
	if err != nil {
		// Unconditional, unlike send: the encoder's only output is s.writer,
		// and its input is already a plain Go value from sl_decode, so there
		// is no local source here for the failure to have come from.
		s.abandoned = true
		return fmt.Errorf("stream error writing segment: %v", err)
	}

	return nil
}

// Write field/value pairs to a stream as a CBOR encoded segment
func (s *Stream) write_content(in ...string) error {
	content := map[string]string{}

	for {
		if len(in) < 2 {
			break
		}
		content[in[0]] = in[1]
		in = in[2:]
	}

	return s.write(content)
}

// Write a file to a stream as raw bytes, returns bytes written
func (s *Stream) write_file(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("stream unable to read file %q", path)
	}
	defer f.Close()

	n, err := s.send(f)
	if err != nil {
		return 0, fmt.Errorf("stream error sending file segment: %v", err)
	}

	return n, nil
}

// Write a raw, unencoded or pre-encoded, segment
func (s *Stream) write_raw(data []byte) error {
	if s == nil || s.writer == nil {
		return fmt.Errorf("stream not open for writing")
	}
	// debug("Stream %d writing raw segment: %d bytes", s.id, len(data))

	timeout := s.timeout.write
	if timeout <= 0 {
		timeout = 30
	}

	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	if w, ok := s.writer.(interface{ SetWriteDeadline(time.Time) error }); ok {
		_ = w.SetWriteDeadline(deadline)
		defer w.SetWriteDeadline(time.Time{})
	}

	_, err := s.writer.Write(data)
	if err != nil {
		// The bytes are already in hand, so the remote is the only thing that
		// can fail here.
		s.abandoned = true
		return fmt.Errorf("stream error writing raw segment: %v", err)
	}

	// debug("Stream %d wrote raw segment", s.id)
	return nil
}

// Starlark methods
func (s *Stream) AttrNames() []string {
	return []string{"read", "write", "close"}
}

func (s *Stream) Attr(name string) (sl.Value, error) {
	switch name {
	case "read":
		return &StreamRead{stream: s}, nil
	case "write":
		return &StreamWrite{stream: s}, nil
	case "close":
		return sl.NewBuiltin("close", s.sl_close), nil
	default:
		return nil, nil
	}
}

// StreamRead is callable as s.read() and exposes s.read.file(path).
type StreamRead struct {
	stream *Stream
}

func (sr *StreamRead) String() string        { return "stream.read" }
func (sr *StreamRead) Type() string          { return "stream.read" }
func (sr *StreamRead) Freeze()               {}
func (sr *StreamRead) Truth() sl.Bool        { return true }
func (sr *StreamRead) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable: stream.read") }
func (sr *StreamRead) Name() string          { return "read" }

// Callable: s.read() -> dict | None: Read the next decoded segment
func (sr *StreamRead) CallInternal(t *sl.Thread, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sr.stream.sl_read(t, nil, args, kwargs)
}

func (sr *StreamRead) AttrNames() []string {
	return []string{"file"}
}

func (sr *StreamRead) Attr(name string) (sl.Value, error) {
	switch name {
	case "file":
		return sl.NewBuiltin("read.file", sr.stream.sl_read_file), nil
	}
	return nil, nil
}

// StreamWrite is callable as s.write(values...) and exposes s.write.{asset,
// cache, file, raw}.
type StreamWrite struct {
	stream *Stream
}

func (sw *StreamWrite) String() string        { return "stream.write" }
func (sw *StreamWrite) Type() string          { return "stream.write" }
func (sw *StreamWrite) Freeze()               {}
func (sw *StreamWrite) Truth() sl.Bool        { return true }
func (sw *StreamWrite) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable: stream.write") }
func (sw *StreamWrite) Name() string          { return "write" }

// Callable: s.write(values...) -> bool: Write one or more encoded segments
func (sw *StreamWrite) CallInternal(t *sl.Thread, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sw.stream.sl_write(t, nil, args, kwargs)
}

func (sw *StreamWrite) AttrNames() []string {
	return []string{"asset", "cache", "file", "raw"}
}

func (sw *StreamWrite) Attr(name string) (sl.Value, error) {
	switch name {
	case "asset":
		return sl.NewBuiltin("write.asset", sw.stream.sl_write_asset), nil
	case "cache":
		return sl.NewBuiltin("write.cache", sw.stream.sl_write_cache), nil
	case "file":
		return sl.NewBuiltin("write.file", sw.stream.sl_write_file), nil
	case "raw":
		return sl.NewBuiltin("write.raw", sw.stream.sl_write_raw), nil
	}
	return nil, nil
}

func (s *Stream) Hash() (uint32, error) {
	return sl.String(fmt.Sprintf("%d", s.id)).Hash()
}

func (s *Stream) Freeze() {}

func (s *Stream) String() string {
	return fmt.Sprintf("Stream %d", s.id)
}

func (s *Stream) Truth() sl.Bool {
	return sl.True
}

func (s *Stream) Type() string {
	return "Stream"
}

// s.read() -> any: Read and decode the next segment from the stream
func (s *Stream) sl_read(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var v any
	err := s.read(&v)
	if err != nil {
		return sl.None, nil
	}
	return sl_encode(v), nil
}

// s.read.file(path, maximum=0) -> int: Read raw bytes from the stream and write
// them to a per-user data file, returns bytes read. Writes to the same
// filesystem as mochi.file.*. With a positive maximum, at most maximum+1 bytes
// are written - the extra byte lets the caller distinguish a source that fits
// exactly from one that overran, delete the file, and refuse the transfer.
func (s *Stream) sl_read_file(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// debug("Stream %d reading rest of stream to file", s.id)

	var file string
	maximum := 0
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "file", &file, "maximum?", &maximum); err != nil {
		s.close_read()
		return nil, err
	}

	user := principal_caller(t)
	if user == nil {
		s.close_read()
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		s.close_read()
		return sl_error(fn, "no app")
	}

	if !valid(file, "filepath") {
		s.close_read()
		return sl_error(fn, "invalid file %q", file)
	}

	// Check storage limit and calculate remaining space (admins exempt)
	remaining, err := user_storage_remaining(user)
	if err != nil {
		s.close_read()
		return sl_error(fn, "unable to measure storage: %v", err)
	}
	if remaining <= 0 {
		s.close_read()
		return sl_error(fn, "storage limit exceeded")
	}

	// Ensure base directory exists and open root for traversal protection
	base := api_file_base(user, app)
	if err := os.MkdirAll(base, 0755); err != nil {
		s.close_read()
		return sl_error(fn, "unable to create files directory: %v", err)
	}
	root, err := os.OpenRoot(base)
	if err != nil {
		s.close_read()
		return sl_error(fn, "unable to access files directory")
	}
	defer root.Close()

	// Create parent directories within the root if needed
	dir := filepath.Dir(file)
	if dir != "." && dir != "" {
		if err := root_mkdir_all(root, dir); err != nil {
			s.close_read()
			return sl_error(fn, "unable to create directory")
		}
	}

	// Open file within root for writing
	f, err := root.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		s.close_read()
		return sl_error(fn, "unable to write file")
	}

	// Use raw_reader() to include any bytes buffered by the CBOR decoder
	// This is critical when read_to_file follows a read() call
	reader := s.raw_reader()

	// Limit reader to remaining storage space, tightened to the caller's
	// expectation when one was given (+1 so an overrun is visible to it).
	if maximum > 0 && int64(maximum)+1 < remaining {
		remaining = int64(maximum) + 1
	}
	limited := io.LimitReader(reader, remaining)
	n, err := io.Copy(f, limited)
	f.Close()

	if err != nil {
		s.close_read()
		return sl_error(fn, "unable to save file %q", file)
	}

	s.close_read()
	// debug("Stream %d read %d bytes to file", s.id, n)

	return sl.MakeInt64(n), nil
}

// s.write(values...) -> None: Write one or more encoded segments to the stream
func (s *Stream) sl_write(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	for _, a := range args {
		err := s.write(sl_decode(a))
		if err != nil {
			return sl.False, nil
		}
	}
	return sl.True, nil
}

// s.write_raw(data) -> None: Send raw bytes without CBOR encoding
func (s *Stream) sl_write_raw(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <data: bytes>")
	}

	data, ok := args[0].(sl.Bytes)
	if !ok {
		return sl_error(fn, "data must be bytes")
	}

	err := s.write_raw([]byte(data))
	if err != nil {
		return sl_error(fn, err)
	}

	return sl.None, nil
}

// sl_write_file is the Go-level implementation behind both s.* and e.* file
// writers. Sends per-user data file contents as raw bytes; returns bytes written.
func (s *Stream) sl_write_file(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	defer s.close_write()
	var file string
	var offset int64
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "file", &file, "offset?", &offset); err != nil {
		return sl_error(fn, "syntax: <file: string>, [offset: integer]")
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	if !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	// Open file using os.Root for traversal protection
	base := api_file_base(user, app)
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl_error(fn, "file not found")
	}
	defer root.Close()

	f, err := root.Open(file)
	if err != nil {
		return sl_error(fn, "file not found")
	}
	defer f.Close()

	// A resuming transfer starts mid-file. Bounds-checked against the file
	// itself, so a peer's stale belief about the size is refused here rather
	// than becoming a short read the receiver cannot tell from truncation.
	if offset < 0 {
		return sl_error(fn, "invalid offset %d", offset)
	}
	if offset > 0 {
		information, err := f.Stat()
		if err != nil {
			return sl_error(fn, "file not found")
		}
		if offset > information.Size() {
			return sl_error(fn, "offset %d beyond the file's %d bytes", offset, information.Size())
		}
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return sl_error(fn, "unable to seek: %v", err)
		}
	}

	// From here the call is only moving bytes, so it takes the transfer bound, not
	// the compute one: a slow peer would otherwise hit the 90-second timeout
	// inside io.Copy, which ignores cancellation.
	starlark_transfer_set(t)

	n, err := s.send(f)
	if err != nil {
		// The cause is carried, not flattened: a peer that went away and a
		// disk that would not read reported the same fixed string, so the
		// operator email could not tell a client disconnect from real trouble.
		return sl_error(fn, "unable to send file: %v", err)
	}

	return sl.MakeInt64(n), nil
}

// s.write.cache(name) -> int or None: Send a cache entry's bytes over the
// stream, returning None on a cache miss so a responder can fill and retry.
// Used by byte-pull responders that keep a local cached copy.
func (s *Stream) sl_write_cache(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	defer s.close_write()
	var name string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name); err != nil {
		return sl_error(fn, "syntax: write.cache(name)")
	}

	path, err := cache_serve_file(t, name)
	if err != nil {
		return sl.None, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return sl.None, nil
	}
	defer f.Close()

	starlark_transfer_set(t) // bulk bytes, not computation - see sl_write_file

	n, err := s.send(f)
	if err != nil {
		return sl_error(fn, "unable to send cache entry: %v", err)
	}
	return sl.MakeInt64(n), nil
}

// s.write.asset(path) -> int: Send the contents of a bundled app asset as raw
// bytes, returns bytes written. Reads from the same filesystem as mochi.app.asset.*.
func (s *Stream) sl_write_asset(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	defer s.close_write()
	if len(args) != 1 {
		return sl_error(fn, "syntax: <path: string>")
	}

	path, ok := sl.AsString(args[0])
	if !ok || !valid(path, "filepath") {
		return sl_error(fn, "invalid path %q", path)
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	user := principal_caller(t)
	file := app_asset_path(app, user, path)
	if file == "" {
		return sl_error(fn, "file not found")
	}

	starlark_transfer_set(t) // bulk bytes, not computation - see sl_write_file

	n, err := s.write_file(file)
	if err != nil {
		return sl_error(fn, "unable to send file")
	}

	return sl.MakeInt64(n), nil
}

// s.close() -> None: Close the write side of the stream (signals EOF to reader)
func (s *Stream) sl_close(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	s.close_write()
	return sl.None, nil
}
