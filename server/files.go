// Mochi server: File utilities
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// object_maximum is the largest single object the platform stores, and so the
// largest it must transfer whole. The chain object_maximum <=
// stream_maximum_default <= the per-client relay byte budget must hold, so
// raising this means raising both others. TestStorageLimitsAgree pins it.
const object_maximum = 10 * 1024 * 1024 * 1024 // 10GB

// file_inline_maximum bounds what a.file() will materialise as Starlark bytes.
// It matches ParseMultipartForm's in-memory spool threshold: below it the part
// is already in memory, above it a.file would allocate a second copy of a body
// the caller's storage quota alone allowed to reach 10GB. Larger uploads go
// through a.files plus a.upload, which stream.
const file_inline_maximum = 32 * 1024 * 1024 // 32MB

// Maximum file storage per user (10GB). Equal to object_maximum, so a single
// object may fill a user's whole allowance - a video is one file, not many.
var file_maximum_storage int64 = 10 * 1024 * 1024 * 1024

// How long an unread cache entry survives before cache_cleanup removes it.
const cache_age_maximum = 7 * 24 * time.Hour

// user_storage_remaining reports how many more bytes the user may store before
// reaching the per-user storage quota (file_maximum_storage), for a caller with
// no request to share a walk with (storage_remaining).
func user_storage_remaining(u *User) (int64, error) {
	return storage_remaining(nil, u)
}

// storage_lifetime is how long a walk of a user's storage is trusted. Walking
// the whole tree on every write made a request storing many files walk it once
// a file. Between walks the writes and deletes core makes keep the figure
// current (storage_step); what grows another way - a database, a repository -
// the next walk counts. A var so tests can change it.
var storage_lifetime = 60 * time.Second

// storage_walk measures a user's storage. A var so tests can count the walks.
var storage_walk = dir_size

// storage_exceeded refuses a write that would take its user past the quota.
var storage_exceeded = errors.New("storage limit exceeded")

// storage_piece is the most a metered write puts on disk in one step under the
// figure's lock (storage_meter.WriteString), so a large write holds neither a
// copy of itself nor the lock for its whole length.
const storage_piece = 1 << 20

// storage_figures holds a storage_figure for each user whose storage has been
// measured, by uid. In memory only: a restart walks again.
var storage_figures sync.Map

// storage_figure is what a user's storage held when last walked, kept current
// by the writes and deletes made since. Its lock is held for the whole of a
// walk and for each change core makes to the user's storage together with the
// change to the figure (storage_step), so a walk sees every such change with
// its accounting or without it, never one without the other.
type storage_figure struct {
	lock     sync.Mutex
	used     int64
	measured time.Time // when the walk the figure stands on began; zero until walked, when it charges and refuses nothing
	finished time.Time // when that walk ended
	expired  bool      // what has landed since may have been counted twice (storage_expire): walk at the next measurement
}

// storage_request records whose storage one request has walked, so it walks
// each at most once however many writes it makes and however long it runs. It
// is carried on the request's Starlark thread (storage_request_of).
type storage_request struct {
	lock   sync.Mutex
	walked map[string]bool
}

// storage_request_of answers the record a thread's request carries, giving a
// thread whose request set none one of its own.
func storage_request_of(t *sl.Thread) *storage_request {
	if t == nil {
		return nil
	}
	if request, ok := t.Local("measured").(*storage_request); ok && request != nil {
		return request
	}
	request := &storage_request{}
	t.SetLocal("measured", request)
	return request
}

// has reports whether the request has walked a user's storage. A nil request
// is none, and has walked nothing.
func (r *storage_request) has(uid string) bool {
	if r == nil {
		return false
	}
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.walked[uid]
}

// add records that the request walked a user's storage.
func (r *storage_request) add(uid string) {
	if r == nil {
		return
	}
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.walked == nil {
		r.walked = map[string]bool{}
	}
	r.walked[uid] = true
}

// current walks the user's storage when the figure is older than
// storage_lifetime and the request has not walked it already, or when it has
// expired. The caller holds the figure's lock, so writers waiting on it share
// the one walk. The figure is dated from the walk's start: a file written
// while the walk ran may be missing from it (storage_delete).
func (f *storage_figure) current(request *storage_request, uid string) error {
	if !f.measured.IsZero() && !f.expired && (time.Since(f.measured) < storage_lifetime || request.has(uid)) {
		return nil
	}
	start := time.Now()
	used, err := storage_walk(user_storage_dir(&User{UID: uid}))
	if err != nil {
		return err
	}
	f.used, f.measured, f.finished, f.expired = used, start, time.Now(), false
	request.add(uid)
	return nil
}

// credit takes bytes freed off the figure. The caller holds the figure's lock.
func (f *storage_figure) credit(bytes int64) {
	f.used = max(f.used-bytes, 0)
}

// storage_remaining answers how many more bytes u may store, walking u's
// storage first when the figure is stale. Administrators are exempt from the
// quota, and their storage is never walked.
func storage_remaining(request *storage_request, u *User) (int64, error) {
	if u != nil && u.administrator() {
		return math.MaxInt64, nil
	}
	figure := storage_figure_of(u.UID)
	figure.lock.Lock()
	defer figure.lock.Unlock()
	if err := figure.current(request, u.UID); err != nil {
		return 0, err
	}
	return file_maximum_storage - figure.used, nil
}

// storage_figure_of answers a user's figure, made unwalked on first use.
func storage_figure_of(uid string) *storage_figure {
	figure, _ := storage_figures.LoadOrStore(uid, &storage_figure{})
	return figure.(*storage_figure)
}

// storage_step makes a change to a user's storage, and the change to their
// figure that goes with it, as one step against a walk: under the figure's
// lock, which a walk holds from start to end. A delete or a write made while a
// walk ran, and accounted for after it, was either counted twice or credited
// for bytes the walk never saw. step is handed the figure, or nil for a user
// with none walked - an administrator's never is - whose change the next walk
// counts.
func storage_step(uid string, step func(figure *storage_figure)) {
	var figure *storage_figure
	if uid != "" {
		if found, ok := storage_figures.Load(uid); ok {
			figure = found.(*storage_figure)
		}
	}
	if figure == nil {
		step(nil)
		return
	}
	figure.lock.Lock()
	defer figure.lock.Unlock()
	if figure.measured.IsZero() {
		step(nil)
		return
	}
	step(figure)
}

// storage_create opens name inside root for writing, emptied, and credits what
// it held: the write that follows is charged as it lands (storage_meter).
func storage_create(root *os.Root, owner string, name string) (file *os.File, err error) {
	storage_step(owner, func(figure *storage_figure) {
		previous := storage_size(root, name)
		file, err = root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err == nil && figure != nil {
			figure.credit(previous)
		}
	})
	return file, err
}

// storage_remove removes name inside root and credits what it held.
func storage_remove(root *os.Root, owner string, name string) (err error) {
	storage_step(owner, func(figure *storage_figure) {
		var size int64
		if information, err := root.Lstat(name); err == nil && information.Mode().IsRegular() {
			size = information.Size()
		}
		err = root.Remove(name)
		if err == nil && figure != nil {
			figure.credit(size)
		}
	})
	return err
}

// storage_rename moves from to to inside root and credits the file the move
// replaces, unless it is the one being moved.
func storage_rename(root *os.Root, owner string, from string, to string) (err error) {
	storage_step(owner, func(figure *storage_figure) {
		var replaced int64
		if destination, err := root.Lstat(to); err == nil && destination.Mode().IsRegular() {
			if source, err := root.Lstat(from); err != nil || !os.SameFile(source, destination) {
				replaced = destination.Size()
			}
		}
		err = root.Rename(from, to)
		if err == nil && figure != nil {
			figure.credit(replaced)
		}
	})
	return err
}

// storage_delete deletes the file at path with remove, and credits it when it
// is older than the walk the figure stands on, so the walk counted it. A newer
// file may have been written where nothing charged it - a repository's
// objects - and crediting it would leave the figure below what is stored; such
// a file stays counted until the next walk.
func storage_delete(uid string, path string, remove func() error) (err error) {
	storage_step(uid, func(figure *storage_figure) {
		information, _ := os.Lstat(path)
		err = remove()
		if err == nil && figure != nil && information != nil && information.Mode().IsRegular() && information.ModTime().Before(figure.measured) {
			figure.credit(information.Size())
		}
	})
	return err
}

// storage_add charges bytes already stored to a user's figure, which there is
// no refusing: what a push or a repack has written to disk counts from then,
// so the next write in the figure's lifetime sees it. A walk that ran while
// they were being written may have counted them already (storage_expire).
func storage_add(uid string, bytes int64) {
	if bytes == 0 {
		return
	}
	storage_step(uid, func(figure *storage_figure) {
		if figure != nil {
			figure.used += bytes
		}
	})
}

// storage_expire ends a figure's trust when a walk finished after since: what
// was written beside the user's storage from then - a push's staging, a
// repack's pack before it is renamed into place - may have been counted by the
// walk and again by storage_add when it landed, so the next measurement walks.
func storage_expire(uid string, since time.Time) {
	storage_step(uid, func(figure *storage_figure) {
		if figure != nil && figure.finished.After(since) {
			figure.expired = true
		}
	})
}

// storage_forget drops a user's figure: a user deleted, whose uid may come
// back, or one whose administrator role changed, whose figure an
// administrator's measurement never keeps.
func storage_forget(uid string) {
	storage_figures.Delete(uid)
}

// storage_owner answers the uid whose storage holds path, or "" for a path
// outside every user's.
func storage_owner(path string) string {
	users := filepath.Join(data_dir, "users")
	for _, base := range []string{users, storage_resolved(users)} {
		relative, err := filepath.Rel(base, path)
		if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			continue
		}
		uid, _, _ := strings.Cut(filepath.ToSlash(relative), "/")
		return uid
	}
	return ""
}

// storage_resolved answers a path with its links resolved, as a repository's
// filesystem names its root, or the path itself.
func storage_resolved(path string) string {
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		return resolved
	}
	return path
}

// storage_stat answers a file's information, or nil.
func storage_stat(path string) os.FileInfo {
	information, err := os.Stat(path)
	if err != nil {
		return nil
	}
	return information
}

// storage_file answers the size of a regular file at path, or 0.
func storage_file(path string) int64 {
	if information := storage_stat(path); information != nil && information.Mode().IsRegular() {
		return information.Size()
	}
	return 0
}

// storage_meter charges what is written through it to a user's storage, and
// refuses the write that would take the user past the quota: writes running at
// once are held to it together, not each to the figure it began with. Each
// piece is checked, written and charged as one step (storage_step), so a walk
// running beside the write counts a piece with its charge or neither.
type storage_meter struct {
	writer  io.Writer
	owner   string
	written int64
}

func (m *storage_meter) Write(data []byte) (int, error) {
	return m.write(len(data), func() (int, error) { return m.writer.Write(data) })
}

// WriteString writes s a piece at a time (storage_piece), without the copy
// io.WriteString makes of a string for a writer that takes only bytes.
func (m *storage_meter) WriteString(s string) (int, error) {
	total := 0
	for len(s) > 0 {
		piece := s[:min(len(s), storage_piece)]
		n, err := m.write(len(piece), func() (int, error) { return io.WriteString(m.writer, piece) })
		total += n
		if err != nil {
			return total, err
		}
		if n < len(piece) {
			return total, io.ErrShortWrite
		}
		s = s[n:]
	}
	return total, nil
}

// write checks, writes and charges size bytes as one step.
func (m *storage_meter) write(size int, write func() (int, error)) (n int, err error) {
	storage_step(m.owner, func(figure *storage_figure) {
		if figure != nil && int64(size) > file_maximum_storage-figure.used {
			err = storage_exceeded
			return
		}
		n, err = write()
		if figure != nil {
			figure.used += int64(n)
		}
	})
	m.written += int64(n)
	return n, err
}

// storage_size answers the size of a regular file inside root, or 0.
func storage_size(root *os.Root, name string) int64 {
	information, err := root.Stat(name)
	if err != nil || !information.Mode().IsRegular() {
		return 0
	}
	return information.Size()
}

var (
	api_file = sls.FromStringDict(sl.String("mochi.file"), sl.StringDict{
		"age":     sl.NewBuiltin("mochi.file.age", api_file_age),
		"size":    sl.NewBuiltin("mochi.file.size", api_file_size),
		"clean":   sl.NewBuiltin("mochi.file.clean", api_file_clean),
		"copy":    sl.NewBuiltin("mochi.file.copy", api_file_copy),
		"maximum": sl.NewBuiltin("mochi.file.maximum", api_file_maximum),
		"delete":  sl.NewBuiltin("mochi.file.delete", api_file_delete),
		"exists":  sl.NewBuiltin("mochi.file.exists", api_file_exists),
		"head":    sl.NewBuiltin("mochi.file.head", api_file_head),
		"list":    sl.NewBuiltin("mochi.file.list", api_file_list),
		"move":    sl.NewBuiltin("mochi.file.move", api_file_move),
		"read":    sl.NewBuiltin("mochi.file.read", api_file_read),
		"type":    sl.NewBuiltin("mochi.file.type", api_file_type),
		"write":   sl.NewBuiltin("mochi.file.write", api_file_write),
	})
)

func file_exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// data_dir_writable_check probes data_dir for writability so main_serve can
// fail early with an actionable message rather than deep inside a later write.
func data_dir_writable_check() error {
	if err := os.MkdirAll(data_dir, 0755); err != nil {
		return err
	}
	probe := filepath.Join(data_dir, ".mochi_write_check")
	f, err := os.OpenFile(probe, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	f.Close()
	os.Remove(probe)
	return nil
}

func file_is_directory(path string) bool {
	information, err := os.Stat(path)
	if err != nil {
		return false
	}
	return information.IsDir()
}

func file_list(path string) ([]string, error) {
	found, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, f := range found {
		files = append(files, f.Name())
	}
	sort.Strings(files)
	return files, nil
}

func file_name_type(name string) string {
	// Folded for the same reason is_image folds: an upper-case extension is a
	// real extension, and reporting application/octet-stream for PHOTO.PNG
	// turned a viewable image into a download.
	switch strings.ToLower(path.Ext(name)) {
	case ".css":
		return "text/css"
	case ".gif":
		return "image/gif"
	case ".html":
		return "text/html"
	case ".jpeg":
		return "image/jpeg"
	case ".jpg":
		return "image/jpeg"
	case ".js":
		return "text/javascript"
	case ".json":
		return "application/json"
	case ".pdf":
		return "application/pdf"
	case ".png":
		return "image/png"
	case ".svg":
		return "image/svg+xml"
	case ".txt":
		return "text/plain"
	case ".webp":
		return "image/webp"
	case ".xml":
		return "application/xml"

	// Media types decide inline playback versus download: content_type_inline
	// admits image/*, video/* and audio/*; octet-stream becomes an attachment.
	case ".avif":
		return "image/avif"
	case ".flac":
		return "audio/flac"
	case ".heic":
		return "image/heif"
	case ".m4a":
		return "audio/mp4"
	case ".m4v":
		return "video/mp4"
	case ".mov":
		return "video/quicktime"
	case ".mp3":
		return "audio/mpeg"
	case ".mp4":
		return "video/mp4"
	case ".oga":
		return "audio/ogg"
	case ".ogg":
		return "audio/ogg"
	case ".ogv":
		return "video/ogg"
	case ".opus":
		return "audio/opus"
	case ".wav":
		return "audio/wav"
	case ".webm":
		return "video/webm"
	}

	return "application/octet-stream"
}

func file_write(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func file_write_from_reader(path string, r io.Reader) bool {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		warn("Unable to create directory for %q: %v", path, err)
		return false
	}

	f, err := os.Create(path)
	if err != nil {
		warn("Unable to open file %q for writing: %v", path, err)
		return false
	}
	defer f.Close()

	_, err = io.Copy(f, r)
	if err != nil {
		warn("Unable to write to file %q: %v", path, err)
		return false
	}

	return true
}

// Helper function to get the path of a file
func api_file_path(u *User, a *App, file string) string {
	return fmt.Sprintf("%s/users/%s/%s/files/%s", data_dir, u.UID, a.id, file)
}

// Helper function to get the base directory for a user's app files (for use with os.Root)
func api_file_base(u *User, a *App) string {
	return fmt.Sprintf("%s/users/%s/%s/files", data_dir, u.UID, a.id)
}

// app_asset_root opens the calling app's active version directory as an
// os.Root, through which every app-asset read is done. os.Root refuses to
// traverse a symlink at any depth; an os.Lstat on the joined path does not, and
// a dev app's directory is writable by whoever authors it.
func app_asset_root(a *App, u *User) (*os.Root, error) {
	av := a.active(u)
	if av == nil {
		return nil, fmt.Errorf("no active app version")
	}
	return os.OpenRoot(av.base)
}

// app_asset_path returns the on-disk path of an app asset, or "" when it does
// not resolve to a real file inside the app's directory (containment comes from
// opening through app_asset_root). Prefer app_asset_root and its handle; this
// is for the callers that must hand a path to something that opens it for them.
func app_asset_path(a *App, u *User, file string) string {
	root, err := app_asset_root(a, u)
	if err != nil {
		return ""
	}
	defer root.Close()
	f, err := root.Open(file)
	if err != nil {
		return ""
	}
	f.Close()
	return filepath.Join(a.active(u).base, file)
}

// Helper function to get the base directory for a user's storage
func user_storage_dir(u *User) string {
	return fmt.Sprintf("%s/users/%s", data_dir, u.UID)
}

// Calculate total size of files in a directory
func dir_size(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, information os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if information != nil && !information.IsDir() {
			size += information.Size()
		}
		return nil
	})
	return size, err
}

// mochi.file.type(name) -> string: The content type a file name's extension
// implies, application/octet-stream when it implies none. Same map as every
// serve path, so callers need not keep their own copy.
func api_file_type(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name); err != nil {
		return nil, err
	}
	return sl.String(file_name_type(name)), nil
}

// mochi.file.clean(name, maximum=255) -> string: Reduce a client-supplied name
// to one every platform accepts and the "filepath" validator passes. Strips
// invisible and forbidden characters, device stems and path components, and
// bounds the byte length; shares its implementation with the validator.
func api_file_clean(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name string
	maximum := 255
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name, "maximum?", &maximum); err != nil {
		return nil, err
	}
	return sl.String(path_clean(name, maximum)), nil
}

// mochi.file.delete(file) -> None: Delete a file
func api_file_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <file: string>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	user, _ := principal_storage(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	base := api_file_base(user, app)
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl.None, nil // Directory doesn't exist, nothing to delete
	}
	defer root.Close()

	storage_remove(root, user.UID, file)
	return sl.None, nil
}

// root_write_file streams source into name inside root, creating any parent
// directories, and returns the number of bytes written. The destination is
// opened through the root so a symlink left in the tree cannot redirect the
// write outside it, which a plain path join would allow. Inside a user's
// storage the bytes are charged as they are written (storage_meter), and a
// write the quota stops is removed and its charge returned. The caller
// measures the user's storage first (storage_remaining): a user whose storage
// has not been walked has no figure to hold the write to.
func root_write_file(root *os.Root, name string, source io.Reader) (int64, error) {
	dir := filepath.Dir(name)
	if dir != "." && dir != "" {
		if err := root_mkdir_all(root, dir); err != nil {
			return 0, err
		}
	}

	owner := storage_owner(root.Name())
	f, err := storage_create(root, owner, name)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	written, err := io.Copy(&storage_meter{writer: f, owner: owner}, source)
	if errors.Is(err, storage_exceeded) {
		f.Close()
		storage_remove(root, owner, name)
	}
	return written, err
}

// mochi.file.copy(source, destination) -> integer: Copy a file within the app's
// directory, returning the bytes copied. The bytes stream rather than being
// held in memory, so this is the way to duplicate an attachment or other large
// object.
func api_file_copy(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var source, destination string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "source", &source, "destination", &destination); err != nil {
		return sl_error(fn, "syntax: <source: string>, <destination: string>")
	}
	if !valid(source, "filepath") {
		return sl_error(fn, "invalid source %q", source)
	}
	if !valid(destination, "filepath") {
		return sl_error(fn, "invalid destination %q", destination)
	}

	user, _ := principal_storage(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	base := api_file_base(user, app)
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl_error(fn, "unable to access files directory")
	}
	defer root.Close()

	f, err := root.Open(source)
	if err != nil {
		return sl_error(fn, "file not found")
	}
	defer f.Close()

	information, err := f.Stat()
	if err != nil || information.IsDir() {
		return sl_error(fn, "file not found")
	}

	// A copy adds its own size to the account, so it is checked like a write.
	remaining, err := storage_remaining(storage_request_of(t), user)
	if err != nil {
		return sl_error(fn, "unable to measure storage: %v", err)
	}
	if information.Size() > remaining {
		return sl_error(fn, "storage limit exceeded")
	}

	written, err := root_write_file(root, destination, f)
	if err != nil {
		return sl_error(fn, "unable to write file")
	}

	return sl.MakeInt64(written), nil
}

// mochi.file.max() -> integer: The largest single object the platform stores, in
// bytes. Exposed so an app refuses an oversized upload against the same figure
// core enforces rather than keeping its own copy.
func api_file_maximum(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sl.MakeInt64(object_maximum), nil
}

// mochi.file.age(file) -> integer or None: Seconds since a file was last
// modified, or None if it does not exist or is a directory. A listing alone
// cannot tell a settled file from one another request is still writing.
func api_file_age(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <file: string>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	user, _ := principal_storage(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	base := api_file_base(user, app)
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl.None, nil
	}
	defer root.Close()

	information, err := root.Stat(file)
	if err != nil || information.IsDir() {
		return sl.None, nil
	}

	return sl.MakeInt64(int64(time.Since(information.ModTime()).Seconds())), nil
}

// mochi.file.size(file) -> integer or None: A file's size in bytes, or None if it
// does not exist or is a directory - the size to declare for bytes about to be
// streamed from it, which a relay is held to.
func api_file_size(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <file: string>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	user, _ := principal_storage(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	root, err := os.OpenRoot(api_file_base(user, app))
	if err != nil {
		return sl.None, nil
	}
	defer root.Close()

	information, err := root.Stat(file)
	if err != nil || information.IsDir() {
		return sl.None, nil
	}
	return sl.MakeInt64(information.Size()), nil
}

// mochi.file.exists(file) -> bool: Check whether a file exists
func api_file_exists(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <file: string>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	user, _ := principal_storage(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	base := api_file_base(user, app)
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl.False, nil // Directory doesn't exist
	}
	defer root.Close()

	_, err = root.Stat(file)
	if err != nil {
		return sl.False, nil
	}
	return sl.True, nil
}

// mochi.file.list(subdirectory) -> list: List files in a subdirectory
func api_file_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <subdirectory: string>")
	}

	dir, ok := sl.AsString(args[0])
	// "" and "." name the app's file root - a valid listing target that the
	// filepath validator (which requires a leading alphanumeric) would reject.
	if dir == "" || dir == "." {
		dir = "."
	} else if !ok || !valid(dir, "filepath") {
		return sl_error(fn, "invalid directory %q", dir)
	}

	user, _ := principal_storage(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	base := api_file_base(user, app)
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl_error(fn, "does not exist")
	}
	defer root.Close()

	information, err := root.Stat(dir)
	if err != nil {
		return sl_error(fn, "does not exist")
	}
	if !information.IsDir() {
		return sl_error(fn, "not a directory")
	}

	// Open the directory within the root
	d, err := root.OpenFile(dir, os.O_RDONLY, 0)
	if err != nil {
		return sl_error(fn, "does not exist")
	}
	defer d.Close()

	entries, err := d.ReadDir(-1)
	if err != nil {
		return sl_error(fn, "unable to list directory")
	}

	var files []string
	for _, e := range entries {
		files = append(files, e.Name())
	}
	sort.Strings(files)

	return sl_encode(files), nil
}

// api_file_open opens a file of the caller's app storage, for the readers
// below, answering it and a function closing it, or the error to answer.
func api_file_open(t *sl.Thread, fn *sl.Builtin, file string) (*os.File, func(), error) {
	if !valid(file, "filepath") {
		_, err := sl_error(fn, "invalid file %q", file)
		return nil, nil, err
	}

	user, _ := principal_storage(t)
	if user == nil {
		_, err := sl_error(fn, "no user")
		return nil, nil, err
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		_, err := sl_error(fn, "no app")
		return nil, nil, err
	}

	root, err := os.OpenRoot(api_file_base(user, app))
	if err != nil {
		_, err := sl_error(fn, "file not found")
		return nil, nil, err
	}
	f, err := root.Open(file)
	if err != nil {
		root.Close()
		_, err := sl_error(fn, "file not found")
		return nil, nil, err
	}
	return f, func() { f.Close(); root.Close() }, nil
}

// mochi.file.read(file, maximum=0) -> bytes or None: Read a file into memory.
// maximum refuses a larger file, answering None so the caller can fall back to
// a streaming path; zero, the default, is unbounded. The bound is checked
// against the file itself, never a size the caller supplies - it may come from
// a peer.
func api_file_read(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var file string
	var maximum int64
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "file", &file, "maximum?", &maximum); err != nil {
		return sl_error(fn, "syntax: <file: string>, [maximum: integer]")
	}
	f, done, err := api_file_open(t, fn, file)
	if err != nil {
		return nil, err
	}
	defer done()

	if maximum > 0 {
		information, err := f.Stat()
		if err != nil {
			return sl_error(fn, "unable to read file")
		}
		if information.Size() > maximum {
			debug("mochi.file.read refusing %q: %d bytes exceeds the caller's limit of %d", file, information.Size(), maximum)
			return sl.None, nil
		}
	}

	data, err := io.ReadAll(f)
	if err != nil {
		return sl_error(fn, "unable to read file")
	}

	return sl_encode(data), nil
}

// file_head_maximum is the most mochi.file.head reads.
const file_head_maximum = 1 << 20

// mochi.file.head(file, length) -> bytes: The first length bytes of a file, or
// the whole of a shorter one, read without the rest: what a file is - its
// signature - from a file of any size. length is 1 to 1 MiB.
func api_file_head(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var file string
	var length int
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "file", &file, "length", &length); err != nil {
		return sl_error(fn, "syntax: <file: string>, <length: integer>")
	}
	if length < 1 || length > file_head_maximum {
		return sl_error(fn, "invalid length %d: from 1 to %d", length, file_head_maximum)
	}
	f, done, err := api_file_open(t, fn, file)
	if err != nil {
		return nil, err
	}
	defer done()

	buffer := make([]byte, length)
	read, err := io.ReadFull(f, buffer)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return sl_error(fn, "unable to read file")
	}
	return sl_encode(buffer[:read]), nil
}

// mochi.file.write(file, data) -> None: Write a file from memory
func api_file_write(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <file: string>, <data: array of bytes>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	var data string
	switch v := args[1].(type) {
	case sl.String:
		data = string(v)
	case sl.Bytes:
		data = string(v)
	default:
		return sl_error(fn, "invalid file data")
	}

	user, _ := principal_storage(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	// Check storage limit (10GB per user across all apps; admins exempt). The
	// bytes are charged as they land (storage_meter), so a write running at the
	// same time is held to what this one leaves, and is refused part way when
	// the two together do not fit.
	remaining, err := storage_remaining(storage_request_of(t), user)
	if err != nil {
		return sl_error(fn, "unable to measure storage: %v", err)
	}
	if int64(len(data)) > remaining {
		return sl_error(fn, "storage limit exceeded")
	}

	// Ensure base directory exists before opening root
	base := api_file_base(user, app)
	if err := os.MkdirAll(base, 0755); err != nil {
		return sl_error(fn, "unable to create files directory: %v", err)
	}

	root, err := os.OpenRoot(base)
	if err != nil {
		return sl_error(fn, "unable to access files directory")
	}
	defer root.Close()

	// Create parent directories within the root if needed
	dir := filepath.Dir(file)
	if dir != "." && dir != "" {
		if err := root_mkdir_all(root, dir); err != nil {
			return sl_error(fn, "unable to create directory")
		}
	}

	f, err := storage_create(root, user.UID, file)
	if err != nil {
		return sl_error(fn, "unable to write file")
	}

	// WriteString writes directly from the string's backing bytes — no
	// []byte() conversion, no duplicate allocation. Important for large
	// writes: a 5 GiB string via Write([]byte(data)) needs 10 GiB heap
	// during the write; WriteString stays at 5 GiB.
	_, err = (&storage_meter{writer: f, owner: user.UID}).WriteString(data)
	f.Close()
	if errors.Is(err, storage_exceeded) {
		storage_remove(root, user.UID, file)
		return sl_error(fn, "storage limit exceeded")
	}
	if err != nil {
		return sl_error(fn, "unable to write file")
	}

	return sl.None, nil
}

// mochi.file.move(from, to) -> None: Rename a file within the app's file storage.
// Both paths are relative to the app's files directory and the destination's
// parents are created. No bytes are copied and no quota is charged.
func api_file_move(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var from, to string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "from", &from, "to", &to); err != nil {
		return sl_error(fn, "syntax: move(from, to)")
	}
	if !valid(from, "filepath") {
		return sl_error(fn, "invalid source %q", from)
	}
	if !valid(to, "filepath") {
		return sl_error(fn, "invalid destination %q", to)
	}

	user, _ := principal_storage(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	base := api_file_base(user, app)
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl_error(fn, "unable to access files directory")
	}
	defer root.Close()

	if dir := filepath.Dir(to); dir != "." && dir != "" {
		if err := root_mkdir_all(root, dir); err != nil {
			return sl_error(fn, "unable to create directory")
		}
	}

	// A file the move replaces is gone, unless it is the one being moved.
	if err := storage_rename(root, user.UID, from, to); err != nil {
		return sl_error(fn, "unable to move file: %v", err)
	}
	return sl.None, nil
}

// temporary_configure points the process's temporary directory at
// cache_dir/tmp. Go spools large multipart uploads to os.TempDir(), which on a
// systemd host is usually a tmpfs - spending RAM rather than relieving it. Set
// here so it follows directories.cache; failure is not fatal and falls back to
// the system default.
func temporary_configure() {
	temporary := filepath.Join(cache_dir, "tmp")
	if err := os.MkdirAll(temporary, 0o700); err != nil {
		warn("Unable to create temporary directory %s, falling back to the system default: %v", temporary, err)
		return
	}
	// Tighten explicitly: MkdirAll leaves an existing directory's mode alone,
	// and this one predates this function — app staging created it 0755. The
	// spooled parts of an upload land here, so other local users have no
	// business listing it.
	if err := os.Chmod(temporary, 0o700); err != nil {
		warn("Unable to restrict permissions on %s: %v", temporary, err)
	}
	if err := os.Setenv("TMPDIR", temporary); err != nil {
		warn("Unable to set TMPDIR to %s: %v", temporary, err)
	}
}

// Periodically clean expired cache files
func cache_manager() {
	for range time.Tick(1 * time.Hour) {
		cache_cleanup()
	}
}

// Remove cache files older than cache_age_maximum, then enforce the byte budget
// over the apps namespace (cache_evict).
func cache_cleanup() {
	cutoff := time.Now().Add(-cache_age_maximum)
	filepath.Walk(cache_dir, func(path string, information os.FileInfo, err error) error {
		if err != nil || information.IsDir() {
			return nil
		}
		if information.ModTime().Before(cutoff) {
			os.Remove(path)
		}
		return nil
	})
	cache_evict()
}

// ---------------------------------------------------------------------------
// Archives
// ---------------------------------------------------------------------------
//
// A zip container an app builds and reads without the entries passing through
// Starlark, so an export streams attachments instead of holding them all
// base64-expanded in memory. Entry names inside an archive are never used as
// paths - write() and extract() both take their paths from the caller - so
// zip-slip does not arise.

// archive_read_maximum bounds mochi.archive.read, which exists for a manifest
// rather than for content. Anything larger is what extract() is for.
const archive_read_maximum = 64 << 20

var api_archive = sls.FromStringDict(sl.String("mochi.archive"), sl.StringDict{
	"write":   sl.NewBuiltin("mochi.archive.write", api_archive_write),
	"list":    sl.NewBuiltin("mochi.archive.list", api_archive_list),
	"read":    sl.NewBuiltin("mochi.archive.read", api_archive_read),
	"extract": sl.NewBuiltin("mochi.archive.extract", api_archive_extract),
})

// mochi.archive.write(destination, entries) -> integer: Build a zip archive at
// destination and return its size. entries is a list of dicts, each with a
// "name" (the path inside the archive) and one of "file" (an app file), "cache"
// (a cache entry, where a copy pulled from a peer lives) or "data" (a string,
// for a manifest). File and cache entries stream in without being read into
// memory; a source that has gone missing is skipped rather than fatal.
func api_archive_write(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var destination string
	var entries sl.Value
	var cache bool
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "destination", &destination, "entries", &entries, "cache?", &cache); err != nil {
		return sl_error(fn, "syntax: <destination: string>, <entries: list>, [cache: bool]")
	}
	if !valid(destination, "filepath") {
		return sl_error(fn, "invalid destination %q", destination)
	}

	list, ok := sl_decode(entries).([]any)
	if !ok {
		return sl_error(fn, "entries must be a list")
	}

	user, _ := principal_storage(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	base := api_file_base(user, app)
	if err := os.MkdirAll(base, 0755); err != nil {
		return sl_error(fn, "unable to create files directory: %v", err)
	}
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl_error(fn, "unable to access files directory")
	}
	defer root.Close()

	// The archive is charged to the account like any other write, and its
	// contents are already stored, so the budget is checked before building
	// rather than discovering it part-way through.
	remaining, err := storage_remaining(storage_request_of(t), user)
	if err != nil {
		return sl_error(fn, "unable to measure storage: %v", err)
	}

	// Cache rather than file storage: an archive built for immediate download is
	// re-obtainable, quota-exempt and evictable, and it resolves for the calling
	// user - a.write.file would read as the entity owner.
	var out io.WriteCloser
	var sink io.Writer
	var cache_path string
	var cache_previous int64
	if cache {
		path, err := cache_file(t, destination)
		if err != nil {
			return sl_error(fn, "invalid destination %q", destination)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return sl_error(fn, "unable to create cache directory: %v", err)
		}
		if information, err := os.Stat(path); err == nil {
			cache_previous = information.Size()
		}
		out, err = os.Create(path)
		if err != nil {
			return sl_error(fn, "unable to write archive")
		}
		cache_path = path
		// The cache is exempt from the per-user quota, so the object ceiling is
		// what bounds this archive - without it counter.exceeded can never fire
		// and one call fills the disk before cache_admit is consulted below.
		remaining = object_maximum
	} else {
		dir := filepath.Dir(destination)
		if dir != "." && dir != "" {
			if err := root_mkdir_all(root, dir); err != nil {
				return sl_error(fn, "unable to create directory")
			}
		}
		out, err = storage_create(root, user.UID, destination)
		if err != nil {
			return sl_error(fn, "unable to write archive")
		}
		sink = &storage_meter{writer: out, owner: user.UID}
	}
	defer out.Close()
	if sink == nil {
		sink = out
	}

	// Account for a cached archive however this returns: a partial archive still
	// occupies cache, and a running total that never learns of those bytes
	// under-evicts. Measured from the file rather than from counter.written so a
	// partial write is counted too - the counter sits on the output side and does
	// hold the compressed total, which is what the quota check above needs.
	if cache_path != "" {
		defer func() {
			var size int64
			if information, err := os.Stat(cache_path); err == nil {
				size = information.Size()
			}
			cache_admit(size - cache_previous)
		}()
	}

	counter := &archive_counter{limit: remaining}
	writer := zip.NewWriter(io.MultiWriter(sink, counter))

	// An archive past its bound - the user's storage, metered as it is written,
	// or the object ceiling of one built in cache - is removed whole, and its
	// charge returned, rather than left part built.
	exceeded := func(err error) bool {
		return counter.exceeded || errors.Is(err, storage_exceeded)
	}
	refuse := func() (sl.Value, error) {
		writer.Close()
		out.Close()
		if cache_path != "" {
			os.Remove(cache_path)
		} else {
			storage_remove(root, user.UID, destination)
		}
		return sl_error(fn, "storage limit exceeded")
	}

	for _, item := range list {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name, _ := entry["name"].(string)
		if name == "" {
			return sl_error(fn, "archive entry has no name")
		}

		if source, ok := entry["file"].(string); ok && source != "" {
			if !valid(source, "filepath") {
				return sl_error(fn, "invalid file %q", source)
			}
			// Opened before the entry is created: a missing file is skipped rather than
			// fatal, and creating the entry first would leave an empty one that an
			// import reads back as a zero-byte attachment.
			f, err := root.Open(source)
			if err != nil {
				continue
			}
			w, err := writer.Create(name)
			if err != nil {
				f.Close()
				if exceeded(err) {
					return refuse()
				}
				return sl_error(fn, "unable to add %q: %v", name, err)
			}
			_, err = io.Copy(w, f)
			f.Close()
			if exceeded(err) {
				return refuse()
			}
			if err != nil {
				return sl_error(fn, "unable to add %q: %v", name, err)
			}
		} else if source, ok := entry["cache"].(string); ok && source != "" {
			// A copy pulled from a peer lives in cache rather than file
			// storage, so an export of someone else's attachment reads from
			// there. Cache space is evictable, and an entry that has gone is
			// skipped exactly as a missing file is.
			path, err := cache_file(t, source)
			if err != nil {
				return sl_error(fn, "invalid cache entry %q", source)
			}
			f, err := os.Open(path)
			if err != nil {
				continue
			}
			w, err := writer.Create(name)
			if err != nil {
				f.Close()
				if exceeded(err) {
					return refuse()
				}
				return sl_error(fn, "unable to add %q: %v", name, err)
			}
			_, err = io.Copy(w, f)
			f.Close()
			if exceeded(err) {
				return refuse()
			}
			if err != nil {
				return sl_error(fn, "unable to add %q: %v", name, err)
			}
		} else if data, ok := entry["data"].(string); ok {
			w, err := writer.Create(name)
			if err != nil {
				if exceeded(err) {
					return refuse()
				}
				return sl_error(fn, "unable to add %q: %v", name, err)
			}
			if _, err := io.WriteString(w, data); err != nil {
				if exceeded(err) {
					return refuse()
				}
				return sl_error(fn, "unable to add %q: %v", name, err)
			}
		}

		if exceeded(nil) {
			return refuse()
		}
	}

	if err := writer.Close(); err != nil {
		if exceeded(err) {
			return refuse()
		}
		return sl_error(fn, "unable to finish archive: %v", err)
	}
	if exceeded(nil) {
		return refuse()
	}

	return sl.MakeInt64(counter.written), nil
}

// archive_counter tallies bytes as an archive is built, so a quota overrun is
// caught while writing rather than after a multi-gigabyte file has landed.
type archive_counter struct {
	written  int64
	limit    int64
	exceeded bool
}

func (c *archive_counter) Write(p []byte) (int, error) {
	c.written += int64(len(p))
	if c.written > c.limit {
		c.exceeded = true
	}
	return len(p), nil
}

// archive is a zip reader over a file held open through the app's os.Root, so
// the handle the containment check resolved is the handle the reader reads.
type archive struct {
	*zip.Reader
	file *os.File
}

func (a *archive) Close() error { return a.file.Close() }

// archive_open opens an archive inside the app's file storage.
func archive_open(t *sl.Thread, fn *sl.Builtin, file string) (*archive, error) {
	if !valid(file, "filepath") {
		return nil, fmt.Errorf("invalid file %q", file)
	}
	user, _ := principal_storage(t)
	if user == nil {
		return nil, fmt.Errorf("no user")
	}
	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return nil, fmt.Errorf("no app")
	}
	root, err := os.OpenRoot(api_file_base(user, app))
	if err != nil {
		return nil, fmt.Errorf("file not found")
	}
	defer root.Close()

	// Read through the root handle. Resolving through it and then opening the
	// joined path again threw the containment away: the second open re-followed
	// whatever the check was meant to exclude, with a window in between.
	f, err := root.Open(file)
	if err != nil {
		return nil, fmt.Errorf("file not found")
	}
	information, err := f.Stat()
	if err != nil || information.IsDir() {
		f.Close()
		return nil, fmt.Errorf("file not found")
	}
	reader, err := zip.NewReader(f, information.Size())
	if err != nil {
		f.Close()
		return nil, err
	}
	return &archive{Reader: reader, file: f}, nil
}

// mochi.archive.list(file) -> list or None: The archive's entries, each a dict
// of name and size. None if the file is not a readable archive.
func api_archive_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var file string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "file", &file); err != nil {
		return sl_error(fn, "syntax: <file: string>")
	}
	reader, err := archive_open(t, fn, file)
	if err != nil {
		return sl.None, nil
	}
	defer reader.Close()

	entries := []map[string]any{}
	for _, f := range reader.File {
		if f.FileInfo().IsDir() {
			continue
		}
		entries = append(entries, map[string]any{
			"name": f.Name,
			"size": int64(f.UncompressedSize64),
		})
	}
	return sl_encode(entries), nil
}

// mochi.archive.read(file, name) -> bytes or None: One entry's contents. For a
// manifest; bulk content belongs in extract(), and an entry beyond
// archive_read_maximum is refused rather than held in memory.
func api_archive_read(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var file, name string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "file", &file, "name", &name); err != nil {
		return sl_error(fn, "syntax: <file: string>, <name: string>")
	}
	reader, err := archive_open(t, fn, file)
	if err != nil {
		return sl.None, nil
	}
	defer reader.Close()

	for _, f := range reader.File {
		if f.Name != name || f.FileInfo().IsDir() {
			continue
		}
		if f.UncompressedSize64 > archive_read_maximum {
			return sl_error(fn, "entry %q is too large to read; use extract", name)
		}
		rc, err := f.Open()
		if err != nil {
			return sl.None, nil
		}
		defer rc.Close()
		data, err := io.ReadAll(io.LimitReader(rc, archive_read_maximum+1))
		if err != nil || int64(len(data)) > archive_read_maximum {
			return sl.None, nil
		}
		return sl_encode(data), nil
	}
	return sl.None, nil
}

// mochi.archive.extract(file, name, destination) -> integer or None: Stream one
// entry out to an app file, returning the bytes written, or None if the entry
// is absent. The destination is the caller's, never the archive's own entry
// name, so a hostile name in a received archive addresses nothing.
func api_archive_extract(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var file, name, destination string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "file", &file, "name", &name, "destination", &destination); err != nil {
		return sl_error(fn, "syntax: <file: string>, <name: string>, <destination: string>")
	}
	if !valid(destination, "filepath") {
		return sl_error(fn, "invalid destination %q", destination)
	}

	user, _ := principal_storage(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	reader, err := archive_open(t, fn, file)
	if err != nil {
		return sl.None, nil
	}
	defer reader.Close()

	remaining, err := storage_remaining(storage_request_of(t), user)
	if err != nil {
		return sl_error(fn, "unable to measure storage: %v", err)
	}

	for _, f := range reader.File {
		if f.Name != name || f.FileInfo().IsDir() {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return sl.None, nil
		}
		defer rc.Close()

		root, err := os.OpenRoot(api_file_base(user, app))
		if err != nil {
			return sl_error(fn, "unable to access files directory")
		}
		defer root.Close()

		// Bounded by the quota rather than by the entry's declared size: a zip
		// header can claim any size it likes, and the decompressed stream is
		// what actually lands on disk.
		// What the quota stops, root_write_file has removed and given back.
		written, err := root_write_file(root, destination, io.LimitReader(rc, remaining+1))
		if errors.Is(err, storage_exceeded) {
			return sl_error(fn, "storage limit exceeded")
		}
		if err != nil {
			return sl_error(fn, "unable to write file")
		}
		if written > remaining {
			storage_remove(root, user.UID, destination)
			return sl_error(fn, "storage limit exceeded")
		}
		return sl.MakeInt64(written), nil
	}
	return sl.None, nil
}
