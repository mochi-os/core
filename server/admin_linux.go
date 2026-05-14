// Mochi server: Linux-only admin machinery — UDS listener, peer-cred auth,
// route registration, snapshot routine + endpoints, audit middleware.
// Copyright Alistair Cunningham 2026
//
// The admin listener exposes /_/admin/* endpoints for mochictl. It binds a
// Unix domain socket at <data_dir>/run/admin.sock with mode 0660 (group
// mochi). On accept, SO_PEERCRED verifies the peer is the mochi user, root,
// or in the mochi group; any other peer is dropped before reaching Gin.
//
// Snapshot writes a `.snap` sibling next to every live `*.db` in the data
// dir using SQLite's online backup API (sqlite3_backup_init), so page
// offsets stay stable and rsync delta is tight. Static files are not
// touched — rsync transfers them from their live paths directly.
//
// Audit middleware records one row per state-changing admin call (snapshot,
// stop, restart, reload). Read-only routes (status, version, config,
// identity, backup) are not audited.

//go:build linux

package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/sys/unix"
)

// -- Listener + peer credentials -------------------------------------------

var (
	admin_router     *gin.Engine
	admin_mochi_uid  uint32
	admin_mochi_gid  uint32
	admin_creds_once sync.Once
)

// peerCredKey is the context key used to attach the peer's Ucred to the
// request context so handlers and middleware can read it.
type peerCredKey struct{}

// admin_peer_cred extracts the peer credentials previously attached by
// admin_listener / ConnContext. Returns nil if not present.
func admin_peer_cred(ctx context.Context) *unix.Ucred {
	if cred, ok := ctx.Value(peerCredKey{}).(*unix.Ucred); ok {
		return cred
	}
	return nil
}

// admin_conn wraps an accepted UDS connection alongside its verified peer
// credentials so the http.Server's ConnContext callback can promote them
// into the request context.
type admin_conn struct {
	net.Conn
	cred *unix.Ucred
}

// admin_socket_path returns the absolute path of the admin UDS.
func admin_socket_path() string {
	return filepath.Join(run_dir(), "admin.sock")
}

// admin_resolve_creds populates admin_mochi_uid/gid from the OS user/group
// database. If the mochi user does not exist (e.g. development environment),
// the server's effective UID is used as a fallback so the developer can run
// mochictl as themselves.
func admin_resolve_creds() {
	admin_creds_once.Do(func() {
		if u, err := user.Lookup("mochi"); err == nil {
			if uid, err := strconv.ParseUint(u.Uid, 10, 32); err == nil {
				admin_mochi_uid = uint32(uid)
			}
		}
		if admin_mochi_uid == 0 {
			admin_mochi_uid = uint32(os.Geteuid())
		}
		if g, err := user.LookupGroup("mochi"); err == nil {
			if gid, err := strconv.ParseUint(g.Gid, 10, 32); err == nil {
				admin_mochi_gid = uint32(gid)
			}
		}
	})
}

// admin_peer_authorized checks SO_PEERCRED on a connected UnixConn. Returns
// true if the peer's UID matches the mochi user, is root, or is in the mochi
// group (checking supplementary groups via /proc/<pid>/status).
func admin_peer_authorized(c *net.UnixConn) (bool, *unix.Ucred) {
	raw, err := c.SyscallConn()
	if err != nil {
		return false, nil
	}
	var cred *unix.Ucred
	var credErr error
	err = raw.Control(func(fd uintptr) {
		cred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	})
	if err != nil || credErr != nil || cred == nil {
		return false, nil
	}
	if cred.Uid == 0 || cred.Uid == admin_mochi_uid {
		return true, cred
	}
	if admin_mochi_gid != 0 && admin_pid_in_group(int(cred.Pid), admin_mochi_gid) {
		return true, cred
	}
	return false, cred
}

// admin_pid_in_group reports whether the given process is a member of gid,
// including supplementary groups. SO_PEERCRED reports only the primary GID,
// so we read /proc/<pid>/status to see the full Groups: list.
func admin_pid_in_group(pid int, gid uint32) bool {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "Groups:") {
			continue
		}
		for _, s := range strings.Fields(strings.TrimPrefix(line, "Groups:")) {
			if g, err := strconv.ParseUint(s, 10, 32); err == nil && uint32(g) == gid {
				return true
			}
		}
	}
	return false
}

// admin_listener wraps a Unix listener and drops any connection that fails
// the peer-credential check before it reaches the HTTP server.
type admin_listener struct {
	inner net.Listener
}

func (l *admin_listener) Accept() (net.Conn, error) {
	for {
		c, err := l.inner.Accept()
		if err != nil {
			return nil, err
		}
		uc, ok := c.(*net.UnixConn)
		if !ok {
			_ = c.Close()
			continue
		}
		authorized, cred := admin_peer_authorized(uc)
		if !authorized {
			if cred != nil {
				warn("admin: rejecting unauthorised peer uid=%d gid=%d pid=%d", cred.Uid, cred.Gid, cred.Pid)
			} else {
				warn("admin: rejecting peer with unreadable creds")
			}
			_ = c.Close()
			continue
		}
		return &admin_conn{Conn: c, cred: cred}, nil
	}
}

func (l *admin_listener) Close() error   { return l.inner.Close() }
func (l *admin_listener) Addr() net.Addr { return l.inner.Addr() }

// admin_start binds the admin UDS and serves the admin Gin router in a
// goroutine. Called once during startup, after run_dir_create.
func admin_start() error {
	admin_resolve_creds()

	path := admin_socket_path()
	// Remove a stale socket from a previous unclean shutdown. EEXIST without
	// a listener bound is harmless; ENOENT is fine.
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale admin socket %s: %w", path, err)
	}

	// Bind with a restrictive umask so the socket is created without world
	// access, then explicitly set the desired mode below.
	old := syscall.Umask(0177)
	ln, err := net.Listen("unix", path)
	syscall.Umask(old)
	if err != nil {
		return fmt.Errorf("listen on admin socket %s: %w", path, err)
	}

	if err := os.Chmod(path, 0660); err != nil {
		_ = ln.Close()
		return fmt.Errorf("chmod admin socket %s: %w", path, err)
	}
	if admin_mochi_gid != 0 {
		if err := os.Chown(path, -1, int(admin_mochi_gid)); err != nil {
			warn("admin: chown %s to mochi group failed: %v (continuing with caller's group)", path, err)
		}
	}

	admin_router = gin.New()
	admin_router.Use(gin.Recovery())
	admin_register_routes(admin_router)

	server := &http.Server{
		Handler: admin_router,
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			if ac, ok := c.(*admin_conn); ok && ac.cred != nil {
				ctx = context.WithValue(ctx, peerCredKey{}, ac.cred)
			}
			return ctx
		},
	}
	go func() {
		info("admin: listening on %s", path)
		if err := server.Serve(&admin_listener{inner: ln}); err != nil && err != http.ErrServerClosed {
			warn("admin: server.Serve returned: %v", err)
		}
	}()

	return nil
}

// admin_register_routes wires every /_/admin/* handler.
func admin_register_routes(r *gin.Engine) {
	admin := r.Group("/_/admin")
	admin.Use(admin_audit_middleware())
	admin.GET("/status", admin_status)
	admin.GET("/version", admin_version)
	admin.GET("/config", admin_config)
	admin.GET("/identity", admin_identity)
	admin.GET("/health", admin_health)
	admin.POST("/snapshot", admin_snapshot)
	admin.GET("/backup", admin_backup)
	admin.POST("/stop", admin_stop)
	admin.POST("/restart", admin_restart)
	admin.POST("/replica/join", admin_replica_join)
	admin.POST("/replica/leave", admin_replica_leave)
	admin.GET("/replica/status", admin_replica_status)
	admin.GET("/replication/status", admin_replication_status)
	admin.GET("/replication/pair", admin_replication_pair)
	admin.POST("/replication/pair/remove", admin_replication_pair_remove)
	admin.POST("/replication/resync", admin_replication_resync)
}

// -- Audit middleware ------------------------------------------------------

// admin_audited_routes maps "<METHOD> <fullPath>" to the subcommand label
// to record. Anything not in this map is not audited.
var admin_audited_routes = map[string]string{
	"POST /_/admin/snapshot":               "admin.snapshot",
	"POST /_/admin/stop":                   "admin.stop",
	"POST /_/admin/restart":                "admin.restart",
	"POST /_/admin/replica/join":           "admin.replica.join",
	"POST /_/admin/replica/leave":          "admin.replica.leave",
	"POST /_/admin/replication/pair/remove": "admin.replication.pair.remove",
	"POST /_/admin/replication/resync":      "admin.replication.resync",
}

// admin_audit_middleware records a daemon-facility audit row after each
// request to a state-changing admin route.
func admin_audit_middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		key := c.Request.Method + " " + c.FullPath()
		op, ok := admin_audited_routes[key]
		if !ok {
			return
		}
		cred := admin_peer_cred(c.Request.Context())
		uid := -1
		gid := -1
		if cred != nil {
			uid = int(cred.Uid)
			gid = int(cred.Gid)
		}
		audit_log_daemon(fmt.Sprintf("%s peer_uid=%d peer_gid=%d status=%d",
			op, uid, gid, c.Writer.Status()))
	}
}

// -- Snapshot routine + handlers -------------------------------------------

const snapshot_lock_name = "snapshot.lock"

// snapshot_summary is the JSON returned by POST /_/admin/snapshot.
type snapshot_summary struct {
	Dbs    int      `json:"database_files"`
	Reaped int      `json:"stale_snapshots_reaped"`
	Bytes  int64    `json:"bytes_written"`
	Ms     int64    `json:"duration_ms"`
	Errors []string `json:"errors,omitempty"`
}

// snapshot_walk_dbs returns all live *.db file paths under root, skipping the
// run/ and cache/ top-level directories and any *.snap or *.snap.tmp files.
func snapshot_walk_dbs(root string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p == root {
				return nil
			}
			base := filepath.Base(p)
			if filepath.Dir(p) == root && (base == "run" || base == "cache") {
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".db") {
			return nil
		}
		paths = append(paths, p)
		return nil
	})
	return paths, err
}

// snapshot_walk_snaps returns all *.db.snap file paths under root.
func snapshot_walk_snaps(root string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p == root {
				return nil
			}
			base := filepath.Base(p)
			if filepath.Dir(p) == root && (base == "run" || base == "cache") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(d.Name(), ".db.snap") {
			paths = append(paths, p)
		}
		return nil
	})
	return paths, err
}

// snapshot_copy_db lives in db_snapshot.go (cross-platform); the
// bootstrap protocol calls it on every platform too.

// snapshot_acquire_lock takes an exclusive lock on <run_dir>/snapshot.lock so
// concurrent snapshot calls don't race. Linux flock is released automatically
// if the process exits, so a crashed snapshot won't leave a stale lock.
func snapshot_acquire_lock() (*os.File, error) {
	lockPath := filepath.Join(run_dir(), snapshot_lock_name)
	f, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("open lock %s: %w", lockPath, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("snapshot already in progress (lock %s busy)", lockPath)
	}
	return f, nil
}

// snapshot_release_lock releases the flock (implicit on close) and removes
// the lockfile so the run/ dir doesn't accumulate leftovers.
func snapshot_release_lock(f *os.File) {
	_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
	_ = f.Close()
	_ = os.Remove(filepath.Join(run_dir(), snapshot_lock_name))
}

// snapshot_in_place writes a `.snap` sibling next to every live DB in the
// data dir, then reaps any stale `.snap` whose live `.db` no longer exists.
// Acquires the snapshot lock for the duration of the call.
func snapshot_in_place() snapshot_summary {
	start := time.Now()
	out := snapshot_summary{}

	lock, err := snapshot_acquire_lock()
	if err != nil {
		out.Errors = append(out.Errors, err.Error())
		out.Ms = time.Since(start).Milliseconds()
		return out
	}
	defer snapshot_release_lock(lock)

	out = snapshot_in_place_locked()
	out.Ms = time.Since(start).Milliseconds()
	return out
}

// snapshot_in_place_locked does the actual snapshot work; the caller must
// already hold the snapshot lock. Used by admin_backup so the lock is held
// for both the snapshot phase and the tar streaming.
func snapshot_in_place_locked() snapshot_summary {
	out := snapshot_summary{}

	// Reap stale snaps first so we know the dir is tidy before writing new ones.
	snaps, err := snapshot_walk_snaps(data_dir)
	if err != nil {
		out.Errors = append(out.Errors, fmt.Sprintf("walk snaps: %v", err))
	}
	for _, snap := range snaps {
		live := strings.TrimSuffix(snap, ".snap")
		if _, err := os.Stat(live); os.IsNotExist(err) {
			if err := os.Remove(snap); err == nil {
				out.Reaped++
			}
		}
	}

	dbs, err := snapshot_walk_dbs(data_dir)
	if err != nil {
		out.Errors = append(out.Errors, fmt.Sprintf("walk dbs: %v", err))
		return out
	}

	for _, src := range dbs {
		tmp := src + ".snap.tmp"
		final := src + ".snap"
		bytes, err := snapshot_copy_db(src, tmp)
		if err != nil {
			out.Errors = append(out.Errors, fmt.Sprintf("%s: %v", src, err))
			_ = os.Remove(tmp)
			continue
		}
		if err := os.Rename(tmp, final); err != nil {
			out.Errors = append(out.Errors, fmt.Sprintf("rename %s: %v", tmp, err))
			_ = os.Remove(tmp)
			continue
		}
		out.Dbs++
		out.Bytes += bytes
	}

	return out
}

// admin_snapshot is the POST /_/admin/snapshot handler. Runs in-place; static
// files are left in their live locations (rsync transfers them directly).
func admin_snapshot(c *gin.Context) {
	out := snapshot_in_place()
	status := http.StatusOK
	if len(out.Errors) > 0 && out.Dbs == 0 {
		status = http.StatusInternalServerError
	}
	c.JSON(status, out)
}

// admin_backup is the GET /_/admin/backup handler. Refreshes the in-place
// `.snap` siblings, then streams a tar.gz of (snapshot DBs + live static
// files) to the response. Holds the snapshot lock throughout so the tar
// sees a consistent set of files.
func admin_backup(c *gin.Context) {
	lock, err := snapshot_acquire_lock()
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	defer snapshot_release_lock(lock)

	// Refresh .snap files in the data dir. They persist after the call —
	// useful for subsequent rsync workflows.
	summary := snapshot_in_place_locked()
	if summary.Dbs == 0 && len(summary.Errors) > 0 {
		c.JSON(http.StatusInternalServerError, summary)
		return
	}

	c.Header("Content-Type", "application/gzip")
	c.Header("Content-Disposition", `attachment; filename="mochi-backup.tar.gz"`)
	gz := gzip.NewWriter(c.Writer)
	tw := tar.NewWriter(gz)

	// Walk the data dir, including .snap files (rewritten to drop the
	// .snap suffix in the tar) and static files. Skip live DB sidecars,
	// in-flight temps, and ephemeral state.
	_ = filepath.WalkDir(data_dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if p == data_dir {
				return nil
			}
			base := filepath.Base(p)
			if filepath.Dir(p) == data_dir && (base == "run" || base == "cache") {
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		switch {
		case strings.HasSuffix(name, ".snap"):
			// .db.snap -> .db inside the archive, so a restore lands the
			// snapshot at the canonical path.
			rel, err := filepath.Rel(data_dir, p)
			if err != nil {
				return nil
			}
			rel = strings.TrimSuffix(rel, ".snap")
			if err := backup_tar_file(tw, p, rel); err != nil {
				warn("backup: tar %s: %v", rel, err)
			}
		case strings.HasSuffix(name, ".db"),
			strings.HasSuffix(name, ".db-wal"),
			strings.HasSuffix(name, ".db-shm"),
			strings.HasSuffix(name, ".db-journal"),
			strings.HasSuffix(name, ".snap.tmp"):
			// Live DB files and in-flight temps are excluded; .snap
			// siblings cover the data.
			return nil
		default:
			rel, err := filepath.Rel(data_dir, p)
			if err != nil {
				return nil
			}
			if err := backup_tar_file(tw, p, rel); err != nil {
				warn("backup: tar static %s: %v", rel, err)
			}
		}
		return nil
	})

	_ = tw.Close()
	_ = gz.Close()
}

// backup_tar_file appends one regular file to the tar writer using the
// given archive-relative name.
func backup_tar_file(tw *tar.Writer, src, rel string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	hdr, err := tar.FileInfoHeader(stat, "")
	if err != nil {
		return err
	}
	hdr.Name = rel
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	_, err = io.Copy(tw, f)
	return err
}
