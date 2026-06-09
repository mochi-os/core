// Mochi server: Unix admin transport — UDS listener, peer-credential auth,
// admin_start, and the flock-based snapshot lock.
// Copyright Alistair Cunningham 2026
//
// The admin listener exposes /_/admin/* (registered in admin_routes.go) over a
// Unix domain socket at <data_dir>/run/admin.sock with mode 0660 (group
// mochi). On accept, the peer credentials verify the peer is the mochi user,
// root, or in the mochi group; any other peer is dropped before reaching Gin.
// The kernel call that reads those credentials differs per OS (SO_PEERCRED on
// Linux, LOCAL_PEERCRED on macOS), so admin_peer_authorized lives in the
// platform files admin_cred_linux.go / admin_cred_darwin.go. The Windows
// transport (named pipe) is in admin_windows.go.

//go:build linux || darwin

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"

	"github.com/gin-gonic/gin"
	"golang.org/x/sys/unix"
)

var (
	admin_mochi_uid  uint32
	admin_mochi_gid  uint32
	admin_creds_once sync.Once
)

// admin_cred_basic_authorized reports whether the peer uid/gid alone clears
// the gate: root, the mochi user, or a primary group of mochi. The per-OS
// admin_peer_authorized calls this first, then falls back to a supplementary-
// group check that needs platform-specific data (/proc on Linux, the xucred
// group list on macOS).
func admin_cred_basic_authorized(uid, gid uint32) bool {
	if uid == 0 || uid == admin_mochi_uid {
		return true
	}
	if admin_mochi_gid != 0 && gid == admin_mochi_gid {
		return true
	}
	return false
}

// admin_conn wraps an accepted UDS connection alongside its verified peer
// credentials so the http.Server's ConnContext callback can promote them
// into the request context.
type admin_conn struct {
	net.Conn
	cred *admin_cred
}

// admin_socket_path returns the absolute path of the admin UDS.
func admin_socket_path() string {
	return filepath.Join(run_dir(), "admin.sock")
}

// admin_resolve_creds populates admin_mochi_uid/gid from the OS user/group
// database (admin_account is "mochi" on Linux, "_mochi" on macOS). If that
// account does not exist (e.g. development environment, or a static darwin
// build that cannot query OpenDirectory), the server's effective UID is used
// as a fallback so the operator can run mochictl as the owner or root.
func admin_resolve_creds() {
	admin_creds_once.Do(func() {
		if u, err := user.Lookup(admin_account); err == nil {
			if uid, err := strconv.ParseUint(u.Uid, 10, 32); err == nil {
				admin_mochi_uid = uint32(uid)
			}
		}
		if admin_mochi_uid == 0 {
			admin_mochi_uid = uint32(os.Geteuid())
		}
		if g, err := user.LookupGroup(admin_account); err == nil {
			if gid, err := strconv.ParseUint(g.Gid, 10, 32); err == nil {
				admin_mochi_gid = uint32(gid)
			}
		}
	})
}

// admin_peer_authorized reads the connected peer's credentials and reports
// whether they clear the admin gate. It lives in the platform files
// admin_cred_linux.go (SO_PEERCRED) and admin_cred_darwin.go (LOCAL_PEERCRED)
// because the kernel call and the supplementary-group lookup differ per OS.

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
				warn("admin: rejecting unauthorised peer uid=%d gid=%d pid=%d", cred.uid, cred.gid, cred.pid)
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
				ctx = context.WithValue(ctx, peer_credential_key{}, ac.cred)
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

// snapshot_acquire_lock takes an exclusive lock on <run_dir>/snapshot.lock so
// concurrent snapshot calls don't race. Linux flock is released automatically
// if the process exits, so a crashed snapshot won't leave a stale lock.
func snapshot_acquire_lock() (*os.File, error) {
	lock_path := filepath.Join(run_dir(), snapshot_lock_name)
	f, err := os.OpenFile(lock_path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("open lock %s: %w", lock_path, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("snapshot already in progress (lock %s busy)", lock_path)
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
