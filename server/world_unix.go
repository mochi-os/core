// Mochi server: Unix world-status transport — the UDS a co-located
// mochi-world server pushes its listing to.
//
// Same shape as the admin socket (admin_unix.go), different authority: the
// admin socket answers to the operator (mode 0660, group mochi), this one to
// the mochi-world SERVICE (group mochi-world), and it serves its own minimal
// engine — world_register_routes only — because its looser group must never
// reach the admin routes.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

//go:build linux || darwin

package main

import (
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
)

var (
	world_gid        uint32
	world_creds_once sync.Once
)

// world_socket_path returns the absolute path of the world-status UDS.
func world_socket_path() string {
	return filepath.Join(run_dir(), "world.sock")
}

// world_resolve_creds populates world_gid from the OS group database. A
// missing group (development machine, no mochi-world package) is fine: the
// socket then answers to root, the server's own user, and the mochi group —
// the same people the admin socket answers to — so a developer's own world
// process still connects.
func world_resolve_creds() {
	world_creds_once.Do(func() {
		if g, err := user.LookupGroup(world_account_group); err == nil {
			if gid, err := strconv.ParseUint(g.Gid, 10, 32); err == nil {
				world_gid = uint32(gid)
			}
		}
	})
}

// world_conn_listener wraps the Unix listener and drops any connection that
// fails the peer-credential check before it reaches the HTTP server.
type world_conn_listener struct {
	inner net.Listener
}

func (l *world_conn_listener) Accept() (net.Conn, error) {
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
		authorized, credential := world_peer_authorized(uc)
		if !authorized {
			if credential != nil {
				warn("world: rejecting unauthorised peer uid=%d gid=%d pid=%d", credential.uid, credential.gid, credential.pid)
			} else {
				warn("world: rejecting peer with unreadable creds")
			}
			_ = c.Close()
			continue
		}
		return c, nil
	}
}

func (l *world_conn_listener) Close() error   { return l.inner.Close() }
func (l *world_conn_listener) Addr() net.Addr { return l.inner.Addr() }

// world_start binds the world-status UDS and serves its minimal router in a
// goroutine. Called once during startup, after run_dir_create.
func world_start() error {
	admin_resolve_creds() // the fallback authorization reuses the admin identities
	world_resolve_creds()

	path := world_socket_path()
	if err := socket_path_check("world", path); err != nil {
		return err
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale world socket %s: %w", path, err)
	}

	old := syscall.Umask(0177)
	ln, err := net.Listen("unix", path)
	syscall.Umask(old)
	if err != nil {
		return fmt.Errorf("listen on world socket %s: %w", path, err)
	}

	if err := os.Chmod(path, 0660); err != nil {
		_ = ln.Close()
		return fmt.Errorf("chmod world socket %s: %w", path, err)
	}
	if world_gid != 0 {
		if err := os.Chown(path, -1, int(world_gid)); err != nil {
			warn("world: chown %s to %s group failed: %v (continuing with caller's group)", path, world_account_group, err)
		}
	}

	router := gin.New()
	router.Use(gin.Recovery())
	world_register_routes(router)

	server := &http.Server{Handler: router}
	go func() {
		info("world: listening on %s", path)
		if err := server.Serve(&world_conn_listener{inner: ln}); err != nil && err != http.ErrServerClosed {
			warn("world: server.Serve returned: %v", err)
		}
	}()

	return nil
}
