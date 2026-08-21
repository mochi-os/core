// Mochi server: Windows world-status transport - the named pipe a co-located
// mochi-world server pushes its listing to.
//
// The pipe's descriptor grants LocalSystem and Administrators, as the admin
// pipe does; there is no packaged mochi-world service account yet, so a Windows
// world server runs elevated. Widen the SDDL to that account when the MSI grows
// one, never to Authenticated Users.//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

//go:build windows

package main

import (
	"fmt"
	"net/http"

	"github.com/Microsoft/go-winio"
	"github.com/gin-gonic/gin"
)

const world_pipe_sddl = "D:P(A;;GA;;;SY)(A;;GA;;;BA)"

// world_socket_path returns the world-status pipe name.
func world_socket_path() string {
	return `\\.\pipe\mochi-world`
}

// world_start binds the world-status pipe and serves its minimal router in a
// goroutine. Called once during startup.
func world_start() error {
	pipe := world_socket_path()
	ln, err := winio.ListenPipe(pipe, &winio.PipeConfig{SecurityDescriptor: world_pipe_sddl})
	if err != nil {
		return fmt.Errorf("listen on world pipe %s: %w", pipe, err)
	}

	router := gin.New()
	router.Use(gin.Recovery())
	world_register_routes(router)

	server := &http.Server{Handler: router}
	go func() {
		info("world: listening on %s", pipe)
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			warn("world: server.Serve returned: %v", err)
		}
	}()

	return nil
}
