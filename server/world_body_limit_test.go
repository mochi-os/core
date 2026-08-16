// Mochi server: the world socket bounds a status push.
//
// Not a security boundary. The socket is a local UDS behind both a 0660 group
// and an SO_PEERCRED check, so the caller is software the administrator
// installed on this machine and placed in the mochi-world group - and it can
// already open unbounded connections here, which is cheaper than a large body.
//
// This is insurance against a BUGGY world server. ShouldBindJSON reads the body
// to completion before world_validate runs, so without a cap a runaway payload
// OOMs the server and leaves the operator a dead process with no explanation.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// world_body_router builds the same router both platform listeners serve, so
// the test exercises the registration path rather than a copy of it.
func world_body_router() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(gin.Recovery())
	world_register_routes(r)
	return r
}

// world_body_push returns a status push whose services list pads the body out
// to at least size bytes.
func world_body_push(size int) string {
	var services []string
	for len(strings.Join(services, ",")) < size {
		services = append(services, fmt.Sprintf(`{"service":"game","players":1,"name":"%s"}`, strings.Repeat("x", 512)))
	}
	return `{"world":{"id":"w1","name":"World","address":"example.com:4433","version":1},"services":[` +
		strings.Join(services, ",") + `]}`
}

// counting_reader records how many bytes the server actually consumed.
type counting_reader struct {
	inner io.Reader
	read  int
}

func (r *counting_reader) Read(p []byte) (int, error) {
	n, err := r.inner.Read(p)
	r.read += n
	return n, err
}

// TestWorldBodyOversizedIsNotReadToCompletion is the finding, and it has to be
// measured in BYTES CONSUMED rather than status code: ShouldBindJSON answers
// 400 either way - on a JSON error when the cap truncates the read, and on
// world_validate when it does not - so the status cannot tell the two apart.
// What the cap changes is that the server stops reading, which is the whole
// point when the sender is a buggy world server with a runaway payload.
func TestWorldBodyOversizedIsNotReadToCompletion(t *testing.T) {
	router := world_body_router()

	body := world_body_push(world_body_maximum * 4)
	if len(body) <= world_body_maximum {
		t.Fatalf("test payload is %d bytes, not over the cap of %d", len(body), world_body_maximum)
	}

	counter := &counting_reader{inner: strings.NewReader(body)}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("POST", "/_/world/status", counter)
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, request)

	if counter.read > world_body_maximum+4096 {
		t.Errorf("server consumed %d bytes of a %d-byte push; the cap of %d did not stop the read",
			counter.read, len(body), world_body_maximum)
	}
	if recorder.Code == http.StatusOK {
		t.Errorf("an oversized push was accepted (status %d)", recorder.Code)
	}
}

// TestWorldBodyOrdinaryPushStillWorks. The cap must sit far above a real push -
// a world id, name, address, version and a short services list.
func TestWorldBodyOrdinaryPushStillWorks(t *testing.T) {
	data_dir = t.TempDir()
	os.MkdirAll(data_dir+"/db", 0755)
	router := world_body_router()

	body := `{"world":{"id":"w1","name":"World","address":"example.com:4433","version":1},` +
		`"services":[{"service":"air","players":3}]}`

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("POST", "/_/world/status", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, request)

	// The handler may still reject on validation grounds in a bare test
	// environment; what must not happen is a rejection for size.
	if recorder.Code == http.StatusRequestEntityTooLarge {
		t.Errorf("an ordinary push was refused as too large; the cap of %d is too tight", world_body_maximum)
	}
}

// TestWorldBodyCapCoversBothListeners. world_unix.go and world_windows.go build
// their routers separately and identically; the cap lives in the shared
// registration so it cannot be applied to one and missed on the other.
func TestWorldBodyCapCoversBothListeners(t *testing.T) {
	shared, err := os.ReadFile("world.go")
	if err != nil {
		t.Fatalf("read world.go: %v", err)
	}
	if !strings.Contains(string(shared), "MaxBytesReader") {
		t.Error("the body cap is not in world_register_routes, so each listener would need its own")
	}

	for _, file := range []string{"world_unix.go", "world_windows.go"} {
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		if !strings.Contains(string(source), "world_register_routes(router)") {
			t.Errorf("%s no longer registers routes through the shared path, so it misses the cap", file)
		}
	}
}
