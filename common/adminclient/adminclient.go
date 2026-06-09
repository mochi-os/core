// Package adminclient is mochictl's HTTP client for the server's admin
// listener. It's a thin wrapper around net/http.Client with a custom
// DialContext that dials the local admin transport — a Unix domain socket on
// Linux/macOS, a named pipe on Windows (see admin_dial in the platform files);
// the rest of mochictl makes regular http.Get / http.Post calls.
//
// Copyright Alistair Cunningham 2026

package adminclient

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// Client is an HTTP client whose Transport dials a single Unix socket. The
// host portion of any URL passed to its methods is ignored.
type Client struct {
	client *http.Client
	socket string
}

// New returns a Client that dials the given UDS path on every request.
// timeout follows http.Client semantics: 0 means no timeout, a positive
// duration sets a deadline covering the entire request (including reading
// the body). Long-running endpoints like /_/admin/backup should pass 0.
func New(socket string, timeout time.Duration) *Client {
	return &Client{
		socket: socket,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return admin_dial(ctx, socket)
				},
			},
		},
	}
}

// Socket returns the UDS path the client is configured to dial.
// Useful for error messages and `mochictl --help` output.
func (c *Client) Socket() string {
	return c.socket
}

// url builds an HTTP URL with a placeholder host (the actual destination is
// the UDS, set by the custom DialContext).
func (c *Client) url(path string) string {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return "http://admin" + path
}

// Get issues a GET request to the given path on the admin socket.
func (c *Client) Get(path string) (*http.Response, error) {
	resp, err := c.client.Get(c.url(path))
	if err != nil {
		return nil, c.connect_error(err)
	}
	return resp, nil
}

// Post issues a POST request to the given path with an optional body.
// Pass nil for body for an empty POST. Default content type is JSON.
func (c *Client) Post(path, kind string, body io.Reader) (*http.Response, error) {
	if kind == "" {
		kind = "application/json"
	}
	resp, err := c.client.Post(c.url(path), kind, body)
	if err != nil {
		return nil, c.connect_error(err)
	}
	return resp, nil
}

// connect_error replaces the noisy net/http error chain with a clean,
// user-facing message. The common cases — server not running and socket
// missing — are detected and rendered as one-line errors.
func (c *Client) connect_error(err error) error {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "connection refused"):
		return fmt.Errorf("server is not running (no listener at %s)", c.socket)
	case strings.Contains(msg, "no such file or directory"):
		return fmt.Errorf("admin socket not found at %s (server not started?)", c.socket)
	case strings.Contains(msg, "permission denied"):
		return fmt.Errorf("permission denied on %s (run as the mochi user, or join the mochi group)", c.socket)
	default:
		return fmt.Errorf("admin socket %s: %v", c.socket, err)
	}
}
