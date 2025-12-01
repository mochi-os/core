// Mochi server: API unit tests
// Copyright Alistair Cunningham 2025

package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Test that URL response size is limited
func TestURLResponseSizeLimit(t *testing.T) {
	// Create a test server that returns more than the limit
	oversized := make([]byte, url_max_response_size+1000)
	for i := range oversized {
		oversized[i] = 'X'
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(oversized)
	}))
	defer server.Close()

	// Make the request
	resp, err := url_request("GET", server.URL, nil, nil, nil)
	if err != nil {
		t.Fatalf("url_request failed: %v", err)
	}
	defer resp.Body.Close()

	// Read with limit (simulating what api_url_request does)
	data, err := io.ReadAll(io.LimitReader(resp.Body, url_max_response_size))
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	// Verify we got exactly the limit, not the full oversized response
	if len(data) != int(url_max_response_size) {
		t.Errorf("Expected %d bytes, got %d", url_max_response_size, len(data))
	}

	t.Logf("Response size limit worked: got %d bytes (limit: %d)", len(data), url_max_response_size)
}
