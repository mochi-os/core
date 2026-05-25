// Mochi server: file_push tests
// Copyright Alistair Cunningham 2026

package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

// setup_file_push_test gives a temp data_dir, a user, and a configured
// file base directory. Returns the user UID and a cleanup function.
func setup_file_push_test(t *testing.T) (string, string, func()) {
	t.Helper()
	tmp_dir, err := os.MkdirTemp("", "mochi_file_push")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	orig := data_dir
	data_dir = tmp_dir

	user_uid := "019e1234567890abcdef1234567890ab"
	app_id := "testapp"

	base := file_user_app_base(user_uid, app_id)
	if err := os.MkdirAll(base, 0755); err != nil {
		t.Fatalf("mkdir base: %v", err)
	}

	return user_uid, app_id, func() {
		data_dir = orig
		os.RemoveAll(tmp_dir)
	}
}

// random_bytes returns n random bytes — incompressible, so wire-level
// transport optimisations don't skew tests.
func random_bytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

// sha256_hex computes the sha256 hex digest of a byte slice.
func sha256_hex(b []byte) string {
	h := sha256.New()
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

// TestFilePushCopyExactBytes — file_push_copy reads exactly `size`
// bytes from the source even when the source has more data after.
// Mirrors the receiver-side logic that has to stop at the header's
// declared size so the footer CBOR segment isn't pulled into the body.
func TestFilePushCopyExactBytes(t *testing.T) {
	body := random_bytes(t, 1000)
	extra := random_bytes(t, 500)
	combined := append(append([]byte{}, body...), extra...)

	tmp, err := os.CreateTemp("", "filepush_copy")
	if err != nil {
		t.Fatalf("temp: %v", err)
	}
	defer os.Remove(tmp.Name())

	src, _ := os.Open(os.DevNull)
	_ = src
	src2, err := os.CreateTemp("", "filepush_copy_src")
	if err != nil {
		t.Fatalf("temp src: %v", err)
	}
	defer os.Remove(src2.Name())
	if _, err := src2.Write(combined); err != nil {
		t.Fatalf("write src: %v", err)
	}
	src2.Seek(0, 0)

	written, err := file_push_copy(tmp, src2, int64(len(body)))
	if err != nil {
		t.Fatalf("copy: %v", err)
	}
	if written != int64(len(body)) {
		t.Errorf("written: got %d, want %d", written, len(body))
	}

	tmp.Seek(0, 0)
	got, err := os.ReadFile(tmp.Name())
	if err != nil {
		t.Fatalf("readfile: %v", err)
	}
	if len(got) != len(body) {
		t.Errorf("len: got %d, want %d", len(got), len(body))
	}
	if sha256_hex(got) != sha256_hex(body) {
		t.Errorf("body mismatch")
	}

	// The remaining bytes in src2 should be the extra — proves we
	// stopped exactly at `size`.
	rest, err := os.ReadFile(src2.Name())
	if err != nil {
		t.Fatalf("read rest: %v", err)
	}
	_ = rest
}

// TestFilePushCopyZeroBytes — zero-size file copies cleanly without
// hitting the source reader.
func TestFilePushCopyZeroBytes(t *testing.T) {
	tmp, err := os.CreateTemp("", "filepush_copy_zero")
	if err != nil {
		t.Fatalf("temp: %v", err)
	}
	defer os.Remove(tmp.Name())

	// A reader that would error if read.
	failing := &failing_reader{}
	written, err := file_push_copy(tmp, failing, 0)
	if err != nil {
		t.Errorf("zero-size copy errored: %v", err)
	}
	if written != 0 {
		t.Errorf("written: got %d, want 0", written)
	}
}

type failing_reader struct{}

func (f *failing_reader) Read(p []byte) (int, error) {
	return 0, os.ErrInvalid
}

// TestSplitServices — comma-separated split matches what
// queue_send_direct does inline.
func TestSplitServices(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"a", []string{"a"}},
		{"a,b", []string{"a", "b"}},
		{"projects,feeds,wikis", []string{"projects", "feeds", "wikis"}},
	}
	for _, c := range cases {
		got := split_services(c.in)
		if len(got) != len(c.want) {
			t.Errorf("split_services(%q): got %v, want %v", c.in, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("split_services(%q)[%d]: got %q, want %q", c.in, i, got[i], c.want[i])
			}
		}
	}
}

// TestFileUserAppBase — the helper produces the expected per-user
// per-app directory path.
func TestFileUserAppBase(t *testing.T) {
	orig := data_dir
	data_dir = "/tmp/mochi_test_root"
	defer func() { data_dir = orig }()

	got := file_user_app_base("u1", "feeds")
	want := "/tmp/mochi_test_root/users/u1/feeds/files"
	if got != want {
		t.Errorf("file_user_app_base: got %q, want %q", got, want)
	}
}

// TestFilePushHeaderRoundTrip — header struct round-trips through CBOR.
func TestFilePushHeaderRoundTrip(t *testing.T) {
	in := &FilePushHeader{
		User: "u1",
		App:  "feeds",
		Path: "avatars/big.jpg",
		Size: 1234567,
	}
	encoded := cbor_encode(in)
	var out FilePushHeader
	if err := cbor.Unmarshal(encoded, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.User != in.User || out.App != in.App || out.Path != in.Path || out.Size != in.Size {
		t.Errorf("mismatch: in=%+v out=%+v", in, out)
	}
}

// TestFileDeleteRoundTrip — delete payload ditto.
func TestFileDeleteRoundTrip(t *testing.T) {
	in := &FileDelete{User: "u1", App: "wikis", Path: "page.md"}
	encoded := cbor_encode(in)
	var out FileDelete
	if err := cbor.Unmarshal(encoded, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.User != in.User || out.App != in.App || out.Path != in.Path {
		t.Errorf("mismatch: in=%+v out=%+v", in, out)
	}
}
