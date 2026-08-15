// Mochi server: Utilities
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/btcsuite/btcutil/base58"
	cbor "github.com/fxamacker/cbor/v2"
	md "github.com/gomarkdown/markdown"
	md_html "github.com/gomarkdown/markdown/html"
	md_parser "github.com/gomarkdown/markdown/parser"
	"github.com/google/uuid"
	"github.com/microcosm-cc/bluemonday"
	"golang.org/x/text/unicode/norm"
	"io"
	"io/fs"
	"math/big"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	rd "runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	alphanumeric = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	// Unambiguous characters: excludes 0, 1, O, I, o, i, l
	unambiguous = "23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz"
)

var (
	locks         = map[string]*sync.Mutex{}
	locks_lock    sync.Mutex
	match_hyphens = regexp.MustCompile(`-`)
	// Every control character except carriage return and line feed. Those two
	// are admitted ON PURPOSE: valid() applies this to every input, and the
	// "text" type - entity data, directory entries - carries multi-line
	// content, so excluding them here rejects every payload with a newline in
	// it. Nothing else depends on the exemption: of the other validators only
	// "line" and "name" could match a newline at all, and both exclude it in
	// their own pattern; the rest use character classes that cannot.
	//
	// A validator needing no newline therefore says so itself rather than
	// asking this to say it. themes.go applies it to font names and override
	// values, which reach a style attribute - harmless, because that output
	// escapes the quote that delimits the attribute and both call sites refuse
	// the semicolon a second declaration would need.
	match_non_controls = regexp.MustCompile("^[\\P{Cc}\\r\\n]*$")
	regex_cache        = map[string]*regexp.Regexp{}
	regex_cache_mu     sync.Mutex
)

func string_in_slice(s string, slice []string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func atoi(s string, def int64) int64 {
	i, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return int64(i)
}

// Convert any value to string, handling complex types
func any_to_string(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case nil:
		return ""
	case bool:
		if v {
			return "true"
		}
		return "false"
	case float64:
		// JSON numbers are always float64
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v))
		}
		return fmt.Sprintf("%g", v)
	case int, int64, uint, uint64:
		return fmt.Sprintf("%d", v)
	default:
		// For arrays/objects, serialize back to JSON string
		if bytes, err := json.Marshal(v); err == nil {
			return string(bytes)
		}
		return fmt.Sprintf("%v", v)
	}
}

func base58_decode(in string, def string) []byte {
	out, _, err := base58.CheckDecode(in)
	if err != nil {
		info("Base58 decoding error for %q; returning default %q: %s", in, def, err)
		return []byte(def)
	}
	return out
}

func base58_encode(in []byte) string {
	return base58.CheckEncode(in, 0)
}

func cbor_encode(in any) []byte {
	return must(cbor.Marshal(in))
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func fingerprint(in string) string {
	hash := sha256.Sum256([]byte(in))

	// Encode using unambiguous characters (55 chars)
	out := make([]byte, 9)
	for i := range out {
		out[i] = unambiguous[int(hash[i])%len(unambiguous)]
	}
	return string(out)
}

// fingerprint_hyphens adds hyphens to a fingerprint for display (e.g., "abc-def-ghi").
func fingerprint_hyphens(in string) string {
	if len(in) < 9 {
		return in
	}
	return in[:3] + "-" + in[3:6] + "-" + in[6:]
}

func fingerprint_no_hyphens(in string) string {
	return strings.ReplaceAll(in, "-", "")
}

func itoa(in int) string {
	return strconv.Itoa(in)
}

func i64toa(in int64) string {
	return strconv.FormatInt(in, 10)
}

func json_encode(in any) string {
	return string(must(json.Marshal(in)))
}

func lock(key string) *sync.Mutex {
	locks_lock.Lock()
	defer locks_lock.Unlock()

	_, found := locks[key]
	if !found {
		locks[key] = &sync.Mutex{}
	}

	return locks[key]
}

// Escape special characters for SQL LIKE patterns
func like_escape(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "%", "\\%")
	s = strings.ReplaceAll(s, "_", "\\_")
	return s
}

var markdown_policy = func() *bluemonday.Policy {
	p := bluemonday.UGCPolicy()
	p.AllowAttrs("target").Matching(regexp.MustCompile(`^_blank$`)).OnElements("a")
	return p
}()

func markdown(in []byte) []byte {
	extensions := md_parser.CommonExtensions | md_parser.NoEmptyLineBeforeBlock
	p := md_parser.NewWithExtensions(extensions)
	r := md_html.NewRenderer(md_html.RendererOptions{Flags: md_html.CommonFlags | md_html.HrefTargetBlank})
	return markdown_policy.SanitizeBytes(md.ToHTML(in, p, r))
}

func must[T any](v T, errors ...error) T {
	if len(errors) == 0 {
		switch e := any(v).(type) {
		case error:
			if e == nil {
				return v
			}
		default:
			return v
		}
		panic(v)
	}
	err := errors[0]
	if err != nil {
		panic(err)
	}
	return v
}

func now() int64 {
	return time.Now().Unix()
}

func random_alphanumeric(length int) string {
	out := make([]rune, length)
	l := big.NewInt(int64(len(alphanumeric)))
	for i := range out {
		index := must(rand.Int(rand.Reader, l))
		out[i] = rune(alphanumeric[index.Int64()])
	}
	return string(out)
}

// Generate a random string using unambiguous characters (no 0, 1, O, I, o, i, l)
func random_unambiguous(length int) string {
	out := make([]rune, length)
	l := big.NewInt(int64(len(unambiguous)))
	for i := range out {
		index := must(rand.Int(rand.Reader, l))
		out[i] = rune(unambiguous[index.Int64()])
	}
	return string(out)
}

// Create directories recursively within an os.Root (compatible with Go < 1.24)
func root_mkdir_all(root *os.Root, path string) error {
	path = filepath.Clean(path)
	parts := strings.Split(path, string(os.PathSeparator))

	current := ""
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		if current != "" {
			current = current + string(os.PathSeparator) + part
		} else {
			current = part
		}

		err := root.Mkdir(current, 0755)
		if err != nil && !os.IsExist(err) {
			info, statErr := root.Stat(current)
			if statErr == nil && !info.IsDir() {
				return fmt.Errorf("path %q is a file", current)
			}
			if statErr != nil && !os.IsExist(err) {
				return err
			}
		}
	}
	return nil
}

func time_local(u *User, t int64) string {
	timezone := "UTC"
	if u != nil {
		timezone = user_preference_get(u, "timezone", "UTC")
	}

	// Handle "auto" timezone by falling back to UTC
	if timezone == "auto" {
		timezone = "UTC"
	}

	l, err := time.LoadLocation(timezone)
	if err == nil {
		return time.Unix(t, 0).In(l).Format(time.DateTime)
	} else {
		warn("Invalid time zone %q:", err)
		return time.Unix(t, 0).Format(time.DateTime)
	}
}

func uid() string {
	u := must(uuid.NewV7())
	return match_hyphens.ReplaceAllLiteralString(u.String(), "")
}

// Extraction bounds for an app package. The largest package shipped today
// unpacks to 34 MB across 185 entries, and the biggest ordinary app to 12 MB
// across 681; legitimate packages expand 1.3x to 3x because their bulk is
// already-compressed assets and minified bundles. A bomb expands thousands of
// times, so there is no overlap to trade off against - these are set for the
// growth of a real package (a 3D game gaining models and audio), not to sit
// close to today's largest.
//
// The byte cap stops one entry decompressing to petabytes; the entry cap stops
// the other shape, millions of tiny files exhausting inodes rather than disk.
const (
	unzip_maximum_bytes   = 256 << 20
	unzip_maximum_entries = 10_000
)

// app_metadata_maximum bounds a single metadata entry read out of a package.
// app.json and labels/en.conf are small; a package whose app.json alone runs
// to megabytes is not one to describe to a caller.
const app_metadata_maximum = 1 << 20

// zip_entry_read returns one named entry's bytes without extracting anything
// to disk, refusing an entry that decompresses past `maximum`. For callers
// that want a file or two out of an archive rather than all of it: nothing is
// written, so a bomb has nowhere to land.
func zip_entry_read(r *zip.Reader, name string, maximum int64) ([]byte, error) {
	for _, f := range r.File {
		if f.Name != name {
			continue
		}
		in, err := f.Open()
		if err != nil {
			return nil, err
		}
		defer in.Close()
		// +1 so an entry that exactly fills the budget reads as an overflow
		// rather than passing on the boundary.
		out, err := io.ReadAll(io.LimitReader(in, maximum+1))
		if err != nil {
			return nil, err
		}
		if int64(len(out)) > maximum {
			return nil, fmt.Errorf("%q is larger than the limit of %d bytes", name, maximum)
		}
		return out, nil
	}
	return nil, fmt.Errorf("%q not found in archive", name)
}

// unzip extracts `file` into `destination`, refusing an archive that expands
// past `maximum` bytes or unzip_maximum_entries files. Traversal is prevented
// by os.Root; these bounds are about volume.
func unzip(file string, destination string, maximum int64) error {
	r, err := zip.OpenReader(file)
	if err != nil {
		return err
	}
	defer r.Close()

	if len(r.File) > unzip_maximum_entries {
		return fmt.Errorf("archive has too many entries: %d (limit %d)", len(r.File), unzip_maximum_entries)
	}

	// Create destination if it doesn't exist
	if err := os.MkdirAll(destination, 0755); err != nil {
		return fmt.Errorf("unable to create destination: %v", err)
	}

	// Use os.Root for traversal-resistant file extraction
	root, err := os.OpenRoot(destination)
	if err != nil {
		return fmt.Errorf("unable to open destination: %v", err)
	}
	defer root.Close()

	var total int64
	for _, f := range r.File {
		if f.FileInfo().IsDir() {
			// Create directory within root (os.Root automatically prevents traversal)
			if err := root_mkdir_all(root, f.Name); err != nil {
				return err
			}
			continue
		}

		// Create parent directories if needed
		dir := filepath.Dir(f.Name)
		if dir != "." && dir != "" {
			if err := root_mkdir_all(root, dir); err != nil {
				return err
			}
		}

		// Create file within root (os.Root automatically prevents traversal)
		d, err := root.OpenFile(f.Name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}

		fa, err := f.Open()
		if err != nil {
			d.Close()
			return err
		}

		// Bound the RUNNING TOTAL rather than trust the header: a bomb can
		// declare a small uncompressed size and still expand without limit.
		// +1 so a copy that exactly fills the budget is still detected as an
		// overflow rather than passing on the boundary.
		written, err := io.Copy(d, io.LimitReader(fa, maximum-total+1))

		d.Close()
		fa.Close()

		if err != nil {
			return err
		}
		total += written
		if total > maximum {
			return fmt.Errorf("archive expands past the limit of %d bytes", maximum)
		}
	}

	return nil
}

// Check if URL targets cloud metadata service (SSRF protection)
func url_is_cloud_metadata(url string) bool {
	return strings.Contains(url, "169.254.169.254") ||
		strings.Contains(url, "metadata.google.internal")
}

// url_allow_private permits outbound requests to loopback, private, and
// link-local addresses. Off by default: mochi.url.*, RSS fetching, link
// previews and mochi.remote.peer() all take an app-supplied URL, so an
// unrestricted client is an SSRF pivot into whatever the host can reach.
// Peer-to-peer traffic is unaffected — libp2p dials multiaddrs directly and
// never goes through url_request — so this only constrains app-driven HTTP.
// Tests that serve from httptest (127.0.0.1) set it, as may a deployment that
// genuinely federates over a LAN.
var url_allow_private = false

// url_address_allowed reports whether a resolved dial address is a permitted
// outbound destination. It is called from the dialer with the address actually
// being connected to, after DNS resolution, so unlike inspecting the request
// URL it also covers hostnames that resolve inward, alternate textual or
// numeric IP forms, DNS rebinding between validation and connection, and
// redirects into internal ranges.
func url_address_allowed(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("blocked outbound request to unresolvable address %q", address)
	}
	if url_allow_private {
		return nil
	}
	// IsPrivate covers RFC 1918 and RFC 4193 unique-local; IsLinkLocalUnicast
	// covers 169.254.0.0/16 (so the cloud metadata service, whatever hostname
	// or notation reaches it) and fe80::/10. Each of these handles the
	// IPv4-in-IPv6 form, so ::ffff:169.254.169.254 is caught too.
	switch {
	case ip.IsLoopback(), ip.IsPrivate(), ip.IsUnspecified(),
		ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast(),
		ip.IsInterfaceLocalMulticast(), ip.IsMulticast():
		return fmt.Errorf("blocked outbound request to non-public address %s", ip)
	}
	// The standard library's helpers stop at the well-known private ranges,
	// which leaves the rest of the special-purpose registry reachable. Carrier
	// -grade NAT is the one that matters in practice: hosting providers and
	// mesh VPNs put internal services on 100.64.0.0/10, so a host there is
	// every bit as internal as a 10.0.0.0/8 one.
	for _, block := range url_blocked_networks {
		if block.Contains(ip) {
			return fmt.Errorf("blocked outbound request to non-public address %s", ip)
		}
	}
	return nil
}

// Special-purpose ranges (IANA IPv4/IPv6 special-purpose address registries)
// that net.IP's classification helpers do not cover. IPv4 entries also match
// their IPv4-in-IPv6 form, because net.IP stores a parsed ::ffff:a.b.c.d as
// the 4-byte address.
var url_blocked_networks = func() []*net.IPNet {
	blocks := []*net.IPNet{}
	for _, cidr := range []string{
		"100.64.0.0/10",   // RFC 6598 carrier-grade NAT / shared address space
		"192.0.0.0/24",    // RFC 6890 IETF protocol assignments
		"192.0.2.0/24",    // RFC 5737 documentation (TEST-NET-1)
		"198.18.0.0/15",   // RFC 2544 benchmarking
		"198.51.100.0/24", // RFC 5737 documentation (TEST-NET-2)
		"203.0.113.0/24",  // RFC 5737 documentation (TEST-NET-3)
		"240.0.0.0/4",     // RFC 1112 reserved, includes 255.255.255.255
		"64:ff9b::/96",    // RFC 6052 IPv4/IPv6 translation
		"100::/64",        // RFC 6666 discard-only
		"2001:db8::/32",   // RFC 3849 documentation
	} {
		_, block, err := net.ParseCIDR(cidr)
		if err != nil {
			panic("url_blocked_networks: bad CIDR " + cidr)
		}
		blocks = append(blocks, block)
	}
	return blocks
}()

// url_transport is the shared outbound transport for every app-driven HTTP
// request. The dialer's Control hook runs per connection, so the check applies
// to the initial request and to every redirect hop on the same client.
var url_transport = &http.Transport{
	// No proxy, deliberately. With one configured the dialer connects to the
	// proxy and the Control hook below sees the proxy's address, not the
	// destination — so HTTP_PROXY in the server's environment would silently
	// void the whole guard and hand apps a route to any internal host. Mochi
	// has no egress-proxy configuration, so nothing legitimately relies on it.
	Proxy: nil,
	DialContext: (&net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
		Control: func(network string, address string, _ syscall.RawConn) error {
			return url_address_allowed(address)
		},
	}).DialContext,
	MaxIdleConns:          100,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
}

// url_max_timeout caps an app-supplied request timeout. A Starlark call is
// already bounded by the compute timeout through the context below, so this
// only stops an app naming an absurd value; it is set above the default
// compute timeout so it never trims a legitimate request inside that window.
const url_max_timeout = 300 * time.Second

// url_timeout selects the request timeout from options: the app's value when it
// is a positive integer, the 30s default otherwise, clamped to url_max_timeout.
// Separated so the clamp can be asserted directly.
func url_timeout(options map[string]string) time.Duration {
	timeout := 30 * time.Second
	if seconds, err := strconv.Atoi(options["timeout"]); err == nil && seconds > 0 {
		timeout = time.Duration(seconds) * time.Second
	}
	if timeout > url_max_timeout {
		timeout = url_max_timeout
	}
	return timeout
}

// Make an HTTP request to a URL.
//
// ctx cancels the request when the calling Starlark action ends, including when
// it ends by timing out. Without it an app could start a request with a long
// timeout, have its action abandoned at the compute timeout, and leave the
// request running with no concurrency slot (released to the caller) and no
// cancellation — accumulating. Non-Starlark callers pass context.Background().
//
// If allowed_domains is non-empty, redirect targets are validated against them.
func url_request(ctx context.Context, method string, url string, options map[string]string, headers map[string]string, body any, allowed_domains ...string) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if url_is_cloud_metadata(url) {
		return nil, fmt.Errorf("access to cloud metadata service is blocked")
	}

	if method == "" {
		method = "GET"
	}

	var br io.Reader
	if body != nil {
		switch b := body.(type) {
		case string:
			br = strings.NewReader(b)

		case []byte:
			br = bytes.NewReader(b)

		default:
			br = strings.NewReader(json_encode(b))
			_, found := headers["Content-Type"]
			if !found {
				headers["Content-Type"] = "application/json"
			}
		}
	}

	r, err := http.NewRequestWithContext(ctx, strings.ToUpper(method), url, br)
	if err != nil {
		return nil, err
	}

	for k, v := range headers {
		r.Header.Set(k, v)
	}

	c := &http.Client{Timeout: url_timeout(options), Transport: url_transport}

	// Redirects are validated for every caller, not only those that passed
	// allowed domains: RSS fetching, link previews and peer discovery supply
	// none, and previously followed redirects entirely unchecked. The dialer
	// already refuses non-public destinations on each hop; these checks add the
	// metadata-name, hop-count and granted-domain limits on top.
	c.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if url_is_cloud_metadata(req.URL.String()) {
			return fmt.Errorf("redirect to cloud metadata service is blocked")
		}
		if len(via) >= 10 {
			return fmt.Errorf("too many redirects")
		}
		if len(allowed_domains) == 0 {
			return nil
		}
		redirect_host := strings.ToLower(req.URL.Hostname())
		for _, domain := range allowed_domains {
			if domain_matches(domain, redirect_host) {
				return nil
			}
		}
		return fmt.Errorf("redirect to %s not allowed by granted url permissions", redirect_host)
	}

	return c.Do(r)
}

func valid(s string, match string) bool {
	//debug("Validating %q (%+v) as %s", s, s, match)
	if !match_non_controls.MatchString(s) {
		return false
	}

	switch match {
	case "action":
		match = "^[0-9a-zA-Z/\\-:_*.]{1,100}$"
	case "apppath":
		match = "^[0-9a-zA-Z-]{0,100}$"
	case "constant":
		match = "^[0-9a-zA-Z/\\-\\._]{1,100}$"
	case "email":
		return email_valid(s)
	case "entity":
		match = "^[\\w]{49,51}$"
	case "peer":
		// A libp2p peer id in base58btc: "12D3KooW..." (52 chars) for the
		// ed25519 keys Mochi issues, "Qm..." (46) for older RSA hosts. The
		// alphabet excludes 0, O, I and l. Bounded either side rather than
		// fixed, since the length follows the key type.
		match = "^[1-9A-HJ-NP-Za-km-z]{44,64}$"
	case "filename":
		match = "^[0-9a-zA-Z _~()-][0-9a-zA-Z _~().-]{0,254}$"
	case "filepath":
		return path_valid(s)
	case "fingerprint":
		match = "^[0-9a-zA-Z]{9}$"
	case "function":
		match = "^[0-9a-zA-Z_]{1,100}$"
	case "id":
		match = "^[0-9a-z]{32}$"
	case "integer":
		match = "^(-)?\\d{1,12}$"
	case "numeric":
		match = "^(-)?\\d+(\\.\\d+)?$"
	case "json":
		match = "^[0-9a-zA-Z{}:\"]{1,1000}$"
	case "line":
		match = "^[^\r\n]{1,1000}$"
	case "locale":
		// BCP 47 language tag, lowercase form (canonical-case normalised in memory).
		// Accepts: en, en-gb, zh-hant, zh-hant-hk, pt-br, pt-419, en-x-pseudo, en-x-pseudo-rtl.
		// Rejects: uppercase variants, underscores, irregular grandfathered tags.
		// Standard subtags are 2-8 chars; the private-use "-x-" extension allows 1-8 char subtags.
		match = "^[a-z]{2,3}(-[a-z0-9]{2,8})*(-x(-[a-z0-9]{1,8})+)?$"
	case "name":
		match = "^[^<>\r\n]{1,1000}$"
	case "natural":
		match = "^\\d{1,9}$"
	case "parampath":
		match = "^[0-9a-zA-Z/:_-]{1,200}$"
	case "path":
		match = "^[0-9a-zA-Z-/]{0,1000}$"
	case "positive":
		// Unlike "natural", excludes zero — that is the only thing this type
		// offers over it. RE2 has no lookahead, so requiring a non-zero first
		// digit is how that is expressed, which also rejects "007".
		match = "^[1-9][0-9]{0,8}$"
	case "privacy":
		match = "^(public|private)$"
	case "text":
		if len(s) > 1000000 {
			return false
		}
		return true
	case "url":
		if len(s) > 10000 {
			return false
		}
		match = "^[\\w\\-\\/:%@.+?&;=~]*$"
	case "version":
		// Reject traversal sequences: a version becomes a path component
		// under data_dir/apps, so ".." must never pass.
		if strings.Contains(s, "..") {
			return false
		}
		// First char alphanumeric (no leading ".", "-" or "_" — mirrors the
		// filepath validator, blocks a bare "."). Hyphen last so it is a
		// literal, not a range endpoint: the old "[0-9a-zA-Z.-_]" made ".-_"
		// a range (0x2E-0x5F) that matched "/", "\\" and ":", allowing path
		// traversal in app install.
		match = "^[0-9a-zA-Z][0-9a-zA-Z._-]{0,19}$"
	}

	return regex_cached(match).MatchString(s)
}

// The ASCII punctuation allowed in a path component. Everything that can act -
// shell expansion, command separation, redirection, wildcards, cmd.exe escapes,
// header injection - is ASCII, so ASCII is held to this exact list while
// non-ASCII is judged by Unicode category in path_component_valid. The list is
// the POSIX portable filename set plus the punctuation all of Linux, Windows
// and macOS accept and no shell or URL parser assigns semantics to.
const path_punctuation = " !#%&'()+,-.=@[]_~"

// Windows reserves these device names case-insensitively, with or without an
// extension: creating "CON.txt" opens the console, not a file.
var path_devices = map[string]bool{
	"con": true, "prn": true, "aux": true, "nul": true,
	"com1": true, "com2": true, "com3": true, "com4": true, "com5": true,
	"com6": true, "com7": true, "com8": true, "com9": true,
	"lpt1": true, "lpt2": true, "lpt3": true, "lpt4": true, "lpt5": true,
	"lpt6": true, "lpt7": true, "lpt8": true, "lpt9": true,
}

func path_ascii(r rune) bool {
	if (r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') {
		return true
	}
	return strings.ContainsRune(path_punctuation, r)
}

// path_valid reports whether s is a relative path safe to store and serve on
// every platform an attachment can reach. A name travels to subscribers over
// P2P, so the rule is the intersection of Linux, Windows and macOS rather than
// the rules of whichever one this server runs on. This replaced an ASCII
// allow-list that refused every accented, CJK and Cyrillic name outright - no
// non-English speaker could name a file in their own language.
//
// None of this is the traversal defence: os.Root confines every file syscall
// to its base directory whatever the name says, and a symlink is invisible to
// any check on the name alone.
func path_valid(s string) bool {
	if s == "" || len(s) > 4096 || !utf8.ValidString(s) {
		return false
	}
	// One canonical form. APFS compares names decomposed, ext4 stores bytes as
	// given, so "café" composed and decomposed are one file on macOS and two on
	// Linux; only NFC input keeps one logical name one name everywhere.
	if norm.NFC.String(s) != s {
		return false
	}
	// fs.ValidPath refuses a rooted path, an empty component and any "." or
	// ".." segment; filepath.IsLocal refuses anything escaping the directory
	// it is joined to.
	if !fs.ValidPath(s) || !filepath.IsLocal(s) {
		return false
	}
	for _, component := range strings.Split(s, "/") {
		if !path_component_valid(component) {
			return false
		}
	}
	return true
}

func path_component_valid(component string) bool {
	// 255 bytes is the component limit on ext4 and APFS; NTFS counts UTF-16
	// units, and 255 bytes always fits.
	if component == "" || len(component) > 255 {
		return false
	}
	first, _ := utf8.DecodeRuneInString(component)
	// A leading dot hides the file - on every component, not just the first:
	// "apt/.git/config" once passed and was served, publishing a hosted git
	// tree's history. A leading hyphen reads as a flag to any tool given the
	// name, a leading tilde as a home directory to a shell, and a combining
	// mark with nothing to combine with renders onto the path separator.
	if strings.ContainsRune(".-~ ", first) || unicode.In(first, unicode.M) {
		return false
	}
	// Windows silently strips a trailing space or dot, so the name that comes
	// back from the filesystem is not the name that went in.
	if strings.HasSuffix(component, " ") || strings.HasSuffix(component, ".") {
		return false
	}
	stem, _, _ := strings.Cut(component, ".")
	if path_devices[strings.ToLower(stem)] {
		return false
	}
	for _, r := range component {
		if r < utf8.RuneSelf {
			if !path_ascii(r) {
				return false
			}
			continue
		}
		// Letters, marks, digits, punctuation and symbols of every script,
		// which admits café, 写真, отчёт and emoji. Excluded by absence:
		// controls (Cc: bell, newline, escape), format characters (Cf: the
		// bidirectional overrides that render "photo<RLO>gnp.exe" as
		// "photo.png", zero-width joiners), private use, unassigned, and every
		// separator except the ASCII space - a no-break or ideographic space
		// makes two names that display identically address different files.
		if !unicode.In(r, unicode.L, unicode.M, unicode.N, unicode.P, unicode.S) {
			return false
		}
	}
	// The confusable guard: a character whose compatibility form is forbidden
	// ASCII is standing in for it visually - the fullwidth solidus renders as
	// "/", the fullwidth colon as ":". Folding legitimate names is harmless
	// here because this only rejects when the fold lands on the forbidden set.
	for _, r := range norm.NFKC.String(component) {
		if r < utf8.RuneSelf && !path_ascii(r) {
			return false
		}
	}
	return true
}

// path_clean reduces an arbitrary client-supplied name to one path_valid
// accepts, so the upload boundary repairs instead of refusing: the browser
// hands over whatever the user's own filesystem allowed. For every legitimate
// name - café.png, 写真.png, Report (final).pdf - this is the identity
// function; only names carrying invisible characters, forbidden ASCII or a
// device stem change. Idempotent, and its output always passes path_valid.
func path_clean(name string, maximum int) string {
	if maximum < 16 || maximum > 255 {
		maximum = 255
	}
	// The last component of whatever path shape arrived; some browsers send a
	// full client-side path.
	if index := strings.LastIndexAny(name, "/\\"); index >= 0 {
		name = name[index+1:]
	}
	if !utf8.ValidString(name) {
		name = strings.ToValidUTF8(name, "_")
	}
	name = norm.NFC.String(name)

	var builder strings.Builder
	for _, r := range name {
		switch {
		case r < utf8.RuneSelf:
			if path_ascii(r) {
				builder.WriteRune(r)
			} else if !unicode.IsControl(r) {
				// Visible but forbidden punctuation leaves a scar; a control
				// character is invisible and is simply dropped.
				builder.WriteRune('_')
			}
		case unicode.In(r, unicode.Zs, unicode.Zl, unicode.Zp):
			builder.WriteRune(' ')
		case !unicode.In(r, unicode.L, unicode.M, unicode.N, unicode.P, unicode.S):
			// Format characters, private use, unassigned: dropped.
		case path_confusable(r):
			builder.WriteRune('_')
		default:
			builder.WriteRune(r)
		}
	}
	// Dropping a character can leave a base and a combining mark adjacent that
	// NFC would compose, so normalise again before judging the result.
	name = norm.NFC.String(builder.String())
	name = path_clean_trim(name)
	for iteration := 0; iteration < 4; iteration++ {
		if stem, _, _ := strings.Cut(name, "."); path_devices[strings.ToLower(stem)] {
			name = "_" + name
		}
		if len(name) <= maximum {
			break
		}
		name = path_clean_trim(path_truncate(name, maximum))
	}
	if name == "" {
		return "file"
	}
	return name
}

func path_confusable(r rune) bool {
	for _, folded := range norm.NFKC.String(string(r)) {
		if folded < utf8.RuneSelf && !path_ascii(folded) {
			return true
		}
	}
	return false
}

func path_clean_trim(name string) string {
	for name != "" {
		r, size := utf8.DecodeRuneInString(name)
		if strings.ContainsRune(".-~ ", r) || unicode.In(r, unicode.M) {
			name = name[size:]
			continue
		}
		break
	}
	for strings.HasSuffix(name, " ") || strings.HasSuffix(name, ".") {
		name = name[:len(name)-1]
	}
	return name
}

// path_truncate shortens name to at most maximum bytes on a rune boundary,
// keeping the extension when there is a plausible one - the extension decides
// the content type on the way back out.
func path_truncate(name string, maximum int) string {
	extension := path.Ext(name)
	if len(extension) > 16 || len(extension) >= maximum {
		extension = ""
	}
	stem := name[:len(name)-len(extension)]
	for len(stem)+len(extension) > maximum && stem != "" {
		_, size := utf8.DecodeLastRuneInString(stem)
		stem = stem[:len(stem)-size]
	}
	return stem + extension
}

func regex_cached(pattern string) *regexp.Regexp {
	regex_cache_mu.Lock()
	defer regex_cache_mu.Unlock()
	if re, ok := regex_cache[pattern]; ok {
		return re
	}
	re := regexp.MustCompile(pattern)
	regex_cache[pattern] = re
	return re
}

// path_scrub removes the server's filesystem prefix from a message bound for
// an HTTP client. The data_dir root and the per-user path segment reveal the
// server's disk layout and the owning user's id while explaining nothing to
// the caller; the app-relative remainder (which database or file failed) is
// kept so the message stays fully diagnostic.
func path_scrub(message string) string {
	if data_dir == "" {
		return message
	}
	root := data_dir + "/"
	for {
		i := strings.Index(message, root)
		if i < 0 {
			return message
		}
		rest := message[i+len(root):]
		if strings.HasPrefix(rest, "users/") {
			if j := strings.Index(rest[len("users/"):], "/"); j >= 0 {
				rest = rest[len("users/")+j+1:]
			}
		}
		message = message[:i] + rest
	}
}

// guard runs f, containing any panic it raises rather than letting it reach
// the goroutine's top and take the process with it.
//
// recover() only ever sees panics raised on its OWN goroutine, so a guard
// around the function that spawns a goroutine catches nothing: every entry
// point that runs on its own goroutine and is fed by remote input needs one of
// its own. That is why this exists at each of the three inbound P2P entry
// points - receive_messages, receive_stream and pubsub_receive - rather than
// once around the dispatch they share.
//
// Said "two" until 2026-08, while receive_messages had no guard. The count was
// the only statement of which entry points were covered, so being wrong about
// it is what let the busiest one stay unguarded without looking like a gap.
//
// `after` runs during recovery to shut the faulted subject down - resetting a
// stream, say - and is itself guarded, so a second panic while cleaning up
// cannot defeat the first.
func guard(name string, after func(), f func()) {
	defer func() {
		fault := recover()
		if fault == nil {
			return
		}
		warn("Panic in %s: %v\n\n%s", name, fault, string(rd.Stack()))
		if after != nil {
			defer func() { _ = recover() }()
			after()
		}
	}()
	f()
}
