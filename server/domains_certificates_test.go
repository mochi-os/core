// Mochi server: certificate storage location tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/acme/autocert"
)

// certificates_dirs points cache_dir and data_dir at temporary directories for
// one test, restoring them afterwards.
func certificates_dirs(t *testing.T) (cache string, data string) {
	t.Helper()
	original_cache, original_data := cache_dir, data_dir
	t.Cleanup(func() { cache_dir, data_dir = original_cache, original_data })
	cache_dir = t.TempDir()
	data_dir = t.TempDir()
	return cache_dir, data_dir
}

// TestCertificatesLiveOutsideTheCache pins that the autocert cache is not under
// cache_dir. cache_cleanup deletes everything there older than cache_max_age,
// which silently destroyed valid certificates and the ACME account key —
// invisibly, because autocert also caches in memory, so the loss only appeared
// on the next restart.
func TestCertificatesLiveOutsideTheCache(t *testing.T) {
	cache, data := certificates_dirs(t)

	path := domains_certificates()
	if within(path, cache) {
		t.Errorf("certificates at %q are under cache_dir %q, where the sweeper deletes them", path, cache)
	}
	if !within(path, data) {
		t.Errorf("certificates at %q are not under data_dir %q", path, data)
	}
}

// TestCertificatesMigrate covers the one-time move from the old location.
// Without it, every install re-issues every certificate and registers a fresh
// ACME account on upgrade — the burst the move exists to prevent.
func TestCertificatesMigrate(t *testing.T) {
	t.Run("brings across an existing cache", func(t *testing.T) {
		cache, _ := certificates_dirs(t)
		source := filepath.Join(cache, "certs")
		if err := os.MkdirAll(source, 0o700); err != nil {
			t.Fatal(err)
		}
		for name, content := range map[string]string{
			"acme_account+key": "account-key-material",
			"mochi-os.org":     "certificate-material",
		} {
			if err := os.WriteFile(filepath.Join(source, name), []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}

		domains_certificates_migrate()

		got, err := os.ReadFile(filepath.Join(domains_certificates(), "acme_account+key"))
		if err != nil {
			t.Fatalf("account key did not survive the move: %v", err)
		}
		if string(got) != "account-key-material" {
			t.Errorf("account key content is %q, want %q", got, "account-key-material")
		}
		if _, err := os.Stat(filepath.Join(domains_certificates(), "mochi-os.org")); err != nil {
			t.Errorf("certificate did not survive the move: %v", err)
		}
		// Key material must not be left behind in a directory deleted on a timer.
		if _, err := os.Stat(source); !os.IsNotExist(err) {
			t.Errorf("old location %s still exists after a complete migration", source)
		}
	})

	t.Run("keeps private permissions", func(t *testing.T) {
		cache, _ := certificates_dirs(t)
		source := filepath.Join(cache, "certs")
		if err := os.MkdirAll(source, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(source, "key"), []byte("secret"), 0o600); err != nil {
			t.Fatal(err)
		}

		domains_certificates_migrate()

		info, err := os.Stat(filepath.Join(domains_certificates(), "key"))
		if err != nil {
			t.Fatalf("file did not survive the move: %v", err)
		}
		if mode := info.Mode().Perm(); mode&0o077 != 0 {
			t.Errorf("migrated key is mode %o, readable beyond its owner", mode)
		}
	})

	t.Run("never clobbers a destination in use", func(t *testing.T) {
		cache, _ := certificates_dirs(t)
		source := filepath.Join(cache, "certs")
		if err := os.MkdirAll(source, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(source, "mochi-os.org"), []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(domains_certificates(), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(domains_certificates(), "mochi-os.org"), []byte("current"), 0o600); err != nil {
			t.Fatal(err)
		}

		domains_certificates_migrate()

		got, _ := os.ReadFile(filepath.Join(domains_certificates(), "mochi-os.org"))
		if string(got) != "current" {
			t.Errorf("migration overwrote the live certificate with %q", got)
		}
	})

	t.Run("does nothing without an old cache", func(t *testing.T) {
		certificates_dirs(t)
		domains_certificates_migrate() // must not panic or create anything
		if entries, err := os.ReadDir(domains_certificates()); err == nil && len(entries) > 0 {
			t.Errorf("migration invented %d file(s) with no source", len(entries))
		}
	})
}

// within reports whether path sits inside directory.
func within(path, directory string) bool {
	relative, err := filepath.Rel(directory, path)
	if err != nil {
		return false
	}
	return relative != ".." && !filepath.IsAbs(relative) &&
		(len(relative) < 2 || relative[:2] != "..")
}

// TestCertificatesSeparateByAuthority pins that a non-default ACME directory
// gets its own cache.
//
// A staging certificate is signed by a root no browser trusts. If one landed
// in the production cache and were served, every visitor would get a full-page
// security error - a worse outcome than the expiry the surrounding work exists
// to prevent. The ACME account key is cached alongside the certificates and
// belongs to exactly one authority, so it must not be shared either.
func TestCertificatesSeparateByAuthority(t *testing.T) {
	certificates_dirs(t)
	production := domains_certificates()

	staging := "https://acme-staging-v02.api.letsencrypt.org/directory"
	ini_acme_for_test(t, staging)
	alternate := domains_certificates()

	if alternate == production {
		t.Fatalf("a staging directory shares the production cache at %q: an untrusted certificate could be served to real visitors", alternate)
	}
	if !within(alternate, production) {
		t.Errorf("staging cache %q is not under the certificates directory %q", alternate, production)
	}
	if !strings.Contains(alternate, "acme-staging-v02.api.letsencrypt.org") {
		t.Errorf("staging cache %q is not named for its authority, so two authorities could collide", alternate)
	}
}

// TestAuthorityNameIsPathSafe pins that a directory URL cannot escape the
// certificates directory or produce an unusable path.
func TestAuthorityNameIsPathSafe(t *testing.T) {
	for _, directory := range []string{
		"https://acme-staging-v02.api.letsencrypt.org/directory",
		"https://example.com:14000/dir",
		"https://../../etc/directory",
		"not a url at all",
		"",
	} {
		name := domains_authority(directory)
		if name == "" {
			t.Errorf("%q produced an empty authority name", directory)
		}
		if strings.ContainsAny(name, "/\\") || strings.Contains(name, "..") {
			t.Errorf("%q produced %q, which escapes the certificates directory", directory, name)
		}
	}
}

// TestACMEManagerUsesConfiguredDirectory pins the wiring, not just the path
// helper: the manager the server builds must actually issue from the
// configured authority and cache into that authority's directory.
func TestACMEManagerUsesConfiguredDirectory(t *testing.T) {
	certificates_dirs(t)

	// Default: no client, so autocert uses its built-in production directory.
	domains_init_acme()
	if domains_acme_manager.Client != nil {
		t.Errorf("a client was configured by default: %+v", domains_acme_manager.Client)
	}

	staging := "https://acme-staging-v02.api.letsencrypt.org/directory"
	ini_acme_for_test(t, staging)
	domains_init_acme()
	if domains_acme_manager.Client == nil {
		t.Fatal("no ACME client configured, so issuance would still go to the default authority")
	}
	if got := domains_acme_manager.Client.DirectoryURL; got != staging {
		t.Errorf("directory URL is %q, want %q", got, staging)
	}
	cache, ok := domains_acme_manager.Cache.(autocert.DirCache)
	if !ok {
		t.Fatalf("cache is %T, not a DirCache", domains_acme_manager.Cache)
	}
	if !strings.Contains(string(cache), "acme-staging-v02.api.letsencrypt.org") {
		t.Errorf("cache is %q, not the authority's own directory: staging certificates could be served as real ones", string(cache))
	}
}

// ini_acme_for_test points the ACME directory setting at a value for one test.
func ini_acme_for_test(t *testing.T, directory string) {
	t.Helper()
	original := domains_acme_directory_override
	domains_acme_directory_override = directory
	t.Cleanup(func() { domains_acme_directory_override = original })
}
