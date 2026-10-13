// Mochi server: Network state tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"fmt"
	"testing"

	sl "go.starlark.net/starlark"
	"golang.org/x/crypto/acme/autocert"
)

// networks_states calls mochi.server.networks as a user and returns the
// networks by name.
func networks_states(t *testing.T, thread *sl.Thread) map[string]map[string]any {
	t.Helper()
	fn := sl.NewBuiltin("mochi.server.networks", api_server_networks)
	value, err := api_server_networks(thread, fn, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	answer := sl_decode(value).(map[string]any)
	states := map[string]map[string]any{}
	for name, state := range answer["networks"].(map[string]any) {
		states[name] = state.(map[string]any)
	}
	return states
}

// TestServerNetworksStates: an enabled network is working while the server
// has a domain, unavailable as "domain" without one, and disabled when its
// setting or the development switch is off; Bluesky carries the week's
// certificate count against the authority's allowance; the call needs
// server/read.
func TestServerNetworksStates(t *testing.T) {
	alice, root := create_handles_test_env(t)
	app := create_external_app("app1")
	thread := create_test_thread(root, app)
	fn := sl.NewBuiltin("mochi.server.networks", api_server_networks)
	if _, err := api_server_networks(create_test_thread(alice, app), fn, nil, nil); err == nil {
		t.Error("mochi.server.networks answered without server/read")
	}
	permission_grant(root, app.id, "server/read")

	states := networks_states(t, thread)
	for _, n := range network_names {
		if states[n]["state"] != "working" || states[n]["reason"] != "" {
			t.Errorf("%s with a domain = %v, want working", n, states[n])
		}
	}
	certificates := states["bluesky"]["certificates"].(map[string]any)
	if fmt.Sprint(certificates["issued"]) != "0" || fmt.Sprint(certificates["limit"]) != "50" {
		t.Errorf("bluesky certificates = %v, want 0 of 50", certificates)
	}
	if _, has := states["xmpp"]["certificates"]; has {
		t.Error("only Bluesky carries the certificate count")
	}

	setting_set("xmpp", "false")
	states = networks_states(t, thread)
	if states["xmpp"]["state"] != "disabled" || states["fediverse"]["state"] != "working" {
		t.Errorf("after switching XMPP off: xmpp %v, fediverse %v", states["xmpp"], states["fediverse"])
	}
	setting_set("xmpp", "true")

	server_domain_name = "localhost"
	states = networks_states(t, thread)
	if states["fediverse"]["state"] != "unavailable" || states["fediverse"]["reason"] != "domain" {
		t.Errorf("without a domain = %v, want unavailable/domain", states["fediverse"])
	}
	server_domain_name = "Example.com"

	networks_development = false
	states = networks_states(t, thread)
	for _, n := range network_names {
		if states[n]["state"] != "disabled" {
			t.Errorf("%s with the switch off = %v, want disabled", n, states[n])
		}
	}
	networks_development = true
}

// TestCertificateCacheRecordsNewNames: the certificate cache counts a
// certificate stored for a new host name, and not a renewal of one it holds,
// the account key or a challenge entry.
func TestCertificateCacheRecordsNewNames(t *testing.T) {
	create_handles_test_env(t)
	cache := domains_cache{autocert.DirCache(t.TempDir())}
	ctx := context.Background()
	put := func(name string) {
		if err := cache.Put(ctx, name, []byte("pem")); err != nil {
			t.Fatal(err)
		}
	}
	put("alice.example.com")
	if n := certificates_recent(); n != 1 {
		t.Fatalf("after one certificate: %d, want 1", n)
	}
	put("alice.example.com")
	put("alice.example.com+rsa")
	put("bob.example.com+token")
	put("acme_account+key")
	if n := certificates_recent(); n != 1 {
		t.Errorf("a renewal, an RSA twin, a challenge token and the account key counted: %d, want 1", n)
	}
	put("bob.example.com")
	if n := certificates_recent(); n != 2 {
		t.Errorf("after a second name: %d, want 2", n)
	}
	// What the cache wrote is what autocert reads back.
	if data, err := cache.Get(ctx, "bob.example.com"); err != nil || string(data) != "pem" {
		t.Errorf("Get after Put = %q, %v", data, err)
	}
}

// TestNetworksSettingsHiddenUntilSwitch: the handle limit, the protocol
// switches and the server description are listed for the owner only while the
// development switch is on.
func TestNetworksSettingsHiddenUntilSwitch(t *testing.T) {
	_, root := create_handles_test_env(t)
	thread := create_test_thread(root, create_external_app("app1"))
	fn := sl.NewBuiltin("mochi.setting.list", api_setting_list)
	listed := func() map[string]bool {
		value, err := api_setting_list(thread, fn, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		names := map[string]bool{}
		for _, s := range sl_decode(value).([]any) {
			names[s.(map[string]any)["name"].(string)] = true
		}
		return names
	}
	for _, name := range []string{"handles", "description", "fediverse", "bluesky", "xmpp", "matrix"} {
		if !listed()[name] {
			t.Errorf("%s not listed with the switch on", name)
		}
	}
	networks_development = false
	names := listed()
	for _, name := range []string{"handles", "description", "fediverse", "bluesky", "xmpp", "matrix"} {
		if names[name] {
			t.Errorf("%s listed with the switch off", name)
		}
	}
	if !names["operator_name"] {
		t.Error("the other settings should still be listed")
	}
	networks_development = true
}

// TestServerPublicURL: the address links back to this server are built from
// - the configured public URL first, else https on the configured domain,
// else nothing where the server has no public address.
func TestServerPublicURL(t *testing.T) {
	create_handles_test_env(t)
	if url := server_public_url(); url != "https://example.com" {
		t.Errorf("with a domain = %q", url)
	}
	setting_set("oauth_public_url", "https://public.example/")
	if url := server_public_url(); url != "https://public.example" {
		t.Errorf("with a public URL = %q", url)
	}
	setting_set("oauth_public_url", "")
	server_domain_name = "localhost"
	if url := server_public_url(); url != "" {
		t.Errorf("without a domain = %q", url)
	}
	fn := sl.NewBuiltin("mochi.server.url", api_server_url)
	value, err := api_server_url(nil, fn, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if text, _ := sl.AsString(value); text != "" {
		t.Errorf("mochi.server.url without a domain = %q", text)
	}
}
