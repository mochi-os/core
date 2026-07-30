// Mochi server: outbound mochi.remote.* rate limiting.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strconv"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// remote_limit_thread builds a Starlark thread carrying an app, which is what
// remote_rate_limit charges against.
func remote_limit_thread(app string) *sl.Thread {
	t := &sl.Thread{}
	t.SetLocal("app", &App{id: app})
	return t
}

// remote_limit_reset clears both buckets so each test starts from a full budget;
// the limiters are package-level and would otherwise leak state between tests.
func remote_limit_reset() {
	for _, limiter := range []*rate_limiter{rate_limit_remote_entity, rate_limit_remote} {
		limiter.lock.Lock()
		limiter.entries = make(map[string]*rate_limit_entry)
		limiter.lock.Unlock()
	}
}

// TestRemoteRateLimitPerTarget is the point of the change: a public action that
// proxies a remote entity's assets can be driven by an anonymous caller, so the
// per-target budget has to run out.
func TestRemoteRateLimitPerTarget(t *testing.T) {
	remote_limit_reset()
	thread := remote_limit_thread("people")

	limit := rate_limit_remote_entity.limit
	for i := 0; i < limit; i++ {
		if reason := remote_rate_limit(thread, "1victim"); reason != nil {
			t.Fatalf("call %d of %d refused (%v); the limit must clear real use - a profile view is five fetches", i+1, limit, reason)
		}
	}

	reason := remote_rate_limit(thread, "1victim")
	if reason == nil {
		t.Fatalf("call %d was allowed; the per-target budget is not enforced", limit+1)
	}
	if !strings.Contains(reason.Error(), "per target") {
		t.Errorf("refusal is %v, want the per-target message - otherwise the aggregate limit tripped first and this test proves nothing about per-target", reason)
	}
}

// TestRemoteRateLimitReportsTheEnforcedNumber catches the message drifting from
// the behaviour. The limit was raised from 120 to 600 during development while
// the message still said 120, so the refusal named a budget that was not the one
// being enforced - anyone debugging it would have been chasing the wrong number.
func TestRemoteRateLimitReportsTheEnforcedNumber(t *testing.T) {
	for _, c := range []struct {
		name    string
		limiter *rate_limiter
		refuse  func(*sl.Thread) error
	}{
		{"per target", rate_limit_remote_entity, func(th *sl.Thread) error {
			for i := 0; i < rate_limit_remote_entity.limit+1; i++ {
				if reason := remote_rate_limit(th, "1victim"); reason != nil {
					return reason
				}
			}
			return nil
		}},
		{"aggregate", rate_limit_remote, func(th *sl.Thread) error {
			for i := 0; i < rate_limit_remote.limit+1; i++ {
				if reason := remote_rate_limit(th, "1entity"+strconv.Itoa(i)); reason != nil {
					return reason
				}
			}
			return nil
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			remote_limit_reset()
			reason := c.refuse(remote_limit_thread("people"))
			if reason == nil {
				t.Fatalf("no refusal, so there is no message to check")
			}
			want := strconv.Itoa(c.limiter.limit)
			if !strings.Contains(reason.Error(), want) {
				t.Errorf("refusal is %v but the enforced limit is %s; the message must name the limit actually applied", reason, want)
			}
		})
	}
}

// TestRemoteRateLimitIsolatesTargets is the reason the key is not the app alone.
// Keying on the app would give shared fate: a flood against one entity would
// exhaust the budget every other user of that app draws on, converting a
// bandwidth nuisance into a denial of service against ourselves.
func TestRemoteRateLimitIsolatesTargets(t *testing.T) {
	remote_limit_reset()
	thread := remote_limit_thread("people")

	for i := 0; i < rate_limit_remote_entity.limit+10; i++ {
		remote_rate_limit(thread, "1flooded")
	}
	if reason := remote_rate_limit(thread, "1flooded"); reason == nil {
		t.Fatalf("the flooded target is not limited, so this test cannot show isolation")
	}

	if reason := remote_rate_limit(thread, "1bystander"); reason != nil {
		t.Errorf("an unrelated target was refused (%v); one entity's traffic must not starve another's", reason)
	}
}

// TestRemoteRateLimitIsolatesApps keeps one app's flood off another's budget.
func TestRemoteRateLimitIsolatesApps(t *testing.T) {
	remote_limit_reset()
	flooding := remote_limit_thread("people")

	for i := 0; i < rate_limit_remote_entity.limit+10; i++ {
		remote_rate_limit(flooding, "1victim")
	}
	if reason := remote_rate_limit(flooding, "1victim"); reason == nil {
		t.Fatalf("the flooding app is not limited, so this test cannot show isolation")
	}

	if reason := remote_rate_limit(remote_limit_thread("feeds"), "1victim"); reason != nil {
		t.Errorf("a different app was refused (%v); budgets are per app", reason)
	}
}

// TestRemoteRateLimitAggregate covers the fan-out the per-target limit cannot:
// an attacker who spreads the load across every entity the directory knows stays
// under each target's budget forever, so the total needs its own ceiling.
func TestRemoteRateLimitAggregate(t *testing.T) {
	remote_limit_reset()
	thread := remote_limit_thread("people")

	var refused error
	// Each target is distinct, so the per-target budget is never reached and only
	// the aggregate can stop this.
	for i := 0; i < rate_limit_remote.limit+1; i++ {
		if reason := remote_rate_limit(thread, "1entity"+strconv.Itoa(i)); reason != nil {
			refused = reason
			break
		}
	}
	if refused == nil {
		t.Fatalf("%d calls across distinct targets were all allowed; the aggregate ceiling is not enforced", rate_limit_remote.limit+1)
	}
	if strings.Contains(refused.Error(), "per target") {
		t.Errorf("refusal is %v, want the aggregate message - each target was distinct so per-target should not have fired", refused)
	}
}

// TestRemoteRateLimitInternalCallsUncharged keeps core's own use of these
// helpers working: with no app in the thread there is no budget to charge and no
// untrusted caller to bound.
func TestRemoteRateLimitInternalCallsUncharged(t *testing.T) {
	remote_limit_reset()
	bare := &sl.Thread{}

	for i := 0; i < rate_limit_remote.limit+rate_limit_remote_entity.limit+10; i++ {
		if reason := remote_rate_limit(bare, "1victim"); reason != nil {
			t.Fatalf("an appless thread was refused at call %d (%v)", i+1, reason)
		}
	}
}

// TestRemoteRateLimitClearsRealUse pins the headroom claim rather than leaving it
// in a comment. A profile view is five fetches (information, avatar, banner,
// favicon, style); the budget must absorb a viewer reloading repeatedly.
func TestRemoteRateLimitClearsRealUse(t *testing.T) {
	remote_limit_reset()
	thread := remote_limit_thread("people")

	const fetches_per_view = 5
	const views = 20
	for i := 0; i < views; i++ {
		for j := 0; j < fetches_per_view; j++ {
			if reason := remote_rate_limit(thread, "1person"); reason != nil {
				t.Fatalf("refused during view %d of %d (%v); %d views of one profile is ordinary use, not abuse",
					i+1, views, reason, views)
			}
		}
	}
}

// TestRemoteRateLimitClearsUpdateSweep pins the constraint that actually sizes
// the per-target ceiling. apps.star's update check queries ONE publisher entity
// once per app in the catalogue, so a cold-cache sweep costs as many calls to a
// single target as there are apps. Exceeding the budget there does not degrade
// gracefully: Starlark has no try/except, so the builtin error aborts the action
// and the updates page 500s. The limit therefore has to clear a catalogue
// several times larger than today's 27 apps.
func TestRemoteRateLimitClearsUpdateSweep(t *testing.T) {
	// Well above today's catalogue, so growth does not quietly walk into the
	// ceiling. If this fails, the limit was lowered - raise it or batch the
	// sweep into one request per publisher instead.
	const catalogue = 400

	remote_limit_reset()
	thread := remote_limit_thread("apps")
	for i := 0; i < catalogue; i++ {
		if reason := remote_rate_limit(thread, "1publisher"); reason != nil {
			t.Fatalf("the update sweep was refused at app %d of %d (%v); a cold-cache sweep of a %d-app catalogue must complete, and a refusal here aborts the whole action rather than skipping one app",
				i+1, catalogue, reason, catalogue)
		}
	}
}
