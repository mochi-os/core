// Mochi server: what a push may do to a repository.
//
// go-git's receive-pack applies a reference update without comparing cmd.Old
// against the current tip and without checking that cmd.New names an object the
// repository holds, and it writes the pack object by object with no rollback.
// The three together meant a push could discard someone else's commits, leave a
// dangling reference, or be refused and still consume the owner's storage - each
// reported to the client as "ok".
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"crypto/sha1"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/storage/memory"
)

// push_hash returns a hash the repository will never hold, distinct per label.
func push_hash(label byte) plumbing.Hash {
	var hash plumbing.Hash
	for i := range hash {
		hash[i] = label
	}
	return hash
}

// push_pack is the canonical empty packfile: the signature, version 2, a zero
// object count and the trailing checksum over those twelve bytes. Decode hands
// the server a non-nil packfile reader whatever the client sent, and go-git
// fails the whole request on an empty one, so a push carrying no new objects
// still has to send this.
func push_pack(t *testing.T) []byte {
	t.Helper()
	header := []byte{'P', 'A', 'C', 'K', 0, 0, 0, 2, 0, 0, 0, 0}
	sum := sha1.Sum(header)
	return append(header, sum[:]...)
}

// push_blob builds a blob and a packfile carrying it, standing in for a client
// sending an object the repository does not hold yet. The empty pack exercises
// the reference half alone; anything about storing objects needs a real one.
func push_blob(t *testing.T, body []byte) (plumbing.Hash, []byte) {
	t.Helper()
	store := memory.NewStorage()
	object := store.NewEncodedObject()
	object.SetType(plumbing.BlobObject)
	writer, err := object.Writer()
	if err != nil {
		t.Fatalf("object writer: %v", err)
	}
	writer.Write(body)
	writer.Close()
	hash, err := store.SetEncodedObject(object)
	if err != nil {
		t.Fatalf("store object: %v", err)
	}
	var pack bytes.Buffer
	if _, err := packfile.NewEncoder(&pack, store, false).Encode([]plumbing.Hash{hash}, 0); err != nil {
		t.Fatalf("encode pack: %v", err)
	}
	return hash, pack.Bytes()
}

// push_send drives git_receive_pack the way the HTTP route does and answers the
// per-reference statuses the client would read.
func push_send(t *testing.T, user *User, repo string, commands ...*packp.Command) map[string]string {
	t.Helper()
	return push_send_pack(t, user, repo, push_pack(t), commands...)
}

// push_send_pack is push_send with a caller-supplied packfile.
func push_send_pack(t *testing.T, user *User, repo string, pack []byte, commands ...*packp.Command) map[string]string {
	t.Helper()
	request := packp.NewReferenceUpdateRequest()
	request.Capabilities.Set(capability.ReportStatus)
	request.Commands = commands
	request.Packfile = io.NopCloser(bytes.NewReader(pack))
	var body bytes.Buffer
	if err := request.Encode(&body); err != nil {
		t.Fatalf("encode push: %v", err)
	}

	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest("POST", "/", nil)
	git_receive_pack(context, git_repo_path(user, test_app, repo), io.NopCloser(&body), user, 1<<20)

	statuses := map[string]string{}
	report := packp.NewReportStatus()
	if err := report.Decode(recorder.Body); err != nil {
		t.Fatalf("decode report (body %q): %v", recorder.Body.String(), err)
	}
	for _, status := range report.CommandStatuses {
		statuses[status.ReferenceName.String()] = status.Status
	}
	return statuses
}

// push_store opens the repository fresh, so a read sees what is on disk rather
// than a view cached before the push.
func push_store(t *testing.T, user *User, repo string) storer.Storer {
	t.Helper()
	store, err := (&git_loader{}).Load(&transport.Endpoint{Path: git_repo_path(user, test_app, repo)})
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	return store
}

// push_object writes an object straight into the repository, standing in for one
// an earlier push landed.
func push_object(t *testing.T, user *User, repo string, body []byte) plumbing.Hash {
	t.Helper()
	store := push_store(t, user, repo)
	object := store.NewEncodedObject()
	object.SetType(plumbing.BlobObject)
	writer, err := object.Writer()
	if err != nil {
		t.Fatalf("object writer: %v", err)
	}
	writer.Write(body)
	writer.Close()
	hash, err := store.SetEncodedObject(object)
	if err != nil {
		t.Fatalf("store object: %v", err)
	}
	return hash
}

// TestPushRefusesAStaleOldHash. Two people pushing to one branch both landed,
// and the second tip won although it did not contain the first one's commits -
// which became unreachable with nobody told. Real git refuses this.
func TestPushRefusesAStaleOldHash(t *testing.T) {
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "stale"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	main := plumbing.NewBranchReferenceName("main")
	first := push_object(t, user, "stale", []byte("first"))
	second := push_object(t, user, "stale", []byte("second"))
	third := push_object(t, user, "stale", []byte("third"))

	if status := push_send(t, user, "stale", &packp.Command{Name: main, Old: plumbing.ZeroHash, New: first}); status[main.String()] != "ok" {
		t.Fatalf("creating the branch was refused: %q", status[main.String()])
	}
	if status := push_send(t, user, "stale", &packp.Command{Name: main, Old: first, New: second}); status[main.String()] != "ok" {
		t.Fatalf("a well-formed update was refused: %q", status[main.String()])
	}

	// Computed against the tip as it was two pushes ago.
	status := push_send(t, user, "stale", &packp.Command{Name: main, Old: first, New: third})
	if status[main.String()] != "stale info" {
		t.Errorf("a push against a stale tip was answered %q, want a refusal", status[main.String()])
	}
	reference, err := push_store(t, user, "stale").Reference(main)
	if err != nil {
		t.Fatalf("read main: %v", err)
	}
	if reference.Hash() != second {
		t.Errorf("main is at %s, want the tip the accepted push set - the refused one moved it", reference.Hash())
	}
}

// TestPushRefusesAReferenceWithNoObject. A command line with no pack, or a pack
// missing that object, left a reference nothing could resolve: every later fetch
// answered 500 and info/refs kept advertising it.
func TestPushRefusesAReferenceWithNoObject(t *testing.T) {
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "dangling"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	main := plumbing.NewBranchReferenceName("main")
	absent := push_hash(0x11)

	status := push_send(t, user, "dangling", &packp.Command{Name: main, Old: plumbing.ZeroHash, New: absent})
	if status[main.String()] != "missing necessary objects" {
		t.Errorf("a create pointing at an absent object was answered %q, want a refusal", status[main.String()])
	}
	if _, err := push_store(t, user, "dangling").Reference(main); err == nil {
		t.Error("the reference was created even though the repository does not hold its object")
	}

	// The same on an update, where the branch already exists.
	present := push_object(t, user, "dangling", []byte("real"))
	push_send(t, user, "dangling", &packp.Command{Name: main, Old: plumbing.ZeroHash, New: present})
	status = push_send(t, user, "dangling", &packp.Command{Name: main, Old: present, New: absent})
	if status[main.String()] != "missing necessary objects" {
		t.Errorf("an update pointing at an absent object was answered %q, want a refusal", status[main.String()])
	}
}

// TestPushDeleteIsBoundToTheTipItSaw. RemoveReference took no old value at all,
// so a delete computed before someone else's push still removed the branch they
// had just moved.
func TestPushDeleteIsBoundToTheTipItSaw(t *testing.T) {
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "delete"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	topic := plumbing.NewBranchReferenceName("topic")
	first := push_object(t, user, "delete", []byte("first"))
	second := push_object(t, user, "delete", []byte("second"))

	push_send(t, user, "delete", &packp.Command{Name: topic, Old: plumbing.ZeroHash, New: first})
	push_send(t, user, "delete", &packp.Command{Name: topic, Old: first, New: second})

	status := push_send(t, user, "delete", &packp.Command{Name: topic, Old: first, New: plumbing.ZeroHash})
	if status[topic.String()] != "stale info" {
		t.Errorf("a delete against a stale tip was answered %q, want a refusal", status[topic.String()])
	}
	if _, err := push_store(t, user, "delete").Reference(topic); err != nil {
		t.Error("the branch was deleted by a command computed against a tip it no longer had")
	}
}

// TestRefusedPushStoresNothing. The meter refuses the object that would take the
// account over quota, but every object before it in the pack had already been
// written, and nothing in core reclaims them: no gc, no prune, no affordance in
// the product. A refused push permanently consumed the owner's storage.
func TestRefusedPushStoresNothing(t *testing.T) {
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "refused"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	path := git_repo_path(user, test_app, "refused")
	before, err := git_size(user, test_app, "refused")
	if err != nil {
		t.Fatalf("git_size: %v", err)
	}

	// A budget that fits two objects but not three, driven through the wrapper
	// exactly as a real over-quota push is.
	staging, err := git_quarantine(path)
	if err != nil {
		t.Fatalf("git_quarantine: %v", err)
	}
	store, err := (&git_loader{budget: 3 * 4096, staging: staging}).Load(&transport.Endpoint{Path: path})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	accepted := 0
	for i := 0; i < 4; i++ {
		body := make([]byte, 4096)
		body[0] = byte(i)
		object := store.NewEncodedObject()
		object.SetType(plumbing.BlobObject)
		writer, _ := object.Writer()
		writer.Write(body)
		writer.Close()
		if _, err := store.SetEncodedObject(object); err != nil {
			break
		}
		accepted++
	}
	if accepted == 0 || accepted == 4 {
		t.Fatalf("the meter accepted %d of 4 objects; the test needs a partial pack to measure", accepted)
	}

	// The push failed, so the staging directory is discarded rather than promoted.
	os.RemoveAll(staging)

	after, err := git_size(user, test_app, "refused")
	if err != nil {
		t.Fatalf("git_size: %v", err)
	}
	if after != before {
		t.Errorf("a refused push left %d bytes in the repository (%d -> %d); it must store nothing",
			after-before, before, after)
	}
}

// TestAcceptedPushPromotesAndCleansUp. The quarantine is only correct if a
// successful push still lands its objects and leaves no staging directory behind
// to consume quota.
func TestAcceptedPushPromotesAndCleansUp(t *testing.T) {
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "accepted"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	path := git_repo_path(user, test_app, "accepted")
	main := plumbing.NewBranchReferenceName("main")

	// The object arrives in the pack, so it is written to the quarantine and
	// only reaches the repository if the promotion runs.
	hash, pack := push_blob(t, []byte("promoted"))
	if status := push_send_pack(t, user, "accepted", pack, &packp.Command{Name: main, Old: plumbing.ZeroHash, New: hash}); status[main.String()] != "ok" {
		t.Fatalf("a well-formed push was refused: %q", status[main.String()])
	}
	if err := push_store(t, user, "accepted").HasEncodedObject(hash); err != nil {
		t.Errorf("the pushed object is not in the repository: %v", err)
	}
	leftover, _ := filepath.Glob(filepath.Join(path, "incoming-*"))
	if len(leftover) != 0 {
		t.Errorf("the push left %d staging directories behind: %v", len(leftover), leftover)
	}
}

// TestQuarantineSweepsAbandonedStaging. Only a killed process leaves a staging
// directory; without the sweep it would consume the owner's storage forever,
// which is the thing quarantine exists to prevent.
func TestQuarantineSweepsAbandonedStaging(t *testing.T) {
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "sweep"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	path := git_repo_path(user, test_app, "sweep")

	abandoned := filepath.Join(path, "incoming-abandoned")
	if err := os.MkdirAll(filepath.Join(abandoned, "objects"), 0755); err != nil {
		t.Fatalf("create abandoned staging: %v", err)
	}
	stale := time.Now().Add(-2 * git_quarantine_maximum)
	if err := os.Chtimes(abandoned, stale, stale); err != nil {
		t.Fatalf("age abandoned staging: %v", err)
	}

	fresh, err := git_quarantine(path)
	if err != nil {
		t.Fatalf("git_quarantine: %v", err)
	}
	defer os.RemoveAll(fresh)

	if _, err := os.Stat(abandoned); err == nil {
		t.Error("a staging directory older than the maximum was left in place")
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("the new staging directory was not created: %v", err)
	}
}

// TestPushLockIsPerOwner. The budget is measured once per request from the
// on-disk total, so pushes running together were each handed the whole remaining
// quota. Serialising them per owner makes the second measure what the first
// stored; two owners must still push at the same time.
func TestPushLockIsPerOwner(t *testing.T) {
	if git_push_lock("alice") != git_push_lock("alice") {
		t.Error("one owner's pushes take different locks, so they do not serialise")
	}
	// Distinct owners share a stripe only by collision. Over a spread of names
	// the lock must not be a single global one.
	shared := 0
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		if git_push_lock(name) == git_push_lock("alice") {
			shared++
		}
	}
	if shared == 8 {
		t.Error("every owner takes the same lock; a push would block every other account's")
	}
}

// TestGitBudgetMeasuredOnlyWhereItIsSpent. Measuring the budget walks the
// owner's whole storage directory, and git_request_maximum reads it for
// receive-pack alone: every other service, including the anonymous fetches the
// public git route admits, was paying for an answer that was then discarded.
// The two must agree, so a service that starts spending the budget is not left
// without one.
func TestGitBudgetMeasuredOnlyWhereItIsSpent(t *testing.T) {
	for _, service := range []string{"git-upload-pack", "info/refs", ""} {
		if git_budget_needed(service) {
			t.Errorf("%q measures the owner's storage, which is a directory walk", service)
		}
		if git_request_maximum(service, 1) != git_request_maximum(service, 1<<40) {
			t.Errorf("%q's body limit varies with the budget, so it must be measured after all", service)
		}
	}
	if !git_budget_needed("git-receive-pack") {
		t.Error("receive-pack does not measure the budget, so a push would be unmetered")
	}
	if git_request_maximum("git-receive-pack", 1) == git_request_maximum("git-receive-pack", 1<<40) {
		t.Error("receive-pack's body limit ignores the budget; the measurement would be pointless")
	}
}

// TestGitAuthenticateRefusesInactiveAccounts. web_action blocks an app request
// from an account that is closing or mid-restore, but both gates read the user
// it resolved from a cookie or a Bearer token. Basic auth produces neither, so
// a git client reached the repository with those gates unevaluated and
// user_by_uid filters only "suspended".
func TestGitAuthenticateRefusesInactiveAccounts(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)
	db_create()

	users := db_open("db/users.db")
	for _, account := range []struct{ uid, status string }{
		{"git-active", "active"},
		{"git-closing", "closing"},
		{"git-restore", "pending-restore"},
	} {
		users.exec("insert into users (uid, username, role, status) values (?, ?, 'user', ?)",
			account.uid, account.uid+"@example.com", account.status)
		users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, ?, 'person', ?)",
			"e-"+account.uid, fingerprint("e-"+account.uid), account.uid, account.uid)
	}

	app := create_external_app("repositories")
	authenticate := func(uid string) *User {
		token := token_create(uid, app.id, "git", []string{"git"}, 0, "", "")
		recorder := httptest.NewRecorder()
		context, _ := gin.CreateTestContext(recorder)
		context.Request = httptest.NewRequest("POST", "/", nil)
		context.Request.SetBasicAuth(uid, token)
		return git_authenticate(context, app)
	}

	if user := authenticate("git-active"); user == nil {
		t.Fatal("an active account was refused, so the test below proves nothing")
	}
	if user := authenticate("git-closing"); user != nil {
		t.Error("an account pending closure can still clone and push through its API token")
	}
	if user := authenticate("git-restore"); user != nil {
		t.Error("an account mid-restore can still clone and push through its API token")
	}
}
