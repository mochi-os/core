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
	"crypto/rand"
	"crypto/sha1"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/storage/filesystem"
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
// object count and the trailing checksum over those twelve bytes. A real client
// sends one of these whenever a push has anything but deletions in it, however
// few objects the remote turns out to need. It sends nothing whatsoever when
// every command is a delete - see TestPushDeleteCarriesNoPack, which is the
// shape this helper cannot express.
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
	statuses := map[string]string{}
	for _, status := range push_report(t, user, repo, pack, commands...).CommandStatuses {
		statuses[status.ReferenceName.String()] = status.Status
	}
	return statuses
}

// push_report is push_send_pack answering the whole report the client reads,
// with an account that may store 1MB more.
func push_report(t *testing.T, user *User, repo string, pack []byte, commands ...*packp.Command) *packp.ReportStatus {
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

	report := packp.NewReportStatus()
	if err := report.Decode(recorder.Body); err != nil {
		t.Fatalf("decode report (body %q): %v", recorder.Body.String(), err)
	}
	return report
}

// push_default seeds the branch HEAD names, so a test working on some other
// branch is not working on the repository's default one - which a push may not
// delete. Without it git_head_settle points HEAD at whatever branch arrived
// first, and every delete in this file would be a delete of the default branch.
func push_default(t *testing.T, user *User, repo string) {
	t.Helper()
	main := plumbing.NewBranchReferenceName("main")
	hash := push_object(t, user, repo, []byte("default branch"))
	push_send(t, user, repo, &packp.Command{Name: main, Old: plumbing.ZeroHash, New: hash})
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
	push_default(t, user, "delete")
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

// TestPushDeleteCarriesNoPack. A delete-only push sends the command list, its
// flush packet, and stops - measured against git 2.53.0, which puts 192 bytes
// of packfile after the flush for every other shape of push and zero for this
// one. go-git points Packfile at the rest of the body whatever is left in it,
// so it was non-nil at end of input and the scanner answered "empty packfile",
// failing the push before any reference moved. No branch or tag could be
// deleted over git, on a server whose own advertisement offers delete-refs.
//
// Every other push test here goes through push_send, which always attaches an
// empty-but-well-formed pack, and the scanner accepts that - which is why this
// stayed hidden.
func TestPushDeleteCarriesNoPack(t *testing.T) {
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "nopack"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	push_default(t, user, "nopack")
	topic := plumbing.NewBranchReferenceName("topic")
	tip := push_object(t, user, "nopack", []byte("tip"))
	push_send(t, user, "nopack", &packp.Command{Name: topic, Old: plumbing.ZeroHash, New: tip})

	status := push_send_pack(t, user, "nopack", nil, &packp.Command{Name: topic, Old: tip, New: plumbing.ZeroHash})
	if status[topic.String()] != "ok" {
		t.Errorf("a delete sent the way a real client sends it was answered %q, want ok", status[topic.String()])
	}
	if _, err := push_store(t, user, "nopack").Reference(topic); err == nil {
		t.Error("the branch is still there after a delete the client was told succeeded")
	}
}

// TestPushDeleteBesideAnUpdateStillReadsThePack. git asks for pack data the
// moment one command is not a deletion, so a delete travelling with an update
// arrives with a pack and must still be read. Skipping it on the wrong signal -
// any delete present, rather than nothing but deletes - would drop the objects
// the update needs and leave a dangling reference.
func TestPushDeleteBesideAnUpdateStillReadsThePack(t *testing.T) {
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "mixed"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	push_default(t, user, "mixed")
	doomed := plumbing.NewBranchReferenceName("doomed")
	kept := plumbing.NewBranchReferenceName("kept")
	tip := push_object(t, user, "mixed", []byte("tip"))
	push_send(t, user, "mixed", &packp.Command{Name: doomed, Old: plumbing.ZeroHash, New: tip})

	// The blob exists only in the pack, so the update can only be answered "ok"
	// if the pack was read.
	hash, pack := push_blob(t, []byte("carried by the pack"))
	status := push_send_pack(t, user, "mixed", pack,
		&packp.Command{Name: doomed, Old: tip, New: plumbing.ZeroHash},
		&packp.Command{Name: kept, Old: plumbing.ZeroHash, New: hash})
	if status[doomed.String()] != "ok" {
		t.Errorf("the delete half was answered %q, want ok", status[doomed.String()])
	}
	if status[kept.String()] != "ok" {
		t.Errorf("the update half was answered %q, want ok - the pack was not read", status[kept.String()])
	}
	store := push_store(t, user, "mixed")
	if _, err := store.Reference(doomed); err == nil {
		t.Error("the deleted branch is still there")
	}
	if store.HasEncodedObject(hash) != nil {
		t.Error("the object the pack carried was not stored")
	}
}

// TestPushMayNotDeleteTheDefaultBranch. Removing the branch HEAD names leaves a
// repository that clones to nothing, and only another push puts it back. The
// repositories app already refuses it; once a delete-only push worked at all,
// git was the way around that rule.
func TestPushMayNotDeleteTheDefaultBranch(t *testing.T) {
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "default"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	main := plumbing.NewBranchReferenceName("main")
	tip := push_object(t, user, "default", []byte("tip"))
	push_send(t, user, "default", &packp.Command{Name: main, Old: plumbing.ZeroHash, New: tip})

	status := push_send_pack(t, user, "default", nil, &packp.Command{Name: main, Old: tip, New: plumbing.ZeroHash})
	if status[main.String()] != "deletion of the current branch prohibited" {
		t.Errorf("deleting the default branch was answered %q, want a refusal", status[main.String()])
	}
	if _, err := push_store(t, user, "default").Reference(main); err != nil {
		t.Error("the default branch was deleted")
	}
}

// TestPushMayDeleteABranchTheOwnerMovedHeadOffOf. The refusal follows HEAD, not
// the name "main": an owner who makes another branch the default must be able to
// delete the one they moved away from.
func TestPushMayDeleteABranchTheOwnerMovedHeadOffOf(t *testing.T) {
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "moved"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	main := plumbing.NewBranchReferenceName("main")
	release := plumbing.NewBranchReferenceName("release")
	tip := push_object(t, user, "moved", []byte("tip"))
	push_send(t, user, "moved", &packp.Command{Name: main, Old: plumbing.ZeroHash, New: tip})
	push_send(t, user, "moved", &packp.Command{Name: release, Old: plumbing.ZeroHash, New: tip})

	// What mochi.git.branch.default.set does: point HEAD at the other branch.
	store := push_store(t, user, "moved")
	if err := store.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, release)); err != nil {
		t.Fatalf("move HEAD: %v", err)
	}

	status := push_send_pack(t, user, "moved", nil, &packp.Command{Name: main, Old: tip, New: plumbing.ZeroHash})
	if status[main.String()] != "ok" {
		t.Errorf("deleting a branch that is no longer the default was answered %q, want ok", status[main.String()])
	}
	if _, err := push_store(t, user, "moved").Reference(main); err == nil {
		t.Error("the branch is still there after a delete the client was told succeeded")
	}
	status = push_send_pack(t, user, "moved", nil, &packp.Command{Name: release, Old: tip, New: plumbing.ZeroHash})
	if status[release.String()] != "deletion of the current branch prohibited" {
		t.Errorf("deleting the new default branch was answered %q, want a refusal", status[release.String()])
	}
}

// TestPushWithNoCommandsIsNotReadAsADelete. An empty command list must not look
// like a delete-only push: there is nothing to delete, the client sent a pack
// like any other push, and treating it as deletes would discard it unread.
func TestPushWithNoCommandsIsNotReadAsADelete(t *testing.T) {
	if git_deletes_only(nil) {
		t.Error("a push carrying no commands was read as a delete-only push")
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

// TestPushOverQuotaIsReported. A push the account's storage cannot take is
// refused as an unpack error, which the client shows the pusher, and moves no
// reference and stores nothing.
func TestPushOverQuotaIsReported(t *testing.T) {
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "full"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	path := git_repo_path(user, test_app, "full")
	main := plumbing.NewBranchReferenceName("main")

	hash, pack := push_blob(t, make([]byte, 2<<20))
	report := push_report(t, user, "full", pack, &packp.Command{Name: main, Old: plumbing.ZeroHash, New: hash})
	if !strings.Contains(report.UnpackStatus, "exceeds the storage available") {
		t.Errorf("THE POINT: a 2MB push against 1MB of storage was reported as %q", report.UnpackStatus)
	}
	store := push_store(t, user, "full")
	if _, err := store.Reference(main); err == nil {
		t.Error("the refused push created its branch")
	}
	if err := store.HasEncodedObject(hash); err == nil {
		t.Error("the refused push stored its object")
	}
	if leftover, _ := filepath.Glob(filepath.Join(path, "incoming-*")); len(leftover) != 0 {
		t.Errorf("the push left staging directories behind: %v", leftover)
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

// push_tag stores an annotated tag on a fresh blob and answers the tag object's
// hash and the blob's, which is what packed-refs records as the tag's peeled value.
func push_tag(t *testing.T, user *User, repo, name string) (plumbing.Hash, plumbing.Hash) {
	t.Helper()
	target := push_object(t, user, repo, []byte("tagged "+name))
	tag := &object.Tag{Name: name, Tagger: object.Signature{Name: "Tagger", Email: "tagger@example.com", When: time.Unix(1700000000, 0)},
		Message: name, TargetType: plumbing.BlobObject, Target: target}
	store := push_store(t, user, repo)
	encoded := store.NewEncodedObject()
	if err := tag.Encode(encoded); err != nil {
		t.Fatalf("encode tag: %v", err)
	}
	hash, err := store.SetEncodedObject(encoded)
	if err != nil {
		t.Fatalf("store tag: %v", err)
	}
	return hash, target
}

// TestPushDeletingAPackedTagLeavesPackedRefsWhole. go-git v5 removed a packed
// annotated tag's own line and kept the "^<peeled>" line beneath it, a line that
// then belongs to no reference; git refuses such a file, and so the repository.
func TestPushDeletingAPackedTagLeavesPackedRefsWhole(t *testing.T) {
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "tags"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	push_default(t, user, "tags")
	first, first_target := push_tag(t, user, "tags", "1.0")
	second, second_target := push_tag(t, user, "tags", "1.1")
	path := git_repo_path(user, test_app, "tags")
	header := "# pack-refs with: peeled fully-peeled sorted \n"
	packed := header +
		first.String() + " refs/tags/1.0\n^" + first_target.String() + "\n" +
		second.String() + " refs/tags/1.1\n^" + second_target.String() + "\n"
	if err := os.WriteFile(filepath.Join(path, "packed-refs"), []byte(packed), 0600); err != nil {
		t.Fatalf("write packed-refs: %v", err)
	}

	name := plumbing.NewTagReferenceName("1.0")
	status := push_send_pack(t, user, "tags", nil, &packp.Command{Name: name, Old: first, New: plumbing.ZeroHash})
	if status[name.String()] != "ok" {
		t.Fatalf("deleting the tag was answered %q, want ok", status[name.String()])
	}

	data, err := os.ReadFile(filepath.Join(path, "packed-refs"))
	if err != nil {
		t.Fatalf("read packed-refs: %v", err)
	}
	want := header + second.String() + " refs/tags/1.1\n^" + second_target.String() + "\n"
	if string(data) != want {
		t.Errorf("packed-refs after the delete:\n%s\nwant:\n%s", data, want)
	}
	if _, err := push_store(t, user, "tags").Reference(plumbing.NewTagReferenceName("1.1")); err != nil {
		t.Errorf("the other tag is gone after deleting one: %v", err)
	}
	if git, err := exec.LookPath("git"); err == nil {
		if output, err := exec.Command(git, "--git-dir", path, "for-each-ref").CombinedOutput(); err != nil {
			t.Errorf("git cannot read the repository after the delete: %v\n%s", err, output)
		}
	}
}

// TestGitPackedTidyKeepsEachReferencesOwnPeel. A peeled line survives only
// directly under a reference; one under the header, under another peeled line
// or under nothing at all is dropped, and a file with nothing to drop is not
// rewritten.
func TestGitPackedTidyKeepsEachReferencesOwnPeel(t *testing.T) {
	directory := t.TempDir()
	file := filepath.Join(directory, "packed-refs")
	a, b, c := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)
	broken := "# pack-refs with: peeled fully-peeled sorted \n^" + c + "\n" +
		a + " refs/tags/1.0\n^" + b + "\n^" + c + "\n" +
		b + " refs/heads/main\n^" + a + "\n" +
		c + " refs/tags/2.0\n^" + a + "\n"
	if err := os.WriteFile(file, []byte(broken), 0600); err != nil {
		t.Fatal(err)
	}
	if err := git_packed_tidy(directory); err != nil {
		t.Fatalf("git_packed_tidy: %v", err)
	}
	data, _ := os.ReadFile(file)
	want := "# pack-refs with: peeled fully-peeled sorted \n" +
		a + " refs/tags/1.0\n^" + b + "\n" +
		b + " refs/heads/main\n^" + a + "\n" +
		c + " refs/tags/2.0\n^" + a + "\n"
	if string(data) != want {
		t.Errorf("tidied packed-refs:\n%s\nwant:\n%s", data, want)
	}
	information, _ := os.Stat(file)
	if information.Mode().Perm() != 0600 {
		t.Errorf("mode after tidy is %o, want 600", information.Mode().Perm())
	}

	before := information.ModTime()
	time.Sleep(10 * time.Millisecond)
	if err := git_packed_tidy(directory); err != nil {
		t.Fatalf("git_packed_tidy on a whole file: %v", err)
	}
	if after, _ := os.Stat(file); !after.ModTime().Equal(before) {
		t.Error("a file with nothing to drop was rewritten")
	}
	if err := git_packed_tidy(t.TempDir()); err != nil {
		t.Errorf("a repository with no packed-refs: %v", err)
	}
}

// TestGitPackedTidyWaitsForTheLock. go-git rewrites packed-refs holding a lock
// on it; a tidy that ignored the lock could replace that rewrite with a copy
// read before it, bringing a removed reference back.
func TestGitPackedTidyWaitsForTheLock(t *testing.T) {
	directory := t.TempDir()
	file := filepath.Join(directory, "packed-refs")
	a := strings.Repeat("a", 40)
	if err := os.WriteFile(file, []byte(a+" refs/tags/1.0\n^"+a+"\n^"+a+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	held, err := osfs.New(directory).OpenFile("packed-refs", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if err := held.Lock(); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- git_packed_tidy(directory) }()
	select {
	case err := <-done:
		t.Fatalf("the tidy ran while another writer held the packed-refs lock (err %v)", err)
	case <-time.After(200 * time.Millisecond):
	}
	held.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("git_packed_tidy: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the tidy did not finish once the lock was released")
	}
	if data, _ := os.ReadFile(file); string(data) != a+" refs/tags/1.0\n^"+a+"\n" {
		t.Errorf("packed-refs after the tidy: %q", data)
	}
}

// push_noise answers bytes that do not compress, so a pack of them is as large
// as they are.
func push_noise(t *testing.T, size int) []byte {
	t.Helper()
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	return data
}

// push_route sends a push of one new blob on a new branch through the
// receive-pack route, the budget measured as the route measures it, and
// answers the branch's status, or the route's refusal.
func push_route(t *testing.T, user *User, repo string, branch string, body []byte) string {
	t.Helper()
	hash, pack := push_blob(t, body)
	name := plumbing.NewBranchReferenceName(branch)
	request := packp.NewReferenceUpdateRequest()
	request.Capabilities.Set(capability.ReportStatus)
	request.Commands = []*packp.Command{{Name: name, Old: plumbing.ZeroHash, New: hash}}
	request.Packfile = io.NopCloser(bytes.NewReader(pack))
	var encoded bytes.Buffer
	if err := request.Encode(&encoded); err != nil {
		t.Fatalf("encode push: %v", err)
	}
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest("POST", "/git-receive-pack", &encoded)
	git_service_rpc(context, git_repo_path(user, test_app, repo), "git-receive-pack", user)
	report := packp.NewReportStatus()
	if err := report.Decode(bytes.NewReader(recorder.Body.Bytes())); err != nil {
		return "refused: " + recorder.Body.String()
	}
	for _, status := range report.CommandStatuses {
		if status.ReferenceName == name {
			return status.Status
		}
	}
	return "refused: " + report.UnpackStatus
}

// push_window sets an account up with room for one push of size bytes and
// not two, and answers its user.
func push_window(t *testing.T, repo string, size int) *User {
	t.Helper()
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, repo); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	used, err := dir_size(user_storage_dir(user))
	if err != nil {
		t.Fatal(err)
	}
	previous := file_maximum_storage
	file_maximum_storage = used + int64(size)*3/2
	t.Cleanup(func() { file_maximum_storage = previous })
	return user
}

// TestPushSeesTheEarlierPushInItsWindow. The owner's storage is walked at
// most once a minute, and a push measures its room from that figure. Before
// the figure, each push walked the storage and saw what the last one stored;
// the figure must carry it, or every push inside the minute is handed the
// same room and an account fills its quota once per push.
func TestPushSeesTheEarlierPushInItsWindow(t *testing.T) {
	size := 64 << 10
	user := push_window(t, "window", size)
	if status := push_route(t, user, "window", "first", push_noise(t, size)); status != "ok" {
		t.Fatalf("setup: the first push, which fits, was answered %q", status)
	}
	if status := push_route(t, user, "window", "second", push_noise(t, size)); status == "ok" {
		t.Error("THE POINT: a second push inside the window passed on the room the first had filled")
	}
	if used, _ := dir_size(user_storage_dir(user)); used > file_maximum_storage {
		t.Errorf("THE POINT: the account stores %d bytes, past its quota of %d", used, file_maximum_storage)
	}
}

// TestPushWaitingBehindAnotherSeesItsBytes. A push's room is measured before
// it takes the owner's push lock, so a push that waited there behind another
// carries a measurement from before the other stored. It measures again
// under the lock.
func TestPushWaitingBehindAnotherSeesItsBytes(t *testing.T) {
	size := 64 << 10
	user := push_window(t, "behind", size)
	if status := push_route(t, user, "behind", "first", push_noise(t, size)); status != "ok" {
		t.Fatalf("setup: the first push, which fits, was answered %q", status)
	}
	// push_report hands the route a budget of a megabyte: the room measured
	// before the first push stored.
	hash, pack := push_blob(t, push_noise(t, size))
	name := plumbing.NewBranchReferenceName("second")
	report := push_report(t, user, "behind", pack, &packp.Command{Name: name, Old: plumbing.ZeroHash, New: hash})
	for _, status := range report.CommandStatuses {
		if status.ReferenceName == name && status.Status == "ok" {
			t.Error("THE POINT: a push measured before the lock passed on room the push ahead of it had filled")
		}
	}
}

// TestPushStoredCountsInTheFigure. What a push stores - a pack kept as it
// came, replacing an index a stopped push left, or objects promoted one by
// one - counts in the storage figure as a walk would count it.
func TestPushStoredCountsInTheFigure(t *testing.T) {
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "counted"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	path := git_repo_path(user, test_app, "counted")
	compare := func(what string) {
		t.Helper()
		walked, err := dir_size(user_storage_dir(user))
		if err != nil {
			t.Fatal(err)
		}
		found, _ := storage_figures.Load(user.UID)
		figure := found.(*storage_figure)
		figure.lock.Lock()
		used := figure.used
		figure.lock.Unlock()
		if used != walked {
			t.Errorf("THE POINT: after %s the figure is %d bytes and a walk finds %d", what, used, walked)
		}
	}

	// A kept pack, over the index a push of the same pack left when it stopped.
	staging := t.TempDir()
	_, pack := push_blob(t, push_noise(t, 20000))
	index, err := git_receive(bytes.NewReader(pack), staging, path, 0)
	if err != nil || index == nil {
		t.Fatalf("setup: git_receive kept nothing: %v", err)
	}
	stale := filepath.Join(path, "objects", "pack", "pack-"+plumbing.Hash(index.PackfileChecksum).String()+".idx")
	if err := os.MkdirAll(filepath.Dir(stale), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stale, make([]byte, 3000), 0444); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-time.Minute)
	if err := os.Chtimes(stale, past, past); err != nil {
		t.Fatal(err)
	}
	if _, err := user_storage_remaining(user); err != nil {
		t.Fatal(err)
	}
	if err := git_store(staging, path, index); err != nil {
		t.Fatalf("git_store: %v", err)
	}
	compare("a kept pack")

	// Objects promoted one by one.
	staging = t.TempDir()
	loose := filesystem.NewStorage(osfs.New(staging), cache.NewObjectLRUDefault())
	for i := range 3 {
		store_blob(t, loose, push_noise(t, 5000+i))
	}
	if err := git_store(staging, path, nil); err != nil {
		t.Fatalf("git_store: %v", err)
	}
	compare("promoted objects")
}

// push_walking reads a push's body and, once most of it has been read and
// staged, runs walk: a walk of the owner's storage made by another request
// while the push is under way.
type push_walking struct {
	reader io.Reader
	at     int
	read   int
	walk   func()
}

func (p *push_walking) Read(data []byte) (int, error) {
	if p.walk != nil && p.read >= p.at {
		p.walk()
		p.walk = nil
	}
	n, err := p.reader.Read(data)
	p.read += n
	return n, err
}

// TestPushStagedDuringAWalkCountedOnce. A push is staged beside the
// repository, inside the owner's storage, where a walk made while it runs
// counts it; the pack is counted again when it lands. The figure must not keep
// both: the owner would be refused room they have until the figure expired.
func TestPushStagedDuringAWalkCountedOnce(t *testing.T) {
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "staged"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	if _, err := user_storage_remaining(user); err != nil {
		t.Fatal(err)
	}
	hash, pack := push_blob(t, push_noise(t, 256<<10))
	name := plumbing.NewBranchReferenceName("staged")
	request := packp.NewReferenceUpdateRequest()
	request.Capabilities.Set(capability.ReportStatus)
	request.Commands = []*packp.Command{{Name: name, Old: plumbing.ZeroHash, New: hash}}
	request.Packfile = io.NopCloser(bytes.NewReader(pack))
	var encoded bytes.Buffer
	if err := request.Encode(&encoded); err != nil {
		t.Fatalf("encode push: %v", err)
	}
	walked := false
	body := &push_walking{reader: &encoded, at: encoded.Len() * 3 / 4, walk: func() {
		found, _ := storage_figures.Load(user.UID)
		figure := found.(*storage_figure)
		figure.lock.Lock()
		figure.measured = figure.measured.Add(-storage_lifetime)
		figure.lock.Unlock()
		if _, err := user_storage_remaining(user); err != nil {
			t.Error(err)
		}
		walked = true
	}}
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest("POST", "/git-receive-pack", body)
	git_service_rpc(context, git_repo_path(user, test_app, "staged"), "git-receive-pack", user)
	report := packp.NewReportStatus()
	if err := report.Decode(bytes.NewReader(recorder.Body.Bytes())); err != nil || len(report.CommandStatuses) != 1 || report.CommandStatuses[0].Status != "ok" {
		t.Fatalf("setup: the push was not stored: %v %q", err, recorder.Body.String())
	}
	if !walked {
		t.Fatal("setup: no walk ran while the push was staged")
	}

	if _, err := user_storage_remaining(user); err != nil {
		t.Fatal(err)
	}
	walk, err := dir_size(user_storage_dir(user))
	if err != nil {
		t.Fatal(err)
	}
	found, _ := storage_figures.Load(user.UID)
	figure := found.(*storage_figure)
	figure.lock.Lock()
	used := figure.used
	figure.lock.Unlock()
	if used != walk {
		t.Errorf("THE POINT: after a push a walk ran beside, the figure is %d bytes and a walk finds %d", used, walk)
	}
}

// push_encode stores obj in store and answers its hash.
func push_encode(t *testing.T, store *memory.Storage, obj interface {
	Encode(plumbing.EncodedObject) error
}) plumbing.Hash {
	t.Helper()
	encoded := store.NewEncodedObject()
	if err := obj.Encode(encoded); err != nil {
		t.Fatalf("encode: %v", err)
	}
	hash, err := store.SetEncodedObject(encoded)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	return hash
}

// TestPushRefusesAPackMissingAnObject. A push was checked only for its new
// tip, so a pack carrying a commit and its tree but not a blob the tree names
// was answered ok: every later clone failed on the gap, and so did every
// repack. What each new tip reaches must be held before it is stored or a
// reference moves onto it, as git's receive-pack checks - on every path a
// push is stored by, and for each command on its own.
func TestPushRefusesAPackMissingAnObject(t *testing.T) {
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "gap"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	store := memory.NewStorage()
	blob := func(body string) plumbing.Hash {
		encoded := store.NewEncodedObject()
		encoded.SetType(plumbing.BlobObject)
		writer, _ := encoded.Writer()
		writer.Write([]byte(body))
		writer.Close()
		hash, err := store.SetEncodedObject(encoded)
		if err != nil {
			t.Fatalf("store blob: %v", err)
		}
		return hash
	}
	signature := object.Signature{Name: "Pusher", Email: "pusher@example.com", When: time.Unix(1700000000, 0)}
	commit := func(message string, content plumbing.Hash) (plumbing.Hash, plumbing.Hash) {
		tree := push_encode(t, store, &object.Tree{Entries: []object.TreeEntry{{Name: "a.txt", Mode: filemode.Regular, Hash: content}}})
		return push_encode(t, store, &object.Commit{Author: signature, Committer: signature, Message: message, TreeHash: tree}), tree
	}
	pack := func(hashes ...plumbing.Hash) []byte {
		var out bytes.Buffer
		if _, err := packfile.NewEncoder(&out, store, false).Encode(hashes, 0); err != nil {
			t.Fatalf("encode pack: %v", err)
		}
		return out.Bytes()
	}
	missing := blob("missing")
	gap, gap_tree := commit("gap", missing)
	kept := blob("kept")
	whole, whole_tree := commit("whole", kept)
	main := plumbing.NewBranchReferenceName("main")
	side := plumbing.NewBranchReferenceName("side")
	loose := plumbing.NewBranchReferenceName("loose")
	later := plumbing.NewBranchReferenceName("later")

	status := push_send_pack(t, user, "gap", pack(gap, gap_tree), &packp.Command{Name: main, Old: plumbing.ZeroHash, New: gap})
	if status[main.String()] != "missing necessary objects" {
		t.Errorf("THE POINT: a pack missing the blob its tree names was answered %q, want a refusal", status[main.String()])
	}
	if _, err := push_store(t, user, "gap").Reference(main); err == nil {
		t.Error("THE POINT: the branch was created on a commit whose tree the repository cannot read whole")
	}
	if push_store(t, user, "gap").HasEncodedObject(gap) == nil {
		t.Error("THE POINT: a refused push stored its objects, where a later push could name them")
	}

	// A push of no objects naming the commit refused above: what it reaches
	// is no more whole for having been pushed before.
	status = push_send(t, user, "gap", &packp.Command{Name: later, Old: plumbing.ZeroHash, New: gap})
	if status[later.String()] == "ok" {
		t.Error("THE POINT: a later push created a branch on the commit a refused push carried")
	}

	// A byte past the pack's checksum keeps it from being kept as it came,
	// so its objects are stored one by one: checked the same.
	status = push_send_pack(t, user, "gap", append(pack(gap, gap_tree), 0), &packp.Command{Name: loose, Old: plumbing.ZeroHash, New: gap})
	if status[loose.String()] != "missing necessary objects" {
		t.Errorf("THE POINT: a pack stored object by object missing a blob was answered %q, want a refusal", status[loose.String()])
	}

	// Each command on its own: the whole one goes ahead beside the refused one.
	status = push_send_pack(t, user, "gap", pack(gap, gap_tree, whole, whole_tree, kept),
		&packp.Command{Name: main, Old: plumbing.ZeroHash, New: gap},
		&packp.Command{Name: side, Old: plumbing.ZeroHash, New: whole})
	if status[main.String()] != "missing necessary objects" || status[side.String()] != "ok" {
		t.Errorf("THE POINT: a push of one whole branch and one with a gap was answered %q and %q, want the gap alone refused", status[side.String()], status[main.String()])
	}

	// The push above stored what it carried for the command it refused, and
	// no reference reaches it: held is not whole.
	status = push_send(t, user, "gap", &packp.Command{Name: later, Old: plumbing.ZeroHash, New: gap})
	if status[later.String()] == "ok" {
		t.Error("THE POINT: a later push created a branch on the commit a partly refused push stored")
	}

	status = push_send_pack(t, user, "gap", pack(gap, gap_tree, missing), &packp.Command{Name: main, Old: plumbing.ZeroHash, New: gap})
	if status[main.String()] != "ok" {
		t.Errorf("CONTROL: the whole pack was answered %q, want ok", status[main.String()])
	}
}

// TestPushTrustsOnlyWhatReferencesReach. The connectivity walk ended at any
// object the repository held, but a push stores objects no command names,
// so a commit with a gap could be stored beside a whole branch and a later
// push could put a branch on it. Only what the references reach is whole, as
// in git's rev-list --not --all; a tag's target is what it is, whatever type
// the tag declares; and a push building on a branch is still answered ok.
func TestPushTrustsOnlyWhatReferencesReach(t *testing.T) {
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "trust"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	store := memory.NewStorage()
	blob := func(body string) plumbing.Hash {
		encoded := store.NewEncodedObject()
		encoded.SetType(plumbing.BlobObject)
		writer, _ := encoded.Writer()
		writer.Write([]byte(body))
		writer.Close()
		hash, err := store.SetEncodedObject(encoded)
		if err != nil {
			t.Fatalf("store blob: %v", err)
		}
		return hash
	}
	signature := object.Signature{Name: "Pusher", Email: "pusher@example.com", When: time.Unix(1700000000, 0)}
	commit := func(message string, content plumbing.Hash, parents ...plumbing.Hash) (plumbing.Hash, plumbing.Hash) {
		inner := push_encode(t, store, &object.Tree{Entries: []object.TreeEntry{{Name: "b.txt", Mode: filemode.Regular, Hash: blob("shared")}}})
		tree := push_encode(t, store, &object.Tree{Entries: []object.TreeEntry{
			{Name: "a.txt", Mode: filemode.Regular, Hash: content},
			{Name: "d", Mode: filemode.Dir, Hash: inner},
		}})
		signature.When = signature.When.Add(time.Minute)
		return push_encode(t, store, &object.Commit{Author: signature, Committer: signature, Message: message, TreeHash: tree, ParentHashes: parents}), tree
	}
	pack := func(hashes ...plumbing.Hash) []byte {
		var out bytes.Buffer
		if _, err := packfile.NewEncoder(&out, store, false).Encode(hashes, 0); err != nil {
			t.Fatalf("encode pack: %v", err)
		}
		return out.Bytes()
	}
	inner := push_encode(t, store, &object.Tree{Entries: []object.TreeEntry{{Name: "b.txt", Mode: filemode.Regular, Hash: blob("shared")}}})
	shared := blob("shared")
	missing := blob("missing")
	gap, gap_tree := commit("gap", missing)
	kept := blob("kept")
	whole, whole_tree := commit("whole", kept)
	main := plumbing.NewBranchReferenceName("main")
	evil := plumbing.NewBranchReferenceName("evil")
	tagged := plumbing.NewTagReferenceName("mislabelled")

	// A whole branch, carrying beside it a commit no command names whose blob
	// is missing.
	status := push_send_pack(t, user, "trust", pack(whole, whole_tree, inner, shared, kept, gap, gap_tree),
		&packp.Command{Name: main, Old: plumbing.ZeroHash, New: whole})
	if status[main.String()] != "ok" {
		t.Fatalf("CONTROL: the whole branch was answered %q, want ok", status[main.String()])
	}
	status = push_send(t, user, "trust", &packp.Command{Name: evil, Old: plumbing.ZeroHash, New: gap})
	if status[evil.String()] == "ok" {
		t.Error("THE POINT: a push of no objects put a branch on a stored commit no reference reached, whose blob is missing")
	}

	// A tag that calls the commit under it a blob.
	tag := push_encode(t, store, &object.Tag{Name: "mislabelled", Tagger: signature, Message: "m", TargetType: plumbing.BlobObject, Target: gap})
	status = push_send_pack(t, user, "trust", pack(tag, gap, gap_tree), &packp.Command{Name: tagged, Old: plumbing.ZeroHash, New: tag})
	if status[tagged.String()] == "ok" {
		t.Error("THE POINT: a tag declaring its target a blob carried a commit whose blob is missing past the walk")
	}

	// Building on the branch: the new commit's tree shares a subtree with its
	// parent's, which the references reach, and a branch on a commit they
	// reach needs no objects at all.
	next, next_tree := commit("next", blob("next"), whole)
	status = push_send_pack(t, user, "trust", pack(next, next_tree, blob("next")), &packp.Command{Name: main, Old: whole, New: next})
	if status[main.String()] != "ok" {
		t.Errorf("CONTROL: a commit building on the branch was answered %q, want ok", status[main.String()])
	}
	again := plumbing.NewBranchReferenceName("again")
	status = push_send(t, user, "trust", &packp.Command{Name: again, Old: plumbing.ZeroHash, New: whole})
	if status[again.String()] != "ok" {
		t.Errorf("CONTROL: a branch on a commit the references reach was answered %q, want ok", status[again.String()])
	}
}
