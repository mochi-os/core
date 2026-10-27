// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/atdata"
	atrepo "github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/events"
	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
	"nhooyr.io/websocket"
)

// A plc_fake stands in for the identity directory: it keeps each identity's
// log and takes an operation only when its signature checks against a
// rotation key of the operation before it (or its own, for the first), its
// prev names the last operation, and a first operation's hash is the
// identifier it is posted under - so a mistake in plc.go is refused here as
// the directory would refuse it.
type plc_fake struct {
	server  *httptest.Server
	lock    sync.Mutex
	logs    map[string][]map[string]any
	refused int
}

func create_plc_fake(t *testing.T) *plc_fake {
	t.Helper()
	f := &plc_fake{logs: map[string][]map[string]any{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		did := parts[0]
		f.lock.Lock()
		defer f.lock.Unlock()
		log := f.logs[did]
		switch {
		case r.Method == "POST" && len(parts) == 1:
			var op map[string]any
			if err := json.NewDecoder(r.Body).Decode(&op); err != nil {
				http.Error(w, "not json", http.StatusBadRequest)
				return
			}
			if err := f.accept(did, log, op); err != nil {
				f.refused++
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			f.logs[did] = append(log, op)
			w.WriteHeader(http.StatusOK)
		case r.Method == "GET" && len(parts) == 3 && parts[1] == "log" && parts[2] == "last":
			if len(log) == 0 {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(log[len(log)-1])
		case r.Method == "GET" && len(parts) == 1:
			if len(log) == 0 {
				http.NotFound(w, r)
				return
			}
			last := log[len(log)-1]
			w.Header().Set("Content-Type", "application/did+ld+json")
			json.NewEncoder(w).Encode(map[string]any{"id": did, "alsoKnownAs": last["alsoKnownAs"]})
		default:
			http.NotFound(w, r)
		}
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

// accept checks an operation as the directory does.
func (f *plc_fake) accept(did string, log []map[string]any, op map[string]any) error {
	encoded, _ := op["sig"].(string)
	signature, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(signature) != 64 {
		return fmt.Errorf("signature is not raw base64url")
	}
	unsigned := map[string]any{}
	for key, value := range op {
		if key != "sig" {
			unsigned[key] = value
		}
	}
	data, err := atdata.MarshalCBOR(unsigned)
	if err != nil {
		return err
	}
	signer := op
	if len(log) > 0 {
		signer = log[len(log)-1]
	}
	verified := false
	for _, key := range object_strings(signer["rotationKeys"]) {
		public, err := atcrypto.ParsePublicDIDKey(key)
		if err == nil && public.HashAndVerify(data, signature) == nil {
			verified = true
		}
	}
	if !verified {
		return fmt.Errorf("signature does not check against a rotation key")
	}
	if len(log) == 0 {
		if op["prev"] != nil {
			return fmt.Errorf("a first operation names a prev")
		}
		derived, err := plc_did(op)
		if err != nil || derived != did {
			return fmt.Errorf("did %s is not the hash of the operation (%s)", did, derived)
		}
		return nil
	}
	last, err := plc_cid(log[len(log)-1])
	if err != nil {
		return err
	}
	if previous, _ := op["prev"].(string); previous != last {
		return fmt.Errorf("prev %q is not the last operation %s", previous, last)
	}
	return nil
}

// last is the last operation of an identity, or nil.
func (f *plc_fake) last(did string) map[string]any {
	f.lock.Lock()
	defer f.lock.Unlock()
	log := f.logs[did]
	if len(log) == 0 {
		return nil
	}
	return log[len(log)-1]
}

// create_atproto_test_env makes alice with her hosted Bluesky account b1 on
// alice.example.com, keys and handle, the fake directory in place of the
// real one, and a name resolver under which every host reaches the server.
func create_atproto_test_env(t *testing.T) (*User, *plc_fake) {
	t.Helper()
	alice := create_delivery_test_env(t)
	if _, err := handle_reserve(alice, "", "alice", "handle"); err != nil {
		t.Fatal(err)
	}
	if err := keypairs_create(alice, "b1", "bluesky"); err != nil {
		t.Fatal(err)
	}
	actor_register(alice, "b1", "bluesky", "example.com", "alice")
	gin.SetMode(gin.TestMode)
	plc := create_plc_fake(t)
	// A relay asked to crawl answers here rather than at the network's.
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(relay.Close)
	saved_plc, saved_lookup, saved_hour, saved_day, saved_relay := bluesky_plc, atproto_lookup, atproto_budget_hour, atproto_budget_day, bluesky_relay
	bluesky_plc, bluesky_relay = plc.server.URL, relay.URL
	atproto_lookup = func(ctx context.Context, host string) ([]string, error) { return []string{"203.0.113.7"}, nil }
	atproto_budget_hour, atproto_budget_day = 1500, 10000
	networks_database().exec("delete from stream")
	networks_database().exec("delete from identities")
	t.Cleanup(func() {
		bluesky_plc, atproto_lookup, atproto_budget_hour, atproto_budget_day, bluesky_relay = saved_plc, saved_lookup, saved_hour, saved_day, saved_relay
	})
	return alice, plc
}

// atproto_request drives the engine with one request and returns the
// recorder.
func atproto_request(t *testing.T, host string, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "https://"+host+path, nil)
	c.Request.Host = host
	if !atproto_route(c) {
		t.Fatal("atproto_route did not answer")
	}
	return w
}

// atproto_json fetches a path and decodes the JSON answer.
func atproto_json(t *testing.T, path string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	w := atproto_request(t, "example.com", path)
	var document map[string]any
	json.Unmarshal(w.Body.Bytes(), &document)
	return w, document
}

// stream_events decodes every frame on the stream after a sequence.
func stream_events(t *testing.T, after int64) []*events.XRPCStreamEvent {
	t.Helper()
	var out []*events.XRPCStreamEvent
	for _, row := range stream_after(after, 100) {
		var event events.XRPCStreamEvent
		if err := event.Deserialize(bytes.NewReader(repository_bytes(row["body"]))); err != nil {
			t.Fatalf("frame %d: %v", row_int(row, "sequence"), err)
		}
		out = append(out, &event)
	}
	return out
}

// atproto_commit_event is the last commit on the stream, verified as a relay
// verifies it: the archive loads, the records named are in it, and where the
// previous root is given the tree inverts to it.
func atproto_commit_event(t *testing.T) *comatproto.SyncSubscribeRepos_Commit {
	t.Helper()
	all := stream_events(t, 0)
	if len(all) == 0 || all[len(all)-1].RepoCommit == nil {
		t.Fatal("the last event is not a commit")
	}
	commit := all[len(all)-1].RepoCommit
	if _, err := atrepo.VerifyCommitMessage(context.Background(), commit); err != nil {
		t.Fatalf("commit event does not verify: %v", err)
	}
	return commit
}

// atproto_thread is a Starlark thread of an app allowed to act as b1.
func atproto_thread(alice *User) *sl.Thread {
	app := create_external_app("app2")
	permission_grant(alice, app.id, "account:b1")
	permission_grant(alice, app.id, "accounts/write")
	permission_grant(alice, app.id, "accounts/read")
	thread := create_test_thread(alice, app)
	thread.SetLocal("context", context.Background())
	return thread
}

// TestAtprotoAccount: switching Bluesky on registers the identity from the
// account's keys, handle and server, makes the repository with the profile in
// it, announces the identity, the account and the commit on the stream, and
// answers the repository interface and the handle's identifier; the archive
// a relay reads loads and its commit checks against the signing key.
func TestAtprotoAccount(t *testing.T) {
	alice, plc := create_atproto_test_env(t)
	ctx := context.Background()
	created := time.Now().UnixMilli()
	if err := atproto_account_create(ctx, alice, "b1"); err != nil {
		t.Fatal(err)
	}
	row := account_hosted_get(alice, "b1")
	did := atproto_did(row)
	if !strings.HasPrefix(did, "did:plc:") || len(did) != 32 {
		t.Fatalf("did = %q", did)
	}
	if row_string(row, "identifier") != "alice.example.com" {
		t.Errorf("identifier = %q", row_string(row, "identifier"))
	}

	// The first operation: the server's rotation key, the signing key, the
	// handle and the server.
	genesis := plc.last(did)
	if genesis == nil {
		t.Fatal("no operation reached the directory")
	}
	rotation, signing := keypair_get(alice, "b1", key_rotation), keypair_get(alice, "b1", key_signing)
	if keys := object_strings(genesis["rotationKeys"]); len(keys) != 1 || keys[0] != rotation.Public {
		t.Errorf("rotationKeys = %v", genesis["rotationKeys"])
	}
	if methods, _ := genesis["verificationMethods"].(map[string]any); methods["atproto"] != signing.Public {
		t.Errorf("verificationMethods = %v", genesis["verificationMethods"])
	}
	if aliases := object_strings(genesis["alsoKnownAs"]); len(aliases) != 1 || aliases[0] != "at://alice.example.com" {
		t.Errorf("alsoKnownAs = %v", genesis["alsoKnownAs"])
	}
	services, _ := genesis["services"].(map[string]any)
	pds, _ := services["atproto_pds"].(map[string]any)
	if pds["endpoint"] != "https://example.com" || pds["type"] != "AtprotoPersonalDataServer" {
		t.Errorf("services = %v", genesis["services"])
	}

	// The identity is findable with no user, and the repository holds the
	// profile.
	user, _, r := atproto_identity(did)
	if user == nil || r == nil || r.head == "" {
		t.Fatalf("identity %s: user %v repository %v", did, user, r)
	}
	if id, value := repository_record(alice, "b1", "app.bsky.actor.profile", "self"); id == "" || value["displayName"] != "Alice" {
		t.Errorf("profile = %s %v", id, value)
	}

	// The stream: identity, account, commit.
	all := stream_events(t, 0)
	if len(all) != 3 || all[0].RepoIdentity == nil || all[1].RepoAccount == nil || all[2].RepoCommit == nil {
		t.Fatalf("stream = %d events", len(all))
	}
	if all[0].RepoIdentity.Did != did || all[0].RepoIdentity.Handle == nil || *all[0].RepoIdentity.Handle != "alice.example.com" || all[0].RepoIdentity.Seq < created {
		t.Errorf("identity event = %+v", all[0].RepoIdentity)
	}
	if !all[1].RepoAccount.Active || all[1].RepoAccount.Seq <= all[0].RepoIdentity.Seq {
		t.Errorf("account event = %+v", all[1].RepoAccount)
	}

	first := atproto_commit_event(t)
	if first.Repo != did || first.Rev != r.rev || first.Commit.String() != r.head || first.PrevData != nil || first.Since != nil || len(first.Ops) != 1 || first.Ops[0].Path != "app.bsky.actor.profile/self" {
		t.Errorf("commit event = %+v", first)
	}

	// The archive a relay reads back.
	w := atproto_request(t, "example.com", "/xrpc/com.atproto.sync.getRepo?did="+did)
	if w.Code != 200 || w.Header().Get("Content-Type") != "application/vnd.ipld.car" {
		t.Fatalf("getRepo: %d %s", w.Code, w.Header().Get("Content-Type"))
	}
	commit, repository, err := atrepo.LoadRepoFromCAR(ctx, bytes.NewReader(w.Body.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	public, err := atcrypto.ParsePublicDIDKey(signing.Public)
	if err != nil {
		t.Fatal(err)
	}
	if err := commit.VerifySignature(public); err != nil {
		t.Errorf("commit signature: %v", err)
	}
	if commit.DID != did || commit.Rev != r.rev {
		t.Errorf("commit = %+v", commit)
	}
	if _, _, err := repository.GetRecordBytes(ctx, syntax.NSID("app.bsky.actor.profile"), syntax.RecordKey("self")); err != nil {
		t.Errorf("profile not in the archive: %v", err)
	}

	// The interface.
	if w := atproto_request(t, "alice.example.com", "/.well-known/atproto-did"); w.Code != 200 || w.Body.String() != did {
		t.Errorf("well-known: %d %q", w.Code, w.Body.String())
	}
	if w := atproto_request(t, "nobody.example.com", "/.well-known/atproto-did"); w.Code != 404 {
		t.Errorf("well-known for an unknown handle: %d", w.Code)
	}
	if _, answer := atproto_json(t, "/xrpc/com.atproto.identity.resolveHandle?handle=alice.example.com"); answer["did"] != did {
		t.Errorf("resolveHandle = %v", answer)
	}
	if _, answer := atproto_json(t, "/xrpc/com.atproto.server.describeServer"); answer["did"] != "did:web:example.com" {
		t.Errorf("describeServer = %v", answer)
	}
	if _, answer := atproto_json(t, "/xrpc/com.atproto.sync.getLatestCommit?did="+did); answer["cid"] != r.head || answer["rev"] != r.rev {
		t.Errorf("getLatestCommit = %v", answer)
	}
	if _, answer := atproto_json(t, "/xrpc/com.atproto.sync.getRepoStatus?did="+did); answer["active"] != true || answer["rev"] != r.rev {
		t.Errorf("getRepoStatus = %v", answer)
	}
	_, repos := atproto_json(t, "/xrpc/com.atproto.sync.listRepos")
	if list, _ := repos["repos"].([]any); len(list) != 1 || list[0].(map[string]any)["did"] != did {
		t.Errorf("listRepos = %v", repos)
	}
	_, record := atproto_json(t, "/xrpc/com.atproto.repo.getRecord?repo=alice.example.com&collection=app.bsky.actor.profile&rkey=self")
	if value, _ := record["value"].(map[string]any); value["displayName"] != "Alice" || record["uri"] != "at://"+did+"/app.bsky.actor.profile/self" {
		t.Errorf("getRecord = %v", record)
	}
	_, described := atproto_json(t, "/xrpc/com.atproto.repo.describeRepo?repo="+did)
	if collections, _ := described["collections"].([]any); len(collections) != 1 || collections[0] != "app.bsky.actor.profile" || described["handle"] != "alice.example.com" {
		t.Errorf("describeRepo = %v", described)
	}
	if w, _ := atproto_json(t, "/xrpc/com.atproto.sync.getRepo?did=did:plc:nobody"); w.Code != 404 {
		t.Errorf("getRepo of an unknown identity: %d", w.Code)
	}
	if w, _ := atproto_json(t, "/xrpc/com.atproto.nothing.here"); w.Code != 501 {
		t.Errorf("an unknown method: %d", w.Code)
	}
}

// TestAtprotoPublish: an app publishes a record through the account API,
// once per key; the commit event carries the record and inverts to the
// previous root; an update and a deletion follow with their previous values;
// a blob uploaded is served back.
func TestAtprotoPublish(t *testing.T) {
	alice, _ := create_atproto_test_env(t)
	ctx := context.Background()
	if err := atproto_account_create(ctx, alice, "b1"); err != nil {
		t.Fatal(err)
	}
	did := atproto_did(account_hosted_get(alice, "b1"))
	thread, fn := atproto_thread(alice), sl.NewBuiltin("test", nil)
	before := repository_get(alice, "b1")

	value, err := api_account_object_create(thread, fn, sl.Tuple{sl.String("b1"), object_dict("text", "Hello Bluesky")}, []sl.Tuple{{sl.String("key"), sl.String("post-1")}})
	if err != nil {
		t.Fatal(err)
	}
	result := sl_decode_map(value)
	uri, _ := result["id"].(string)
	if !strings.HasPrefix(uri, "at://"+did+"/app.bsky.feed.post/") {
		t.Fatalf("id = %q", uri)
	}
	if page, _ := result["url"].(string); !strings.HasPrefix(page, "https://bsky.app/profile/"+did+"/post/") {
		t.Errorf("url = %q", result["url"])
	}
	if data, _ := result["data"].(map[string]any); data["committed"] != true {
		t.Errorf("data = %v", result["data"])
	}
	again, err := api_account_object_create(thread, fn, sl.Tuple{sl.String("b1"), object_dict("text", "Hello again")}, []sl.Tuple{{sl.String("key"), sl.String("post-1")}})
	if err != nil {
		t.Fatal(err)
	}
	if sl_decode_map(again)["id"] != uri {
		t.Error("the same key made a second post")
	}
	_, _, rkey, _ := bluesky_uri(uri)

	// The event inverts to the root before it.
	event := atproto_commit_event(t)
	if event.PrevData == nil || event.PrevData.String() != before.root || event.Since == nil || *event.Since != before.rev {
		t.Errorf("prevData = %v since = %v, want %s %s", event.PrevData, event.Since, before.root, before.rev)
	}
	if len(event.Ops) != 1 || event.Ops[0].Action != "create" || event.Ops[0].Path != "app.bsky.feed.post/"+rkey || event.Ops[0].Prev != nil {
		t.Errorf("ops = %+v", event.Ops[0])
	}
	_, record := atproto_json(t, "/xrpc/com.atproto.repo.getRecord?repo="+did+"&collection=app.bsky.feed.post&rkey="+rkey)
	if v, _ := record["value"].(map[string]any); v["text"] != "Hello Bluesky" || v["$type"] != "app.bsky.feed.post" || v["createdAt"] == nil {
		t.Errorf("record = %v", record)
	}

	// An update carries the previous value; the record changes.
	if _, err := api_account_object_update(thread, fn, sl.Tuple{sl.String("b1"), sl.String(uri), object_dict("text", "Edited")}, nil); err != nil {
		t.Fatal(err)
	}
	event = atproto_commit_event(t)
	if len(event.Ops) != 1 || event.Ops[0].Action != "update" || event.Ops[0].Prev == nil {
		t.Errorf("update ops = %+v", event.Ops[0])
	}
	if _, v := repository_record(alice, "b1", "app.bsky.feed.post", rkey); v["text"] != "Edited" {
		t.Errorf("record after the update = %v", v)
	}
	_, listed := atproto_json(t, "/xrpc/com.atproto.repo.listRecords?repo="+did+"&collection=app.bsky.feed.post")
	if records, _ := listed["records"].([]any); len(records) != 1 {
		t.Errorf("listRecords = %v", listed)
	}
	if _, err := api_account_object_update(thread, fn, sl.Tuple{sl.String("b1"), sl.String("at://did:plc:other/app.bsky.feed.post/" + rkey), object_dict("text", "Not mine")}, nil); err == nil {
		t.Error("a record of another identity was updated")
	}

	// A deletion; the record is gone from the interface and the tree.
	if _, err := api_account_object_delete(thread, fn, sl.Tuple{sl.String("b1"), sl.String(uri)}, nil); err != nil {
		t.Fatal(err)
	}
	event = atproto_commit_event(t)
	if len(event.Ops) != 1 || event.Ops[0].Action != "delete" || event.Ops[0].Prev == nil || event.Ops[0].Cid != nil {
		t.Errorf("delete ops = %+v", event.Ops[0])
	}
	if w, _ := atproto_json(t, "/xrpc/com.atproto.repo.getRecord?repo="+did+"&collection=app.bsky.feed.post&rkey="+rkey); w.Code != 400 {
		t.Errorf("a deleted record answered %d", w.Code)
	}
	archive := atproto_request(t, "example.com", "/xrpc/com.atproto.sync.getRepo?did="+did)
	_, repository, err := atrepo.LoadRepoFromCAR(ctx, bytes.NewReader(archive.Body.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repository.GetRecordBytes(ctx, syntax.NSID("app.bsky.feed.post"), syntax.RecordKey(rkey)); err == nil {
		t.Error("the deleted record is still in the archive")
	}
	// Blocks nothing reaches are gone: the archive holds all that is kept.
	n, _ := db_user(alice, "user").row("select count(*) as n from nodes where account='b1'")
	if row_int(n, "n") > 4 {
		t.Errorf("%d blocks kept for a repository of one record", row_int(n, "n"))
	}

	// A blob.
	upload, err := api_account_upload(thread, fn, sl.Tuple{sl.String("b1"), sl.Bytes("\x89PNG\r\n\x1a\nimage"), sl.String("image/png")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	blob := sl_decode_map(upload)
	id, _ := blob["id"].(string)
	reference, _ := blob["data"].(map[string]any)
	if id == "" || reference["$type"] != "blob" || reference["mimeType"] != "image/png" {
		t.Fatalf("upload = %v", blob)
	}
	if w := atproto_request(t, "example.com", "/xrpc/com.atproto.sync.getBlob?did="+did+"&cid="+id); w.Code != 200 || w.Header().Get("Content-Type") != "image/png" || !strings.HasPrefix(w.Body.String(), "\x89PNG") {
		t.Errorf("getBlob: %d %s", w.Code, w.Header().Get("Content-Type"))
	}
	if _, blobs := atproto_json(t, "/xrpc/com.atproto.sync.listBlobs?did="+did); len(blobs["cids"].([]any)) != 1 {
		t.Errorf("listBlobs = %v", blobs)
	}
	posted, err := api_account_object_create(thread, fn, sl.Tuple{sl.String("b1"), sl_encode(map[string]any{"text": "With a picture", "embed": map[string]any{"$type": "app.bsky.embed.images", "images": []any{map[string]any{"image": reference, "alt": "A picture"}}}})}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, v := repository_record(alice, "b1", "app.bsky.feed.post", strings.TrimPrefix(sl_decode_map(posted)["id"].(string), "at://"+did+"/app.bsky.feed.post/")); v == nil {
		t.Error("the post with the picture is not in the repository")
	}
	atproto_commit_event(t)
}

// TestAtprotoBudget: a sourced write waits once four fifths of the budget
// is spent while a person's own write still goes, and takes the waiting
// one with it; what waits is readable at once; the manager commits what
// waits when the budget frees.
func TestAtprotoBudget(t *testing.T) {
	// Numbered from the last given alone, as before the stream counted from
	// the time.
	saved_clock := stream_clock
	stream_clock = func() int64 { return 0 }
	t.Cleanup(func() { stream_clock = saved_clock })
	alice, _ := create_atproto_test_env(t)
	ctx := context.Background()
	if err := atproto_account_create(ctx, alice, "b1"); err != nil {
		t.Fatal(err)
	}
	r := repository_get(alice, "b1")
	db := db_user(alice, "user")
	waiting := func() int64 {
		n, _ := db.row("select count(*) as n from waiting where account='b1'")
		return row_int(n, "n")
	}
	post := func(text string) map[string]any {
		return map[string]any{"$type": "app.bsky.feed.post", "text": text, "createdAt": time.Now().UTC().Format(time.RFC3339)}
	}
	if _, committed, err := repository_write(ctx, r, "app.bsky.feed.post", "0000", post("First"), false); err != nil || !committed {
		t.Fatalf("first write: committed %v, %v", committed, err)
	}
	// Four events so far; a limit of five leaves room for one own write
	// (under 5) and none for a sourced one (under 4).
	atproto_budget_hour = 5
	_, committed, err := repository_write(ctx, r, "app.bsky.feed.post", "aaaa", post("Sourced"), true)
	if err != nil || committed {
		t.Fatalf("sourced write: committed %v, %v", committed, err)
	}
	if waiting() != 1 {
		t.Fatalf("%d waiting, want 1", waiting())
	}
	if _, v := repository_record(alice, "b1", "app.bsky.feed.post", "aaaa"); v == nil || v["text"] != "Sourced" {
		t.Error("the waiting record is not readable")
	}
	if stream_last() != 4 {
		t.Errorf("stream advanced to %d for a waiting write", stream_last())
	}
	// The person's own write goes, and takes the waiting one with it.
	_, committed, err = repository_write(ctx, r, "app.bsky.feed.post", "bbbb", post("Own"), false)
	if err != nil || !committed {
		t.Fatalf("own write: committed %v, %v", committed, err)
	}
	if waiting() != 0 {
		t.Errorf("%d waiting after the own write, want 0", waiting())
	}
	event := atproto_commit_event(t)
	if len(event.Ops) != 2 {
		t.Errorf("ops = %d, want the waiting one and the own one", len(event.Ops))
	}
	// Five events: nothing more goes, own or sourced, until the budget frees.
	_, committed, _ = repository_write(ctx, r, "app.bsky.feed.post", "cccc", post("Later"), true)
	if committed {
		t.Error("a sourced write went over the budget")
	}
	if made := atproto_drain(ctx); made != 0 {
		t.Errorf("the manager made %d commits over the budget", made)
	}
	atproto_budget_hour = 1500
	if made := atproto_drain(ctx); made != 1 {
		t.Errorf("the manager made %d commits, want 1", made)
	}
	if waiting() != 0 {
		t.Errorf("%d waiting after the drain", waiting())
	}
	event = atproto_commit_event(t)
	if len(event.Ops) != 1 || event.Ops[0].Path != "app.bsky.feed.post/cccc" {
		t.Errorf("drained ops = %+v", event.Ops)
	}
	hour, day := atproto_budget_used()
	if hour != 6 || day != 6 {
		t.Errorf("budget used = %d %d, want 6 6", hour, day)
	}
}

// TestAtprotoSubscribe: a subscription from a cursor replays the frames after
// it and then each new one as it happens; a cursor ahead of the stream is
// answered with an error frame.
func TestAtprotoSubscribe(t *testing.T) {
	alice, _ := create_atproto_test_env(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := atproto_account_create(ctx, alice, "b1"); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, _ := gin.CreateTestContext(w)
		c.Request = r
		atproto_route(c)
	}))
	defer server.Close()
	address := "ws" + strings.TrimPrefix(server.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos"

	read := func(conn *websocket.Conn) *events.XRPCStreamEvent {
		t.Helper()
		kind, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if kind != websocket.MessageBinary {
			t.Fatalf("frame is %v, not binary", kind)
		}
		var event events.XRPCStreamEvent
		if err := event.Deserialize(bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		return &event
	}

	conn, _, err := websocket.Dial(ctx, address+"?cursor=0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	first := read(conn)
	if first.RepoIdentity == nil {
		t.Fatalf("first frame = %+v", first)
	}
	// Each event numbered past the one before, though not always the next
	// number: the numbers follow the time.
	seen := first.RepoIdentity.Seq
	second := read(conn)
	if second.RepoAccount == nil || second.RepoAccount.Seq <= seen {
		t.Fatalf("second frame = %+v", second)
	}
	seen = second.RepoAccount.Seq
	if event := read(conn); event.RepoCommit == nil || event.RepoCommit.Seq <= seen {
		t.Fatalf("third frame = %+v", event)
	} else {
		seen = event.RepoCommit.Seq
	}
	// A new commit arrives live.
	r := repository_get(alice, "b1")
	if _, _, err := repository_write(ctx, r, "app.bsky.feed.post", "live", map[string]any{"$type": "app.bsky.feed.post", "text": "Live", "createdAt": "2026-10-15T00:00:00Z"}, false); err != nil {
		t.Fatal(err)
	}
	if event := read(conn); event.RepoCommit == nil || event.RepoCommit.Seq <= seen || len(event.RepoCommit.Ops) != 1 || event.RepoCommit.Ops[0].Path != "app.bsky.feed.post/live" {
		t.Fatalf("live frame = %+v", event)
	} else {
		seen = event.RepoCommit.Seq
	}

	// From the present: nothing replayed, the next commit comes.
	fresh, _, err := websocket.Dial(ctx, address, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close(websocket.StatusNormalClosure, "")
	if _, _, err := repository_write(ctx, r, "app.bsky.feed.post", "next", map[string]any{"$type": "app.bsky.feed.post", "text": "Next", "createdAt": "2026-10-15T00:00:00Z"}, false); err != nil {
		t.Fatal(err)
	}
	if event := read(fresh); event.RepoCommit == nil || event.RepoCommit.Seq <= seen || event.RepoCommit.Ops[0].Path != "app.bsky.feed.post/next" {
		t.Fatalf("fresh frame = %+v", event)
	}

	// Ahead of the stream.
	ahead, _, err := websocket.Dial(ctx, address+"?cursor="+strconv.FormatInt(stream_last()+1000, 10), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ahead.Close(websocket.StatusNormalClosure, "")
	if event := read(ahead); event.Error == nil || event.Error.Error != "FutureCursor" {
		t.Fatalf("ahead frame = %+v", event)
	}
}

// TestAtprotoStreamRestored: a server restored from an older backup, its
// stream holding fewer events than it gave, numbers the next past every one
// it gave before, which a relay's cursor holds; the numbers never go below
// the time.
func TestAtprotoStreamRestored(t *testing.T) {
	create_atproto_test_env(t)
	saved := stream_clock
	t.Cleanup(func() { stream_clock = saved })
	clock := int64(1000)
	stream_clock = func() int64 { return clock }
	did := "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	if given := stream_append(did, "#identity", map[string]any{"did": did}); given != 1000 {
		t.Errorf("THE POINT: at the time 1000 the stream numbered an event %d", given)
	}
	if next := stream_append(did, "#identity", map[string]any{"did": did}); next != 1001 {
		t.Errorf("the next event in the same moment is %d, want 1001", next)
	}
	// Later, restored from a backup that held none of them.
	clock = 5000
	networks_database().exec("delete from stream")
	networks_database().exec("update sqlite_sequence set seq=0 where name='stream'")
	if restored := stream_append(did, "#identity", map[string]any{"did": did}); restored != 5000 {
		t.Errorf("THE POINT: restored, the stream numbers its next event %d, want 5000, past the 1001 it gave before", restored)
	}
	// A clock set back does not take the numbers back with it.
	clock = 10
	if next := stream_append(did, "#identity", map[string]any{"did": did}); next != 5001 {
		t.Errorf("with the clock set back, the next event is %d, want 5001", next)
	}
}

// TestAtprotoNotify: a commit asks the relay to crawl its domain, the first
// after a start at once and then at most every twenty minutes, so a relay
// that gave the server up as offline after a restart follows it again.
func TestAtprotoNotify(t *testing.T) {
	alice, _ := create_atproto_test_env(t)
	ctx := context.Background()
	var lock sync.Mutex
	asked := []string{}
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		lock.Lock()
		asked = append(asked, r.URL.Path+" "+string(body))
		lock.Unlock()
	}))
	t.Cleanup(relay.Close)
	saved := bluesky_relay
	bluesky_relay = relay.URL
	t.Cleanup(func() { bluesky_relay = saved })
	if err := atproto_account_create(ctx, alice, "b1"); err != nil {
		t.Fatal(err)
	}
	count := func() int {
		time.Sleep(200 * time.Millisecond)
		lock.Lock()
		defer lock.Unlock()
		return len(asked)
	}
	// A start: nothing asked yet this run.
	atproto_notified_lock.Lock()
	atproto_notified = map[string]time.Time{}
	atproto_notified_lock.Unlock()
	before := count()
	r := repository_get(alice, "b1")
	for _, key := range []string{"one", "two"} {
		if _, _, err := repository_write(ctx, r, "app.bsky.feed.post", key, map[string]any{"$type": "app.bsky.feed.post", "text": key, "createdAt": "2026-10-24T00:00:00Z"}, false); err != nil {
			t.Fatal(err)
		}
	}
	if n := count() - before; n != 1 {
		t.Fatalf("THE POINT: two commits after a start asked the relay %d times, want once", n)
	}
	lock.Lock()
	last := asked[len(asked)-1]
	lock.Unlock()
	if !strings.HasPrefix(last, "/xrpc/com.atproto.sync.requestCrawl ") || !strings.Contains(last, `"hostname"`) {
		t.Errorf("the relay was asked %q", last)
	}
	// Twenty minutes on, the next commit asks again.
	atproto_notified_lock.Lock()
	for domain := range atproto_notified {
		atproto_notified[domain] = time.Now().Add(-atproto_notify_interval)
	}
	atproto_notified_lock.Unlock()
	before = count()
	if _, _, err := repository_write(ctx, r, "app.bsky.feed.post", "three", map[string]any{"$type": "app.bsky.feed.post", "text": "three", "createdAt": "2026-10-24T00:00:00Z"}, false); err != nil {
		t.Fatal(err)
	}
	if n := count() - before; n != 1 {
		t.Errorf("THE POINT: a commit twenty minutes on asked the relay %d times, want once", n)
	}
}

// TestAtprotoKeys: a recovery key the user makes is listed above the server's
// own in the identity; a replaced signing key is named in the identity, signs
// a fresh commit and is announced; the account's row says whether a recovery
// key is kept.
func TestAtprotoKeys(t *testing.T) {
	alice, plc := create_atproto_test_env(t)
	ctx := context.Background()
	if err := atproto_account_create(ctx, alice, "b1"); err != nil {
		t.Fatal(err)
	}
	row := account_hosted_get(alice, "b1")
	did := atproto_did(row)
	if present := account_present(alice, row); present["recovery"] != false || present["did"] != did || present["url"] != "https://bsky.app/profile/"+did {
		t.Errorf("present = %v", present)
	}
	thread, fn := atproto_thread(alice), sl.NewBuiltin("test", nil)

	made, err := api_account_key_recovery(thread, fn, sl.Tuple{sl.String("b1")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	recovery := sl_decode_map(made)
	last := plc.last(did)
	keys := object_strings(last["rotationKeys"])
	if len(keys) != 2 || keys[0] != recovery["public"] || keys[1] != keypair_get(alice, "b1", key_rotation).Public {
		t.Errorf("rotationKeys after the recovery key = %v", keys)
	}
	if present := account_present(alice, account_hosted_get(alice, "b1")); present["recovery"] != true {
		t.Errorf("present after the recovery key = %v", present)
	}

	old := keypair_get(alice, "b1", key_signing).Public
	if _, err := api_account_key_replace(thread, fn, sl.Tuple{sl.String("b1")}, nil); err != nil {
		t.Fatal(err)
	}
	signing := keypair_get(alice, "b1", key_signing)
	if signing.Public == old {
		t.Fatal("the signing key did not change")
	}
	last = plc.last(did)
	if methods, _ := last["verificationMethods"].(map[string]any); methods["atproto"] != signing.Public {
		t.Errorf("verificationMethods after the replacement = %v", last["verificationMethods"])
	}
	if keys := object_strings(last["rotationKeys"]); len(keys) != 2 || keys[0] != recovery["public"] {
		t.Errorf("rotationKeys after the replacement = %v", keys)
	}
	all := stream_events(t, 0)
	if len(all) < 2 || all[len(all)-1].RepoIdentity == nil || all[len(all)-2].RepoCommit == nil {
		t.Fatalf("the replacement did not end with a commit and an identity event (%d events)", len(all))
	}
	commit, _, err := atrepo.LoadCommitFromCAR(ctx, bytes.NewReader(all[len(all)-2].RepoCommit.Blocks))
	if err != nil {
		t.Fatal(err)
	}
	public, _ := atcrypto.ParsePublicDIDKey(signing.Public)
	if err := commit.VerifySignature(public); err != nil {
		t.Errorf("the fresh commit does not check against the new key: %v", err)
	}
	if plc.refused != 0 {
		t.Errorf("the directory refused %d operations", plc.refused)
	}
}

// TestAtprotoRemove: removing the account ends it on the stream, tombstones
// the identity and drops the repository.
func TestAtprotoRemove(t *testing.T) {
	alice, plc := create_atproto_test_env(t)
	ctx := context.Background()
	if err := atproto_account_create(ctx, alice, "b1"); err != nil {
		t.Fatal(err)
	}
	did := atproto_did(account_hosted_get(alice, "b1"))
	r := repository_get(alice, "b1")
	if _, _, err := repository_write(ctx, r, "app.bsky.feed.post", "gone", map[string]any{"$type": "app.bsky.feed.post", "text": "Gone", "createdAt": "2026-10-15T00:00:00Z"}, false); err != nil {
		t.Fatal(err)
	}
	thread, fn := atproto_thread(alice), sl.NewBuiltin("test", nil)
	upload, err := api_account_upload(thread, fn, sl.Tuple{sl.String("b1"), sl.Bytes("bytes"), sl.String("image/png")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	blob := sl_decode_map(upload)["id"].(string)

	if _, err := api_account_remove(thread, fn, sl.Tuple{sl.String("b1")}, nil); err != nil {
		t.Fatal(err)
	}
	if last := plc.last(did); last == nil || last["type"] != "plc_tombstone" {
		t.Errorf("last operation = %v", last)
	}
	all := stream_events(t, 0)
	final := all[len(all)-1].RepoAccount
	if final == nil || final.Active || final.Status == nil || *final.Status != "deleted" {
		t.Errorf("last event = %+v", all[len(all)-1])
	}
	if user, _, _ := atproto_identity(did); user != nil {
		t.Error("the identity is still registered")
	}
	db := db_user(alice, "user")
	for _, table := range []string{"repositories", "nodes", "records", "blobs"} {
		n, _ := db.row("select count(*) as n from " + table + " where account='b1'")
		if row_int(n, "n") != 0 {
			t.Errorf("%d rows left in %s", row_int(n, "n"), table)
		}
	}
	if _, _, found := repository_blob(alice, "b1", blob); found {
		t.Error("the blob is still served")
	}
	if w, _ := atproto_json(t, "/xrpc/com.atproto.sync.getRepo?did="+did); w.Code != 404 {
		t.Errorf("getRepo after the removal: %d", w.Code)
	}
}

// TestAtprotoCreateRefused: a handle whose host does not reach this server
// is refused before anything is registered, and the account is not made;
// once it reaches, the account is made through the API with its identity.
func TestAtprotoCreateRefused(t *testing.T) {
	alice, plc := create_atproto_test_env(t)
	db := db_user(alice, "user")
	db.exec("delete from accounts where id='b1'")
	keypairs_delete(db, "b1")
	networks_database().exec("delete from actors where id='b1'")
	thread, fn := atproto_thread(alice), sl.NewBuiltin("test", nil)
	add := func() (map[string]any, error) {
		value, err := api_account_add(thread, fn, sl.Tuple{sl.String("bluesky")}, []sl.Tuple{{sl.String("handle"), sl.String("alice")}})
		if err != nil {
			return nil, err
		}
		return sl_decode_map(value), nil
	}

	atproto_lookup = func(ctx context.Context, host string) ([]string, error) {
		if host == "alice.example.com" {
			return []string{"198.51.100.1"}, nil
		}
		return []string{"203.0.113.7"}, nil
	}
	if _, err := add(); err == nil || !strings.Contains(err.Error(), "does not reach this server") {
		t.Fatalf("a handle that does not reach the server was accepted: %v", err)
	}
	if len(accounts_hosted(alice, "bluesky")) != 0 {
		t.Error("the refused account was kept")
	}
	if n, _ := networks_database().row("select count(*) as n from identities"); row_int(n, "n") != 0 {
		t.Error("an identity was registered for the refused account")
	}
	if len(plc.logs) != 0 {
		t.Error("the directory heard of the refused account")
	}

	atproto_lookup = func(ctx context.Context, host string) ([]string, error) { return []string{"203.0.113.7"}, nil }
	answer, err := add()
	if err != nil {
		t.Fatal(err)
	}
	id, _ := answer["id"].(string)
	if answer["identifier"] != "alice.example.com" || id == "" {
		t.Errorf("answer = %v", answer)
	}
	row := account_hosted_get(alice, id)
	if row == nil || atproto_did(row) == "" || plc.last(atproto_did(row)) == nil {
		t.Fatalf("the account has no identity: %v", row)
	}
	if r := repository_get(alice, id); r == nil || r.head == "" {
		t.Error("the account has no repository")
	}
	if n, _ := networks_database().row("select count(*) as n from identities"); row_int(n, "n") != 1 {
		t.Errorf("%d identities, want 1", row_int(n, "n"))
	}
}

// TestAtprotoProfileRefresh: when the person's own profile changes, which the
// people app reports, the profile record of their hosted Bluesky account is
// written again with it.
func TestAtprotoProfileRefresh(t *testing.T) {
	alice, _ := create_atproto_test_env(t)
	if err := atproto_account_create(context.Background(), alice, "b1"); err != nil {
		t.Fatal(err)
	}
	saved := account_change_delay
	account_change_delay = 20 * time.Millisecond
	t.Cleanup(func() { account_change_delay = saved })
	db_open("db/users.db").exec("update entities set name='Alice Liddell' where user=? and class='person'", alice.UID)
	if _, err := sl.Call(atproto_thread(alice), sl.NewBuiltin("mochi.account.refresh", api_account_refresh), nil, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, value := repository_record(alice, "b1", "app.bsky.actor.profile", "self"); value["displayName"] != "Alice Liddell" {
		t.Errorf("THE POINT: the profile record follows the person's profile: %v", value)
	}
}

// TestAtprotoReaches: the host check itself.
func TestAtprotoReaches(t *testing.T) {
	saved := atproto_lookup
	defer func() { atproto_lookup = saved }()
	atproto_lookup = func(ctx context.Context, host string) ([]string, error) {
		switch host {
		case "example.com":
			return []string{"203.0.113.7", "2001:db8::7"}, nil
		case "alice.example.com":
			return []string{"2001:db8::7"}, nil
		case "bob.example.com":
			return []string{"198.51.100.1"}, nil
		}
		return nil, fmt.Errorf("no such host")
	}
	ctx := context.Background()
	if err := atproto_reaches(ctx, "alice.example.com", "example.com"); err != nil {
		t.Errorf("a host sharing an address was refused: %v", err)
	}
	if err := atproto_reaches(ctx, "bob.example.com", "example.com"); err == nil {
		t.Error("a host elsewhere was accepted")
	}
	if err := atproto_reaches(ctx, "carol.example.com", "example.com"); err == nil {
		t.Error("a host that does not resolve was accepted")
	}
	if err := atproto_reaches(ctx, "example.com", "example.com"); err != nil {
		t.Errorf("the domain itself was refused: %v", err)
	}
}

// TestAtprotoReachesHostsFile: a server whose hosts file names its own domain
// as 127.0.1.1, as Debian and Ubuntu write it, is reached at the addresses on
// its interfaces, and a host leading to loopback reaches nobody outside.
func TestAtprotoReachesHostsFile(t *testing.T) {
	saved, saved_interfaces := atproto_lookup, domain_interfaces
	defer func() { atproto_lookup, domain_interfaces = saved, saved_interfaces }()
	atproto_lookup = func(ctx context.Context, host string) ([]string, error) {
		switch host {
		case "self.test":
			return []string{"127.0.1.1"}, nil
		case "dave.self.test":
			return []string{"2001:db8::9", "203.0.113.9"}, nil
		case "erin.self.test":
			return []string{"198.51.100.1"}, nil
		case "loop.self.test":
			return []string{"127.0.1.1"}, nil
		}
		return nil, fmt.Errorf("no such host")
	}
	domain_interfaces = func() []string { return []string{"203.0.113.9", "2001:db8::9"} }
	ctx := context.Background()
	if err := atproto_reaches(ctx, "dave.self.test", "self.test"); err != nil {
		t.Errorf("THE POINT: a host leading to this machine was refused: %v", err)
	}
	if err := atproto_reaches(ctx, "erin.self.test", "self.test"); err == nil {
		t.Error("a host elsewhere was accepted")
	}
	if err := atproto_reaches(ctx, "loop.self.test", "self.test"); err == nil {
		t.Error("THE POINT: a host leading to loopback was accepted")
	}
}

// TestAtprotoStreamSequence: a sequence number is never given twice - after
// the replay window has emptied the stream the next event carries on from
// the last, and a stream table from before its sequence was AUTOINCREMENT is
// rebuilt with its events, carrying on from their highest.
func TestAtprotoStreamSequence(t *testing.T) {
	// Numbered from the last given alone, as before the stream counted from
	// the time.
	saved_clock := stream_clock
	stream_clock = func() int64 { return 0 }
	t.Cleanup(func() { stream_clock = saved_clock })
	create_atproto_test_env(t)
	db := networks_database()
	var last int64
	for range 3 {
		last = stream_append("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "#identity", map[string]any{"did": "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"})
	}
	db.exec("delete from stream")
	if next := stream_append("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "#identity", map[string]any{"did": "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"}); next != last+1 {
		t.Errorf("after the window emptied the stream, the next event is %d, want %d", next, last+1)
	}

	db.exec("drop table stream")
	db.exec("create table stream (sequence integer not null primary key, did text not null, kind text not null, body blob not null, created integer not null)")
	db.exec("insert into stream (sequence, did, kind, body, created) values (40, 'did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', '#identity', x'00', 1), (41, 'did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', '#identity', x'00', 2)")
	db.exec("delete from sqlite_sequence where name='stream'")
	atproto_stream_rebuild(db)
	row, _ := db.row("select sql from sqlite_master where type='table' and name='stream'")
	if !strings.Contains(strings.ToLower(row_string(row, "sql")), "autoincrement") {
		t.Fatalf("the stream table was not rebuilt: %v", row)
	}
	if n, _ := db.row("select count(*) as n from stream"); row_int(n, "n") != 2 {
		t.Errorf("%d events kept, want 2", row_int(n, "n"))
	}
	if held, _ := db.exists("select 1 from sqlite_master where type='index' and name='stream_created'"); !held {
		t.Error("the rebuilt table has no index on created")
	}
	db.exec("delete from stream")
	if next := stream_append("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "#identity", map[string]any{"did": "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"}); next != 42 {
		t.Errorf("after the rebuild and an empty window, the next event is %d, want 42", next)
	}
	// Rebuilt again once the window has emptied it, the table would lose the
	// last number it gave.
	db.exec("delete from stream")
	atproto_stream_rebuild(db)
	if next := stream_append("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "#identity", map[string]any{"did": "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"}); next != 43 {
		t.Errorf("an AUTOINCREMENT stream table was rebuilt again: the next event is %d, want 43", next)
	}
}

// TestAtprotoRemoveMissing: deleting a record the repository does not hold -
// one an app deleted before Mochi took back what it stood for - commits
// nothing, since a commit deleting nothing is refused by relays.
func TestAtprotoRemoveMissing(t *testing.T) {
	alice, _ := create_atproto_test_env(t)
	ctx := context.Background()
	if err := atproto_account_create(ctx, alice, "b1"); err != nil {
		t.Fatal(err)
	}
	r := repository_get(alice, "b1")
	head, events := r.head, len(stream_events(t, 0))
	committed, err := repository_remove(ctx, r, "app.bsky.feed.like", "3missing", false)
	if err != nil || committed || r.head != head || len(stream_events(t, 0)) != events {
		t.Errorf("THE POINT: removing a missing record committed: %v %v, %d events after %d", committed, err, len(stream_events(t, 0)), events)
	}
	row := account_hosted_get(alice, "b1")
	if err := atproto_delete(ctx, alice, row, "at://"+atproto_did(row)+"/app.bsky.feed.like/3missing"); err != nil || len(stream_events(t, 0)) != events {
		t.Errorf("deleting a missing record = %v, %d events after %d", err, len(stream_events(t, 0)), events)
	}
}

// TestRestoreHosted: a restored user's hosted accounts on handles held again
// here carry over, each network's registry naming them theirs; restored onto
// another domain, a Bluesky account moves its identity here under the same
// name, and the others, whose addresses stay with their domain, are dropped.
func TestRestoreHosted(t *testing.T) {
	alice, _, plc := create_appview_test_env(t)
	did := atproto_did(account_hosted_get(alice, "b1"))
	// A server whose registries know nothing of her.
	networks_database().exec("delete from actors")
	networks_database().exec("delete from identities")
	restore_hosted(alice.UID)
	if a := actor_get("f1"); a == nil || a.User != alice.UID || a.Network != "fediverse" {
		t.Errorf("THE POINT: the fediverse account's actor = %+v", a)
	}
	if user, _, _ := atproto_identity(did); user == nil || user.UID != alice.UID {
		t.Errorf("THE POINT: the Bluesky identity is not found as hers: %v", user)
	}

	// Onto another domain: the accounts were on one this server does not
	// serve, and none of her handles came with her.
	for _, id := range []string{"f1", "b1"} {
		data := account_data(account_hosted_get(alice, id))
		data["domain"] = "old.example"
		db_user(alice, "user").account_set(id, map[string]any{"data": json_encode(data)})
	}
	db_open("db/users.db").exec("delete from handles where user=?", alice.UID)
	networks_database().exec("delete from actors")
	networks_database().exec("delete from identities")
	plc.lock.Lock()
	before := len(plc.logs[did])
	plc.lock.Unlock()
	restore_hosted(alice.UID)
	if account_hosted_get(alice, "f1") != nil {
		t.Error("THE POINT: the fediverse account, whose address stays with its domain, was kept")
	}
	row := account_hosted_get(alice, "b1")
	if row == nil || row_string(row, "identifier") != "alice.example.com" || account_data(row)["domain"] != "example.com" {
		t.Fatalf("THE POINT: the Bluesky account carried over = %v", row)
	}
	plc.lock.Lock()
	log := plc.logs[did]
	plc.lock.Unlock()
	if len(log) != before+1 {
		t.Fatalf("THE POINT: operations on the identity = %d, want one more than %d", len(log), before)
	}
	last := log[len(log)-1]
	endpoint := last["services"].(map[string]any)["atproto_pds"].(map[string]any)["endpoint"]
	if fmt.Sprint(last["alsoKnownAs"]) != "[at://alice.example.com]" || endpoint != atproto_endpoint("example.com") {
		t.Errorf("THE POINT: the identity moved here = %v at %v", last["alsoKnownAs"], endpoint)
	}
	if h := handle_get("example.com", "alice"); h == nil || h.User != alice.UID {
		t.Errorf("THE POINT: the name here = %+v", h)
	}
	if user, _, _ := atproto_identity(did); user == nil || user.UID != alice.UID {
		t.Errorf("THE POINT: the carried identity is not found as hers")
	}
}
