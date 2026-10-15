// Mochi server: AT Protocol repositories
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// A hosted Bluesky account's data is a repository: its records, keyed by
// collection and record key, in a Merkle search tree whose root each commit
// signs with the account's key. The tree and the records are blocks addressed
// by content, kept in the user's own database, and the blobs the records
// reference are files in the user's directory. Every commit goes out on the
// server's update stream (atproto.go) as the diff of blocks a relay needs to
// verify it, paced under the relay's budget for a new server: a commit of a
// person's own post goes at once while the budget holds, a sourced post waits
// when it is tight, and what waits is written in one commit when the budget
// frees. The format comes from indigo's packages, so a repository Mochi
// writes is one every relay and client reads.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/atdata"
	atrepo "github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/repo/mst"
	"github.com/bluesky-social/indigo/atproto/syntax"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	"github.com/multiformats/go-multihash"
)

// The content ids a repository uses: DAG-CBOR for records, tree nodes and
// commits, raw for blobs, both over SHA-256.
var cid_dagcbor = cid.Prefix{Version: 1, Codec: cid.DagCBOR, MhType: multihash.SHA2_256, MhLength: 32}
var cid_raw = cid.Prefix{Version: 1, Codec: cid.Raw, MhType: multihash.SHA2_256, MhLength: 32}

// repository_clock issues commit revisions, which must rise within a
// repository; one clock for the server keeps them rising across all of them.
var repository_clock = syntax.NewTIDClock(0)

// repository_blob_maximum caps a blob an account stores: Bluesky's own limit
// for a video is fifty megabytes, and the apps cut media to the network's
// limits before uploading.
var repository_blob_maximum = 50 << 20

// A repository is a hosted account's row: its identifier, the root of its
// tree, its last commit and revision; root and head are empty before the
// first commit.
type repository struct {
	user    *User
	account string
	did     string
	root    string
	head    string
	rev     string
}

// A repository_operation is one change a commit makes: a record created or
// replaced (cid and data set) or deleted (cid nil). previous is filled in from
// the tree as the commit is made.
type repository_operation struct {
	path     string
	cid      *cid.Cid
	previous *cid.Cid
	data     []byte
}

// A repository_event is what a commit answered: its id, revision and the
// stream sequence it went out under.
type repository_event struct {
	head     string
	rev      string
	sequence int64
}

// repository_tables makes the tables a user's repositories live in, beside
// the accounts: the repositories themselves, their blocks, an index of their
// records, their blobs, and the operations waiting for the budget.
func repository_tables(db *DB) {
	db.exec("create table if not exists repositories (account text not null primary key, did text not null, root text not null default '', head text not null default '', rev text not null default '', created integer not null, updated integer not null default 0)")
	db.exec("create table if not exists nodes (account text not null, cid text not null, data blob not null, primary key (account, cid))")
	db.exec("create table if not exists records (account text not null, collection text not null, rkey text not null, cid text not null, created integer not null, updated integer not null default 0, primary key (account, collection, rkey))")
	db.exec("create table if not exists blobs (account text not null, cid text not null, type text not null default '', size integer not null default 0, created integer not null, primary key (account, cid))")
	db.exec("create table if not exists waiting (id text not null primary key, account text not null, path text not null, cid text not null default '', sourced integer not null default 0, created integer not null)")
	db.exec("create index if not exists waiting_account on waiting(account, created)")
}

// repository_cid is the content id of a block under a prefix.
func repository_cid(prefix cid.Prefix, data []byte) cid.Cid {
	c, _ := prefix.Sum(data)
	return c
}

// repository_bytes reads a blob column, which the driver may hand back as
// bytes or as text.
func repository_bytes(v any) []byte {
	switch value := v.(type) {
	case []byte:
		return value
	case string:
		return []byte(value)
	}
	return nil
}

// repository_create makes an account's repository row, with no commit yet.
func repository_create(user *User, account string, did string) *repository {
	db := db_user(user, "user")
	db.exec("insert or ignore into repositories (account, did, created) values (?, ?, ?)", account, did, now())
	return repository_get(user, account)
}

// repository_get reads an account's repository, or nil before it is made.
func repository_get(user *User, account string) *repository {
	row, _ := db_user(user, "user").row("select account, did, root, head, rev from repositories where account=?", account)
	if row == nil {
		return nil
	}
	return &repository{user: user, account: account, did: row_string(row, "did"), root: row_string(row, "root"), head: row_string(row, "head"), rev: row_string(row, "rev")}
}

// repository_delete removes an account's repository with its blocks, records,
// blobs and waiting operations.
func repository_delete(user *User, account string) {
	db := db_user(user, "user")
	rows, _ := db.rows("select cid from blobs where account=?", account)
	for _, row := range rows {
		os.Remove(repository_blob_path(user, row_string(row, "cid")))
	}
	for _, table := range []string{"repositories", "nodes", "records", "blobs", "waiting"} {
		db.exec("delete from "+table+" where account=?", account)
	}
}

// === Blocks ===

// repository_source reads an account's blocks for the tree loader.
type repository_source struct {
	db      *DB
	account string
}

func (s *repository_source) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	row, _ := s.db.row("select data from nodes where account=? and cid=?", s.account, c.String())
	if row == nil {
		return nil, fmt.Errorf("block %s not found", c)
	}
	return blocks.NewBlockWithCid(repository_bytes(row["data"]), c)
}

// repository_store collects the blocks a commit writes, in the order written,
// for the database and the event's archive. It is the blockstore the tree
// writes its changed nodes to.
type repository_store struct {
	order  []cid.Cid
	blocks map[cid.Cid][]byte
}

func repository_store_new() *repository_store {
	return &repository_store{blocks: map[cid.Cid][]byte{}}
}

func (s *repository_store) Put(ctx context.Context, block blocks.Block) error {
	c := block.Cid()
	if _, seen := s.blocks[c]; !seen {
		s.order = append(s.order, c)
	}
	s.blocks[c] = block.RawData()
	return nil
}

func (s *repository_store) PutMany(ctx context.Context, list []blocks.Block) error {
	for _, block := range list {
		s.Put(ctx, block)
	}
	return nil
}

func (s *repository_store) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	data, found := s.blocks[c]
	if !found {
		return nil, fmt.Errorf("block %s not found", c)
	}
	return blocks.NewBlockWithCid(data, c)
}

func (s *repository_store) Has(ctx context.Context, c cid.Cid) (bool, error) {
	_, found := s.blocks[c]
	return found, nil
}

func (s *repository_store) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	data, found := s.blocks[c]
	if !found {
		return 0, fmt.Errorf("block %s not found", c)
	}
	return len(data), nil
}

func (s *repository_store) DeleteBlock(ctx context.Context, c cid.Cid) error {
	delete(s.blocks, c)
	return nil
}

func (s *repository_store) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	ch := make(chan cid.Cid, len(s.order))
	for _, c := range s.order {
		ch <- c
	}
	close(ch)
	return ch, nil
}

func (s *repository_store) HashOnRead(enabled bool) {}

// tree loads the repository's tree: empty before the first commit.
func (r *repository) tree(ctx context.Context) (*mst.Tree, error) {
	if r.root == "" {
		tree := mst.NewEmptyTree()
		return &tree, nil
	}
	root, err := cid.Decode(r.root)
	if err != nil {
		return nil, err
	}
	return mst.LoadTreeFromStore(ctx, &repository_source{db_user(r.user, "user"), r.account}, root)
}

// repository_nodes lists the content ids of every node of a tree, computing
// any not yet computed.
func repository_nodes(node *mst.Node, out *[]cid.Cid) error {
	if node == nil {
		return nil
	}
	c := node.CID
	if c == nil || node.Dirty {
		data := node.NodeData()
		_, computed, err := data.Bytes()
		if err != nil {
			return err
		}
		c = computed
	}
	*out = append(*out, *c)
	for _, entry := range node.Entries {
		if entry.Child != nil {
			if err := repository_nodes(entry.Child, out); err != nil {
				return err
			}
		} else if entry.ChildCID != nil {
			*out = append(*out, *entry.ChildCID)
		}
	}
	return nil
}

// === Records ===

// repository_encode puts a record in the network's form: parsed under the
// data model, encoded as DAG-CBOR, addressed by its content.
func repository_encode(record map[string]any) (map[string]any, []byte, cid.Cid, error) {
	raw, err := json.Marshal(record)
	if err != nil {
		return nil, nil, cid.Undef, err
	}
	parsed, err := atdata.UnmarshalJSON(raw)
	if err != nil {
		return nil, nil, cid.Undef, fmt.Errorf("record: %w", err)
	}
	data, err := atdata.MarshalCBOR(parsed)
	if err != nil {
		return nil, nil, cid.Undef, err
	}
	return parsed, data, repository_cid(cid_dagcbor, data), nil
}

// repository_record reads a record of an account as it stands, committed or
// waiting: its content id and its value, or empty when there is none.
func repository_record(user *User, account string, collection string, rkey string) (string, map[string]any) {
	db := db_user(user, "user")
	row, _ := db.row("select n.cid, n.data from records r join nodes n on n.account=r.account and n.cid=r.cid where r.account=? and r.collection=? and r.rkey=?", account, collection, rkey)
	if row == nil {
		return "", nil
	}
	value, err := atdata.UnmarshalCBOR(repository_bytes(row["data"]))
	if err != nil {
		return "", nil
	}
	return row_string(row, "cid"), value
}

// repository_records lists an account's records of a collection, newest
// record key first (or oldest with reverse), from the cursor given, up to
// limit of them; the cursor is the record key to continue from.
func repository_records(user *User, account string, collection string, limit int, cursor string, reverse bool) ([]map[string]any, string) {
	db := db_user(user, "user")
	order, compare := "desc", "<"
	if reverse {
		order, compare = "asc", ">"
	}
	query := "select r.rkey, n.cid, n.data from records r join nodes n on n.account=r.account and n.cid=r.cid where r.account=? and r.collection=?"
	args := []any{account, collection}
	if cursor != "" {
		query += " and r.rkey" + compare + "?"
		args = append(args, cursor)
	}
	query += " order by r.rkey " + order + " limit ?"
	args = append(args, limit+1)
	rows, _ := db.rows(query, args...)
	var out []map[string]any
	next := ""
	for i, row := range rows {
		if i == limit {
			next = out[len(out)-1]["rkey"].(string)
			break
		}
		value, err := atdata.UnmarshalCBOR(repository_bytes(row["data"]))
		if err != nil {
			continue
		}
		out = append(out, map[string]any{"rkey": row_string(row, "rkey"), "cid": row_string(row, "cid"), "value": value})
	}
	return out, next
}

// repository_write records a change to an account's repository and commits
// it, or leaves it waiting when the budget is tight and the change may wait
// (a sourced post, decision 30). The record is readable at once either way;
// a relay learns of it with the commit. Answers the record's content id and
// whether the commit was made now.
func repository_write(ctx context.Context, r *repository, collection string, rkey string, record map[string]any, sourced bool) (string, bool, error) {
	_, data, c, err := repository_encode(record)
	if err != nil {
		return "", false, err
	}
	db := db_user(r.user, "user")
	db.exec("insert or ignore into nodes (account, cid, data) values (?, ?, ?)", r.account, c.String(), data)
	db.exec("insert into records (account, collection, rkey, cid, created, updated) values (?, ?, ?, ?, ?, 0) on conflict(account, collection, rkey) do update set cid=excluded.cid, updated=?", r.account, collection, rkey, c.String(), now(), now())
	path := collection + "/" + rkey
	if !atproto_budget_allows(sourced) {
		db.exec("insert into waiting (id, account, path, cid, sourced, created) values (?, ?, ?, ?, ?, ?)", uid(), r.account, path, c.String(), sourced, now())
		return c.String(), false, nil
	}
	_, err = repository_commit(ctx, r, append(repository_waiting(r), repository_operation{path: path, cid: &c, data: data}))
	return c.String(), err == nil, err
}

// repository_remove deletes a record, now or when the budget allows as
// repository_write does.
func repository_remove(ctx context.Context, r *repository, collection string, rkey string, sourced bool) (bool, error) {
	db := db_user(r.user, "user")
	db.exec("delete from records where account=? and collection=? and rkey=?", r.account, collection, rkey)
	path := collection + "/" + rkey
	db.exec("delete from waiting where account=? and path=?", r.account, path)
	if !atproto_budget_allows(sourced) {
		db.exec("insert into waiting (id, account, path, cid, sourced, created) values (?, ?, ?, '', ?, ?)", uid(), r.account, path, sourced, now())
		return false, nil
	}
	_, err := repository_commit(ctx, r, append(repository_waiting(r), repository_operation{path: path}))
	return err == nil, err
}

// repository_waiting reads the operations of a repository that wait for the
// budget, oldest first, each the last word on its path, and drops their
// rows: they go in the commit being made.
func repository_waiting(r *repository) []repository_operation {
	db := db_user(r.user, "user")
	rows, _ := db.rows("select id, path, cid from waiting where account=? order by created", r.account)
	latest := map[string]repository_operation{}
	var order []string
	for _, row := range rows {
		path := row_string(row, "path")
		if _, seen := latest[path]; !seen {
			order = append(order, path)
		}
		op := repository_operation{path: path}
		if text := row_string(row, "cid"); text != "" {
			if c, err := cid.Decode(text); err == nil {
				op.cid = &c
				if node, _ := db.row("select data from nodes where account=? and cid=?", r.account, text); node != nil {
					op.data = repository_bytes(node["data"])
				}
			}
		}
		latest[path] = op
		db.exec("delete from waiting where id=?", row_string(row, "id"))
	}
	var ops []repository_operation
	for _, path := range order {
		ops = append(ops, latest[path])
	}
	return ops
}

// repository_key is the account's signing key in the form the commit signs
// with.
func repository_key(user *User, account string) (*atcrypto.PrivateKeyP256, error) {
	key := keypair_get(user, account, key_signing)
	if key == nil {
		return nil, fmt.Errorf("account has no signing key")
	}
	private, err := key_p256_private(key.Private)
	if err != nil {
		return nil, err
	}
	return atcrypto.ParsePrivateBytesP256(private.D.FillBytes(make([]byte, 32)))
}

// repository_commit applies operations to the tree, signs the new root as a
// commit, keeps the blocks, drops the blocks no longer reached, and sends the
// commit out on the stream with the blocks a relay needs: the commit, the
// changed nodes and the records written. A commit with no operations
// re-signs the tree, which is how a new signing key is announced.
func repository_commit(ctx context.Context, r *repository, ops []repository_operation) (*repository_event, error) {
	key, err := repository_key(r.user, r.account)
	if err != nil {
		return nil, err
	}
	tree, err := r.tree(ctx)
	if err != nil {
		return nil, fmt.Errorf("repository %s: %w", r.account, err)
	}
	for i := range ops {
		applied, err := atrepo.ApplyOp(tree, ops[i].path, ops[i].cid)
		if err != nil {
			return nil, fmt.Errorf("record %s: %w", ops[i].path, err)
		}
		ops[i].previous = applied.Prev
	}
	store := repository_store_new()
	root, err := tree.WriteDiffBlocks(ctx, store)
	if err != nil {
		return nil, err
	}
	commit := atrepo.Commit{DID: r.did, Version: atrepo.ATPROTO_REPO_VERSION, Data: *root, Rev: repository_clock.Next().String()}
	if err := commit.Sign(key); err != nil {
		return nil, err
	}
	var encoded bytes.Buffer
	if err := commit.MarshalCBOR(&encoded); err != nil {
		return nil, err
	}
	head := repository_cid(cid_dagcbor, encoded.Bytes())

	// The blocks, in the database and in the event's archive: the commit
	// first as its root, then the nodes changed, then the records written.
	db := db_user(r.user, "user")
	db.exec("insert or ignore into nodes (account, cid, data) values (?, ?, ?)", r.account, head.String(), encoded.Bytes())
	var archive bytes.Buffer
	if err := car.WriteHeader(&car.CarHeader{Roots: []cid.Cid{head}, Version: 1}, &archive); err != nil {
		return nil, err
	}
	carutil.LdWrite(&archive, head.Bytes(), encoded.Bytes())
	for _, c := range store.order {
		data := store.blocks[c]
		db.exec("insert or ignore into nodes (account, cid, data) values (?, ?, ?)", r.account, c.String(), data)
		carutil.LdWrite(&archive, c.Bytes(), data)
	}
	written := map[cid.Cid]bool{}
	for _, op := range ops {
		if op.cid != nil && op.data != nil && !written[*op.cid] {
			written[*op.cid] = true
			db.exec("insert or ignore into nodes (account, cid, data) values (?, ?, ?)", r.account, op.cid.String(), op.data)
			carutil.LdWrite(&archive, op.cid.Bytes(), op.data)
		}
	}

	previous_root, previous_rev := r.root, r.rev
	r.root, r.head, r.rev = root.String(), head.String(), commit.Rev
	db.exec("update repositories set root=?, head=?, rev=?, updated=? where account=?", r.root, r.head, r.rev, now(), r.account)
	repository_prune(r, tree)

	// The event: what changed, with the previous root for a relay to check
	// the diff against (sync 1.1), and the previous revision.
	var changes []any
	for _, op := range ops {
		action := "create"
		switch {
		case op.cid == nil:
			action = "delete"
		case op.previous != nil:
			action = "update"
		}
		change := map[string]any{"action": action, "path": op.path, "cid": nil}
		if op.cid != nil {
			change["cid"] = atdata.CIDLink(*op.cid)
		}
		if op.previous != nil {
			change["prev"] = atdata.CIDLink(*op.previous)
		}
		changes = append(changes, change)
	}
	if changes == nil {
		changes = []any{}
	}
	body := map[string]any{
		"rebase": false,
		"tooBig": false,
		"repo":   r.did,
		"commit": atdata.CIDLink(head),
		"rev":    commit.Rev,
		"since":  nil,
		"blocks": atdata.Bytes(archive.Bytes()),
		"ops":    changes,
		"blobs":  []any{},
		"time":   time.Now().UTC().Format(time.RFC3339Nano),
	}
	if previous_rev != "" {
		body["since"] = previous_rev
	}
	if previous_root != "" {
		if c, err := cid.Decode(previous_root); err == nil {
			body["prevData"] = atdata.CIDLink(c)
		}
	}
	sequence := stream_append(r.did, "#commit", body)
	return &repository_event{head: r.head, rev: r.rev, sequence: sequence}, nil
}

// repository_prune drops the blocks of an account that nothing reaches any
// more: not the commit, not a node of the tree, not a record in it, and not
// a record waiting to be committed.
func repository_prune(r *repository, tree *mst.Tree) {
	live := map[string]bool{r.head: true}
	var nodes []cid.Cid
	if err := repository_nodes(tree.Root, &nodes); err != nil {
		return
	}
	for _, c := range nodes {
		live[c.String()] = true
	}
	tree.Walk(func(key []byte, value cid.Cid) error {
		live[value.String()] = true
		return nil
	})
	db := db_user(r.user, "user")
	rows, _ := db.rows("select cid from records where account=? union select cid from waiting where account=? and cid!=''", r.account, r.account)
	for _, row := range rows {
		live[row_string(row, "cid")] = true
	}
	rows, _ = db.rows("select cid from nodes where account=?", r.account)
	for _, row := range rows {
		if c := row_string(row, "cid"); !live[c] {
			db.exec("delete from nodes where account=? and cid=?", r.account, c)
		}
	}
}

// repository_archive is the whole repository as a CAR: the commit as its
// root, every node of the tree and every record. Empty before the first
// commit.
func repository_archive(ctx context.Context, r *repository) ([]byte, error) {
	if r.head == "" {
		return nil, fmt.Errorf("no commit")
	}
	head, err := cid.Decode(r.head)
	if err != nil {
		return nil, err
	}
	tree, err := r.tree(ctx)
	if err != nil {
		return nil, err
	}
	var ids []cid.Cid
	if err := repository_nodes(tree.Root, &ids); err != nil {
		return nil, err
	}
	tree.Walk(func(key []byte, value cid.Cid) error {
		ids = append(ids, value)
		return nil
	})
	source := &repository_source{db_user(r.user, "user"), r.account}
	var archive bytes.Buffer
	if err := car.WriteHeader(&car.CarHeader{Roots: []cid.Cid{head}, Version: 1}, &archive); err != nil {
		return nil, err
	}
	seen := map[cid.Cid]bool{}
	for _, c := range append([]cid.Cid{head}, ids...) {
		if seen[c] {
			continue
		}
		seen[c] = true
		block, err := source.Get(ctx, c)
		if err != nil {
			return nil, err
		}
		carutil.LdWrite(&archive, c.Bytes(), block.RawData())
	}
	return archive.Bytes(), nil
}

// === Blobs ===

// repository_blob_path is where a blob's bytes live: in the user's own
// directory, named by content.
func repository_blob_path(user *User, id string) string {
	return filepath.Join(user_storage_dir(user), "blobs", id)
}

// repository_blob_store keeps a blob for an account and answers its content
// id; the same bytes stored twice are one blob.
func repository_blob_store(user *User, account string, data []byte, kind string) (string, error) {
	if len(data) == 0 || len(data) > repository_blob_maximum {
		return "", fmt.Errorf("a blob must be between 1 and %d bytes", repository_blob_maximum)
	}
	id := repository_cid(cid_raw, data).String()
	path := repository_blob_path(user, id)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	if _, err := os.Stat(path); err != nil {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			return "", err
		}
	}
	if kind == "" {
		kind = "application/octet-stream"
	}
	db_user(user, "user").exec("insert or ignore into blobs (account, cid, type, size, created) values (?, ?, ?, ?, ?)", account, id, kind, len(data), now())
	return id, nil
}

// repository_blob reads a blob of an account: its type and bytes, or false.
func repository_blob(user *User, account string, id string) (string, []byte, bool) {
	row, _ := db_user(user, "user").row("select type from blobs where account=? and cid=?", account, id)
	if row == nil || strings.ContainsAny(id, "/\\") {
		return "", nil, false
	}
	data, err := os.ReadFile(repository_blob_path(user, id))
	if err != nil {
		return "", nil, false
	}
	return row_string(row, "type"), data, true
}

// repository_blob_reference is how a record names a blob.
func repository_blob_reference(id string, kind string, size int) map[string]any {
	return map[string]any{"$type": "blob", "ref": map[string]any{"$link": id}, "mimeType": kind, "size": size}
}

// === The budget ===
//
// The relay admits a new server at 1,500 events an hour and 10,000 a day
// (decision 30). Every event this server sends counts, so the stream's own
// rows are the measure. A person's own writes go out while the budget holds;
// a sourced post waits once four fifths of it is spent, and the manager
// writes what waits as the budget frees, one commit per account.

var atproto_budget_hour int64 = 1500
var atproto_budget_day int64 = 10000

// atproto_budget_used counts the events of the last hour and the last day.
func atproto_budget_used() (int64, int64) {
	db := networks_database()
	hour, _ := db.row("select count(*) as n from stream where created>?", now()-3600)
	day, _ := db.row("select count(*) as n from stream where created>?", now()-86400)
	return row_int(hour, "n"), row_int(day, "n")
}

// atproto_budget_allows says whether a commit may go out now: under the
// limits for a person's own write, under four fifths of them for a sourced
// one.
func atproto_budget_allows(sourced bool) bool {
	hour, day := atproto_budget_used()
	share := 1.0
	if sourced {
		share = 0.8
	}
	return float64(hour) < float64(atproto_budget_hour)*share && float64(day) < float64(atproto_budget_day)*share
}

var atproto_drain_lock sync.Mutex

// atproto_drain commits what waits, account by account, oldest first, for as
// long as the budget allows; an account whose oldest wait is a person's own
// write goes before one whose waits are all sourced. Answers how many
// commits it made.
func atproto_drain(ctx context.Context) int {
	atproto_drain_lock.Lock()
	defer atproto_drain_lock.Unlock()
	made := 0
	for _, row := range atproto_waiting_accounts() {
		user := user_by_uid(row_string(row, "user"))
		if user == nil {
			continue
		}
		account := row_string(row, "account")
		sourced := row_int(row, "own") == 0
		if !atproto_budget_allows(sourced) {
			break
		}
		r := repository_get(user, account)
		if r == nil {
			db_user(user, "user").exec("delete from waiting where account=?", account)
			continue
		}
		ops := repository_waiting(r)
		if len(ops) == 0 {
			continue
		}
		if _, err := repository_commit(ctx, r, ops); err != nil {
			warn("Repository %s: %v", account, err)
			continue
		}
		made++
	}
	return made
}

// atproto_waiting_accounts lists the accounts with waiting operations across
// every user with a hosted Bluesky account: user, account, whether any wait
// is a person's own write, and the oldest wait; those with an own write
// first, then oldest first.
func atproto_waiting_accounts() []map[string]any {
	var out []map[string]any
	rows, _ := networks_database().rows("select distinct user from identities")
	for _, row := range rows {
		user := user_by_uid(row_string(row, "user"))
		if user == nil {
			continue
		}
		waits, _ := db_user(user, "user").rows("select account, max(case when sourced=0 then 1 else 0 end) as own, min(created) as oldest from waiting group by account")
		for _, wait := range waits {
			wait["user"] = user.UID
			out = append(out, wait)
		}
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0; j-- {
			a, b := out[j-1], out[j]
			if row_int(a, "own") > row_int(b, "own") || (row_int(a, "own") == row_int(b, "own") && row_int(a, "oldest") <= row_int(b, "oldest")) {
				break
			}
			out[j-1], out[j] = b, a
		}
	}
	return out
}

// atproto_manager writes what waits as the budget frees, every half minute,
// and drops stream rows past the replay window once an hour.
func atproto_manager() {
	go func() {
		for range time.Tick(30 * time.Second) {
			if network_enabled("bluesky") {
				atproto_drain(context.Background())
			}
		}
	}()
	for range time.Tick(time.Hour) {
		stream_cleanup()
	}
}
