// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Decryption in core (the federation plan, phase 27): one encryption device
// for each of the user's Matrix and XMPP accounts, held here. An encrypted
// message is decrypted as it arrives and handed to the apps as the message it
// carries; what an app sends to an encrypted conversation is encrypted. The
// apps see plaintext conversations and never a key.
//
// Matrix: Olm and Megolm through mautrix-go's crypto package. A hosted
// account's device is one of the engine's own key store (keyring.go), its
// requests answered in process; a linked account's is the device of the
// session core signs in, its requests sent to the account's server. Its
// store is the user's encryption.db. OMEMO is omemo.go's.

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/crypto"
	"maunium.net/go/mautrix/crypto/attachment"
	"maunium.net/go/mautrix/crypto/backup"
	"maunium.net/go/mautrix/crypto/signatures"
	"maunium.net/go/mautrix/crypto/ssss"
	"maunium.net/go/mautrix/crypto/verificationhelper"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// An encryption_matrix is the device of one of a user's Matrix accounts.
type encryption_matrix struct {
	user    *User
	account string
	address string
	device  string
	hosted  *matrix_hosted
	linked  *linked
	client  *mautrix.Client
	machine *crypto.OlmMachine
	rooms   *encryption_rooms
	syncer  *mautrix.DefaultSyncer
	helper  *verificationhelper.VerificationHelper

	// lock holds the device while it takes what was sent to it, so a hosted
	// device's deliveries are read once.
	lock sync.Mutex
	// The verifications under way, as the apps are shown them, and those in
	// rooms this device takes part in: one it asked for or accepted.
	verifying sync.Map
	taking    sync.Map
	// The other side's MAC in a room's verification, held until the user
	// confirms the emoji, and the flows confirmed. The library's confirm,
	// taken after the MAC came, checks the other person's master key
	// against this account's own and fails; the MAC taken after the
	// confirm checks it against the key the MAC names.
	held      sync.Map
	confirmed sync.Map
	// The steps of verifications in rooms and the user's own, taken one at
	// a time in the order they came, away from what read them: a hosted
	// room's events are read with the room held, and answering a step sends
	// to it; and a step of the user's finishes before the other side's
	// answer to it is taken.
	steps   chan func(context.Context)
	stepped sync.Mutex
	ended   bool
}

var (
	encryption_lock     sync.Mutex
	encryption_opening  sync.Mutex
	encryption_matrices = map[string]*encryption_matrix{}
	encryption_stores   = map[string]*dbutil.Database{}
	// encryption_running counts the devices' work under way away from what
	// asked for it.
	encryption_running sync.WaitGroup
)

// encryption_log is where the crypto library's warnings go: core's log.
type encryption_log struct{}

func (encryption_log) Write(p []byte) (int, error) {
	var line struct {
		Message string `json:"message"`
		Error   string `json:"error"`
	}
	json.Unmarshal(p, &line)
	// Each of the library's messages its own, so one repeating does not
	// hide the others.
	debug("Encryption: "+strings.ReplaceAll(line.Message, "%", "%%")+" %s", line.Error)
	return len(p), nil
}

var encryption_logger = zerolog.New(encryption_log{}).Level(zerolog.WarnLevel)

// encryption_store is the user's encryption database, where each of their
// accounts' devices keeps its keys and sessions.
func encryption_store(user *User) (*dbutil.Database, error) {
	encryption_lock.Lock()
	defer encryption_lock.Unlock()
	if store := encryption_stores[user.UID]; store != nil {
		return store, nil
	}
	db := encryption_database(user)
	if db == nil {
		return nil, fmt.Errorf("the encryption database cannot be opened")
	}
	store, err := dbutil.NewWithDB(db.internal.DB, "sqlite3")
	if err != nil {
		return nil, err
	}
	encryption_stores[user.UID] = store
	return store, nil
}

// The encryption databases opened whose tables are made, by their handle: a
// database opened again, as after its user went, has them made again.
var encryption_made sync.Map

// encryption_database is the user's encryption database as core's own, its
// tables made the first time: the Matrix devices, the events waiting for
// their keys and the keys of files sent encrypted here, and the OMEMO
// devices' (omemo.go). The crypto library's own are its to make.
func encryption_database(user *User) *DB {
	db := db_open(fmt.Sprintf("users/%s/encryption.db", user.UID))
	if db == nil {
		return nil
	}
	if _, made := encryption_made.Load(db); !made {
		db.exec("create table if not exists devices (account text not null primary key, device text not null, since text not null default '', created integer not null)")
		db.exec("create table if not exists pending (account text not null, room text not null, session text not null, event text not null, created integer not null)")
		db.exec("create index if not exists pending_session on pending(account, room, session)")
		db.exec("create table if not exists files (account text not null, url text not null, file text not null, created integer not null, primary key (account, url))")
		omemo_tables(db)
		encryption_made.Store(db, true)
	}
	return db
}

// encryption_pickle is the key a device's pickled keys are kept under.
func encryption_pickle(user *User) []byte {
	sum := sha256.Sum256([]byte("mochi encryption " + user.UID))
	return sum[:]
}

// encryption_matrix_of is the device of a user's Matrix account, opened the
// first time: made on the account's server, its keys uploaded.
func encryption_matrix_of(user *User, account string) (*encryption_matrix, error) {
	key := user.UID + "|" + account
	encryption_lock.Lock()
	d := encryption_matrices[key]
	encryption_lock.Unlock()
	if d != nil {
		return d, nil
	}
	// Devices are opened one at a time: two machines over one store would
	// each make and upload keys.
	encryption_opening.Lock()
	defer encryption_opening.Unlock()
	encryption_lock.Lock()
	d = encryption_matrices[key]
	encryption_lock.Unlock()
	if d != nil {
		return d, nil
	}
	row, _ := db_user(user, "user").row("select * from accounts where id=?", account)
	d = &encryption_matrix{user: user, account: account}
	if h := matrix_hosted_of(user, row); h != nil {
		d.hosted, d.address = h, h.address
	} else if l := linked_from(user, row); l != nil && l.kind == "matrix" {
		d.linked, d.address = l, l.get("id")
	} else {
		return nil, fmt.Errorf("not a Matrix account")
	}
	store, err := encryption_store(user)
	if err != nil {
		return nil, err
	}
	db := encryption_database(user)
	if found, _ := db.row("select device, since from devices where account=?", account); found != nil {
		d.device = row_string(found, "device")
	}
	switch {
	case d.device != "":
	case d.hosted != nil:
		d.device = keyring_device(d.address, "Mochi")
		db.exec("insert or replace into devices (account, device, created) values (?, ?, ?)", account, d.device, now())
	default:
		d.device = d.linked.get("device")
		if d.device == "" {
			return nil, fmt.Errorf("the account's session has no device")
		}
		db.exec("insert or replace into devices (account, device, created) values (?, ?, ?)", account, d.device, now())
	}
	base := "https://mochi.invalid"
	if d.linked != nil {
		base = d.linked.get("base")
	}
	client, err := mautrix.NewClient(base, id.UserID(d.address), "")
	if err != nil {
		return nil, err
	}
	client.DeviceID = id.DeviceID(d.device)
	// No bound on the whole call: request bounds each attempt, and waits
	// between them for the account's budget.
	client.Client = &http.Client{Transport: &encryption_transport{d: d}}
	client.Log = encryption_logger
	d.client = client

	cryptostore := crypto.NewSQLCryptoStore(store, dbutil.NoopLogger, account, id.DeviceID(d.device), encryption_pickle(user))
	ctx := context.Background()
	if err := cryptostore.DB.Upgrade(ctx); err != nil {
		return nil, err
	}
	memory, _ := mautrix.NewMemoryStateStore().(*mautrix.MemoryStateStore)
	d.rooms = &encryption_rooms{MemoryStateStore: memory, d: d}
	client.StateStore = d.rooms
	d.machine = crypto.NewOlmMachine(client, &encryption_logger, cryptostore, d.rooms)
	// Keys go to every device of a room's people the user has not
	// distrusted, as other clients share them.
	d.machine.ShareKeysMinTrust = id.TrustStateUnset
	d.machine.SessionReceived = d.received
	if err := d.machine.Load(ctx); err != nil {
		return nil, err
	}
	client.Crypto = &encryption_crypto{d: d}
	d.syncer = mautrix.NewDefaultSyncer()
	d.syncer.ParseEventContent = true
	d.syncer.OnSync(d.machine.ProcessSyncResponse)
	client.Syncer = d.syncer
	d.helper = verificationhelper.NewVerificationHelper(client, d.machine, nil, d, false, false, true)
	if err := d.helper.Init(ctx); err != nil {
		return nil, err
	}
	if err := d.machine.ShareKeys(ctx, -1); err != nil {
		return nil, fmt.Errorf("the device's keys were not uploaded: %w", err)
	}
	d.restore(ctx)
	encryption_lock.Lock()
	if existing := encryption_matrices[key]; existing != nil {
		encryption_lock.Unlock()
		return existing, nil
	}
	encryption_matrices[key] = d
	encryption_lock.Unlock()
	if d.hosted != nil {
		d.poll(ctx)
	}
	return d, nil
}

// encryption_drop_user forgets the devices held for a user, as the user
// goes with their databases.
func encryption_drop_user(uid string) {
	encryption_lock.Lock()
	for key, d := range encryption_matrices {
		if strings.HasPrefix(key, uid+"|") {
			d.end()
			delete(encryption_matrices, key)
		}
	}
	delete(encryption_stores, uid)
	encryption_lock.Unlock()
	omemo_lock.Lock()
	for key := range omemo_devices {
		if strings.HasPrefix(key, uid+"|") {
			delete(omemo_devices, key)
		}
	}
	omemo_lock.Unlock()
}

// encryption_forget forgets the encryption device of an account as it goes:
// its keys, its sessions and what waited for it, and a hosted Matrix
// account's device in the key store, of which the account's rooms are told.
func encryption_forget(user *User, row map[string]any) {
	account := row_string(row, "id")
	encryption_lock.Lock()
	if d := encryption_matrices[user.UID+"|"+account]; d != nil {
		d.end()
	}
	delete(encryption_matrices, user.UID+"|"+account)
	encryption_lock.Unlock()
	omemo_device_close(user, account)
	db := encryption_database(user)
	if db == nil {
		return
	}
	if h := matrix_hosted_of(user, row); h != nil {
		if found, _ := db.row("select device from devices where account=?", account); found != nil {
			keyring_delete(h.address, row_string(found, "device"))
		}
	}
	for _, table := range []string{"devices", "pending", "files"} {
		db.exec("delete from "+table+" where account=?", account)
	}
	tables, _ := db.rows("select name from sqlite_master where type='table' and name like 'crypto_%'")
	for _, table := range tables {
		name := row_string(table, "name")
		if has, _ := db.exists("select 1 from pragma_table_info(?) where name='account_id'", name); has {
			db.exec("delete from "+name+" where account_id=?", account)
		}
	}
}

// === Requests ===

// An encryption_transport carries a device's requests: a hosted account's to
// the key store in process, a linked account's to its server with its
// session's credential.
type encryption_transport struct {
	d *encryption_matrix
}

func (t *encryption_transport) RoundTrip(r *http.Request) (*http.Response, error) {
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(io.LimitReader(r.Body, keyring_body_maximum+1))
		r.Body.Close()
	}
	status, answer, err := t.d.request(r.Context(), r.Method, r.URL, body)
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(answer)), Request: r}, nil
}

// request sends one request of the device's, by its address on the client
// interface. A linked account's request over the account's budget on its
// server waits for the next window rather than failing: setting up recovery
// and a verification each take a burst of requests, and a step that failed
// would leave the other side waiting.
func (d *encryption_matrix) request(ctx context.Context, method string, address *url.URL, body []byte) (int, []byte, error) {
	if d.linked != nil {
		target := d.linked.get("base") + address.EscapedPath()
		if address.RawQuery != "" {
			target += "?" + address.RawQuery
		}
		headers := map[string]string{}
		if len(body) > 0 {
			headers["Content-Type"] = "application/json"
		}
		for {
			status, data, _, err := d.linked.request(ctx, method, target, headers, body)
			var limited *RateLimitError
			if !errors.As(err, &limited) {
				return status, data, err
			}
			select {
			case <-ctx.Done():
				return 0, nil, err
			case <-time.After(time.Duration(max(limited.Retry, 1)) * time.Second):
			}
		}
	}
	path := strings.TrimPrefix(address.EscapedPath(), "/_matrix/client/v3/")
	parts := strings.Split(path, "/")
	for i, part := range parts {
		if unescaped, err := url.PathUnescape(part); err == nil {
			parts[i] = unescaped
		}
	}
	// A room's members and state, which the engine holds.
	if len(parts) >= 3 && parts[0] == "rooms" && method == "GET" {
		switch {
		case parts[2] == "joined_members" || parts[2] == "members":
			members := map[string]any{}
			state, _ := rooms_current(parts[1])
			for member, membership := range rooms_members(state) {
				if membership == spec.Join || (parts[2] == "members" && membership == spec.Invite) {
					members[member] = map[string]any{}
				}
			}
			return 200, []byte(json_encode(map[string]any{"joined": members})), nil
		case len(parts) == 5 && parts[2] == "state":
			if content := encryption_room_state(parts[1], parts[3]); content != nil {
				return 200, []byte(json_encode(content)), nil
			}
			return 404, []byte(`{"errcode":"M_NOT_FOUND","error":"No such state"}`), nil
		}
	}
	if address.RawQuery != "" {
		path += "?" + address.RawQuery
	}
	answer := keyring_request(ctx, d.account, d.address, d.device, method, path, body)
	return answer.status, []byte(json_encode(answer.body)), nil
}

// encryption_room_state is the content of a hosted room's state event of a
// type, or nil.
func encryption_room_state(room string, kind string) map[string]any {
	state, _ := rooms_current(room)
	event := state[rooms_key(kind, "")]
	if event == "" {
		return nil
	}
	pdu := rooms_events([]string{event})[event]
	if pdu == nil {
		return nil
	}
	var content map[string]any
	json.Unmarshal(pdu.Content(), &content)
	return content
}

// === Rooms ===

// encryption_rooms is what the device knows of rooms: whether each is
// encrypted and who is in it, from the engine's state for a hosted account
// and the account's server for a linked one.
type encryption_rooms struct {
	*mautrix.MemoryStateStore
	d *encryption_matrix
}

func (r *encryption_rooms) IsEncrypted(ctx context.Context, room id.RoomID) (bool, error) {
	content, err := r.GetEncryptionEvent(ctx, room)
	return content != nil, err
}

func (r *encryption_rooms) GetEncryptionEvent(ctx context.Context, room id.RoomID) (*event.EncryptionEventContent, error) {
	if content, _ := r.MemoryStateStore.GetEncryptionEvent(ctx, room); content != nil {
		return content, nil
	}
	var raw map[string]any
	if r.d.hosted != nil {
		raw = encryption_room_state(room.String(), "m.room.encryption")
	} else {
		code, body, err := r.d.request(ctx, "GET", &url.URL{Path: "/_matrix/client/v3/rooms/" + room.String() + "/state/m.room.encryption/"}, nil)
		if err != nil {
			return nil, err
		}
		if code == 200 {
			json.Unmarshal(body, &raw)
		}
	}
	if raw == nil {
		return nil, nil
	}
	var content event.EncryptionEventContent
	json.Unmarshal([]byte(json_encode(raw)), &content)
	if content.Algorithm == "" {
		return nil, nil
	}
	r.MemoryStateStore.SetEncryptionEvent(ctx, room, &content)
	return &content, nil
}

func (r *encryption_rooms) GetRoomJoinedOrInvitedMembers(ctx context.Context, room id.RoomID) ([]id.UserID, error) {
	code, body, err := r.d.request(ctx, "GET", &url.URL{Path: "/_matrix/client/v3/rooms/" + room.String() + "/joined_members"}, nil)
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, fmt.Errorf("the room's members could not be read: %d", code)
	}
	var answer struct {
		Joined map[id.UserID]any `json:"joined"`
	}
	json.Unmarshal(body, &answer)
	members := make([]id.UserID, 0, len(answer.Joined))
	for member := range answer.Joined {
		members = append(members, member)
	}
	slices.Sort(members)
	return members, nil
}

// === What is sent to the device ===

// poll takes what was sent to a hosted account's device since it last
// looked: messages to it, whose devices changed, its key counts.
func (d *encryption_matrix) poll(ctx context.Context) {
	if d.hosted == nil {
		return
	}
	d.lock.Lock()
	defer d.lock.Unlock()
	db := encryption_database(d.user)
	row, _ := db.row("select since from devices where account=?", d.account)
	since := row_string(row, "since")
	for range 10 {
		synced := keyring_sync(d.address, d.device, since)
		next, _ := synced["next"].(string)
		resp := &mautrix.RespSync{NextBatch: next}
		events, _ := synced["events"].([]map[string]any)
		for _, e := range events {
			kind, _ := e["type"].(string)
			sender, _ := e["sender"].(string)
			evt := &event.Event{Sender: id.UserID(sender), Type: event.Type{Type: kind, Class: event.ToDeviceEventType}}
			evt.Content.VeryRaw = json.RawMessage(json_encode(e["content"]))
			resp.ToDevice.Events = append(resp.ToDevice.Events, evt)
		}
		changed, _ := synced["changed"].([]string)
		for _, user := range changed {
			resp.DeviceLists.Changed = append(resp.DeviceLists.Changed, id.UserID(user))
		}
		counts, _ := synced["counts"].(map[string]any)
		resp.DeviceOTKCount.SignedCurve25519 = int(as_int64(counts["signed_curve25519"]))
		fallback, _ := synced["fallback"].([]string)
		for _, algorithm := range fallback {
			resp.FallbackKeys = append(resp.FallbackKeys, id.KeyAlgorithm(algorithm))
		}
		d.syncer.ProcessResponse(ctx, resp, since)
		db.exec("update devices set since=? where account=?", next, d.account)
		if next == since || (len(events) == 0 && len(changed) == 0) {
			return
		}
		since = next
	}
}

// encryption_delivered tells a hosted account's device here that something
// was sent to it, which it takes at once.
func encryption_delivered(address string, device string) {
	user, account := federation_user(address)
	if user == nil {
		return
	}
	row, _ := encryption_database(user).row("select device from devices where account=?", account)
	if row_string(row, "device") != device {
		return
	}
	encryption_running.Go(func() {
		if d, err := encryption_matrix_of(user, account); err == nil {
			d.poll(context.Background())
		}
	})
}

// sync takes what a linked account's sync brought for its device.
func (d *encryption_matrix) sync(ctx context.Context, raw []byte, since string) {
	var resp mautrix.RespSync
	if json.Unmarshal(raw, &resp) != nil {
		return
	}
	d.lock.Lock()
	defer d.lock.Unlock()
	// Rooms' state, which tells the device which are encrypted and who is
	// in them; the timeline is the account's sync loop's to read.
	for room, joined := range resp.Rooms.Join {
		for _, e := range joined.State.Events {
			e.RoomID = room
			mautrix.UpdateStateStore(ctx, d.rooms, e)
		}
		for _, e := range joined.Timeline.Events {
			if e.StateKey != nil {
				e.RoomID = room
				mautrix.UpdateStateStore(ctx, d.rooms, e)
			}
		}
		joined.Timeline.Events = nil
	}
	d.syncer.ProcessResponse(ctx, &resp, since)
}

// === Decrypting ===

// decrypt reads an encrypted event of a room: the event it carries, or nil
// where its key has not come, the event then kept until it does.
func (d *encryption_matrix) decrypt(ctx context.Context, room string, e *matrix_event) *matrix_event {
	if d.hosted != nil {
		d.poll(ctx)
	}
	evt := d.parse(room, e)
	if evt == nil {
		return nil
	}
	decrypted, err := d.machine.DecryptMegolmEvent(ctx, evt)
	if err != nil {
		if errors.Is(err, crypto.ErrNoSessionFound) {
			// It waits for its key, for thirty days at most.
			if content, ok := evt.Content.Parsed.(*event.EncryptedEventContent); ok {
				db := encryption_database(d.user)
				db.exec("delete from pending where account=? and created<?", d.account, now()-30*86400)
				db.exec("insert into pending (account, room, session, event, created) values (?, ?, ?, ?, ?)", d.account, room, content.SessionID.String(), json_encode(e), now())
			}
		} else {
			debug("Encryption: account %s: event %s in %s not decrypted: %v", d.account, e.ID, room, err)
		}
		return nil
	}
	out := *e
	out.Type = decrypted.Type.Type
	out.Content = decrypted.Content.Raw
	if out.Content == nil {
		json.Unmarshal(decrypted.Content.VeryRaw, &out.Content)
	}
	return &out
}

// parse is a room's event as the library reads it.
func (d *encryption_matrix) parse(room string, e *matrix_event) *event.Event {
	evt := &event.Event{Sender: id.UserID(e.Sender), ID: id.EventID(e.ID), Timestamp: e.Time, RoomID: id.RoomID(room), Type: event.Type{Type: e.Type, Class: event.MessageEventType}}
	evt.Content.VeryRaw = json.RawMessage(json_encode(e.Content))
	if err := evt.Content.ParseRaw(evt.Type); err != nil {
		return nil
	}
	return evt
}

// dispatch hands a room's verification event to the device.
func (d *encryption_matrix) dispatch(ctx context.Context, room string, e *matrix_event) {
	// A step after the request names its verification by the request it
	// relates to. It is this device's only where the device asked for it
	// or accepted it: one another device of the user's accepted is shown
	// taken there, and its steps left to that device.
	var request string
	if strings.HasPrefix(e.Type, "m.key.verification.") {
		relates, _ := e.Content["m.relates_to"].(map[string]any)
		request, _ = relates["event_id"].(string)
		if request == "" {
			return
		}
		if _, mine := d.taking.Load(request); !mine {
			if e.Sender == d.address && e.Type == "m.key.verification.ready" {
				if _, shown := d.verifying.Load(request); shown {
					d.verification(id.VerificationTransactionID(request), func(v *encryption_verification) {
						v.Phase, v.Reason = "cancelled", string(event.VerificationCancelCodeAccepted)
					})
				}
			}
			return
		}
	}
	if e.Sender == d.address {
		return
	}
	evt := d.parse(room, e)
	if evt == nil {
		return
	}
	// The library looks a step up by the request's id as the event's.
	if request != "" {
		evt.ID = id.EventID(request)
	}
	if !d.step(func(ctx context.Context) {
		if e.Type == "m.key.verification.mac" {
			if _, confirmed := d.confirmed.Load(request); !confirmed {
				d.held.Store(request, evt)
				return
			}
		}
		d.syncer.Dispatch(ctx, evt)
	}) {
		debug("Encryption: account %s: verification step %s dropped", d.account, e.ID)
	}
}

// step queues a step of a verification for the device's worker, reporting
// whether it was taken.
func (d *encryption_matrix) step(work func(context.Context)) bool {
	d.stepped.Lock()
	defer d.stepped.Unlock()
	if d.ended {
		return false
	}
	if d.steps == nil {
		d.steps = make(chan func(context.Context), 64)
		go func(steps chan func(context.Context)) {
			// The library's warnings go to core's log.
			ctx := encryption_logger.WithContext(context.Background())
			for work := range steps {
				work(ctx)
			}
		}(d.steps)
	}
	select {
	case d.steps <- work:
		return true
	default:
		return false
	}
}

// end ends the device's work, as its account goes.
func (d *encryption_matrix) end() {
	d.stepped.Lock()
	defer d.stepped.Unlock()
	d.ended = true
	if d.steps != nil {
		close(d.steps)
	}
}

// received reads again the events of a room that waited for a key now come,
// and hands them to the apps.
func (d *encryption_matrix) received(ctx context.Context, room id.RoomID, session id.SessionID, first uint32) {
	encryption_running.Go(func() { d.backup(context.Background(), room, session) })
	db := encryption_database(d.user)
	rows, _ := db.rows("select rowid, event from pending where account=? and room=? and session=?", d.account, room.String(), session.String())
	if len(rows) == 0 {
		return
	}
	encryption_running.Go(func() {
		var events []map[string]any
		for _, row := range rows {
			var e matrix_event
			if json.Unmarshal([]byte(row_string(row, "event")), &e) != nil {
				continue
			}
			db.exec("delete from pending where rowid=?", row_int(row, "rowid"))
			events = append(events, d.conversation(context.Background(), room.String(), &e)...)
		}
		if len(events) > 0 {
			linked_inbound(d.user, d.account, "matrix", events)
		}
	})
}

// conversation is what an event decrypted late is to the apps: a message
// that was shown waiting is filled in, anything else comes as itself.
func (d *encryption_matrix) conversation(ctx context.Context, room string, e *matrix_event) []map[string]any {
	reader := &matrix_state{user: d.address, typing: map[string][]string{}, direct: map[string]bool{}, names: map[string]string{}, download: content_address}
	if d.linked != nil {
		reader.download = func(mxc string) string { return matrix_download(d.linked, mxc) }
	} else {
		state, _ := rooms_current(room)
		reader.direct[room] = rooms_direct(d.address, room)
		reader.names = rooms_names(room, state)
	}
	reader.decrypt = func(ctx context.Context, room string, e *matrix_event) *matrix_event { return d.decrypt(ctx, room, e) }
	reader.remember, reader.verify = d.remember, d.dispatch
	message := reader.timeline(room, e)
	if message == nil {
		return nil
	}
	// The message was shown waiting when it came: now it is filled in.
	if message["kind"] == "message" {
		message["kind"] = "decrypted"
	}
	return []map[string]any{message}
}

// === Encrypting ===

// encrypt is the content an event of a room's is sent as: encrypted for its
// members' devices where the room is encrypted, else nil.
func (d *encryption_matrix) encrypt(ctx context.Context, room string, kind string, content map[string]any) (map[string]any, error) {
	encrypted, err := d.rooms.IsEncrypted(ctx, id.RoomID(room))
	if err != nil || !encrypted {
		return nil, err
	}
	sealed, err := d.client.Crypto.Encrypt(ctx, id.RoomID(room), event.Type{Type: kind, Class: event.MessageEventType}, content)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	json.Unmarshal([]byte(json_encode(sealed)), &out)
	return out, nil
}

// encryption_crypto is the device as the library's client uses it.
type encryption_crypto struct {
	d *encryption_matrix
}

func (c *encryption_crypto) Encrypt(ctx context.Context, room id.RoomID, kind event.Type, content any) (*event.EncryptedEventContent, error) {
	// Content already encrypted goes as it is: the library's verification
	// encrypts a request in a room and then sends it to be encrypted again.
	if sealed, ok := content.(*event.EncryptedEventContent); ok {
		return sealed, nil
	}
	sealed, err := c.d.machine.EncryptMegolmEvent(ctx, room, kind, content)
	if errors.Is(err, crypto.ErrSessionExpired) || errors.Is(err, crypto.ErrNoGroupSession) || errors.Is(err, crypto.ErrSessionNotShared) {
		members, err := c.d.rooms.GetRoomJoinedOrInvitedMembers(ctx, room)
		if err != nil {
			return nil, err
		}
		if err := c.d.machine.ShareGroupSession(ctx, room, members); err != nil {
			return nil, err
		}
		sealed, err = c.d.machine.EncryptMegolmEvent(ctx, room, kind, content)
		return sealed, err
	}
	return sealed, err
}

func (c *encryption_crypto) Decrypt(ctx context.Context, evt *event.Event) (*event.Event, error) {
	return c.d.machine.DecryptMegolmEvent(ctx, evt)
}

func (c *encryption_crypto) WaitForSession(ctx context.Context, room id.RoomID, sender id.SenderKey, session id.SessionID, timeout time.Duration) bool {
	return c.d.machine.WaitForSession(ctx, room, sender, session, timeout)
}

func (c *encryption_crypto) RequestSession(ctx context.Context, room id.RoomID, sender id.SenderKey, session id.SessionID, user id.UserID, device id.DeviceID) {
	c.d.machine.SendRoomKeyRequest(ctx, room, sender, session, "", map[id.UserID][]id.DeviceID{user: {device}})
}

func (c *encryption_crypto) Init(ctx context.Context) error {
	return nil
}

// === The key backup ===

// encryption_unbase64 reads base64 with or without its padding.
func encryption_unbase64(text string) []byte {
	if out, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(text, "=")); err == nil {
		return out
	}
	return nil
}

// backup puts a room key in the account's key backup, where one is set up
// and its key known here.
func (d *encryption_matrix) backup(ctx context.Context, room id.RoomID, session id.SessionID) {
	d.backup_put(ctx, map[id.RoomID][]id.SessionID{room: {session}})
}

// === Files ===

// remember keeps the key of an encrypted file a message carries, by the
// address the apps fetch it at, so the fetch is decrypted.
func (d *encryption_matrix) remember(address string, file map[string]any) {
	encryption_database(d.user).exec("insert or replace into files (account, url, file, created) values (?, ?, ?, ?)", d.account, address, json_encode(file), now())
}

// encryption_file is the key of a file sent encrypted to a Matrix room at an
// address, or nil.
func encryption_file(user *User, account string, address string) *event.EncryptedFileInfo {
	row, _ := encryption_database(user).row("select file from files where account=? and url=?", account, address)
	if row == nil {
		return nil
	}
	var file event.EncryptedFileInfo
	if json.Unmarshal([]byte(row_string(row, "file")), &file) != nil || file.Key.Key == "" {
		return nil
	}
	return &file
}

// encryption_shared is the key of a file shared encrypted on XMPP at an
// address, as its aesgcm address names it, or "".
func encryption_shared(user *User, account string, address string) string {
	row, _ := encryption_database(user).row("select file from files where account=? and url=?", account, address)
	var file struct {
		Key string `json:"aesgcm"`
	}
	if row == nil || json.Unmarshal([]byte(row_string(row, "file")), &file) != nil {
		return ""
	}
	return file.Key
}

// encryption_opened is a fetched file as the apps are given it: decrypted
// where it was sent encrypted.
func encryption_opened(user *User, account string, address string, data []byte) ([]byte, error) {
	if key := encryption_shared(user, account, address); key != "" {
		return omemo_file_open(key, data)
	}
	file := encryption_file(user, account, address)
	if file == nil {
		return data, nil
	}
	if err := file.PrepareForDecryption(); err != nil {
		return nil, err
	}
	plain := slices.Clone(data)
	if err := file.DecryptInPlace(plain); err != nil {
		return nil, err
	}
	return plain, nil
}

// encryption_sealed is a file to upload for an encrypted Matrix room:
// encrypted with a key of its own, answered with the key to keep once its
// address is known.
func encryption_sealed(data []byte) ([]byte, *event.EncryptedFileInfo) {
	file := &event.EncryptedFileInfo{EncryptedFile: *attachment.NewEncryptedFile()}
	return file.Encrypt(data), file
}

// === Sending ===

// encryption_seal is an event of a room as it is sent: encrypted for its
// members' devices where the room is encrypted, a file it names by an
// address uploaded encrypted named with its key.
func encryption_seal(ctx context.Context, user *User, account string, room string, kind string, content map[string]any) (string, map[string]any, error) {
	d, err := encryption_matrix_of(user, account)
	if err != nil {
		if encryption_room_encrypted(ctx, user, account, room) {
			return "", nil, fmt.Errorf("the room is encrypted and the account's device is unavailable: %w", err)
		}
		return kind, content, nil
	}
	if mxc, _ := content["url"].(string); mxc != "" {
		if file := encryption_file(user, account, mxc); file != nil {
			file.URL = id.ContentURIString(mxc)
			var described map[string]any
			json.Unmarshal([]byte(json_encode(file)), &described)
			content = maps.Clone(content)
			delete(content, "url")
			content["file"] = described
		}
	}
	sealed, err := d.encrypt(ctx, room, kind, content)
	if err != nil {
		return "", nil, err
	}
	if sealed == nil {
		return kind, content, nil
	}
	return "m.room.encrypted", sealed, nil
}

// encryption_room_encrypted reports whether a room of an account is
// encrypted, without its device: from the engine's state, or the account's
// server.
func encryption_room_encrypted(ctx context.Context, user *User, account string, room string) bool {
	row, _ := db_user(user, "user").row("select * from accounts where id=?", account)
	if matrix_hosted_of(user, row) != nil {
		return encryption_room_state(room, "m.room.encryption") != nil
	}
	if l := linked_from(user, row); l != nil && l.kind == "matrix" {
		code, _, err := matrix_request(ctx, l, "GET", "client/v3/rooms/"+url.PathEscape(room)+"/state/m.room.encryption/", nil, nil)
		return err != nil || code == 200
	}
	return false
}

// === Verification ===

// An encryption_verification is a verification under way as the apps are
// shown it.
type encryption_verification struct {
	Flow    string   `json:"flow"`
	Person  string   `json:"person"`
	Device  string   `json:"device"`
	Room    string   `json:"room,omitempty"`
	Phase   string   `json:"phase"`
	Started bool     `json:"started"`
	Emoji   []string `json:"emoji,omitempty"`
	Names   []string `json:"names,omitempty"`
	Reason  string   `json:"reason,omitempty"`
	Updated int64    `json:"updated"`
}

func (d *encryption_matrix) verification(flow id.VerificationTransactionID, change func(v *encryption_verification)) {
	value, _ := d.verifying.LoadOrStore(flow.String(), &encryption_verification{Flow: flow.String()})
	v := value.(*encryption_verification)
	change(v)
	v.Updated = now()
}

func (d *encryption_matrix) VerificationRequested(ctx context.Context, flow id.VerificationTransactionID, from id.UserID, device id.DeviceID) {
	d.verification(flow, func(v *encryption_verification) {
		v.Person, v.Device, v.Phase = from.String(), device.String(), "requested"
	})
}

func (d *encryption_matrix) VerificationReady(ctx context.Context, flow id.VerificationTransactionID, device id.DeviceID, sas bool, scan bool, code *verificationhelper.QRCode) {
	d.verification(flow, func(v *encryption_verification) {
		v.Phase = "ready"
		if device != "" {
			v.Device = device.String()
		}
	})
}

func (d *encryption_matrix) ShowSAS(ctx context.Context, flow id.VerificationTransactionID, emoji []rune, names []string, decimals []int) {
	d.verification(flow, func(v *encryption_verification) {
		v.Phase, v.Emoji, v.Names = "compare", nil, names
		for _, r := range emoji {
			v.Emoji = append(v.Emoji, string(r))
		}
	})
}

func (d *encryption_matrix) VerificationCancelled(ctx context.Context, flow id.VerificationTransactionID, code event.VerificationCancelCode, reason string) {
	debug("Encryption: account %s: verification %s cancelled: %s %s", d.account, flow, code, reason)
	d.held.Delete(string(flow))
	d.confirmed.Delete(string(flow))
	d.verification(flow, func(v *encryption_verification) {
		v.Phase, v.Reason, v.Emoji, v.Names = "cancelled", string(code), nil, nil
	})
}

func (d *encryption_matrix) VerificationDone(ctx context.Context, flow id.VerificationTransactionID, method event.VerificationMethod) {
	d.held.Delete(string(flow))
	d.confirmed.Delete(string(flow))
	d.verification(flow, func(v *encryption_verification) {
		v.Phase, v.Emoji, v.Names = "done", nil, nil
	})
}

// verifications are those under way, and those ended in the last minutes.
func (d *encryption_matrix) verifications() []*encryption_verification {
	var out []*encryption_verification
	d.verifying.Range(func(key, value any) bool {
		v := value.(*encryption_verification)
		if (v.Phase == "done" || v.Phase == "cancelled") && now()-v.Updated > 300 {
			d.verifying.Delete(key)
			return true
		}
		out = append(out, v)
		return true
	})
	slices.SortFunc(out, func(a, b *encryption_verification) int { return int(a.Updated - b.Updated) })
	return out
}

// encryption_signing_needed is a verification asked of a device without the
// account's cross-signing keys, with which a verification ends by signing
// the other side's: recovery is set up, or its key entered, first.
var encryption_signing_needed = errors.New("the account's cross-signing keys are needed")

// verify asks one of the user's other devices, or another person in a room
// the two share, to verify.
func (d *encryption_matrix) verify(ctx context.Context, person string, room string) (*encryption_verification, error) {
	if d.machine.CrossSigningKeys == nil {
		return nil, encryption_signing_needed
	}
	var flow id.VerificationTransactionID
	var err error
	if person == d.address || room == "" {
		flow, err = d.helper.StartVerification(ctx, id.UserID(d.address))
	} else {
		flow, err = d.helper.StartInRoomVerification(ctx, id.RoomID(room), id.UserID(person))
	}
	if err != nil {
		return nil, err
	}
	d.taking.Store(flow.String(), true)
	d.verification(flow, func(v *encryption_verification) {
		v.Person, v.Room, v.Phase, v.Started = person, room, "requested", true
	})
	value, _ := d.verifying.Load(flow.String())
	return value.(*encryption_verification), nil
}

// act takes a step of a verification: accepting it, starting the emoji
// comparison, confirming they match, saying they do not, or cancelling.
func (d *encryption_matrix) act(ctx context.Context, flow string, action string) error {
	txn := id.VerificationTransactionID(flow)
	var work func(context.Context) error
	switch action {
	case "accept":
		if d.machine.CrossSigningKeys == nil {
			return encryption_signing_needed
		}
		d.taking.Store(flow, true)
		work = func(ctx context.Context) error { return d.helper.AcceptVerification(ctx, txn) }
	case "start":
		work = func(ctx context.Context) error { return d.helper.StartSAS(ctx, txn) }
	case "confirm":
		work = func(ctx context.Context) error {
			if err := d.helper.ConfirmSAS(ctx, txn); err != nil {
				return err
			}
			d.confirmed.Store(flow, true)
			if mac, held := d.held.LoadAndDelete(flow); held {
				d.syncer.Dispatch(ctx, mac.(*event.Event))
			}
			return nil
		}
	case "mismatch":
		work = func(ctx context.Context) error {
			return d.helper.CancelVerification(ctx, txn, event.VerificationCancelCodeSASMismatch, "The emoji do not match")
		}
	case "cancel":
		work = func(ctx context.Context) error {
			return d.helper.CancelVerification(ctx, txn, event.VerificationCancelCodeUser, "Cancelled")
		}
	default:
		return fmt.Errorf("unknown action")
	}
	// The step is the worker's, so the other side's answer to it waits for
	// it to finish.
	done := make(chan error, 1)
	if !d.step(func(ctx context.Context) { done <- work(ctx) }) {
		return fmt.Errorf("the device is busy")
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// === Cross-signing, the key backup and recovery ===

// encryption_authenticate reports whether a request failed for want of the
// account's password: its server answered 401, which mautrix gives as a
// value, wrapped.
func encryption_authenticate(err error) bool {
	var http mautrix.HTTPError
	return errors.As(err, &http) && http.IsStatus(401)
}

// setup makes the account's cross-signing keys, its secret storage and its
// key backup, signs the device, and answers the recovery key, shown to the
// user once. A linked account's server may want the account's password to
// take new cross-signing keys; it is used and not kept.
func (d *encryption_matrix) setup(ctx context.Context, password string) (string, error) {
	var uia mautrix.UIACallback
	if password != "" {
		uia = func(answer *mautrix.RespUserInteractive) any {
			return &mautrix.ReqUIAuthLogin{
				BaseAuthData: mautrix.BaseAuthData{Type: mautrix.AuthTypePassword, Session: answer.Session},
				User:         d.address,
				Password:     password,
			}
		}
	}
	recovery, keys, err := d.machine.GenerateAndUploadCrossSigningKeys(ctx, uia, "")
	if encryption_authenticate(err) {
		return "", encryption_password_needed
	}
	if err != nil {
		return "", err
	}
	if err := d.keep(ctx, keys); err != nil {
		return "", err
	}
	if err := d.machine.SignOwnDevice(ctx, d.machine.OwnIdentity()); err != nil {
		return "", err
	}
	if err := d.machine.SignOwnMasterKey(ctx); err != nil {
		return "", err
	}
	// The key backup, its key kept in secret storage under the recovery
	// key and here.
	key, err := d.ssss(ctx, recovery)
	if err != nil {
		return "", err
	}
	backed, err := backup.NewMegolmBackupKey()
	if err != nil {
		return "", err
	}
	auth := backup.MegolmAuthData{PublicKey: id.Ed25519(base64.RawStdEncoding.EncodeToString(backed.PublicKey().Bytes()))}
	signature, err := d.machine.GetAccount().SignJSON(map[string]any{"public_key": auth.PublicKey})
	if err != nil {
		return "", err
	}
	auth.Signatures = signatures.NewSingleSignature(id.UserID(d.address), id.KeyAlgorithmEd25519, d.device, signature)
	made, err := d.client.CreateKeyBackupVersion(ctx, &mautrix.ReqRoomKeysVersionCreate[backup.MegolmAuthData]{Algorithm: id.KeyBackupAlgorithmMegolmBackupV1, AuthData: auth})
	if err != nil {
		return "", err
	}
	if err := d.machine.SSSS.SetEncryptedAccountData(ctx, event.AccountDataMegolmBackupKey, backed.Bytes(), key); err != nil {
		return "", err
	}
	d.machine.CryptoStore.PutSecret(ctx, id.SecretMegolmBackupV1, base64.RawStdEncoding.EncodeToString(backed.Bytes()))
	d.machine.SetKeyBackupVersion(ctx, made.Version)
	d.backup_all(ctx)
	return recovery, nil
}

// encryption_password_needed is a setup the account's server wants its
// password for.
var encryption_password_needed = errors.New("the account's password is needed")

// keep keeps the account's cross-signing private keys among the device's
// secrets, so they sign the user's other devices after a restart.
func (d *encryption_matrix) keep(ctx context.Context, keys *crypto.CrossSigningKeysCache) error {
	if keys == nil {
		return nil
	}
	seeds := map[id.Secret][]byte{id.SecretXSMaster: keys.MasterKey.Seed(), id.SecretXSSelfSigning: keys.SelfSigningKey.Seed(), id.SecretXSUserSigning: keys.UserSigningKey.Seed()}
	for name, seed := range seeds {
		if err := d.machine.CryptoStore.PutSecret(ctx, name, base64.RawStdEncoding.EncodeToString(seed)); err != nil {
			return err
		}
	}
	return nil
}

// restore takes the cross-signing private keys and the key backup's
// version from the device's secrets, where it has them.
func (d *encryption_matrix) restore(ctx context.Context) {
	master, _ := d.machine.CryptoStore.GetSecret(ctx, id.SecretXSMaster)
	self, _ := d.machine.CryptoStore.GetSecret(ctx, id.SecretXSSelfSigning)
	user, _ := d.machine.CryptoStore.GetSecret(ctx, id.SecretXSUserSigning)
	if master != "" && self != "" && user != "" {
		d.machine.ImportCrossSigningKeys(crypto.CrossSigningSeeds{MasterKey: encryption_unbase64(master), SelfSigningKey: encryption_unbase64(self), UserSigningKey: encryption_unbase64(user)})
	}
	if secret, _ := d.machine.CryptoStore.GetSecret(ctx, id.SecretMegolmBackupV1); secret != "" {
		if key, err := backup.MegolmBackupKeyFromBytes(encryption_unbase64(secret)); err == nil {
			if version, err := d.machine.GetAndVerifyLatestKeyBackupVersion(ctx, key); err == nil && version != nil {
				d.machine.SetKeyBackupVersion(ctx, version.Version)
			}
		}
	}
}

// ssss is the secret storage key a recovery key unlocks.
func (d *encryption_matrix) ssss(ctx context.Context, recovery string) (*ssss.Key, error) {
	keyID, data, err := d.machine.SSSS.GetDefaultKeyData(ctx)
	if err != nil {
		return nil, err
	}
	key, err := data.VerifyRecoveryKey(keyID, recovery)
	if err != nil && !errors.Is(err, ssss.ErrUnverifiableKey) {
		return nil, err
	}
	return key, nil
}

// encryption_recovery_refused is a recovery key that is not the account's.
var encryption_recovery_refused = errors.New("that is not the account's recovery key")

// recover takes a recovery key the user set up on another client: the
// account's cross-signing keys, which sign this device, and the history's
// keys from the key backup, with which the messages that waited are read.
func (d *encryption_matrix) recover(ctx context.Context, recovery string) (int, error) {
	if err := d.machine.VerifyWithRecoveryKey(ctx, recovery); err != nil {
		return 0, encryption_recovery_refused
	}
	key, err := d.ssss(ctx, recovery)
	if err != nil {
		return 0, err
	}
	if d.machine.CrossSigningKeys != nil {
		d.keep(ctx, d.machine.CrossSigningKeys)
	}
	restored := 0
	if secret, err := d.machine.SSSS.GetDecryptedAccountData(ctx, event.AccountDataMegolmBackupKey, key); err == nil {
		if backed, err := backup.MegolmBackupKeyFromBytes(secret); err == nil {
			d.machine.CryptoStore.PutSecret(ctx, id.SecretMegolmBackupV1, base64.RawStdEncoding.EncodeToString(backed.Bytes()))
			before := d.count(ctx)
			if version, err := d.machine.DownloadAndStoreLatestKeyBackup(ctx, backed); err == nil {
				d.machine.SetKeyBackupVersion(ctx, version)
			}
			restored = d.count(ctx) - before
		}
	}
	d.retry(ctx)
	return max(restored, 0), nil
}

// count is how many room keys the device holds.
func (d *encryption_matrix) count(ctx context.Context) int {
	row, _ := encryption_database(d.user).row("select count(*) as n from crypto_megolm_inbound_session where account_id=?", d.account)
	return int(row_int(row, "n"))
}

// retry reads again every event that waited for a key.
func (d *encryption_matrix) retry(ctx context.Context) {
	rows, _ := encryption_database(d.user).rows("select distinct room, session from pending where account=?", d.account)
	for _, row := range rows {
		d.received(ctx, id.RoomID(row_string(row, "room")), id.SessionID(row_string(row, "session")), 0)
	}
}

// backup_all puts every room key the device holds in the account's backup,
// in one request.
func (d *encryption_matrix) backup_all(ctx context.Context) {
	rows, _ := encryption_database(d.user).rows("select room_id, session_id from crypto_megolm_inbound_session where account_id=?", d.account)
	sessions := map[id.RoomID][]id.SessionID{}
	for _, row := range rows {
		room := id.RoomID(row_string(row, "room_id"))
		sessions[room] = append(sessions[room], id.SessionID(row_string(row, "session_id")))
	}
	if len(sessions) > 0 {
		d.backup_put(ctx, sessions)
	}
}

// backup_put seals room keys under the backup's key and puts them in the
// account's backup.
func (d *encryption_matrix) backup_put(ctx context.Context, sessions map[id.RoomID][]id.SessionID) {
	version := d.machine.KeyBackupVersion()
	if version == "" {
		return
	}
	failed := func(err error) {
		debug("Encryption: account %s: room keys not backed up: %v", d.account, err)
	}
	secret, err := d.machine.CryptoStore.GetSecret(ctx, id.SecretMegolmBackupV1)
	if err != nil || secret == "" {
		failed(fmt.Errorf("no backup key: %v", err))
		return
	}
	key, err := backup.MegolmBackupKeyFromBytes(encryption_unbase64(secret))
	if err != nil {
		failed(err)
		return
	}
	request := &mautrix.ReqKeyBackup{Rooms: map[id.RoomID]mautrix.ReqRoomKeyBackup{}}
	for room, ids := range sessions {
		for _, session := range ids {
			igs, err := d.machine.CryptoStore.GetGroupSession(ctx, room, session)
			if err != nil || igs == nil {
				failed(fmt.Errorf("no session %s: %v", session, err))
				continue
			}
			first := igs.Internal.FirstKnownIndex()
			exported, err := igs.Internal.Export(first)
			if err != nil {
				failed(err)
				continue
			}
			sealed, err := backup.EncryptSessionData(key, backup.MegolmSessionData{
				Algorithm:          id.AlgorithmMegolmV1,
				ForwardingKeyChain: igs.ForwardingChains,
				SenderClaimedKeys:  backup.SenderClaimedKeys{Ed25519: igs.SigningKey},
				SenderKey:          igs.SenderKey,
				SessionKey:         string(exported),
			})
			if err != nil {
				failed(err)
				continue
			}
			if request.Rooms[room].Sessions == nil {
				request.Rooms[room] = mautrix.ReqRoomKeyBackup{Sessions: map[id.SessionID]mautrix.ReqKeyBackupData{}}
			}
			request.Rooms[room].Sessions[session] = mautrix.ReqKeyBackupData{
				FirstMessageIndex: int(first),
				ForwardedCount:    len(igs.ForwardingChains),
				IsVerified:        false,
				SessionData:       json.RawMessage(json_encode(sealed)),
			}
		}
	}
	if len(request.Rooms) == 0 {
		return
	}
	if _, err := d.client.PutKeysInBackup(ctx, version, request); err != nil {
		failed(err)
	}
}

// status is the device as the apps show it.
func (d *encryption_matrix) status(ctx context.Context) map[string]any {
	identity := d.machine.OwnIdentity()
	keys, _ := d.machine.GetOwnCrossSigningPublicKeys(ctx)
	_, verified, _ := d.machine.GetOwnVerificationStatus(ctx)
	_, data, _ := d.machine.SSSS.GetDefaultKeyData(ctx)
	return map[string]any{
		"network":     "matrix",
		"address":     d.address,
		"device":      d.device,
		"fingerprint": identity.SigningKey.Fingerprint(),
		"signing":     keys != nil,
		"keys":        d.machine.CrossSigningKeys != nil,
		"verified":    verified,
		"recovery":    data != nil,
		"backup":      d.machine.KeyBackupVersion() != "",
	}
}

// devices are a person's devices, the user's own among them.
func (d *encryption_matrix) devices(ctx context.Context, person string) []map[string]any {
	found, err := d.machine.FetchKeys(ctx, []id.UserID{id.UserID(person)}, true)
	if err != nil {
		return nil
	}
	var out []map[string]any
	for device, info := range found[id.UserID(person)] {
		out = append(out, map[string]any{
			"id":          device.String(),
			"name":        info.Name,
			"fingerprint": info.SigningKey.Fingerprint(),
			"verified":    d.machine.IsDeviceTrusted(ctx, info),
			"own":         person == d.address && device.String() == d.device,
		})
	}
	slices.SortFunc(out, func(a, b map[string]any) int { return strings.Compare(a["id"].(string), b["id"].(string)) })
	return out
}

// === The apps' functions ===

var api_account_encryption = sls.FromStringDict(sl.String("mochi.account.encryption"), sl.StringDict{
	"act":           sl.NewBuiltin("mochi.account.encryption.act", api_account_encryption_act),
	"devices":       sl.NewBuiltin("mochi.account.encryption.devices", api_account_encryption_devices),
	"recover":       sl.NewBuiltin("mochi.account.encryption.recover", api_account_encryption_recover),
	"setup":         sl.NewBuiltin("mochi.account.encryption.setup", api_account_encryption_setup),
	"status":        sl.NewBuiltin("mochi.account.encryption.status", api_account_encryption_status),
	"trust":         sl.NewBuiltin("mochi.account.encryption.trust", api_account_encryption_trust),
	"verifications": sl.NewBuiltin("mochi.account.encryption.verifications", api_account_encryption_verifications),
	"verify":        sl.NewBuiltin("mochi.account.encryption.verify", api_account_encryption_verify),
})

// encryption_device is the encryption device of a Matrix or XMPP account the
// app acts on: one of the two.
func encryption_device(t *sl.Thread, fn *sl.Builtin, account string) (*encryption_matrix, *omemo_device, error) {
	user, row, _, err := account_acting_on(t, fn, account, "matrix", "xmpp")
	if err != nil {
		return nil, nil, err
	}
	switch row_string(row, "type") {
	case "matrix":
		d, err := encryption_matrix_of(user, account)
		return d, nil, err
	case "xmpp":
		d, err := omemo_device_of(user, account)
		if err == nil && !d.published.Load() {
			omemo_start(user, account)
		}
		return nil, d, err
	}
	return nil, nil, fmt.Errorf("the account has no encryption device")
}

// encryption_matrix_only is the Matrix device of an account, for what only
// Matrix has.
func encryption_matrix_only(t *sl.Thread, fn *sl.Builtin, account string) (*encryption_matrix, error) {
	m, _, err := encryption_device(t, fn, account)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("only a Matrix account has this")
	}
	return m, nil
}

// mochi.account.encryption.status(account) -> dict: The encryption device of a Matrix or XMPP account, which core holds and which decrypts the account's messages before the app hears them: {network, address, device, ...}; on Matrix its fingerprint and whether the account has cross-signing keys (signing) and this device holds them (keys), whether this device is verified, whether secret storage holds a recovery key (recovery) and whether its room keys are backed up (backup); on XMPP its OMEMO fingerprints, one for each version (fingerprints: [{version, fingerprint}]). Requires the account's permission
func api_account_encryption_status(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account); err != nil {
		return nil, err
	}
	m, o, err := encryption_device(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if m != nil {
		return sl_encode(m.status(thread_context(t))), nil
	}
	return sl_encode(o.status()), nil
}

// mochi.account.encryption.devices(account, person) -> list: A person's devices on a Matrix or XMPP account's network, the user's own address naming theirs, someone in an XMPP room named as the room shows them (room@service/nick) being the person it shows them to be: [{id, name, fingerprint, verified, own}] on Matrix, [{id, version, fingerprint, trust, own}] on XMPP, trust being verified, distrusted or undecided. Requires the account's permission
func api_account_encryption_devices(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, person string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "person", &person); err != nil {
		return nil, err
	}
	m, o, err := encryption_device(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if m != nil {
		return sl_encode(m.devices(thread_context(t), person)), nil
	}
	address, ok := o.person(person)
	if !ok {
		return sl_error(fn, "invalid person")
	}
	found, err := o.devices(thread_context(t), address)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl_encode(found), nil
}

// mochi.account.encryption.trust(account, person, device, trust) -> None: Record the user's word on a device of a person's, having compared its fingerprint: "verified", "distrusted", or "undecided" to take it back. An XMPP person's devices are trusted blindly until one is verified, after which only those verified are encrypted to; a distrusted device is neither encrypted to nor read. Requires the account's permission
func api_account_encryption_trust(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, person, device, trust string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "person", &person, "device", &device, "trust", &trust); err != nil {
		return nil, err
	}
	if trust != "verified" && trust != "distrusted" && trust != "undecided" {
		return sl_error(fn, "invalid trust")
	}
	m, o, err := encryption_device(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if m != nil {
		if err := m.trust(thread_context(t), person, device, trust); err != nil {
			return sl_error(fn, "%v", err)
		}
		return sl.None, nil
	}
	address, ok := o.person(person)
	id, failure := strconv.ParseUint(device, 10, 32)
	if !ok || failure != nil {
		return sl_error(fn, "invalid device")
	}
	if err := o.trust(address, uint32(id), trust); err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl.None, nil
}

// mochi.account.encryption.verify(account, person, room="") -> dict: Ask to verify a Matrix account's other devices, the user's own address naming them, or a person in a room the two share, by comparing emoji: the verification as encryption.verifications lists it, or {recovery: True} where core's device does not yet hold the account's cross-signing keys, which a verification ends by signing with: recovery is set up, or its key entered, first. Requires the account's permission
func api_account_encryption_verify(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, person, room string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "person", &person, "room?", &room); err != nil {
		return nil, err
	}
	m, err := encryption_matrix_only(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	v, err := m.verify(thread_context(t), person, room)
	if errors.Is(err, encryption_signing_needed) {
		return sl_encode(map[string]any{"recovery": true}), nil
	}
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl_encode(encryption_plain(v)), nil
}

// mochi.account.encryption.verifications(account) -> list: A Matrix account's verifications under way and those ended in the last minutes, oldest first: [{flow, person, device, room, phase, started, emoji, names, reason, updated}], phase being requested, ready, compare (emoji and their names given), done or cancelled, started whether this side asked. An XMPP account has none. Requires the account's permission
func api_account_encryption_verifications(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account); err != nil {
		return nil, err
	}
	m, _, err := encryption_device(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if m == nil {
		return sl.NewList(nil), nil
	}
	return sl_encode(encryption_plain(m.verifications())), nil
}

// encryption_plain is a value as the apps are given it: its JSON's maps and
// lists.
func encryption_plain(v any) any {
	var out any
	json.Unmarshal([]byte(json_encode(v)), &out)
	if out == nil {
		return []any{}
	}
	return out
}

// mochi.account.encryption.act(account, flow, action) -> dict: Take a step of a Matrix account's verification: "accept" one asked of the user, "start" comparing emoji, "confirm" they match, "mismatch" when they do not, or "cancel". Answers {} or, accepting before core's device holds the account's cross-signing keys, {recovery: True}. Requires the account's permission
func api_account_encryption_act(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, flow, action string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "flow", &flow, "action", &action); err != nil {
		return nil, err
	}
	m, err := encryption_matrix_only(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	err = m.act(thread_context(t), flow, action)
	if errors.Is(err, encryption_signing_needed) {
		return sl_encode(map[string]any{"recovery": true}), nil
	}
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl_encode(map[string]any{}), nil
}

// mochi.account.encryption.setup(account, password="") -> dict: Make a Matrix account's cross-signing keys, its secret storage and its key backup, and sign its device: {recovery} with the recovery key, shown to the user once for their other clients. A linked account's server may want the account's password first, when the answer is {password: True}; the password is used and not kept. Requires the account's permission
func api_account_encryption_setup(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, password string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "password?", &password); err != nil {
		return nil, err
	}
	m, err := encryption_matrix_only(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	recovery, err := m.setup(thread_context(t), password)
	if errors.Is(err, encryption_password_needed) {
		return sl_encode(map[string]any{"password": true}), nil
	}
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl_encode(map[string]any{"recovery": recovery}), nil
}

// mochi.account.encryption.recover(account, key) -> dict: Take a Matrix account's recovery key set up on another client: the account's cross-signing keys, which sign core's device, and the room keys of its history from the key backup, with which the messages that waited are read. Answers {restored} with how many room keys came, or {refused: True} where the key is not the account's. Requires the account's permission
func api_account_encryption_recover(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, key string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "key", &key); err != nil {
		return nil, err
	}
	m, err := encryption_matrix_only(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	restored, err := m.recover(thread_context(t), strings.TrimSpace(key))
	if errors.Is(err, encryption_recovery_refused) {
		return sl_encode(map[string]any{"refused": true}), nil
	}
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl_encode(map[string]any{"restored": restored}), nil
}

// trust records the user's word on a Matrix device of someone's, compared by
// its fingerprint.
func (d *encryption_matrix) trust(ctx context.Context, person string, device string, trust string) error {
	found, err := d.machine.GetOrFetchDevice(ctx, id.UserID(person), id.DeviceID(device))
	if err != nil || found == nil {
		return fmt.Errorf("unknown device")
	}
	switch trust {
	case "verified":
		found.Trust = id.TrustStateVerified
	case "distrusted":
		found.Trust = id.TrustStateBlacklisted
	default:
		found.Trust = id.TrustStateUnset
	}
	return d.machine.CryptoStore.PutDevice(ctx, id.UserID(person), found)
}
