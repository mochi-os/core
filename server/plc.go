// Mochi server: did:plc identities
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// A hosted Bluesky account's identifier is a did:plc (decision 20), an entry
// in the directory that names its signing key, its handle and the server
// that holds its repository, and moves with the account between servers.
// Each change is an operation signed by a rotation key and chained to the
// last by content id; the directory keeps the log. The server holds a
// rotation key of its own, and the recovery key the user keeps outranks it
// (the "Keys" section of the federation plan): listed first, it can undo
// within three days anything a compromised server does.

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/atdata"
)

// plc_timeout bounds one request to the directory.
const plc_timeout = 20 * time.Second

// plc_operation builds an unsigned operation: the keys that may change the
// identity in order of authority, the signing key, the handle as an
// at:// alias, the server, and the operation it follows (none for the first).
func plc_operation(rotation []string, signing string, handle string, endpoint string, previous string) map[string]any {
	keys := make([]any, 0, len(rotation))
	for _, key := range rotation {
		keys = append(keys, key)
	}
	op := map[string]any{
		"type":                "plc_operation",
		"rotationKeys":        keys,
		"verificationMethods": map[string]any{"atproto": signing},
		"alsoKnownAs":         []any{"at://" + handle},
		"services":            map[string]any{"atproto_pds": map[string]any{"type": "AtprotoPersonalDataServer", "endpoint": endpoint}},
		"prev":                nil,
	}
	if previous != "" {
		op["prev"] = previous
	}
	return op
}

// plc_sign signs an operation with a rotation key: the signature over the
// operation's DAG-CBOR form without the signature, in the raw low-s form,
// written base64url without padding.
func plc_sign(op map[string]any, private string) error {
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
	signature, err := key_sign_p256(private, data)
	if err != nil {
		return err
	}
	op["sig"] = base64.RawURLEncoding.EncodeToString(signature)
	return nil
}

// plc_did derives the identifier from a signed first operation: the SHA-256
// of its DAG-CBOR form, base32, the first 24 characters.
func plc_did(op map[string]any) (string, error) {
	data, err := atdata.MarshalCBOR(op)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	encoded := strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:]))
	return "did:plc:" + encoded[:24], nil
}

// plc_cid is the content id of a signed operation, which the next names as
// its prev.
func plc_cid(op map[string]any) (string, error) {
	data, err := atdata.MarshalCBOR(op)
	if err != nil {
		return "", err
	}
	return repository_cid(cid_dagcbor, data).String(), nil
}

// plc_request sends one request to the directory and answers the status and
// the body. The directory is reached as any other host is, under the host
// policy and the limiter.
func plc_request(ctx context.Context, method string, path string, body any) (int, []byte, error) {
	var reader *bytes.Reader
	if body != nil {
		reader = bytes.NewReader([]byte(json_encode(body)))
	} else {
		reader = bytes.NewReader(nil)
	}
	ctx, cancel := context.WithTimeout(ctx, plc_timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(bluesky_plc, "/")+path, reader)
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := network_request(ctx, nil, "", request, 3)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := read_limited(response.Body, resolve_document_maximum)
	if err != nil {
		return response.StatusCode, nil, err
	}
	return response.StatusCode, data, nil
}

// plc_submit sends an operation for an identifier.
func plc_submit(ctx context.Context, did string, op map[string]any) error {
	status, body, err := plc_request(ctx, "POST", "/"+did, op)
	if err != nil {
		return fmt.Errorf("the identity directory could not be reached: %w", err)
	}
	if status < 200 || status > 299 {
		return fmt.Errorf("the identity directory refused the operation: %v", linked_failure(status, body))
	}
	return nil
}

// plc_last reads the last operation of an identifier, with its content id.
func plc_last(ctx context.Context, did string) (map[string]any, string, error) {
	status, body, err := plc_request(ctx, "GET", "/"+did+"/log/last", nil)
	if err != nil {
		return nil, "", fmt.Errorf("the identity directory could not be reached: %w", err)
	}
	if status != 200 {
		return nil, "", fmt.Errorf("the identity %s could not be read: status %d", did, status)
	}
	var op map[string]any
	if err := json.Unmarshal(body, &op); err != nil {
		return nil, "", fmt.Errorf("the identity %s has no readable log", did)
	}
	id, err := plc_cid(op)
	if err != nil {
		return nil, "", err
	}
	return op, id, nil
}

// plc_keys are the rotation keys an account's operations list: the recovery
// key the user holds first, where one has been made, then the server's own.
func plc_keys(user *User, account string) ([]string, string, error) {
	rotation := keypair_get(user, account, key_rotation)
	if rotation == nil {
		return nil, "", fmt.Errorf("account has no rotation key")
	}
	keys := []string{}
	if recovery := keypair_get(user, account, key_recovery); recovery != nil {
		keys = append(keys, recovery.Public)
	}
	return append(keys, rotation.Public), rotation.Private, nil
}

// plc_create registers a new identity for an account, from its keys, its
// handle and its server, and answers the identifier.
func plc_create(ctx context.Context, user *User, account string, handle string, endpoint string) (string, error) {
	signing := keypair_get(user, account, key_signing)
	if signing == nil {
		return "", fmt.Errorf("account has no signing key")
	}
	keys, private, err := plc_keys(user, account)
	if err != nil {
		return "", err
	}
	op := plc_operation(keys, signing.Public, handle, endpoint, "")
	if err := plc_sign(op, private); err != nil {
		return "", err
	}
	did, err := plc_did(op)
	if err != nil {
		return "", err
	}
	if err := plc_submit(ctx, did, op); err != nil {
		return "", err
	}
	return did, nil
}

// plc_update writes a new operation for an identity: the last one's keys,
// handle and server with the change applied, chained to it and signed by the
// server's rotation key. The change sees the operation before it is signed.
func plc_update(ctx context.Context, user *User, account string, did string, change func(op map[string]any)) error {
	last, previous, err := plc_last(ctx, did)
	if err != nil {
		return err
	}
	if kind, _ := last["type"].(string); kind == "plc_tombstone" {
		return fmt.Errorf("the identity %s is tombstoned", did)
	}
	keys, private, err := plc_keys(user, account)
	if err != nil {
		return err
	}
	signing := ""
	if methods, _ := last["verificationMethods"].(map[string]any); methods != nil {
		signing, _ = methods["atproto"].(string)
	}
	handle := ""
	for _, alias := range object_strings(last["alsoKnownAs"]) {
		if h, ok := strings.CutPrefix(alias, "at://"); ok {
			handle = h
			break
		}
	}
	endpoint := ""
	if services, _ := last["services"].(map[string]any); services != nil {
		if pds, _ := services["atproto_pds"].(map[string]any); pds != nil {
			endpoint, _ = pds["endpoint"].(string)
		}
	}
	op := plc_operation(keys, signing, handle, endpoint, previous)
	change(op)
	if err := plc_sign(op, private); err != nil {
		return err
	}
	return plc_submit(ctx, did, op)
}

// plc_tombstone ends an identity: a tombstone chained to its last operation,
// after which nothing can be done with it again.
func plc_tombstone(ctx context.Context, user *User, account string, did string) error {
	_, previous, err := plc_last(ctx, did)
	if err != nil {
		return err
	}
	_, private, err := plc_keys(user, account)
	if err != nil {
		return err
	}
	op := map[string]any{"type": "plc_tombstone", "prev": previous}
	if err := plc_sign(op, private); err != nil {
		return err
	}
	return plc_submit(ctx, did, op)
}
