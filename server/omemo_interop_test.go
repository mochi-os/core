// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// An omemo_peer is libomemo.js's device on the other end of a pipe
// (testdata/omemo.mjs).
type omemo_peer struct {
	t   *testing.T
	in  *json.Encoder
	out *bufio.Scanner
}

func (p *omemo_peer) call(request map[string]any) map[string]any {
	p.t.Helper()
	if err := p.in.Encode(request); err != nil {
		p.t.Fatal(err)
	}
	if !p.out.Scan() {
		p.t.Fatalf("libomemo.js ended: %v", p.out.Err())
	}
	var answer map[string]any
	json.Unmarshal(p.out.Bytes(), &answer)
	if answer["error"] != nil {
		p.t.Fatalf("libomemo.js: %s: %v", request["op"], answer["error"])
	}
	return answer
}

func omemo_b64(value any) []byte {
	out, _ := base64.StdEncoding.DecodeString(value.(string))
	return out
}

// TestOmemoInterop: sessions with libomemo.js, as Converse.js runs it, in
// each version and each direction - begun by it from Mochi's bundle and by
// Mochi from its bundle - carry messages both ways, the key exchange ending
// once answered, and its messages out of order. It runs where
// MOCHI_LIBOMEMO names the library's module (dist/libomemo.esm.js) and
// MOCHI_NODE how to run node, inside the build sandbox: the library is
// third-party code.
func TestOmemoInterop(t *testing.T) {
	module := os.Getenv("MOCHI_LIBOMEMO")
	if module == "" {
		t.Skip("MOCHI_LIBOMEMO is not set")
	}
	command := strings.Fields(os.Getenv("MOCHI_NODE"))
	if len(command) == 0 {
		command = []string{"node"}
	}
	cmd := exec.Command(command[0], append(command[1:], "testdata/omemo.mjs")...)
	cmd.Env = append(os.Environ(), "MOCHI_LIBOMEMO="+module)
	cmd.Stderr = os.Stderr
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stdin.Close()
		cmd.Wait()
	})
	p := &omemo_peer{t: t, in: json.NewEncoder(stdin), out: bufio.NewScanner(stdout)}
	p.out.Buffer(make([]byte, 1<<20), 1<<20)

	for _, version := range []string{omemo_legacy, omemo_current} {
		t.Run(version, func(t *testing.T) {
			p.t = t
			mochi := omemo_test_new()
			own := mochi.bundle(version)
			encode := func(key []byte) string {
				if version == omemo_legacy {
					key = omemo_typed(key)
				}
				return base64.StdEncoding.EncodeToString(key)
			}
			identity := encode(mochi.identity.Public)
			if version == omemo_current {
				identity = base64.StdEncoding.EncodeToString(omemo_identity(mochi.identity.Public))
			}
			published := map[string]any{"identity": identity, "registration": 5,
				"signed": map[string]any{"id": own.signed, "key": encode(own.key), "signature": base64.StdEncoding.EncodeToString(own.signature)},
				"prekey": map[string]any{"id": own.prekey, "key": encode(own.once)}}

			// libomemo.js begins a session from Mochi's bundle.
			p.call(map[string]any{"op": "begin", "version": version, "device": 1, "bundle": published})
			first := p.call(map[string]any{"op": "encrypt", "version": version, "device": 1, "plain": base64.StdEncoding.EncodeToString([]byte("from js"))})
			if first["kex"] != true {
				t.Fatalf("THE POINT: its first message carries the key exchange: %v", first)
			}
			s, plain := mochi.accept(t, version, omemo_b64(first["body"]))
			if string(plain) != "from js" {
				t.Fatalf("THE POINT: its first message read: %q", plain)
			}
			answer, kex, err := s.encrypt(mochi.identity.Public, []byte("from mochi"), 5)
			if err != nil || kex {
				t.Fatalf("Mochi's answer: %v %v", err, kex)
			}
			read := p.call(map[string]any{"op": "decrypt", "version": version, "device": 1, "body": base64.StdEncoding.EncodeToString(answer), "kex": false})
			if string(omemo_b64(read["plain"])) != "from mochi" {
				t.Fatalf("THE POINT: Mochi's answer read by libomemo.js: %v", read)
			}
			var later [][]byte
			for _, text := range []string{"one", "two", "three"} {
				m := p.call(map[string]any{"op": "encrypt", "version": version, "device": 1, "plain": base64.StdEncoding.EncodeToString([]byte(text))})
				if m["kex"] == true {
					t.Fatalf("THE POINT: the key exchange sent after its answer")
				}
				later = append(later, omemo_b64(m["body"]))
			}
			for _, i := range []int{2, 0, 1} {
				if plain, err := s.decrypt(mochi.identity.Public, later[i]); err != nil || string(plain) != []string{"one", "two", "three"}[i] {
					t.Fatalf("THE POINT: its message %d read out of order: %q %v", i, plain, err)
				}
			}

			// Mochi begins a session from libomemo.js's bundle.
			theirs := p.call(map[string]any{"op": "bundle", "version": version})
			b := &omemo_bundle{signature: omemo_b64(theirs["signed"].(map[string]any)["signature"])}
			b.signed = uint32(theirs["signed"].(map[string]any)["id"].(float64))
			b.prekey = uint32(theirs["prekey"].(map[string]any)["id"].(float64))
			if version == omemo_legacy {
				b.identity = omemo_untyped(omemo_b64(theirs["identity"]))
				b.key = omemo_untyped(omemo_b64(theirs["signed"].(map[string]any)["key"]))
				b.once = omemo_untyped(omemo_b64(theirs["prekey"].(map[string]any)["key"]))
			} else {
				b.edwards = omemo_b64(theirs["identity"])
				b.identity = omemo_montgomery(b.edwards)
				b.key = omemo_b64(theirs["signed"].(map[string]any)["key"])
				b.once = omemo_b64(theirs["prekey"].(map[string]any)["key"])
			}
			begun, err := omemo_begin(version, mochi.identity, b)
			if err != nil {
				t.Fatalf("THE POINT: a session begun from libomemo.js's bundle: %v", err)
			}
			opening, kex, _ := begun.encrypt(mochi.identity.Public, []byte("hello js"), 5)
			if !kex {
				t.Fatal("Mochi's first message carries no key exchange")
			}
			read = p.call(map[string]any{"op": "decrypt", "version": version, "device": 2, "body": base64.StdEncoding.EncodeToString(opening), "kex": true})
			if string(omemo_b64(read["plain"])) != "hello js" {
				t.Fatalf("THE POINT: Mochi's key exchange read by libomemo.js: %v", read)
			}
			back := p.call(map[string]any{"op": "encrypt", "version": version, "device": 2, "plain": base64.StdEncoding.EncodeToString([]byte("hello mochi"))})
			if plain, err := begun.decrypt(mochi.identity.Public, omemo_b64(back["body"])); err != nil || string(plain) != "hello mochi" {
				t.Fatalf("THE POINT: libomemo.js's answer read: %q %v", plain, err)
			}
			again, kex, _ := begun.encrypt(mochi.identity.Public, []byte("again"), 5)
			if kex {
				t.Fatal("THE POINT: Mochi's key exchange sent after its answer")
			}
			read = p.call(map[string]any{"op": "decrypt", "version": version, "device": 2, "body": base64.StdEncoding.EncodeToString(again), "kex": false})
			if string(omemo_b64(read["plain"])) != "again" {
				t.Fatalf("Mochi's later message read: %v", read)
			}
		})
	}
}
