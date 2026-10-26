// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: Apache-2.0

package pack

import (
	"bytes"
	"io"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/storage/memory"
)

// cycle_storer holds objects whole, and hands some back as stored deltas,
// as a repository's packs do when the encoder reuses them.
type cycle_storer struct {
	*memory.Storage
	deltas map[plumbing.Hash]plumbing.EncodedObject
}

func (s *cycle_storer) DeltaObject(kind plumbing.ObjectType, hash plumbing.Hash) (plumbing.EncodedObject, error) {
	if delta, found := s.deltas[hash]; found {
		return delta, nil
	}
	return s.EncodedObject(kind, hash)
}

// cycle_delta is a stored delta: the delta's own bytes, and the object it
// rebuilds from its base.
type cycle_delta struct {
	plumbing.EncodedObject
	base   plumbing.Hash
	actual plumbing.Hash
	size   int64
}

func (d *cycle_delta) BaseHash() plumbing.Hash   { return d.base }
func (d *cycle_delta) ActualHash() plumbing.Hash { return d.actual }
func (d *cycle_delta) ActualSize() int64         { return d.size }

func cycle_blob(t *testing.T, store *memory.Storage, contents string) plumbing.EncodedObject {
	t.Helper()
	blob := store.NewEncodedObject()
	blob.SetType(plumbing.BlobObject)
	writer, err := blob.Writer()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(writer, contents); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	if _, err := store.SetEncodedObject(blob); err != nil {
		t.Fatal(err)
	}
	return blob
}

// cycle_stored is target as a store holds it: a delta against base.
func cycle_stored(t *testing.T, base, target plumbing.EncodedObject) plumbing.EncodedObject {
	t.Helper()
	delta, err := GetDelta(base, target)
	if err != nil {
		t.Fatal(err)
	}
	return &cycle_delta{EncodedObject: delta, base: base.Hash(), actual: target.Hash(), size: target.Size()}
}

// cycle_encode packs these objects with deltas reused, and checks that the
// pack it writes gives every object back as it was.
func cycle_encode(t *testing.T, store *cycle_storer, objects ...plumbing.EncodedObject) {
	t.Helper()
	// A cycle followed for ever ends in a stack overflow, which no test can
	// catch: keep the stack small, so a regression dies at once rather than
	// after a gigabyte.
	defer debug.SetMaxStack(debug.SetMaxStack(16 << 20))

	var hashes []plumbing.Hash
	for _, object := range objects {
		hashes = append(hashes, object.Hash())
	}
	var buffer bytes.Buffer
	if _, err := NewEncoder(&buffer, store, false).Encode(hashes, 10); err != nil {
		t.Fatalf("encode: %v", err)
	}

	read := memory.NewStorage()
	parser, err := packfile.NewParserWithStorage(packfile.NewScanner(bytes.NewReader(buffer.Bytes())), read)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parser.Parse(); err != nil {
		t.Fatalf("the pack written does not parse: %v", err)
	}
	for _, object := range objects {
		got, err := read.EncodedObject(plumbing.AnyObject, object.Hash())
		if err != nil {
			t.Errorf("object %s is not in the pack: %v", object.Hash(), err)
			continue
		}
		if got.Size() != object.Size() || got.Type() != object.Type() {
			t.Errorf("object %s came back as %s of %d bytes, want %s of %d", object.Hash(), got.Type(), got.Size(), object.Type(), object.Size())
		}
	}
}

// cycle_chains fixes the reused deltas of these objects, as the encoder does
// before its own search, and fails if any base chain loops back on itself.
// The encoder writes a looping chain's object whole, but a chain the fixing
// left looping is a cycle it failed to break.
func cycle_chains(t *testing.T, store *cycle_storer, objects ...plumbing.EncodedObject) {
	t.Helper()
	defer debug.SetMaxStack(debug.SetMaxStack(16 << 20))
	var hashes []plumbing.Hash
	for _, object := range objects {
		hashes = append(hashes, object.Hash())
	}
	fixed, err := newDeltaSelector(store).objectsToPack(hashes, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range fixed {
		step := object
		for i := 0; step != nil; i++ {
			if i > len(fixed) {
				t.Errorf("the base chain of %s loops", object.Hash())
				break
			}
			step = step.Base
		}
	}
}

// An object held in two packs is read from whichever the store finds first,
// so one read can give A as a delta of B and the next B as a delta of A. The
// chain must be broken, not followed: following it overflowed the stack,
// which no recover catches, and killed the server on every clone.
func TestEncodeBreaksADeltaCycle(t *testing.T) {
	store := &cycle_storer{Storage: memory.NewStorage(), deltas: map[plumbing.Hash]plumbing.EncodedObject{}}
	text := strings.Repeat("a line of a file that changes a little between versions\n", 200)
	a := cycle_blob(t, store.Storage, text+"the first ending\n")
	b := cycle_blob(t, store.Storage, text+"the second ending\n")
	store.deltas[a.Hash()] = cycle_stored(t, b, a)
	store.deltas[b.Hash()] = cycle_stored(t, a, b)

	cycle_chains(t, store, a, b)
	cycle_chains(t, store, b, a)
	cycle_encode(t, store, a, b)
	cycle_encode(t, store, b, a)
}

// A longer cycle, through a third object, is broken the same way.
func TestEncodeBreaksALongerDeltaCycle(t *testing.T) {
	store := &cycle_storer{Storage: memory.NewStorage(), deltas: map[plumbing.Hash]plumbing.EncodedObject{}}
	text := strings.Repeat("a line of a file that changes a little between versions\n", 200)
	a := cycle_blob(t, store.Storage, text+"one\n")
	b := cycle_blob(t, store.Storage, text+"two\n")
	c := cycle_blob(t, store.Storage, text+"three\n")
	store.deltas[a.Hash()] = cycle_stored(t, b, a)
	store.deltas[b.Hash()] = cycle_stored(t, c, b)
	store.deltas[c.Hash()] = cycle_stored(t, a, c)

	cycle_chains(t, store, a, b, c)
	cycle_encode(t, store, a, b, c)
}

// A delta naming itself as its base is the shortest cycle.
func TestEncodeBreaksADeltaOnItself(t *testing.T) {
	store := &cycle_storer{Storage: memory.NewStorage(), deltas: map[plumbing.Hash]plumbing.EncodedObject{}}
	a := cycle_blob(t, store.Storage, strings.Repeat("a line\n", 200))
	store.deltas[a.Hash()] = cycle_stored(t, a, a)

	cycle_chains(t, store, a)
	cycle_encode(t, store, a)
}

// Control: a chain without a cycle keeps its reused deltas, whatever order
// its objects come in. c is a delta of a, which is a delta of b; a's chain is
// fixed first, so a mark left on it would read as a cycle when c reaches it.
func TestEncodeKeepsReusedDeltas(t *testing.T) {
	store := &cycle_storer{Storage: memory.NewStorage(), deltas: map[plumbing.Hash]plumbing.EncodedObject{}}
	text := strings.Repeat("a line of a file that changes a little between versions\n", 200)
	a := cycle_blob(t, store.Storage, text+"one\n")
	b := cycle_blob(t, store.Storage, text+"two\n")
	c := cycle_blob(t, store.Storage, text+"three\n")
	store.deltas[a.Hash()] = cycle_stored(t, b, a)
	store.deltas[c.Hash()] = cycle_stored(t, a, c)

	objects, err := newDeltaSelector(store).ObjectsToPack([]plumbing.Hash{a.Hash(), c.Hash(), b.Hash()}, 10)
	if err != nil {
		t.Fatal(err)
	}
	bases := map[plumbing.Hash]plumbing.Hash{}
	for _, object := range objects {
		if object.Base != nil {
			bases[object.Hash()] = object.Base.Hash()
		}
	}
	if bases[a.Hash()] != b.Hash() || bases[c.Hash()] != a.Hash() {
		t.Errorf("the stored deltas were not all reused: bases %v, want %s on %s and %s on %s", bases, a.Hash(), b.Hash(), c.Hash(), a.Hash())
	}
	cycle_encode(t, store, a, c, b)
}
