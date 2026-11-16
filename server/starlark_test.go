// Mochi server: Starlark unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"math"
	"math/big"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.starlark.net/resolve"
	sl "go.starlark.net/starlark"
)

// Test sl_encode with various Go types
func TestSlEncode(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		validate func(sl.Value) bool
	}{
		{
			name:  "nil",
			input: nil,
			validate: func(v sl.Value) bool {
				return v == sl.None
			},
		},
		{
			name:  "string",
			input: "hello",
			validate: func(v sl.Value) bool {
				s, ok := sl.AsString(v)
				return ok && s == "hello"
			},
		},
		{
			name:  "empty string",
			input: "",
			validate: func(v sl.Value) bool {
				s, ok := sl.AsString(v)
				return ok && s == ""
			},
		},
		{
			name:  "int",
			input: 42,
			validate: func(v sl.Value) bool {
				i, ok := v.(sl.Int)
				if !ok {
					return false
				}
				val, _ := i.Int64()
				return val == 42
			},
		},
		{
			name:  "int64",
			input: int64(9223372036854775807),
			validate: func(v sl.Value) bool {
				i, ok := v.(sl.Int)
				if !ok {
					return false
				}
				val, _ := i.Int64()
				return val == 9223372036854775807
			},
		},
		{
			name:  "bool true",
			input: true,
			validate: func(v sl.Value) bool {
				return v == sl.True
			},
		},
		{
			name:  "bool false",
			input: false,
			validate: func(v sl.Value) bool {
				return v == sl.False
			},
		},
		{
			name:  "string slice",
			input: []string{"a", "b", "c"},
			validate: func(v sl.Value) bool {
				t, ok := v.(sl.Tuple)
				if !ok || len(t) != 3 {
					return false
				}
				s0, _ := sl.AsString(t[0])
				s1, _ := sl.AsString(t[1])
				s2, _ := sl.AsString(t[2])
				return s0 == "a" && s1 == "b" && s2 == "c"
			},
		},
		{
			name:  "map string string",
			input: map[string]string{"key": "value"},
			validate: func(v sl.Value) bool {
				d, ok := v.(*sl.Dict)
				if !ok {
					return false
				}
				val, found, _ := d.Get(sl.String("key"))
				if !found {
					return false
				}
				s, _ := sl.AsString(val)
				return s == "value"
			},
		},
		{
			name:  "map string any",
			input: map[string]any{"name": "test", "count": int64(5)},
			validate: func(v sl.Value) bool {
				d, ok := v.(*sl.Dict)
				if !ok {
					return false
				}
				name, found, _ := d.Get(sl.String("name"))
				if !found {
					return false
				}
				s, _ := sl.AsString(name)
				return s == "test"
			},
		},
		{
			name:  "any slice",
			input: []any{"hello", int64(42), true},
			validate: func(v sl.Value) bool {
				t, ok := v.(sl.Tuple)
				if !ok || len(t) != 3 {
					return false
				}
				s, _ := sl.AsString(t[0])
				return s == "hello"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sl_encode(tt.input)
			if !tt.validate(result) {
				t.Errorf("sl_encode(%v) validation failed, got %v", tt.input, result)
			}
		})
	}
}

// Test sl_decode with various Starlark types
func TestSlDecode(t *testing.T) {
	tests := []struct {
		name     string
		input    sl.Value
		expected any
	}{
		{
			name:     "None",
			input:    sl.None,
			expected: nil,
		},
		{
			name:     "True",
			input:    sl.True,
			expected: true,
		},
		{
			name:     "False",
			input:    sl.False,
			expected: false,
		},
		{
			name:     "Int",
			input:    sl.MakeInt(42),
			expected: int64(42),
		},
		{
			name:     "String",
			input:    sl.String("hello"),
			expected: "hello",
		},
		{
			name:     "Float",
			input:    sl.Float(3.14),
			expected: float64(3.14),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sl_decode(tt.input)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("sl_decode(%v) = %v (%T), want %v (%T)", tt.input, result, result, tt.expected, tt.expected)
			}
		})
	}
}

// Test sl_decode with List
func TestSlDecodeList(t *testing.T) {
	list := sl.NewList([]sl.Value{sl.String("a"), sl.String("b"), sl.MakeInt(3)})
	result := sl_decode(list)

	arr, ok := result.([]any)
	if !ok {
		t.Fatalf("sl_decode(List) should return []any, got %T", result)
	}

	if len(arr) != 3 {
		t.Fatalf("sl_decode(List) length = %d, want 3", len(arr))
	}

	if arr[0] != "a" {
		t.Errorf("arr[0] = %v, want 'a'", arr[0])
	}
	if arr[1] != "b" {
		t.Errorf("arr[1] = %v, want 'b'", arr[1])
	}
	if arr[2] != int64(3) {
		t.Errorf("arr[2] = %v, want 3", arr[2])
	}
}

// Test sl_decode with Tuple
func TestSlDecodeTuple(t *testing.T) {
	tuple := sl.Tuple{sl.String("x"), sl.MakeInt(10)}
	result := sl_decode(tuple)

	arr, ok := result.([]any)
	if !ok {
		t.Fatalf("sl_decode(Tuple) should return []any, got %T", result)
	}

	if len(arr) != 2 {
		t.Fatalf("sl_decode(Tuple) length = %d, want 2", len(arr))
	}

	if arr[0] != "x" {
		t.Errorf("arr[0] = %v, want 'x'", arr[0])
	}
	if arr[1] != int64(10) {
		t.Errorf("arr[1] = %v, want 10", arr[1])
	}
}

// Test sl_decode with Dict
func TestSlDecodeDict(t *testing.T) {
	dict := sl.NewDict(2)
	dict.SetKey(sl.String("name"), sl.String("test"))
	dict.SetKey(sl.String("count"), sl.MakeInt(5))

	result := sl_decode(dict)

	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("sl_decode(Dict) should return map[string]any, got %T", result)
	}

	if m["name"] != "test" {
		t.Errorf("m['name'] = %v, want 'test'", m["name"])
	}
	if m["count"] != int64(5) {
		t.Errorf("m['count'] = %v, want 5", m["count"])
	}
}

// Test sl_encode_tuple
func TestSlEncodeTuple(t *testing.T) {
	result := sl_encode_tuple("hello", 42, true)

	if len(result) != 3 {
		t.Fatalf("sl_encode_tuple length = %d, want 3", len(result))
	}

	s, _ := sl.AsString(result[0])
	if s != "hello" {
		t.Errorf("result[0] = %v, want 'hello'", result[0])
	}

	i, ok := result[1].(sl.Int)
	if !ok {
		t.Fatalf("result[1] should be Int, got %T", result[1])
	}
	val, _ := i.Int64()
	if val != 42 {
		t.Errorf("result[1] = %v, want 42", val)
	}

	if result[2] != sl.True {
		t.Errorf("result[2] = %v, want True", result[2])
	}
}

// Test roundtrip encode/decode
func TestSlRoundtrip(t *testing.T) {
	tests := []struct {
		name  string
		input any
	}{
		{"string", "hello world"},
		{"int", int64(12345)},
		{"bool", true},
		{"string slice", []string{"a", "b", "c"}},
		{"map", map[string]string{"key": "value"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := sl_encode(tt.input)
			decoded := sl_decode(encoded)

			// For slices, we need special handling since []string becomes []any
			switch v := tt.input.(type) {
			case []string:
				arr, ok := decoded.([]any)
				if !ok {
					t.Errorf("roundtrip failed: expected []any, got %T", decoded)
					return
				}
				if len(arr) != len(v) {
					t.Errorf("roundtrip length mismatch: got %d, want %d", len(arr), len(v))
					return
				}
				for i, s := range v {
					if arr[i] != s {
						t.Errorf("roundtrip mismatch at %d: got %v, want %v", i, arr[i], s)
					}
				}
			case map[string]string:
				m, ok := decoded.(map[string]any)
				if !ok {
					t.Errorf("roundtrip failed: expected map[string]any, got %T", decoded)
					return
				}
				for k, val := range v {
					if m[k] != val {
						t.Errorf("roundtrip mismatch for key %s: got %v, want %v", k, m[k], val)
					}
				}
			default:
				if !reflect.DeepEqual(decoded, tt.input) {
					t.Errorf("roundtrip failed: got %v (%T), want %v (%T)", decoded, decoded, tt.input, tt.input)
				}
			}
		})
	}
}

// Benchmark sl_encode
func BenchmarkSlEncode(b *testing.B) {
	inputs := []any{
		"hello",
		42,
		map[string]string{"key": "value", "foo": "bar"},
		[]string{"a", "b", "c", "d", "e"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl_encode(inputs[i%len(inputs)])
	}
}

// Benchmark sl_decode
func BenchmarkSlDecode(b *testing.B) {
	inputs := []sl.Value{
		sl.String("hello"),
		sl.MakeInt(42),
		sl.Tuple{sl.String("a"), sl.String("b"), sl.String("c")},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl_decode(inputs[i%len(inputs)])
	}
}

// Test that Starlark timeout properly cancels execution
func TestStarlarkTimeoutCancellation(t *testing.T) {
	// Save original timeout and set a short one for testing
	original_timeout := starlark_default_timeout
	starlark_default_timeout = 100 * time.Millisecond
	defer func() { starlark_default_timeout = original_timeout }()

	// Initialize semaphore if not already done
	if starlark_semaphore == nil {
		starlark_semaphore = make(chan struct{}, 4)
	}

	// Enable recursion for this test
	resolve.AllowRecursion = true

	// Create a Starlark interpreter with an infinite loop function
	s := &Starlark{
		thread:  &sl.Thread{Name: "test"},
		globals: sl.StringDict{},
	}

	// Define an infinite loop function using recursion (while loops not enabled)
	code := `
def infinite_loop():
    return infinite_loop()
`
	globals, err := sl.ExecFile(s.thread, "test.star", code, s.globals)
	if err != nil {
		t.Fatalf("Failed to execute Starlark code: %v", err)
	}
	s.globals = globals

	// Call the infinite loop - should timeout and cancel
	start := time.Now()
	_, err = s.call("infinite_loop", nil)
	elapsed := time.Since(start)

	// Verify we got an error (either timeout or cancellation)
	if err == nil {
		t.Fatal("Expected error from cancelled execution, got nil")
	}

	// Verify it didn't take much longer than the timeout
	if elapsed > 500*time.Millisecond {
		t.Errorf("Execution took %v, expected ~100ms timeout", elapsed)
	}

	t.Logf("Timeout worked: elapsed=%v, error=%v", elapsed, err)
}

// Test that Starlark step limit stops runaway scripts
func TestStarlarkStepLimit(t *testing.T) {
	// Use a long timeout so we know step limit triggered, not timeout.
	// Generous to absorb -race overhead — under the race detector,
	// hitting 1B Starlark steps can take >10s on a contended host.
	original_timeout := starlark_default_timeout
	starlark_default_timeout = 30 * time.Second
	defer func() { starlark_default_timeout = original_timeout }()

	// Initialize semaphore if not already done
	if starlark_semaphore == nil {
		starlark_semaphore = make(chan struct{}, 4)
	}

	// Create a Starlark interpreter
	s := &Starlark{
		thread:  &sl.Thread{Name: "test"},
		globals: sl.StringDict{},
	}

	// Define a nested loop using smaller ranges to hit step limit
	// Each range() is small enough to allocate quickly, but total iterations exceed step limit
	code := `
def cpu_hog():
    x = 0
    for i in range(100000):
        for j in range(100000):
            x += 1
    return x
`
	globals, err := sl.ExecFile(s.thread, "test.star", code, s.globals)
	if err != nil {
		t.Fatalf("Failed to execute Starlark code: %v", err)
	}
	s.globals = globals

	// Call the CPU hog - should hit step limit quickly
	start := time.Now()
	_, err = s.call("cpu_hog", nil)
	elapsed := time.Since(start)

	// Verify we got an error
	if err == nil {
		t.Fatal("Expected error from step limit, got nil")
	}

	// Either the step limit or the timeout may stop the runaway script: under
	// -race the per-step atomics serialise enough that wall clock can win. What
	// matters is that cpu_hog was cancelled rather than running to completion.
	if !strings.Contains(err.Error(), "cancelled") {
		t.Errorf("expected cancellation error, got: %v", err)
	}

	t.Logf("Step limit worked: elapsed=%v, error=%v", elapsed, err)
}

// sl_json_append is value as sl_json_writer writes it whole.
func sl_json_append(data []byte, value sl.Value) ([]byte, error) {
	w := sl_json_writer{data: data}
	err := w.value(value, nil)
	return w.data, err
}

// sl_json_values are Starlark values an action may answer with, each shape the
// encoder treats on its own.
func sl_json_values(t *testing.T) []sl.Value {
	t.Helper()
	var values []sl.Value
	for b := 0; b < 256; b++ {
		values = append(values, sl.String(string([]byte{byte(b)})), sl.String("a"+string([]byte{byte(b)})+"z"))
	}
	for _, text := range []string{"", "plain", "<script>&amp;</script>", "line\u2028separator\u2029", "\xff\xfe", "\xe2\x82", "\xf0\x9f\x98", "café ☕ 😀", "tab\tnew\nline\r\b\f", "quote\" back\\", "\x7f", "\u0000"} {
		values = append(values, sl.String(text))
	}
	random := rand.New(rand.NewSource(1))
	for range 2000 {
		data := make([]byte, random.Intn(24))
		random.Read(data)
		values = append(values, sl.String(data))
	}
	for _, f := range []float64{0, math.Copysign(0, -1), 1, -1.5, 0.1, 1.0 / 3, 123.456, 1e-6, 9.99e-7, 1e-7, 1.5e-10, 1e20, 1e21, 1.5e21, 5e-324, math.MaxFloat64, -math.MaxFloat64, 1e100, 12345678912345678} {
		values = append(values, sl.Float(f))
	}
	huge := new(big.Int).Lsh(big.NewInt(1), 70)
	for _, i := range []sl.Value{sl.MakeInt(0), sl.MakeInt(-1), sl.MakeInt64(math.MaxInt64), sl.MakeInt64(math.MinInt64), sl.MakeBigInt(huge), sl.MakeBigInt(new(big.Int).Neg(huge))} {
		values = append(values, i)
	}
	values = append(values, sl.None, sl.True, sl.False, sl.Bytes(""), sl.Bytes("abc\xff"), sl.NewList(nil), sl.Tuple{}, sl.NewDict(0), sl.NewSet(1))

	row := sl.NewDict(8)
	row.SetKey(sl.String("zeta"), sl.MakeInt(1))
	row.SetKey(sl.String("alpha"), sl.String("<b>"))
	row.SetKey(sl.MakeInt(7), sl.String("a key that is not a string is left out"))
	row.SetKey(sl.String("Émile"), sl.Float(2.5))
	row.SetKey(sl.String("a\u2028b"), sl.None)
	row.SetKey(sl.String("Alpha"), sl.NewList([]sl.Value{sl.True, sl.Tuple{sl.MakeInt(1), sl.String("x")}, sl.NewDict(0)}))
	row.SetKey(sl.String("\xff"), sl.Bytes("raw"))
	values = append(values, row, sl.NewList([]sl.Value{row, row}), sl.Tuple{row, sl.NewList([]sl.Value{sl.None})})
	return append(values, sl_held_values(t)...)
}

// sl_held_values are values that hold themselves - a list, and a dictionary
// twice over, which branches at every level - and one nested past the depth
// core follows.
func sl_held_values(t *testing.T) []sl.Value {
	t.Helper()
	globals, err := sl.ExecFile(&sl.Thread{}, "held.star", `
listed = []
listed.append(listed)
keyed = {"name": "outer"}
keyed["left"] = keyed
keyed["right"] = [keyed, (keyed,)]
deep = []
for i in range(1500):
    deep = [deep]
`, nil)
	if err != nil {
		t.Fatal(err)
	}
	return []sl.Value{globals["listed"], globals["keyed"], globals["deep"]}
}

// TestSlDecodeLeavesOutAValueThatHoldsItself: Starlark lets a list or a
// dictionary hold itself. Followed without a bound, such a value recursed
// until the stack ran out - a fatal error no recover catches, so one app's
// value ended the whole server - and one held twice over branched at every
// level. The inner one is left out, as null; so is anything nested past the
// depth core follows.
func TestSlDecodeLeavesOutAValueThatHoldsItself(t *testing.T) {
	held := sl_held_values(t)
	if got := sl_decode(held[0]); !reflect.DeepEqual(got, []any{nil}) {
		t.Errorf("THE POINT: a list holding itself decoded as %v, want [nil]", got)
	}
	want := map[string]any{"name": "outer", "left": nil, "right": []any{nil, []any{nil}}}
	if got := sl_decode(held[1]); !reflect.DeepEqual(got, want) {
		t.Errorf("THE POINT: a dictionary holding itself twice over decoded as %v, want %v", got, want)
	}
	depth := 0
	for value := sl_decode(held[2]); value != nil; depth++ {
		list, ok := value.([]any)
		if !ok || len(list) != 1 {
			break
		}
		value = list[0]
	}
	if depth != sl_depth_maximum {
		t.Errorf("a value nested 1,500 deep decoded %d deep, want cut at %d", depth, sl_depth_maximum)
	}
}

// TestActionAnswerJSONMatchesMarshal: an action's answer is written straight
// from its Starlark value, not through sl_decode's tree of Go values and
// json.Marshal, which allocated thirteen times a large answer's size. The bytes
// must be the very ones json.Marshal(sl_decode(value)) gave: pages, the Android
// client and other servers parse them, and HTML escaping keeps an answer
// rendered into a page from closing its script.
func TestActionAnswerJSONMatchesMarshal(t *testing.T) {
	for _, value := range sl_json_values(t) {
		want, refused := json.Marshal(sl_decode(value))
		got, failure := sl_json_append(nil, value)
		if (refused != nil) != (failure != nil) {
			t.Errorf("%s: error %v, json.Marshal's %v", value.String(), failure, refused)
			continue
		}
		if refused == nil && string(got) != string(want) {
			t.Errorf("THE POINT: %s is written as %q, json.Marshal wrote %q", value.String(), got, want)
		}
	}
	for _, f := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		list := sl.NewList([]sl.Value{sl.String("before"), sl.Float(f)})
		if _, err := sl_json_append(nil, list); err == nil {
			t.Errorf("%v was written, though JSON has no such number", f)
		}
	}
}

// TestActionAnswerRendersAsJSONDid: through gin, the answer carries the same
// status, type and body as c.JSON gave it, keeps a type the action set
// itself, and a NaN writes nothing and is recorded against the request.
func TestActionAnswerRendersAsJSONDid(t *testing.T) {
	gin.SetMode(gin.TestMode)
	render := func(value sl.Value, typed bool, old bool) (*httptest.ResponseRecorder, *gin.Context) {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
		if typed {
			c.Header("Content-Type", "text/plain")
		}
		if old {
			c.JSON(http.StatusOK, sl_decode(value))
		} else {
			c.Render(http.StatusOK, sl_json{value: value})
		}
		c.Writer.WriteHeaderNow()
		return recorder, c
	}
	for _, value := range sl_json_values(t)[:40] {
		for _, typed := range []bool{false, true} {
			want, _ := render(value, typed, true)
			got, _ := render(value, typed, false)
			if got.Code != want.Code || got.Body.String() != want.Body.String() || got.Header().Get("Content-Type") != want.Header().Get("Content-Type") {
				t.Errorf("%s (typed %v): %d %q %q, c.JSON gave %d %q %q", value.String(), typed, got.Code, got.Header().Get("Content-Type"), got.Body.String(), want.Code, want.Header().Get("Content-Type"), want.Body.String())
			}
		}
	}
	// Larger than one piece, so written in several.
	rows := make([]sl.Value, 0, 5000)
	for i := range 5000 {
		row := sl.NewDict(3)
		row.SetKey(sl.String("id"), sl.MakeInt(i))
		row.SetKey(sl.String("name"), sl.String(strings.Repeat("<name> ", i%7)))
		row.SetKey(sl.String("score"), sl.Float(float64(i)/7))
		rows = append(rows, row)
	}
	large := sl.NewList(rows)
	want, _ := render(large, false, true)
	got, _ := render(large, false, false)
	if want.Body.Len() <= 2*sl_json_piece || got.Body.String() != want.Body.String() || got.Code != want.Code {
		t.Errorf("THE POINT: an answer of %d bytes, written in pieces, came out as %d bytes unlike c.JSON's", want.Body.Len(), got.Body.Len())
	}

	// A value JSON cannot carry fails the answer before a byte of it goes, in a
	// small answer and in one past the first piece alike.
	for _, broken := range []sl.Value{
		sl.NewList([]sl.Value{sl.String("before"), sl.Float(math.NaN())}),
		sl.NewList(append(append([]sl.Value{}, rows...), sl.Float(math.Inf(1)))),
	} {
		want, old := render(broken, false, true)
		got, c := render(broken, false, false)
		if got.Code != want.Code || got.Body.Len() != 0 || want.Body.Len() != 0 || len(c.Errors) != len(old.Errors) || !c.IsAborted() {
			t.Errorf("THE POINT: an answer holding a NaN or an infinity: %d with %d bytes and %d errors, c.JSON gave %d with %d bytes and %d", got.Code, got.Body.Len(), len(c.Errors), want.Code, want.Body.Len(), len(old.Errors))
		}
	}
}
