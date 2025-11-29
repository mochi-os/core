// Mochi server: Starlark unit tests
// Copyright Alistair Cunningham 2025

package main

import (
	"reflect"
	"testing"

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
