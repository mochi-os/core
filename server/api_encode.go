// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/base64"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

var api_encode = sls.FromStringDict(sl.String("mochi.encode"), sl.StringDict{
	"base64": sl.NewBuiltin("mochi.encode.base64", api_encode_base64),
})

var api_decode = sls.FromStringDict(sl.String("mochi.decode"), sl.StringDict{
	"base64": sl.NewBuiltin("mochi.decode.base64", api_decode_base64),
})

// mochi.encode.base64(data) -> string: Standard base64 encoding of data.
// Accepts either a string or bytes — useful for embedding binary content
// (attachment data, generated files) in JSON.
func api_encode_base64(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <data: string|bytes>")
	}
	var data []byte
	switch v := args[0].(type) {
	case sl.String:
		data = []byte(string(v))
	case sl.Bytes:
		data = []byte(v)
	default:
		return sl_error(fn, "data must be a string or bytes")
	}
	return sl.String(base64.StdEncoding.EncodeToString(data)), nil
}

// mochi.decode.base64(text) -> bytes or None: Decode standard base64 text.
// Returns None when the input is not valid base64, so callers can validate
// untrusted input without a failed-call error.
func api_decode_base64(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <text: string>")
	}
	text, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "text must be a string")
	}
	data, err := base64.StdEncoding.DecodeString(text)
	if err != nil {
		return sl.None, nil
	}
	return sl.Bytes(data), nil
}
