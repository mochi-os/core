// mochictl: response formatting (human / tab-separated / JSON).
// Copyright Alistair Cunningham 2026
//
// The server always returns JSON. Format selection happens in the client:
//
//	default:  human-friendly aligned `Key: value` lines (capitalised labels)
//	-t:       tab-separated `key<TAB>value`, dot-flattened nested keys (TSV)
//	-j:       indented JSON pass-through
//
// Inputs that aren't valid JSON (e.g. plain-text error bodies) are written
// straight to stdout regardless of mode.

//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// render writes body to stdout in the format selected by global flags. If
// order is non-empty it controls the order of the top-level object keys;
// any keys not in the order list come after in alphabetical order. Nested
// objects always sort alphabetically.
func render(body []byte, order ...string) error {
	if flag_json {
		return render_json(body)
	}

	// Use json.Number for numeric values so we don't lose precision on large
	// integers (uint64 > 2^53 truncates as float64) and don't end up with
	// scientific notation on plain `%v` formatting.
	var v any
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		// Not JSON — passthrough.
		os.Stdout.Write(body)
		if len(body) > 0 && body[len(body)-1] != '\n' {
			fmt.Println()
		}
		return nil
	}

	if flag_tabs {
		render_tabs(v, "", order)
		return nil
	}
	render_human(v, "", longest_key(v), order)
	return nil
}

// render_json pretty-prints JSON (re-marshalled with 2-space indent so the
// output is consistent regardless of whether the server compacted it).
func render_json(body []byte) error {
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		os.Stdout.Write(body)
		return nil
	}
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

// render_tabs writes one record per line as `key<TAB>value`. Nested
// objects are dot-flattened (`section.key`). Arrays use indexed keys
// (`field[0]`, `field[1]`). order applies to the top-level only.
func render_tabs(v any, prefix string, order []string) {
	switch x := v.(type) {
	case map[string]any:
		var keys []string
		if prefix == "" && len(order) > 0 {
			keys = ordered_keys(x, order)
		} else {
			keys = sorted_keys(x)
		}
		for _, k := range keys {
			full := k
			if prefix != "" {
				full = prefix + "." + k
			}
			switch sub := x[k].(type) {
			case map[string]any, []any:
				render_tabs(sub, full, nil)
			default:
				fmt.Printf("%s\t%s\n", full, format_scalar(sub))
			}
		}
	case []any:
		for i, item := range x {
			full := fmt.Sprintf("%s[%d]", prefix, i)
			switch item.(type) {
			case map[string]any, []any:
				render_tabs(item, full, nil)
			default:
				fmt.Printf("%s\t%s\n", full, format_scalar(item))
			}
		}
	default:
		if prefix == "" {
			fmt.Println(format_scalar(x))
		} else {
			fmt.Printf("%s\t%s\n", prefix, format_scalar(x))
		}
	}
}

// render_human writes aligned `Label: value` lines for object fields,
// indented bullet lists for arrays, and recurses into nested objects.
// width is the column width to align values to. order applies to the
// top-level only (nested objects always sort alphabetically).
func render_human(v any, indent string, width int, order []string) {
	switch x := v.(type) {
	case map[string]any:
		var keys []string
		if indent == "" && len(order) > 0 {
			keys = ordered_keys(x, order)
		} else {
			keys = sorted_keys(x)
		}
		for _, k := range keys {
			label := humanise(k)
			switch sub := x[k].(type) {
			case map[string]any:
				fmt.Printf("%s%s:\n", indent, label)
				render_human(sub, indent+"  ", longest_key(sub), nil)
			case []any:
				if len(sub) == 0 {
					fmt.Printf("%s%-*s (none)\n", indent, width, label+":")
					continue
				}
				fmt.Printf("%s%s:\n", indent, label)
				for _, item := range sub {
					switch it := item.(type) {
					case map[string]any:
						render_human(it, indent+"  ", longest_key(it), nil)
					default:
						fmt.Printf("%s  - %v\n", indent, it)
					}
				}
			default:
				if sub == nil {
					fmt.Printf("%s%-*s (none)\n", indent, width, label+":")
				} else {
					fmt.Printf("%s%-*s %s\n", indent, width, label+":", humanise_value(k, sub))
				}
			}
		}
	case []any:
		for _, item := range x {
			switch it := item.(type) {
			case map[string]any:
				render_human(it, indent, longest_key(it), nil)
			default:
				fmt.Printf("%s- %v\n", indent, it)
			}
		}
	default:
		fmt.Println(x)
	}
}

// humanise turns "snake_case_key" into "Snake case key". Technical-unit
// suffixes (`_ms`, `_seconds`, `_bytes`) are stripped first because the
// humanised value already carries the unit (e.g. "1.6 GB", "5m 3s").
func humanise(k string) string {
	for _, suffix := range []string{"_ms", "_seconds", "_bytes"} {
		if strings.HasSuffix(k, suffix) {
			k = strings.TrimSuffix(k, suffix)
			break
		}
	}
	out := strings.ReplaceAll(k, "_", " ")
	if out == "" {
		return out
	}
	return strings.ToUpper(out[:1]) + out[1:]
}

// humanise_value formats a leaf value for human-readable output, applying
// field-name-based heuristics:
//   - "uptime" or "*_seconds": render as a compact duration ("5s", "5m 3s")
//   - "*_ms": render in ms or rounded to the nearest second
//   - keys containing "bytes": render with a binary unit suffix ("1.6 GB")
//
// Falls back to a clean number/string representation for everything else.
func humanise_value(key string, v any) string {
	switch {
	case key == "uptime" || strings.HasSuffix(key, "_seconds"):
		if n, ok := to_int(v); ok {
			return humanise_duration(n)
		}
	case strings.HasSuffix(key, "_ms"):
		if n, ok := to_int(v); ok {
			if n < 1000 {
				return fmt.Sprintf("%dms", n)
			}
			// Round to the nearest second.
			return humanise_duration((n + 500) / 1000)
		}
	case strings.Contains(key, "bytes"):
		if n, ok := to_int(v); ok {
			return humanise_bytes(n)
		}
	}
	return format_scalar(v)
}

// humanise_duration renders a number of seconds in a compact form.
// Examples: 5 -> "5s", 65 -> "1m 5s", 3725 -> "1h 2m 5s", 86461 -> "1d 0h 1m".
func humanise_duration(seconds int64) string {
	if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	}
	d := seconds / 86400
	h := (seconds % 86400) / 3600
	m := (seconds % 3600) / 60
	s := seconds % 60
	switch {
	case d > 0:
		return fmt.Sprintf("%dd %dh %dm", d, h, m)
	case h > 0:
		return fmt.Sprintf("%dh %dm %ds", h, m, s)
	default:
		return fmt.Sprintf("%dm %ds", m, s)
	}
}

// to_int extracts an int64 from a JSON-decoded value (json.Number or
// already-numeric). Returns ok=false if the value isn't a whole number.
func to_int(v any) (int64, bool) {
	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i, true
		}
	case float64:
		if x == float64(int64(x)) {
			return int64(x), true
		}
	case int:
		return int64(x), true
	case int64:
		return x, true
	}
	return 0, false
}

// format_scalar prints any leaf value cleanly, avoiding scientific notation
// on large whole-number floats and preserving json.Number's exact text.
func format_scalar(v any) string {
	switch x := v.(type) {
	case json.Number:
		return x.String()
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case nil:
		return ""
	}
	return fmt.Sprintf("%v", v)
}

// humanise_bytes renders a byte count with a binary (1024-base) unit.
func humanise_bytes(b int64) string {
	const k = 1024
	if b < k {
		return fmt.Sprintf("%d B", b)
	}
	units := []string{"KB", "MB", "GB", "TB", "PB"}
	v := float64(b)
	i := -1
	for v >= k && i < len(units)-1 {
		v /= k
		i++
	}
	return fmt.Sprintf("%.1f %s", v, units[i])
}

// longest_key returns the column width needed to align humanised keys for an
// object. Returns 0 for non-objects.
func longest_key(v any) int {
	m, ok := v.(map[string]any)
	if !ok {
		return 0
	}
	max := 0
	for k := range m {
		// +1 for the trailing colon in the format string.
		if l := len(humanise(k)) + 1; l > max {
			max = l
		}
	}
	return max
}

// sorted_keys returns the keys of m in lexicographic order so output is stable.
func sorted_keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ordered_keys returns the keys of m in the supplied order (for keys that
// exist in m), followed by any remaining keys in alphabetical order.
func ordered_keys(m map[string]any, order []string) []string {
	seen := make(map[string]bool, len(order))
	out := make([]string, 0, len(m))
	for _, k := range order {
		if _, ok := m[k]; ok && !seen[k] {
			out = append(out, k)
			seen[k] = true
		}
	}
	rest := make([]string, 0, len(m)-len(seen))
	for k := range m {
		if !seen[k] {
			rest = append(rest, k)
		}
	}
	sort.Strings(rest)
	return append(out, rest...)
}
