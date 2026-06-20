// Mochi shared ini parsing + MOCHI_<SECTION>_<KEY> env overrides.
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package ini

import (
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"

	goini "gopkg.in/ini.v1"
)

var (
	file                *goini.File
	match_commas_spaces = regexp.MustCompile("[\\s,]+")
)

// Load reads the INI file at the given path. Subsequent accessor calls return
// values from this file unless overridden by a MOCHI_<SECTION>_<KEY> env var.
func Load(path string) error {
	var err error
	file, err = goini.Load(path)
	return err
}

// Loaded reports whether Load has been called successfully.
func Loaded() bool {
	return file != nil
}

// env returns the MOCHI_<SECTION>_<KEY> environment override and whether it
// was set. Section and key are uppercased; empty env vars are treated as
// explicit overrides to empty string, not fall-throughs.
func env(section, key string) (string, bool) {
	return os.LookupEnv("MOCHI_" + strings.ToUpper(section) + "_" + strings.ToUpper(key))
}

func Bool(section, key string, def bool) bool {
	if v, ok := env(section, key); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
		log.Printf("Invalid bool in MOCHI_%s_%s=%q, falling back to config file",
			strings.ToUpper(section), strings.ToUpper(key), v)
	}
	if file == nil {
		return def
	}
	return file.Section(section).Key(key).MustBool(def)
}

func Int(section, key string, def int) int {
	if v, ok := env(section, key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Printf("Invalid int in MOCHI_%s_%s=%q, falling back to config file",
			strings.ToUpper(section), strings.ToUpper(key), v)
	}
	if file == nil {
		return def
	}
	return file.Section(section).Key(key).MustInt(def)
}

func String(section, key, def string) string {
	if v, ok := env(section, key); ok {
		return v
	}
	if file == nil {
		return def
	}
	return file.Section(section).Key(key).MustString(def)
}

// Strings returns the value parsed as a comma- or whitespace-separated list.
func Strings(section, key string) []string {
	var raw string
	if v, ok := env(section, key); ok {
		raw = v
	} else if file != nil {
		raw = file.Section(section).Key(key).MustString("")
	}
	s := match_commas_spaces.Split(raw, -1)
	if len(s) == 1 && s[0] == "" {
		return nil
	}
	return s
}

// Ints returns the value parsed as a comma- or whitespace-separated list of
// integers; non-integer entries are silently skipped.
func Ints(section, key string) []int {
	strs := Strings(section, key)
	if strs == nil {
		return nil
	}
	result := make([]int, 0, len(strs))
	for _, s := range strs {
		if n, err := strconv.Atoi(s); err == nil {
			result = append(result, n)
		}
	}
	return result
}

// Effective returns the merged view of (file values + MOCHI_<SECTION>_<KEY>
// env overrides). Each top-level key is a section name; values are
// section-key-to-value maps. Sensitive keys (anything matching *password,
// *secret, *key, *token, case-insensitive) are returned with the value
// replaced by "***redacted***".
func Effective() map[string]map[string]string {
	out := map[string]map[string]string{}
	if file != nil {
		for _, sec := range file.Sections() {
			name := sec.Name()
			if name == "DEFAULT" {
				continue
			}
			m, ok := out[name]
			if !ok {
				m = map[string]string{}
				out[name] = m
			}
			for _, k := range sec.Keys() {
				m[k.Name()] = redact(k.Name(), k.Value())
			}
		}
	}
	// Walk MOCHI_<SECTION>_<KEY> env vars to surface overrides for keys not
	// present in the file at all.
	for _, kv := range os.Environ() {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			continue
		}
		name, val := kv[:eq], kv[eq+1:]
		if !strings.HasPrefix(name, "MOCHI_") {
			continue
		}
		rest := strings.ToLower(name[len("MOCHI_"):])
		under := strings.IndexByte(rest, '_')
		if under <= 0 || under == len(rest)-1 {
			continue
		}
		section, key := rest[:under], rest[under+1:]
		m, ok := out[section]
		if !ok {
			m = map[string]string{}
			out[section] = m
		}
		m[key] = redact(key, val)
	}
	return out
}

// redact masks values whose key name suggests a credential.
func redact(key, value string) string {
	low := strings.ToLower(key)
	for _, marker := range []string{"password", "secret", "key", "token"} {
		if strings.HasSuffix(low, marker) || strings.Contains(low, marker) {
			if value == "" {
				return ""
			}
			return "***redacted***"
		}
	}
	return value
}
