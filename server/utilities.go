// Mochi server: Utilities
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"crypto/rand"
	sha "crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/btcsuite/btcutil/base58"
	cbor "github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"math/big"
	"regexp"
	"strconv"
	"time"
)

const (
	alphanumeric = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

var (
	match_filename     = regexp.MustCompile("^[0-9a-zA-Z][0-9a-zA-Z-._ ()]{0,254}$")
	match_hyphens      = regexp.MustCompile(`-`)
	match_non_controls = regexp.MustCompile("^[\\P{Cc}\\r\\n]*$")
)

func atoi(s string, def int64) int64 {
	i, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return int64(i)
}

func base58_decode(in string, def string) []byte {
	out, _, err := base58.CheckDecode(in)
	if err != nil {
		info("Base58 decoding error for '%s'; returning default '%s': %s", in, def, err)
		return []byte(def)
	}
	return out
}

func base58_encode(in []byte) string {
	return base58.CheckEncode(in, 0)
}

func cbor_encode(in any) []byte {
	out, err := cbor.Marshal(in)
	check(err)
	return out
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func error_message(message string, values ...any) error {
	return errors.New(fmt.Sprintf(message, values...))
}

func fingerprint(in string) string {
	s := sha.New()
	s.Write([]byte(in))
	encoded := base58_encode(s.Sum(nil))
	return encoded[len(encoded)-9:]
}

func fingerprint_hyphens(in string) string {
	return in[:3] + "-" + in[3:6] + "-" + in[6:]
}

func json_decode(out any, j string) bool {
	err := json.Unmarshal([]byte(j), out)
	if err != nil {
		return false
	}
	return true
}

func json_encode(in any) string {
	j, err := json.Marshal(in)
	check(err)
	return string(j)
}

func now() int64 {
	return time.Now().Unix()
}

func now_string() string {
	return strconv.FormatInt(time.Now().Unix(), 10)
}

func random_alphanumeric(length int) string {
	out := make([]rune, length)
	l := big.NewInt(int64(len(alphanumeric)))
	for i := range out {
		index, err := rand.Int(rand.Reader, l)
		check(err)
		out[i] = rune(alphanumeric[index.Int64()])
	}
	return string(out)
}

func sha1(in []byte) string {
	s := sha.New()
	s.Write(in)
	return hex.EncodeToString(s.Sum(nil))
}

func time_local(u *User, t int64) string {
	timezone := "UTC"
	if u != nil {
		timezone = u.Timezone
	}

	l, err := time.LoadLocation(timezone)
	if err == nil {
		return time.Unix(t, 0).In(l).Format(time.DateTime)
	} else {
		warn("Invalid time zone '%s':", err)
		return time.Unix(t, 0).Format(time.DateTime)
	}
}

func uid() string {
	u, err := uuid.NewV7()
	check(err)
	return match_hyphens.ReplaceAllLiteralString(u.String(), "")
}

func valid(s string, match string) bool {
	//debug("Validating '%s' (%#v) as %s", s, s, match)
	if !match_non_controls.MatchString(s) {
		return false
	}

	switch match {
	case "constant":
		match = "^[0-9a-z-/]{1,100}$"
	case "entity":
		match = "^[\\w]{50,51}$"
	case "filename":
		match = "^[^<>\r\n\\;\"'`\\.][^<>\r\n\\;\"'`]{0,253}$"
	case "id":
		match = "^[\\w-]{32}$"
	case "integer":
		match = "^(-)?\\d{1,9}$"
	case "line":
		match = "^[^\r\n]{1,1000}$"
	case "name":
		match = "^[^<>\r\n\\;\"'`]{1,1000}$"
	case "natural":
		match = "^\\d{1,9}$"
	case "path":
		match = "^[0-9a-zA-Z-/]{1,1000}$"
	case "postive":
		match = "^\\d{1,9}$"
	case "privacy":
		match = "^(public|private)$"
	case "text":
		if len(s) > 10000 {
			return false
		}
		return true
	case "uid":
		match = "^[0-9a-z]{32}"
	case "url":
		if len(s) > 10000 {
			return false
		}
		match = "^[\\w\\-\\/:%@.+?&;=~]*$"
	}

	m, err := regexp.MatchString(match, s)
	check(err)
	if !m {
		return false
	}

	return true
}
