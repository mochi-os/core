// Comms server: Utilities
// Copyright Alistair Cunningham 2024

package main

import (
	"crypto/rand"
	sha "crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const alphanumeric = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

var match_non_controls = regexp.MustCompile("^[\\P{Cc}\\r\\n]*$")
var match_hyphens = regexp.MustCompile(`-`)

func atoi(s string, def int64) int64 {
	i, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return int64(i)
}

func base64_decode(s string, def string) []byte {
	pad := 4 - len(s)%4
	if pad < 4 {
		s = s + strings.Repeat("=", pad)
	}
	bytes, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		log_info("Base64 decoding error for '%s'; returning default '%s': %s", s, def, err)
		return []byte(def)
	}
	return bytes
}

func base64_encode(b []byte) string {
	return base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(b)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func error_message(message string, values ...any) error {
	return errors.New(fmt.Sprintf(message, values...))
}

//TODO Use base 58? https://pkg.go.dev/github.com/btcsuite/btcutil/base58
func fingerprint(in string) string {
	s := sha.New()
	s.Write([]byte(in))
	b64 := base64_encode(s.Sum(nil))
	return b64[0:9]
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
		log_warn("Invalid time zone '%s':", err)
		return time.Unix(t, 0).Format(time.DateTime)
	}
}

func uid() string {
	u, err := uuid.NewV7()
	check(err)
	return match_hyphens.ReplaceAllLiteralString(u.String(), "")
}

func valid(s string, match string) bool {
	//log_debug("Validating '%s' (%#v) as %s", s, s, match)
	if !match_non_controls.MatchString(s) {
		return false
	}

	switch match {
	case "constant":
		match = "^[0-9a-z-/]{1,100}$"
	case "id":
		match = "^[\\w-]{32}$"
	case "line":
		match = "^[^\r\n]{1,1000}$"
	case "name":
		match = "^[^<>\r\n\\;\"'`]{1,1000}$"
	case "privacy":
		match = "^(public|private)$"
	case "public":
		match = "^[\\w-=]{43,44}$"
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
