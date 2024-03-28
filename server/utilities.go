// Comms server: Utilities
// Copyright Alistair Cunningham 2024

package main

import (
	"crypto/rand"
	sha "crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"math/big"
	"os"
	"regexp"
	"strconv"
	"time"
)

const alphanumeric = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

var match_non_controls = regexp.MustCompile("^[\\P{Cc}\\n]*$")
var match_hyphens = regexp.MustCompile(`-`)

func append_space(sp *string, s string) {
	if *sp == "" {
		*sp = s
	} else {
		*sp = *sp + " " + s
	}
}

func atoi(s string, def int) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return i
}

func base64_decode(s string) []byte {
	bytes, err := base64.URLEncoding.DecodeString(s)
	fatal(err)
	return bytes
}

func base64_encode(b []byte) string {
	return base64.URLEncoding.EncodeToString(b)
}

func error_message(message string, values ...any) error {
	return errors.New(fmt.Sprintf(message, values...))
}

func fatal(err error) {
	if err != nil {
		log_error(err.Error())
		os.Exit(1)
	}
}

func fingerprint(in string) string {
	s := sha.New()
	s.Write([]byte(in))
	b64 := base64_encode(s.Sum(nil))
	return b64[0:10]
}

func random_alphanumeric(length int) string {
	out := make([]rune, length)
	l := big.NewInt(int64(len(alphanumeric)))
	for i := range out {
		index, err := rand.Int(rand.Reader, l)
		fatal(err)
		out[i] = rune(alphanumeric[index.Int64()])
	}
	return string(out)
}

func sha1(in []byte) string {
	s := sha.New()
	s.Write(in)
	return hex.EncodeToString(s.Sum(nil))
}

func time_unix() int64 {
	return time.Now().Unix()
}

func uid() string {
	u, err := uuid.NewV7()
	fatal(err)
	return match_hyphens.ReplaceAllLiteralString(u.String(), "")
}

func valid(s string, match string) bool {
	//log_debug("valid( '%s', '%s' )", s, match)
	if !match_non_controls.MatchString(s) {
		return false
	}

	if match == "constant" {
		match = "^[0-9a-z-]{1,100}$"

	} else if match == "id" {
		match = "^[\\w-=]{1,1000}$"

	} else if match == "name" {
		match = "^[^<>\r\n\\;\"'`]{1,1000}$"

	} else if match == "public" {
		match = "^[\\w-=]{44}$"

	} else if match == "text" {
		if len(s) > 10000 {
			return false
		}
		return true

	} else if match == "uid" {
		match = "^[0-9a-z]{32}"

	} else if match == "url" {
		if len(s) > 10000 {
			return false
		}
		match = "^[\\w\\-\\/:%@.+?&;=~]*$"
	}

	m, err := regexp.MatchString(match, s)
	fatal(err)
	if !m {
		return false
	}

	return true
}
