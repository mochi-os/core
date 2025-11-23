// Mochi server: Utilities
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"archive/zip"
	"bytes"
	"crypto/rand"
	sha "crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/btcsuite/btcutil/base58"
	cbor "github.com/fxamacker/cbor/v2"
	md "github.com/gomarkdown/markdown"
	"github.com/google/uuid"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	alphanumeric = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

var (
	locks              = map[string]*sync.Mutex{}
	locks_lock         sync.Mutex
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

func cbor_decode(out any, in []byte) bool {
	err := cbor.Unmarshal(in, out)
	if err != nil {
		return false
	}
	return true
}

func cbor_encode(in any) []byte {
	return must(cbor.Marshal(in))
}

func check(err error) {
	if err != nil {
		panic(err)
	}
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

func fingerprint_no_hyphens(in string) string {
	return strings.ReplaceAll(in, "-", "")
}

func itoa(in int) string {
	return strconv.Itoa(in)
}

func i64toa(in int64) string {
	return strconv.FormatInt(in, 10)
}

func json_decode(out any, j string) bool {
	err := json.Unmarshal([]byte(j), out)
	if err != nil {
		return false
	}
	return true
}

func json_encode(in any) string {
	return string(must(json.Marshal(in)))
}

func lock(key string) *sync.Mutex {
	locks_lock.Lock()
	defer locks_lock.Unlock()

	_, found := locks[key]
	if !found {
		locks[key] = &sync.Mutex{}
	}

	return locks[key]
}

func markdown(in []byte) []byte {
	return md.ToHTML(in, nil, nil)
}

func must[T any](v T, errors ...error) T {
	if len(errors) == 0 {
		switch e := any(v).(type) {
		case error:
			if e == nil {
				return v
			}
		default:
			return v
		}
		panic(v)
	}
	err := errors[0]
	if err != nil {
		panic(err)
	}
	return v
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
		index := must(rand.Int(rand.Reader, l))
		out[i] = rune(alphanumeric[index.Int64()])
	}
	return string(out)
}

func sha1(in []byte) string {
	s := sha.New()
	s.Write(in)
	return hex.EncodeToString(s.Sum(nil))
}

func structs_to_maps[T any](v []T) *[]map[string]any {
	result := make([]map[string]any, 0, len(v))

	for _, s := range v {
		sv := reflect.ValueOf(s)
		st := reflect.TypeOf(s)

		if sv.Kind() == reflect.Struct {
			m := make(map[string]any)
			for i := 0; i < sv.NumField(); i++ {
				f := st.Field(i)
				name := strings.ToLower(f.Name)
				tag := f.Tag.Get("map")
				if tag != "" && tag != "-" {
					name = tag
				}
				m[name] = sv.Field(i).Interface()
			}
			result = append(result, m)
		}
	}

	return &result
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
	u := must(uuid.NewV7())
	return match_hyphens.ReplaceAllLiteralString(u.String(), "")
}

func unzip(file string, destination string) error {
	r, err := zip.OpenReader(file)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		path := filepath.Join(destination, f.Name)

		if !strings.HasPrefix(path, filepath.Clean(destination)+string(os.PathSeparator)) {
			return fmt.Errorf("Invalid file path '%s'", path)
		}

		if f.FileInfo().IsDir() {
			file_mkdir(path)
			continue
		}

		file_mkdir_for_file(path)

		d, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}

		fa, err := f.Open()
		if err != nil {
			d.Close()
			return err
		}

		_, err = io.Copy(d, fa)

		d.Close()
		fa.Close()

		if err != nil {
			return err
		}
	}

	return nil
}

func url_request(method string, url string, options map[string]string, headers map[string]string, body any) (*http.Response, error) {
	if method == "" {
		method = "GET"
	}

	var br io.Reader
	if body != nil {
		switch b := body.(type) {
		case string:
			br = strings.NewReader(b)

		case []byte:
			br = bytes.NewReader(b)

		default:
			br = strings.NewReader(json_encode(b))
			_, found := headers["Content-Type"]
			if !found {
				headers["Content-Type"] = "application/json"
			}
		}
	}

	r, err := http.NewRequest(strings.ToUpper(method), url, br)
	if err != nil {
		return nil, err
	}

	for k, v := range headers {
		r.Header.Set(k, v)
	}

	timeout := 30 * time.Second
	t, found := options["timeout"]
	if found {
		seconds, err := strconv.Atoi(t)
		if err == nil && seconds > 0 {
			timeout = time.Duration(seconds) * time.Second
		}
	}

	c := &http.Client{Timeout: timeout}
	return c.Do(r)
}

func valid(s string, match string) bool {
	//debug("Validating '%s' (%+v) as %s", s, s, match)
	if !match_non_controls.MatchString(s) {
		return false
	}

	switch match {
	case "action":
		match = "^[0-9a-zA-Z/-:]{1,100}$"
	case "constant":
		match = "^[0-9a-zA-Z/\\-\\._]{1,100}$"
	case "entity":
		match = "^[\\w]{49,51}$"
	case "filename":
		match = "^[0-9a-zA-Z -_~()][0-9a-zA-Z -_~().]{0,254}$"
	case "filepath":
		match = "^[0-9a-zA-Z/]{0,1000}[0-9a-zA-Z -_~()][0-9a-zA-Z -_~().]{0,254}$"
	case "fingerprint":
		match = "^[0-9a-zA-Z]{9}$"
	case "function":
		match = "^[0-9a-zA-Z_]{1,100}$"
	case "id":
		match = "^[0-9a-z]{32}"
	case "integer":
		match = "^(-)?\\d{1,12}$"
	case "json":
		match = "^[0-9a-zA-Z{}:\"]{1,1000}$"
	case "line":
		match = "^[^\r\n]{1,1000}$"
	case "name":
		match = "^[^<>\r\n\\;\"'`]{1,1000}$"
	case "natural":
		match = "^\\d{1,9}$"
	case "path":
		match = "^[0-9a-zA-Z-/]{0,1000}$"
	case "postive":
		match = "^\\d{1,9}$"
	case "privacy":
		match = "^(public|private)$"
	case "text":
		if len(s) > 10000 {
			return false
		}
		return true
	case "url":
		if len(s) > 10000 {
			return false
		}
		match = "^[\\w\\-\\/:%@.+?&;=~]*$"
	case "version":
		match = "^[0-9a-zA-Z.-_]{1,20}$"
	}

	m := must(regexp.MatchString(match, s))
	if !m {
		return false
	}

	return true
}
