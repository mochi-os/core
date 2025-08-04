// Mochi server: Read ini file
// Copyright Alistair Cunningham 2025

package main

import (
	"gopkg.in/ini.v1"
	"regexp"
)

var (
	ini_file            *ini.File
	match_commas_spaces = regexp.MustCompile("[\\S,]+")
)

func ini_bool(section string, key string, def bool) bool {
	return ini_file.Section(section).Key(key).MustBool(def)
}

func ini_int(section string, key string, def int) int {
	return ini_file.Section(section).Key(key).MustInt(def)
}

func ini_load(file string) error {
	var err error
	ini_file, err = ini.Load(file)
	return err
}

func ini_string(section string, key string, def string) string {
	return ini_file.Section(section).Key(key).MustString(def)
}

func ini_strings_commas(section string, key string) []string {
	s := match_commas_spaces.Split(ini_file.Section(section).Key(key).MustString(""), -1)
	if len(s) == 1 && s[0] == "" {
		return nil
	}
	return s
}
