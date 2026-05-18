// mochictl: replication subcommands (operator inspection / mgmt).
// Copyright Alistair Cunningham 2026
//
// Sibling to replica.go. The `mochictl replica` family is for the
// joining replica's perspective (join / leave / status of THIS server's
// pair attempt). The `mochictl replication` family below is for ongoing
// operator inspection / management of the pair set as it exists right
// now — list members, kick a member, summary view.
//
// `mochictl replication status`        → GET /_/admin/replication/status
// `mochictl replication pair list`     → GET /_/admin/replication/pair
// `mochictl replication pair remove`   → POST /_/admin/replication/pair/remove
// `mochictl replication resync`        → POST /_/admin/replication/resync

//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// cmd_replication_resync handles `mochictl replication resync <peer-id>`.
// POSTs the peer-id to the admin endpoint; the server seeds bootstrap
// rows for every scope and re-emits manifest-requests, forcing a
// re-walk that fetches anything missing on the local side. Idempotent
// — files whose local copy already matches by size + sha256 are
// skipped at manifest-diff time.
//
// Usage:
//   mochictl replication resync <peer-id>
func cmd_replication_resync(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mochictl replication resync <peer-id>")
	}
	peer := args[0]

	body, err := json.Marshal(map[string]string{"peer": peer})
	if err != nil {
		return err
	}

	resp, err := client().Post("/_/admin/replication/resync", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return http_error(resp.StatusCode, raw)
	}

	if flag_json {
		fmt.Println(string(raw))
		return nil
	}
	if !flag_verbose {
		return nil
	}
	fmt.Printf("Re-bootstrap initiated against peer %s. Track progress with 'mochictl replication status'.\n", peer)
	return nil
}

// cmd_replication_pair_remove handles `mochictl replication pair remove <peer-id>`.
// POSTs the peer-id to the admin endpoint; the server kicks that member
// from the local pair table and announces the new set to the rest.
func cmd_replication_pair_remove(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mochictl replication pair remove <peer-id>")
	}
	peer := args[0]

	body, err := json.Marshal(map[string]string{"peer": peer})
	if err != nil {
		return err
	}

	resp, err := client().Post("/_/admin/replication/pair/remove", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return http_error(resp.StatusCode, raw)
	}

	if flag_json {
		fmt.Println(string(raw))
		return nil
	}
	if !flag_verbose {
		return nil
	}
	var result struct {
		Removed string   `json:"removed"`
		Members []string `json:"members"`
	}
	if err := json.Unmarshal(raw, &result); err == nil {
		fmt.Printf("Removed %s. Remaining pair members: %s\n", result.Removed, strings.Join(result.Members, ", "))
	}
	return nil
}
