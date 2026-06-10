// mochictl: broadcast subcommands (operator visibility).
// Copyright Alistair Cunningham 2026
//
// `mochictl broadcast lag` -> GET /_/admin/broadcast/lag
//   Surfaces subscribers that have fallen behind the broadcast owner
//   without firing user-visible errors. From the broadcast investigation
//   session report; see claude/sessions/2026-05-25-broadcast-resync-
//   seq-643-investigation.md and task #83.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
)

// cmd_broadcast_lag handles `mochictl broadcast lag [threshold]`.
// Optional positional threshold (default 0) maps to the ?threshold=
// query param - only rows with lag > threshold are reported. With
// -j / -t the response is dumped raw for scripted consumption.
func cmd_broadcast_lag(args []string) error {
	path := "/_/admin/broadcast/lag"
	if len(args) > 0 && args[0] != "" {
		if _, err := strconv.Atoi(args[0]); err == nil {
			path = path + "?threshold=" + args[0]
		}
	}
	if flag_json || flag_tabs {
		return get_dump(path, "rows")
	}

	resp, err := client().Get(path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return http_error(resp.StatusCode, body)
	}

	var payload struct {
		Rows []struct {
			User         string `json:"user"`
			App          string `json:"app"`
			Peer         string `json:"peer"`
			Key          string `json:"key"`
			ReceivedLast int64  `json:"received_last"`
			OwnerLogMax  *int64 `json:"owner_log_max"`
			Lag          *int64 `json:"lag"`
			Pending      int    `json:"pending"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		os.Stdout.Write(body)
		return nil
	}

	if len(payload.Rows) == 0 {
		fmt.Println("No broadcast lag on this server above the threshold.")
		fmt.Println("Note: lag is only computed when this host owns the broadcast (matching _log present locally). Pure-subscriber streams report received_last only; cross-host lag needs a separate query against the owner.")
		return nil
	}

	// Column widths sized to the longest values in this batch so the
	// rendered table stays aligned across heterogeneous app names.
	user_w, app_w, key_w := 4, 3, 3
	for _, r := range payload.Rows {
		if len(r.User) > user_w {
			user_w = len(r.User)
		}
		if len(r.App) > app_w {
			app_w = len(r.App)
		}
		if len(r.Key) > key_w {
			key_w = len(r.Key)
		}
	}
	// Cap key width so a 50-char entity-id doesn't dominate the line.
	if key_w > 24 {
		key_w = 24
	}
	fmt.Printf("%-*s  %-*s  %-*s  %12s  %12s  %12s  %s\n",
		user_w, "USER", app_w, "APP", key_w, "KEY",
		"RECEIVED", "OWNER_MAX", "LAG", "PENDING")
	for _, r := range payload.Rows {
		key := r.Key
		if len(key) > key_w {
			key = key[:key_w-1] + "…"
		}
		owner_str := "-"
		lag_str := "-"
		if r.OwnerLogMax != nil {
			owner_str = fmt.Sprintf("%d", *r.OwnerLogMax)
			lag_str = fmt.Sprintf("%d", *r.Lag)
		}
		fmt.Printf("%-*s  %-*s  %-*s  %12d  %12s  %12s  %d\n",
			user_w, r.User, app_w, r.App, key_w, key,
			r.ReceivedLast, owner_str, lag_str, r.Pending)
	}
	return nil
}
