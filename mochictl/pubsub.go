// mochictl: pubsub subcommands (operator visibility into GossipSub).
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// `mochictl pubsub status` -> GET /_/admin/pubsub/status
//   Per-topic mesh peer count + published/received counters for the
//   /mochi/2 GossipSub topic. See claude/plans/pubsub.md.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)

// cmd_pubsub_status handles `mochictl pubsub status`.
//
// With -j / -t the response is dumped raw for scripted consumption.
// Default output is a per-topic table.
func cmd_pubsub_status(args []string) error {
	path := "/_/admin/pubsub/status"
	if flag_json || flag_tabs {
		return get_dump(path, "topics")
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
		Topics []struct {
			Topic     string `json:"topic"`
			Peers     int    `json:"peers"`
			Published int64  `json:"published"`
			Received  int64  `json:"received"`
			Last      int64  `json:"last"`
		} `json:"topics"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		os.Stdout.Write(body)
		return nil
	}

	fmt.Printf("%-10s  %6s  %10s  %10s  %s\n", "TOPIC", "PEERS", "PUBLISHED", "RECEIVED", "LAST RECEIVED")
	for _, t := range payload.Topics {
		last := "never"
		if t.Last > 0 {
			last = time.Unix(t.Last, 0).Format("2006-01-02 15:04:05")
		}
		fmt.Printf("%-10s  %6d  %10d  %10d  %s\n", t.Topic, t.Peers, t.Published, t.Received, last)
	}
	return nil
}
