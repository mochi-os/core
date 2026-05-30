// Mochi server: /_/admin/pubsub/status handler.
//
// Operator visibility into the GossipSub /mochi/2 topic: the live mesh
// peer count plus the published / received / last-received counters. Used
// by `mochictl pubsub status`.
//
// Copyright Alistair Cunningham 2026

package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
	p2p_pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// PubsubTopic is one topic's row in the status response.
type PubsubTopic struct {
	Topic     string `json:"topic"`
	Peers     int    `json:"peers"`     // live GossipSub mesh peer count (ListPeers)
	Published int64  `json:"published"` // messages this host has flooded
	Received  int64  `json:"received"`  // messages this host has accepted for decode
	Last      int64  `json:"last"`      // unix time of the last received message, 0 if none
}

// PubsubStatus is the response body.
type PubsubStatus struct {
	Topics []PubsubTopic `json:"topics"`
}

// pubsub_topic_peers returns the live mesh peer count for a joined topic,
// 0 if the topic isn't joined yet.
func pubsub_topic_peers(t *p2p_pubsub.Topic) int {
	if t == nil {
		return 0
	}
	return len(t.ListPeers())
}

// admin_pubsub_status is GET /_/admin/pubsub/status.
func admin_pubsub_status(c *gin.Context) {
	out := PubsubStatus{
		Topics: []PubsubTopic{
			{
				Topic:     "/mochi/2",
				Peers:     pubsub_topic_peers(net_pubsub),
				Published: pubsub_published.Load(),
				Received:  pubsub_received.Load(),
				Last:      pubsub_last.Load(),
			},
		},
	}
	c.JSON(http.StatusOK, out)
}
