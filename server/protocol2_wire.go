// Mochi server: Protocol 2 — wire glue between the queue / message
// layer and the /mochi/2/messages sender.
//
// queue_send_direct + message_attempt_send_real both build a wire
// message from queue/Message fields; this file factors out the common
// "build a Frame and ship it via peer_send" path so the two callers
// stay consistent and the dual-write path stays in one place.
//
// Copyright Alistair Cunningham 2026

package main

import (
	"strings"

	cbor "github.com/fxamacker/cbor/v2"
)

// frame_for_queue builds a v2 message Frame from a queue.db row.
// Used by queue_send_direct's v2 branch. The queue row's content is
// already CBOR-encoded as a map; decode it back so the frame can ship
// it as a structured Content (and so the receiver doesn't have to
// repeat the work).
func frame_for_queue(q *QueueEntry) (*Frame, error) {
	content := map[string]any{}
	if len(q.Content) > 0 {
		if err := cbor.Unmarshal(q.Content, &content); err != nil {
			return nil, err
		}
	}
	var services []string
	if q.FromServices != "" {
		services = strings.Split(q.FromServices, ",")
	}
	return &Frame{
		Type:     frame_type_message,
		ID:       q.ID,
		From:     q.FromEntity,
		To:       q.ToEntity,
		Service:  q.Service,
		Event:    q.Event,
		FromApp:  q.FromApp,
		Services: services,
		Priority: frame_priority_for(q.Priority),
		Content:  content,
		Data:     q.Data,
	}, nil
}

// frame_for_message builds a v2 message Frame from a *Message. Used
// by message_attempt_send_real's v2 branch.
func frame_for_message(m *Message, content []byte) (*Frame, error) {
	contentMap := map[string]any{}
	if len(content) > 0 {
		if err := cbor.Unmarshal(content, &contentMap); err != nil {
			return nil, err
		}
	}
	return &Frame{
		Type:     frame_type_message,
		ID:       m.ID,
		From:     m.From,
		To:       m.To,
		Service:  m.Service,
		Event:    m.Event,
		FromApp:  m.FromApp,
		Services: m.Services,
		Priority: frame_priority_for(queue_priority(m.Service, m.Event)),
		Content:  contentMap,
		Data:     m.data,
	}, nil
}
