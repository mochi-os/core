// Comms server: Publish/Subscribes
// Copyright Alistair Cunningham 2024

package main

import (
	libp2p_pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type PubSub struct {
	Name    string
	Topic   string
	Publish func(*libp2p_pubsub.Topic)
}

var pubsubs = map[string]*PubSub{}

func (a *App) pubsub(topic string, publish func(*libp2p_pubsub.Topic)) {
	pubsubs[a.name] = &PubSub{Name: a.name, Topic: topic, Publish: publish}
}
