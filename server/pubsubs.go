// Comms server: Publish/Subscribes
// Copyright Alistair Cunningham 2024

package main

import (
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type AppPubSub struct {
	Name    string
	Topic   string
	Publish func(*pubsub.Topic)
}

var pubsubs = map[string]*AppPubSub{}

func (a *App) register_pubsub(topic string, publish func(*pubsub.Topic)) {
	pubsubs[a.Name] = &AppPubSub{Name: a.Name, Topic: topic, Publish: publish}
}
