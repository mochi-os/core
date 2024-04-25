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

func register_pubsub(name string, topic string, publish func(*pubsub.Topic)) {
	//log_debug("App register pubsub: name='%s', topic='%s'", name, topic)
	_, found := apps[name]
	if !found {
		log_warn("register_pubsub() called for non-installed app '%s'", name)
		return
	}
	pubsubs[name] = &AppPubSub{Name: name, Topic: topic, Publish: publish}
}
