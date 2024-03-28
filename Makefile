# Makefile for Comms
# Copyright Alistair Cunningham 2024

all: comms-server

clean:
	rm -f comms-server

comms-server: clean
	go build -o comms-server server/*.go

format:
	go fmt server/*.go

static: clean
	go build -ldflags="-extldflags=-static" -tags sqlite_omit_load_extension -o comms-server server/*.go
