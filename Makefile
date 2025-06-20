# Makefile for Comms
# Copyright Alistair Cunningham 2024

all: comms-server

clean:
	rm -f comms-server

comms-server: clean
	go build -o comms-server server/*.go

format:
	go fmt server/*.go

run:
	./comms-server

run2:
	./comms-server -data /var/lib/comms2 -cache /var/cache/comms2 -port 1444 -web 8081

static: clean
	go build -ldflags="-extldflags=-static" -tags sqlite_omit_load_extension -o comms-server server/*.go
