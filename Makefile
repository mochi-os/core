# Makefile for Comms
# Copyright Alistair Cunningham 2024

all: comms-server

clean:
	rm -f comms-server

comms-server: clean
	go build -o comms-server server/*.go

format:
	go fmt server/*.go
