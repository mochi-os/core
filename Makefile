# Makefile for Comms
# Copyright Alistair Cunningham 2024

build = /tmp/comms-server_0.1-2_amd64

all: comms-server

clean:
	rm -f comms-server

comms-server: clean
	go build -o comms-server server/*.go

deb: comms-server
	mkdir $(build)
	cp -av build/deb/* $(build)
	cp -av install/* $(build)
	cp -av comms-server $(build)/usr/bin
	strip $(build)/usr/bin/comms-server
	upx -qq $(build)/usr/bin/comms-server
	dpkg-deb --build --root-owner-group $(build)
	rm -r $(build)
	ls -l $(build).deb

format:
	go fmt server/*.go

run:
	./comms-server

run2:
	./comms-server -data /var/lib/comms2 -cache /var/cache/comms2 -port 1444 -web 8081

static: clean
	go build -ldflags="-extldflags=-static" -tags sqlite_omit_load_extension -o comms-server server/*.go
