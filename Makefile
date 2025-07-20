# Makefile for Comms
# Copyright Alistair Cunningham 2024

version = $(shell cat version | tr -d '\n')
build = /tmp/comms-server_$(version)_amd64

all: comms-server

clean:
	rm -f comms-server

comms-server: clean
	go build -o comms-server server/*.go

deb: comms-server
	rm -rf $(build) $(build).deb
	mkdir $(build)
	cp -av build/deb/* $(build)
	sed 's/_VERSION_/$(version)/' build/deb/DEBIAN/control > $(build)/DEBIAN/control
	cp -av install/* $(build)
	mkdir -p -m 0755 $(build)/usr/bin
	mkdir -p -m 0755 $(build)/var/cache/comms
	mkdir -p -m 0755 $(build)/var/lib/comms
	cp -av comms-server $(build)/usr/bin
	strip $(build)/usr/bin/comms-server
	upx -qq $(build)/usr/bin/comms-server
	dpkg-deb --build --root-owner-group $(build)
	rm -rf $(build)
	ls -l $(build).deb

format:
	go fmt server/*.go

run:
	./comms-server

run2:
	./comms-server -f /etc/comms/comms2.conf

static: clean
	go build -ldflags="-extldflags=-static" -tags sqlite_omit_load_extension -o comms-server server/*.go
