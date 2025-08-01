# Makefile for Mochi
# Copyright Alistair Cunningham 2024

version = $(shell cat version | tr -d '\n')
build = /tmp/mochi-server_$(version)_amd64

all: mochi-server

clean:
	rm -f mochi-server

mochi-server: clean
	go build -o mochi-server server/*.go

deb: mochi-server
	rm -rf $(build) $(build).deb
	mkdir -p -m 0775 $(build) $(build)/usr/bin $(build)/var/cache/mochi $(build)/var/lib/mochi
	cp -av build/deb/* $(build)
	sed 's/_VERSION_/$(version)/' build/deb/DEBIAN/control > $(build)/DEBIAN/control
	cp -av install/* $(build)
	cp -av mochi-server $(build)/usr/bin
	strip $(build)/usr/bin/mochi-server
	upx -qq $(build)/usr/bin/mochi-server
	dpkg-deb --build --root-owner-group $(build)
	rm -rf $(build)
	ls -l $(build).deb

format:
	go fmt server/*.go

run:
	./mochi-server

-include local/Makefile
