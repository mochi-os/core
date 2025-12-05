# Makefile for Mochi
# Copyright Alistair Cunningham 2024-2025

version = 0.2.0
build = /tmp/mochi-server_$(version)_amd64
deb = $(build).deb

all: mochi-server

clean:
	rm -f mochi-server

mochi-server: $(shell find server)
	go build -v -ldflags "-X main.build_version=$(version)" -o mochi-server server/*.go

$(deb): clean mochi-server
	mkdir -p -m 0775 $(build) $(build)/usr/bin $(build)/var/cache/mochi $(build)/var/lib/mochi
	cp -av build/deb/* $(build)
	sed 's/_VERSION_/$(version)/' build/deb/DEBIAN/control > $(build)/DEBIAN/control
	cp -av install/* $(build)
	cp -av mochi-server $(build)/usr/bin
	strip $(build)/usr/bin/mochi-server
	upx -qq $(build)/usr/bin/mochi-server
	dpkg-deb --build --root-owner-group $(build)
	rm -rf $(build)
	ls -l $(deb)

apt: clean $(deb)
	rm ../apt/pool/main/mochi-server_*.deb
	cp $(deb) ../apt/pool/main
	./build/deb/scripts/apt-repository-update ../apt `cat local/gpg.txt | tr -d '\n'`
	rsync -av --delete ../apt/ root@packages.mochi-os.org:/srv/apt/

deb: $(deb)

format:
	go fmt server/*.go

run: mochi-server
	./mochi-server

-include local/Makefile
