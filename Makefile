# Makefile for Mochi
# Copyright Alistair Cunningham 2024-2025

version = 0.3.4

# Linux build paths
build_linux = /tmp/mochi-server_$(version)_linux_amd64
deb = $(build_linux).deb

# Windows build paths
build_windows = /tmp/mochi-server_$(version)_windows_amd64
msi = $(build_windows).msi

all: mochi-server

clean:
	rm -f mochi-server mochi-server.exe

mochi-server: $(shell find server -name '*.go')
	go build -v -ldflags "-X main.build_version=$(version)" -o mochi-server ./server

$(deb): clean mochi-server
	mkdir -p -m 0775 $(build_linux) $(build_linux)/usr/bin $(build_linux)/var/cache/mochi $(build_linux)/var/lib/mochi
	cp -av build/deb/* $(build_linux)
	sed 's/_VERSION_/$(version)/' build/deb/DEBIAN/control > $(build_linux)/DEBIAN/control
	cp -av install/* $(build_linux)
	cp -av mochi-server $(build_linux)/usr/bin
	strip $(build_linux)/usr/bin/mochi-server
	upx -qq $(build_linux)/usr/bin/mochi-server
	dpkg-deb --build --root-owner-group $(build_linux)
	rm -rf $(build_linux)
	ls -l $(deb)

deb: $(deb)

# Windows executable (cross-compile from Linux)
# Requires: apt install gcc-mingw-w64-x86-64 (for CGO/SQLite support)
mochi-server.exe: $(shell find server -name '*.go')
	CGO_ENABLED=1 CC=x86_64-w64-mingw32-gcc GOOS=windows GOARCH=amd64 go build -v -ldflags "-X main.build_version=$(version)" -o mochi-server.exe ./server

# Windows MSI installer (requires wixl from msitools package on Linux, or WiX on Windows)
$(msi): mochi-server.exe
	mkdir -p $(build_windows)
	cp mochi-server.exe $(build_windows)/
	cp build/msi/mochi.conf $(build_windows)/
	wixl -v -D Version=$(version) -D SourceDir=$(build_windows) -o $(msi) build/msi/mochi.wxs
	rm -rf $(build_windows)
	ls -l $(msi)

msi: $(msi)

windows: mochi-server.exe

release: $(deb)
	git tag -a $(version) -m "$(version)"
	rm ../apt/pool/main/mochi-server_*.deb
	cp $(deb) ../apt/pool/main
	./build/deb/scripts/apt-repository-update ../apt `cat local/gpg.txt | tr -d '\n'`
	rsync -av --delete ../apt/ root@packages.mochi-os.org:/srv/apt/

format:
	go fmt server/*.go

run: mochi-server
	./mochi-server

-include local/Makefile
