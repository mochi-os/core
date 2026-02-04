# Makefile for Mochi
# Copyright Alistair Cunningham 2024-2025

version = 0.3.4

# Linux build paths
build_linux_amd64 = /tmp/mochi-server_$(version)_linux_amd64
build_linux_arm64 = /tmp/mochi-server_$(version)_linux_arm64
build_linux_armhf = /tmp/mochi-server_$(version)_linux_armhf
deb_amd64 = $(build_linux_amd64).deb
deb_arm64 = $(build_linux_arm64).deb
deb_armhf = $(build_linux_armhf).deb

# macOS build paths
build_darwin_amd64 = /tmp/mochi-server_$(version)_darwin_amd64
build_darwin_arm64 = /tmp/mochi-server_$(version)_darwin_arm64

# Windows build paths
build_windows = /tmp/mochi-server_$(version)_windows_amd64
msi = $(build_windows).msi

all: mochi-server

clean:
	rm -f mochi-server mochi-server.exe mochi-server-linux-arm64 mochi-server-linux-arm mochi-server-darwin-amd64 mochi-server-darwin-arm64

mochi-server: $(shell find server -name '*.go')
	go build -v -ldflags "-X main.build_version=$(version)" -o mochi-server ./server

# AMD64 .deb package
$(deb_amd64): mochi-server
	mkdir -p -m 0775 $(build_linux_amd64) $(build_linux_amd64)/usr/bin $(build_linux_amd64)/var/cache/mochi $(build_linux_amd64)/var/lib/mochi
	cp -av build/deb/* $(build_linux_amd64)
	sed 's/_VERSION_/$(version)/' build/deb/DEBIAN/control > $(build_linux_amd64)/DEBIAN/control
	cp -av install/* $(build_linux_amd64)
	cp -av mochi-server $(build_linux_amd64)/usr/bin
	strip $(build_linux_amd64)/usr/bin/mochi-server
	upx -qq $(build_linux_amd64)/usr/bin/mochi-server
	dpkg-deb --build --root-owner-group $(build_linux_amd64)
	rm -rf $(build_linux_amd64)
	ls -l $(deb_amd64)

deb-amd64: $(deb_amd64)

# Linux ARM64 executable (cross-compile)
# Requires: apt install gcc-aarch64-linux-gnu
mochi-server-linux-arm64: $(shell find server -name '*.go')
	CGO_ENABLED=1 CC=aarch64-linux-gnu-gcc GOOS=linux GOARCH=arm64 go build -v -ldflags "-X main.build_version=$(version)" -o mochi-server-linux-arm64 ./server

# Linux ARM 32-bit executable (cross-compile)
# Requires: apt install gcc-arm-linux-gnueabihf
mochi-server-linux-arm: $(shell find server -name '*.go')
	CGO_ENABLED=1 CC=arm-linux-gnueabihf-gcc GOOS=linux GOARCH=arm GOARM=7 go build -v -ldflags "-X main.build_version=$(version)" -o mochi-server-linux-arm ./server

linux-arm64: mochi-server-linux-arm64

linux-arm: mochi-server-linux-arm

linux-arm-all: mochi-server-linux-arm64 mochi-server-linux-arm

# ARM64 .deb package
$(deb_arm64): mochi-server-linux-arm64
	mkdir -p -m 0775 $(build_linux_arm64) $(build_linux_arm64)/usr/bin $(build_linux_arm64)/var/cache/mochi $(build_linux_arm64)/var/lib/mochi
	cp -av build/deb/* $(build_linux_arm64)
	sed -e 's/_VERSION_/$(version)/' -e 's/Architecture: amd64/Architecture: arm64/' build/deb/DEBIAN/control > $(build_linux_arm64)/DEBIAN/control
	cp -av install/* $(build_linux_arm64)
	cp -av mochi-server-linux-arm64 $(build_linux_arm64)/usr/bin/mochi-server
	aarch64-linux-gnu-strip $(build_linux_arm64)/usr/bin/mochi-server
	dpkg-deb --build --root-owner-group $(build_linux_arm64)
	rm -rf $(build_linux_arm64)
	ls -l $(deb_arm64)

deb-arm64: $(deb_arm64)

# ARMHF .deb package
$(deb_armhf): mochi-server-linux-arm
	mkdir -p -m 0775 $(build_linux_armhf) $(build_linux_armhf)/usr/bin $(build_linux_armhf)/var/cache/mochi $(build_linux_armhf)/var/lib/mochi
	cp -av build/deb/* $(build_linux_armhf)
	sed -e 's/_VERSION_/$(version)/' -e 's/Architecture: amd64/Architecture: armhf/' build/deb/DEBIAN/control > $(build_linux_armhf)/DEBIAN/control
	cp -av install/* $(build_linux_armhf)
	cp -av mochi-server-linux-arm $(build_linux_armhf)/usr/bin/mochi-server
	arm-linux-gnueabihf-strip $(build_linux_armhf)/usr/bin/mochi-server
	dpkg-deb --build --root-owner-group $(build_linux_armhf)
	rm -rf $(build_linux_armhf)
	ls -l $(deb_armhf)

deb-armhf: $(deb_armhf)

deb: deb-amd64 deb-arm64 deb-armhf

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

# macOS executables (cross-compile from Linux)
# Note: CGO cross-compilation for macOS requires osxcross toolchain
mochi-server-darwin-amd64: $(shell find server -name '*.go')
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build -v -ldflags "-X main.build_version=$(version)" -o mochi-server-darwin-amd64 ./server

mochi-server-darwin-arm64: $(shell find server -name '*.go')
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -v -ldflags "-X main.build_version=$(version)" -o mochi-server-darwin-arm64 ./server

macos: mochi-server-darwin-amd64 mochi-server-darwin-arm64

release: deb
	git tag -a $(version) -m "$(version)"
	rm -f ../apt/pool/main/mochi-server_*.deb
	cp $(deb_amd64) $(deb_arm64) $(deb_armhf) ../apt/pool/main
	./build/deb/scripts/apt-repository-update ../apt `cat local/gpg.txt | tr -d '\n'`
	rsync -av --delete ../apt/ root@packages.mochi-os.org:/srv/apt/

format:
	go fmt server/*.go

run: mochi-server
	./mochi-server

-include local/Makefile
