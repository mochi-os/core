# Makefile for Mochi
# Copyright Alistair Cunningham 2024-2026

version = 0.4.55

# Build outputs land in ~/mochi/bin/ (one level up from core/), so source
# directories never collide with binary names.
bin = ../bin

# Linux build paths
build_linux_amd64 = /tmp/mochi-server_$(version)_linux_amd64
build_linux_arm64 = /tmp/mochi-server_$(version)_linux_arm64
build_linux_armhf = /tmp/mochi-server_$(version)_linux_armhf
deb_amd64 = $(build_linux_amd64).deb
deb_arm64 = $(build_linux_arm64).deb
deb_armhf = $(build_linux_armhf).deb
rpm_x86_64 = /tmp/mochi-server-$(version)-1.x86_64.rpm
rpm_aarch64 = /tmp/mochi-server-$(version)-1.aarch64.rpm
rpm_armv7hl = /tmp/mochi-server-$(version)-1.armv7hl.rpm
rpmbuild_dir = /tmp/mochi-rpmbuild

# macOS build paths
build_darwin_amd64 = /tmp/mochi-server_$(version)_darwin_amd64
build_darwin_arm64 = /tmp/mochi-server_$(version)_darwin_arm64
pkg_amd64 = /tmp/mochi-server_$(version)_darwin_amd64.pkg
pkg_arm64 = /tmp/mochi-server_$(version)_darwin_arm64.pkg

# Windows build paths
build_windows = /tmp/mochi-server_$(version)_windows_amd64
msi = $(build_windows).msi

# Build flags. build_platform tags release builds so the daily update_manager
# can poll the right packages.mochi-os.org/<path>/versions.json. Empty for
# `make` from source — those binaries don't poll.
#
# -s -w drops the Go symbol table and DWARF debug info: smaller binary
# without needing the cross-arch `*-strip` binutils. Pure-Go means
# CGO_ENABLED=0 everywhere.
ldflags_linux   = -s -w -X main.build_version=$(version) -X main.build_platform=linux
ldflags_windows = -s -w -X main.build_version=$(version) -X main.build_platform=windows
ldflags_macos   = -s -w -X main.build_version=$(version) -X main.build_platform=macos
ldflags_mochictl = -s -w -X main.build_version=$(version)

all: $(bin)/mochi-server $(bin)/mochictl

clean:
	rm -f $(bin)/mochi-server $(bin)/mochi-server.exe $(bin)/mochi-server-linux-arm64 $(bin)/mochi-server-linux-arm $(bin)/mochi-server-darwin-amd64 $(bin)/mochi-server-darwin-arm64
	rm -f $(bin)/mochictl $(bin)/mochictl-linux-arm64 $(bin)/mochictl-linux-arm $(bin)/mochictl.1 $(bin)/mochi-server.8 $(bin)/mochi.conf.5 $(bin)/mochi.7

# Order-only prerequisite: create $(bin) but don't trigger rebuilds when its
# mtime changes.
$(bin):
	mkdir -p $(bin)

# --------------------------------------------------------------------------
# Native Linux amd64 binaries
#
# SQLite is bundled via github.com/ncruces/go-sqlite3 (pure-Go, WASM via
# wazero). cgo is no longer required at all, so every target below is a
# plain GOOS/GOARCH build with CGO_ENABLED=0 — no cross-toolchains.
# --------------------------------------------------------------------------

$(bin)/mochi-server: $(shell find server -name '*.go') $(shell find common -name '*.go') | $(bin)
	CGO_ENABLED=0 go build -v -ldflags "$(ldflags_linux)" -o $(bin)/mochi-server ./server

# Phony alias for the historical name.
mochi-server: $(bin)/mochi-server

$(bin)/mochictl: $(shell find mochictl -name '*.go') $(shell find common -name '*.go') | $(bin)
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -v -ldflags "$(ldflags_mochictl)" -o $(bin)/mochictl ./mochictl

mochictl: $(bin)/mochictl

# Man page: docs/mochictl.1.md -> $(bin)/mochictl.1 via pandoc.
# Requires: apt install pandoc
$(bin)/mochictl.1: docs/mochictl.1.md | $(bin)
	pandoc -s -t man docs/mochictl.1.md -o $(bin)/mochictl.1
	@mkdir -p $(HOME)/.local/share/man/man1 && \
	    cp $(bin)/mochictl.1 $(HOME)/.local/share/man/man1/mochictl.1 && \
	    echo "  installed to $(HOME)/.local/share/man/man1/mochictl.1 (run \`man mochictl\` to view)"

mochictl.1: $(bin)/mochictl.1

# mochi-server(8) man page — same pandoc-to-roff flow, but section 8.
$(bin)/mochi-server.8: docs/mochi-server.8.md | $(bin)
	pandoc -s -t man docs/mochi-server.8.md -o $(bin)/mochi-server.8
	@mkdir -p $(HOME)/.local/share/man/man8 && \
	    cp $(bin)/mochi-server.8 $(HOME)/.local/share/man/man8/mochi-server.8 && \
	    echo "  installed to $(HOME)/.local/share/man/man8/mochi-server.8 (run \`man mochi-server\` to view)"

mochi-server.8: $(bin)/mochi-server.8

# mochi.conf(5) — file-format reference for /etc/mochi/mochi.conf.
$(bin)/mochi.conf.5: docs/mochi.conf.5.md | $(bin)
	pandoc -s -t man docs/mochi.conf.5.md -o $(bin)/mochi.conf.5
	@mkdir -p $(HOME)/.local/share/man/man5 && \
	    cp $(bin)/mochi.conf.5 $(HOME)/.local/share/man/man5/mochi.conf.5 && \
	    echo "  installed to $(HOME)/.local/share/man/man5/mochi.conf.5 (run \`man mochi.conf\` to view)"

mochi.conf.5: $(bin)/mochi.conf.5

# mochi(7) — high-level overview of the project: peers, entities, apps.
$(bin)/mochi.7: docs/mochi.7.md | $(bin)
	pandoc -s -t man docs/mochi.7.md -o $(bin)/mochi.7
	@mkdir -p $(HOME)/.local/share/man/man7 && \
	    cp $(bin)/mochi.7 $(HOME)/.local/share/man/man7/mochi.7 && \
	    echo "  installed to $(HOME)/.local/share/man/man7/mochi.7 (run \`man 7 mochi\` to view)"

mochi.7: $(bin)/mochi.7

# --------------------------------------------------------------------------
# Linux ARM cross-compile binaries
# --------------------------------------------------------------------------

$(bin)/mochi-server-linux-arm64: $(shell find server -name '*.go') $(shell find common -name '*.go') | $(bin)
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -v -ldflags "$(ldflags_linux)" -o $(bin)/mochi-server-linux-arm64 ./server

mochi-server-linux-arm64: $(bin)/mochi-server-linux-arm64

$(bin)/mochi-server-linux-arm: $(shell find server -name '*.go') $(shell find common -name '*.go') | $(bin)
	CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=7 go build -v -ldflags "$(ldflags_linux)" -o $(bin)/mochi-server-linux-arm ./server

mochi-server-linux-arm: $(bin)/mochi-server-linux-arm

$(bin)/mochictl-linux-arm64: $(shell find mochictl -name '*.go') $(shell find common -name '*.go') | $(bin)
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -v -ldflags "$(ldflags_mochictl)" -o $(bin)/mochictl-linux-arm64 ./mochictl

mochictl-linux-arm64: $(bin)/mochictl-linux-arm64

$(bin)/mochictl-linux-arm: $(shell find mochictl -name '*.go') $(shell find common -name '*.go') | $(bin)
	GOOS=linux GOARCH=arm GOARM=7 CGO_ENABLED=0 go build -v -ldflags "$(ldflags_mochictl)" -o $(bin)/mochictl-linux-arm ./mochictl

mochictl-linux-arm: $(bin)/mochictl-linux-arm

linux-arm64: $(bin)/mochi-server-linux-arm64

linux-arm: $(bin)/mochi-server-linux-arm

linux-arm-all: $(bin)/mochi-server-linux-arm64 $(bin)/mochi-server-linux-arm

# --------------------------------------------------------------------------
# .deb packages
# --------------------------------------------------------------------------

# AMD64 .deb package
$(deb_amd64): $(bin)/mochi-server $(bin)/mochictl $(bin)/mochictl.1 $(bin)/mochi-server.8 $(bin)/mochi.conf.5 $(bin)/mochi.7
	mkdir -p -m 0775 $(build_linux_amd64) $(build_linux_amd64)/usr/bin $(build_linux_amd64)/usr/sbin $(build_linux_amd64)/var/cache/mochi $(build_linux_amd64)/var/lib/mochi
	cp -av build/deb/* $(build_linux_amd64)
	sed 's/_VERSION_/$(version)/' build/deb/DEBIAN/control > $(build_linux_amd64)/DEBIAN/control
	cp -av install/* $(build_linux_amd64)
	cp -av $(bin)/mochi-server $(build_linux_amd64)/usr/sbin
	cp -av $(bin)/mochictl $(build_linux_amd64)/usr/bin
	upx -qq $(build_linux_amd64)/usr/sbin/mochi-server
	mkdir -p $(build_linux_amd64)/usr/share/man/man1 $(build_linux_amd64)/usr/share/man/man5 $(build_linux_amd64)/usr/share/man/man7 $(build_linux_amd64)/usr/share/man/man8
	cp $(bin)/mochictl.1     $(build_linux_amd64)/usr/share/man/man1/
	cp $(bin)/mochi.conf.5   $(build_linux_amd64)/usr/share/man/man5/
	cp $(bin)/mochi.7        $(build_linux_amd64)/usr/share/man/man7/
	cp $(bin)/mochi-server.8 $(build_linux_amd64)/usr/share/man/man8/
	dpkg-deb --build --root-owner-group $(build_linux_amd64)
	rm -rf $(build_linux_amd64)
	ls -l $(deb_amd64)

deb-amd64: $(deb_amd64)

# ARM64 .deb package
$(deb_arm64): $(bin)/mochi-server-linux-arm64 $(bin)/mochictl-linux-arm64 $(bin)/mochictl.1 $(bin)/mochi-server.8 $(bin)/mochi.conf.5 $(bin)/mochi.7
	mkdir -p -m 0775 $(build_linux_arm64) $(build_linux_arm64)/usr/bin $(build_linux_arm64)/usr/sbin $(build_linux_arm64)/var/cache/mochi $(build_linux_arm64)/var/lib/mochi
	cp -av build/deb/* $(build_linux_arm64)
	sed -e 's/_VERSION_/$(version)/' -e 's/Architecture: amd64/Architecture: arm64/' build/deb/DEBIAN/control > $(build_linux_arm64)/DEBIAN/control
	cp -av install/* $(build_linux_arm64)
	cp -av $(bin)/mochi-server-linux-arm64 $(build_linux_arm64)/usr/sbin/mochi-server
	cp -av $(bin)/mochictl-linux-arm64 $(build_linux_arm64)/usr/bin/mochictl
	mkdir -p $(build_linux_arm64)/usr/share/man/man1 $(build_linux_arm64)/usr/share/man/man5 $(build_linux_arm64)/usr/share/man/man7 $(build_linux_arm64)/usr/share/man/man8
	cp $(bin)/mochictl.1     $(build_linux_arm64)/usr/share/man/man1/
	cp $(bin)/mochi.conf.5   $(build_linux_arm64)/usr/share/man/man5/
	cp $(bin)/mochi.7        $(build_linux_arm64)/usr/share/man/man7/
	cp $(bin)/mochi-server.8 $(build_linux_arm64)/usr/share/man/man8/
	dpkg-deb --build --root-owner-group $(build_linux_arm64)
	rm -rf $(build_linux_arm64)
	ls -l $(deb_arm64)

deb-arm64: $(deb_arm64)

# ARMHF .deb package
$(deb_armhf): $(bin)/mochi-server-linux-arm $(bin)/mochictl-linux-arm $(bin)/mochictl.1 $(bin)/mochi-server.8 $(bin)/mochi.conf.5 $(bin)/mochi.7
	mkdir -p -m 0775 $(build_linux_armhf) $(build_linux_armhf)/usr/bin $(build_linux_armhf)/usr/sbin $(build_linux_armhf)/var/cache/mochi $(build_linux_armhf)/var/lib/mochi
	cp -av build/deb/* $(build_linux_armhf)
	sed -e 's/_VERSION_/$(version)/' -e 's/Architecture: amd64/Architecture: armhf/' build/deb/DEBIAN/control > $(build_linux_armhf)/DEBIAN/control
	cp -av install/* $(build_linux_armhf)
	cp -av $(bin)/mochi-server-linux-arm $(build_linux_armhf)/usr/sbin/mochi-server
	cp -av $(bin)/mochictl-linux-arm $(build_linux_armhf)/usr/bin/mochictl
	mkdir -p $(build_linux_armhf)/usr/share/man/man1 $(build_linux_armhf)/usr/share/man/man5 $(build_linux_armhf)/usr/share/man/man7 $(build_linux_armhf)/usr/share/man/man8
	cp $(bin)/mochictl.1     $(build_linux_armhf)/usr/share/man/man1/
	cp $(bin)/mochi.conf.5   $(build_linux_armhf)/usr/share/man/man5/
	cp $(bin)/mochi.7        $(build_linux_armhf)/usr/share/man/man7/
	cp $(bin)/mochi-server.8 $(build_linux_armhf)/usr/share/man/man8/
	dpkg-deb --build --root-owner-group $(build_linux_armhf)
	rm -rf $(build_linux_armhf)
	ls -l $(deb_armhf)

deb-armhf: $(deb_armhf)

deb: deb-amd64 deb-arm64 deb-armhf

# --------------------------------------------------------------------------
# .rpm packages
# --------------------------------------------------------------------------

# x86_64 .rpm package
# Requires: apt install rpm
$(rpm_x86_64): $(bin)/mochi-server $(bin)/mochictl $(bin)/mochictl.1 $(bin)/mochi-server.8 $(bin)/mochi.conf.5 $(bin)/mochi.7
	rm -rf $(rpmbuild_dir)
	mkdir -p $(rpmbuild_dir)/SOURCES $(rpmbuild_dir)/SPECS $(rpmbuild_dir)/BUILD $(rpmbuild_dir)/RPMS $(rpmbuild_dir)/SRPMS
	cp $(bin)/mochi-server $(rpmbuild_dir)/SOURCES/
	cp $(bin)/mochictl $(rpmbuild_dir)/SOURCES/
	cp $(bin)/mochictl.1 $(rpmbuild_dir)/SOURCES/
	cp $(bin)/mochi-server.8 $(rpmbuild_dir)/SOURCES/
	cp $(bin)/mochi.conf.5 $(rpmbuild_dir)/SOURCES/
	cp $(bin)/mochi.7 $(rpmbuild_dir)/SOURCES/
	cp install/usr/share/bash-completion/completions/mochictl $(rpmbuild_dir)/SOURCES/mochictl.bash
	cp install/usr/share/zsh/site-functions/_mochictl $(rpmbuild_dir)/SOURCES/_mochictl
	cp install/etc/mochi/mochi.conf $(rpmbuild_dir)/SOURCES/
	cp install/etc/systemd/system/mochi-server.service $(rpmbuild_dir)/SOURCES/
	rpmbuild -bb --define "_topdir $(rpmbuild_dir)" --define "_version $(version)" --target x86_64 build/rpm/mochi-server.spec
	cp $(rpmbuild_dir)/RPMS/x86_64/mochi-server-$(version)-1.x86_64.rpm $(rpm_x86_64)
	rm -rf $(rpmbuild_dir)
	ls -l $(rpm_x86_64)

rpm-x86_64: $(rpm_x86_64)

# aarch64 .rpm package
$(rpm_aarch64): $(bin)/mochi-server-linux-arm64 $(bin)/mochictl-linux-arm64 $(bin)/mochictl.1 $(bin)/mochi-server.8 $(bin)/mochi.conf.5 $(bin)/mochi.7
	rm -rf $(rpmbuild_dir)
	mkdir -p $(rpmbuild_dir)/SOURCES $(rpmbuild_dir)/SPECS $(rpmbuild_dir)/BUILD $(rpmbuild_dir)/RPMS $(rpmbuild_dir)/SRPMS
	cp $(bin)/mochi-server-linux-arm64 $(rpmbuild_dir)/SOURCES/mochi-server
	cp $(bin)/mochictl-linux-arm64 $(rpmbuild_dir)/SOURCES/mochictl
	cp $(bin)/mochictl.1 $(rpmbuild_dir)/SOURCES/
	cp $(bin)/mochi-server.8 $(rpmbuild_dir)/SOURCES/
	cp $(bin)/mochi.conf.5 $(rpmbuild_dir)/SOURCES/
	cp $(bin)/mochi.7 $(rpmbuild_dir)/SOURCES/
	cp install/usr/share/bash-completion/completions/mochictl $(rpmbuild_dir)/SOURCES/mochictl.bash
	cp install/usr/share/zsh/site-functions/_mochictl $(rpmbuild_dir)/SOURCES/_mochictl
	cp install/etc/mochi/mochi.conf $(rpmbuild_dir)/SOURCES/
	cp install/etc/systemd/system/mochi-server.service $(rpmbuild_dir)/SOURCES/
	rpmbuild -bb --define "_topdir $(rpmbuild_dir)" --define "_version $(version)" --target aarch64 build/rpm/mochi-server.spec
	cp $(rpmbuild_dir)/RPMS/aarch64/mochi-server-$(version)-1.aarch64.rpm $(rpm_aarch64)
	rm -rf $(rpmbuild_dir)
	ls -l $(rpm_aarch64)

rpm-aarch64: $(rpm_aarch64)

# armv7hl .rpm package
$(rpm_armv7hl): $(bin)/mochi-server-linux-arm $(bin)/mochictl-linux-arm $(bin)/mochictl.1 $(bin)/mochi-server.8 $(bin)/mochi.conf.5 $(bin)/mochi.7
	rm -rf $(rpmbuild_dir)
	mkdir -p $(rpmbuild_dir)/SOURCES $(rpmbuild_dir)/SPECS $(rpmbuild_dir)/BUILD $(rpmbuild_dir)/RPMS $(rpmbuild_dir)/SRPMS
	cp $(bin)/mochi-server-linux-arm $(rpmbuild_dir)/SOURCES/mochi-server
	cp $(bin)/mochictl-linux-arm $(rpmbuild_dir)/SOURCES/mochictl
	cp $(bin)/mochictl.1 $(rpmbuild_dir)/SOURCES/
	cp $(bin)/mochi-server.8 $(rpmbuild_dir)/SOURCES/
	cp $(bin)/mochi.conf.5 $(rpmbuild_dir)/SOURCES/
	cp $(bin)/mochi.7 $(rpmbuild_dir)/SOURCES/
	cp install/usr/share/bash-completion/completions/mochictl $(rpmbuild_dir)/SOURCES/mochictl.bash
	cp install/usr/share/zsh/site-functions/_mochictl $(rpmbuild_dir)/SOURCES/_mochictl
	cp install/etc/mochi/mochi.conf $(rpmbuild_dir)/SOURCES/
	cp install/etc/systemd/system/mochi-server.service $(rpmbuild_dir)/SOURCES/
	rpmbuild -bb --define "_topdir $(rpmbuild_dir)" --define "_version $(version)" --target armv7hl build/rpm/mochi-server.spec
	cp $(rpmbuild_dir)/RPMS/armv7hl/mochi-server-$(version)-1.armv7hl.rpm $(rpm_armv7hl)
	rm -rf $(rpmbuild_dir)
	ls -l $(rpm_armv7hl)

rpm-armv7hl: $(rpm_armv7hl)

rpm: rpm-x86_64 rpm-aarch64 rpm-armv7hl

# --------------------------------------------------------------------------
# Windows
# --------------------------------------------------------------------------

# Windows executable (cross-compile from Linux)
$(bin)/mochi-server.exe: $(shell find server -name '*.go') $(shell find common -name '*.go') | $(bin)
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -v -ldflags "$(ldflags_windows)" -o $(bin)/mochi-server.exe ./server

mochi-server.exe: $(bin)/mochi-server.exe

# Windows MSI installer (requires wixl from msitools package on Linux, or WiX on Windows)
$(msi): $(bin)/mochi-server.exe
	mkdir -p $(build_windows)
	cp $(bin)/mochi-server.exe $(build_windows)/
	cp build/msi/mochi.conf $(build_windows)/
	wixl -v -D Version=$(version) -D SourceDir=$(build_windows) -D WIXL -o $(msi) build/msi/mochi.wxs
	rm -rf $(build_windows)
	ls -l $(msi)

msi: $(msi)

windows: $(bin)/mochi-server.exe

# --------------------------------------------------------------------------
# macOS
# --------------------------------------------------------------------------

$(bin)/mochi-server-darwin-amd64: $(shell find server -name '*.go') $(shell find common -name '*.go') | $(bin)
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -v -ldflags "$(ldflags_macos)" -o $(bin)/mochi-server-darwin-amd64 ./server

mochi-server-darwin-amd64: $(bin)/mochi-server-darwin-amd64

$(bin)/mochi-server-darwin-arm64: $(shell find server -name '*.go') $(shell find common -name '*.go') | $(bin)
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -v -ldflags "$(ldflags_macos)" -o $(bin)/mochi-server-darwin-arm64 ./server

mochi-server-darwin-arm64: $(bin)/mochi-server-darwin-arm64

# macOS .pkg installers
# Requires: bomutils (/opt/bomutils), xar
$(pkg_amd64): $(bin)/mochi-server-darwin-amd64
	PATH="/opt/bomutils/bin:$$PATH" ./build/scripts/build-pkg $(bin)/mochi-server-darwin-amd64 $(version) amd64 $(pkg_amd64)

$(pkg_arm64): $(bin)/mochi-server-darwin-arm64
	PATH="/opt/bomutils/bin:$$PATH" ./build/scripts/build-pkg $(bin)/mochi-server-darwin-arm64 $(version) arm64 $(pkg_arm64)

pkg-amd64: $(pkg_amd64)

pkg-arm64: $(pkg_arm64)

pkg: pkg-amd64 pkg-arm64

macos: $(bin)/mochi-server-darwin-amd64 $(bin)/mochi-server-darwin-arm64

# --------------------------------------------------------------------------
# Docker
# --------------------------------------------------------------------------

docker_image = ghcr.io/mochi-os/mochi-server
docker_minor = $(word 1,$(subst ., ,$(version))).$(word 2,$(subst ., ,$(version)))

# Build for the host arch only — fast iteration during development. Tags as
# :dev so it can't be confused with a real release.
docker-local:
	docker build --build-arg VERSION=$(version) -t $(docker_image):dev .

# Run Trivy against the locally-built image. Fails (exit 1) on any HIGH or
# CRITICAL finding — useful as a manual pre-release check, intentionally NOT
# wired into make release because Trivy occasionally flags transitive deps
# that don't affect us in practice. Mounts the host's docker.sock so the
# trivy container can inspect images without needing its own daemon.
# First run downloads the ~50 MB vulnerability DB from mirror.gcr.io and
# may take several minutes; subsequent runs use the cache at ~/.cache/trivy.
docker-scan: docker-local
	docker run --rm \
	    -v /var/run/docker.sock:/var/run/docker.sock \
	    -v $(HOME)/.cache/trivy:/root/.cache/trivy \
	    -v $(CURDIR)/.trivyignore:/.trivyignore:ro \
	    aquasec/trivy:latest image \
	    --severity HIGH,CRITICAL --exit-code 1 --no-progress \
	    --timeout 15m \
	    --ignorefile /.trivyignore \
	    $(docker_image):dev

# Multi-arch build + push to GHCR. Tags applied:
#     X.Y.Z      exact version (matches deb/rpm/pkg)
#     X.Y        newest patch in this minor line
#     latest     docker convention for the newest production release
#     production explicit alias, matches versions.json track names
# --sbom and --provenance attach a Software Bill of Materials and SLSA
# build provenance so consumers can audit the image contents.
# Requires a multi-arch buildx builder (docker buildx create --use
# --platform linux/amd64,linux/arm64) and docker login ghcr.io with a
# PAT scoped to write:packages.
docker:
	docker buildx build \
	    --platform linux/amd64,linux/arm64 \
	    --sbom=true --provenance=true \
	    --build-arg VERSION=$(version) \
	    --tag $(docker_image):$(version) \
	    --tag $(docker_image):$(docker_minor) \
	    --tag $(docker_image):latest \
	    --tag $(docker_image):production \
	    --push \
	    .

# --------------------------------------------------------------------------
# Release
# --------------------------------------------------------------------------

release: clean deb rpm msi pkg docker
	git tag -fa $(version) -m "$(version)"
	rm -f ../packages/apt/pool/main/mochi-server_*.deb
	cp $(deb_amd64) $(deb_arm64) $(deb_armhf) ../packages/apt/pool/main
	./build/scripts/apt-repository-update ../packages/apt `cat local/gpg.txt | tr -d '\n'`
	echo '{"tracks": {"production": "$(version)"}}' > ../packages/apt/versions.json
	rm -f ../packages/rpm/Packages/mochi-server-*.rpm
	cp $(rpm_x86_64) $(rpm_aarch64) $(rpm_armv7hl) ../packages/rpm/Packages
	./build/scripts/rpm-repository-update ../packages/rpm
	echo '{"tracks": {"production": "$(version)"}}' > ../packages/rpm/versions.json
	cp $(msi) ../packages/windows/mochi-server.msi
	echo '{"tracks": {"production": "$(version)"}}' > ../packages/windows/versions.json
	cp $(pkg_amd64) ../packages/macos/mochi-server-amd64.pkg
	cp $(pkg_arm64) ../packages/macos/mochi-server-arm64.pkg
	echo '{"tracks": {"production": "$(version)"}}' > ../packages/macos/versions.json
	mkdir -p ../packages/docker
	echo '{"tracks": {"production": "$(version)"}}' > ../packages/docker/versions.json
	rsync -av --delete ../packages/ root@packages.mochi-os.org:/srv/packages/

format:
	go fmt server/*.go

run: $(bin)/mochi-server
	$(bin)/mochi-server

-include local/Makefile
