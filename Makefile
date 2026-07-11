# Makefile for Mochi
# Copyright © 2026 Mochi OÜ
# SPDX-License-Identifier: AGPL-3.0-only
# This file is part of Mochi, licensed under the GNU AGPL v3 with the
# Mochi Application Interface Exception - see license.txt and license-exception.md.

version = 0.4.207

# Build outputs land in ~/mochi/bin/ (one level up from core/), so source
# directories never collide with binary names.
bin = ../bin

# Parallel jobs for the release build phase. The independent packaging
# branches (deb / rpm / msi / pkg / docker) build concurrently; override with
# `make release JOBS=N`.
JOBS ?= $(shell nproc)

# Per-phase timing lines (`>>> ...`) are also appended here and printed as a
# consolidated summary at the end of `release`, so the breakdown survives the
# thousands of lines of build/buildx output that otherwise bury and truncate it.
timing = /tmp/mochi-release-timing.txt

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
# Per-arch rpmbuild trees so the three rpm targets can build concurrently
# under `make -j` without clobbering a shared _topdir.
rpmbuild_x86_64  = /tmp/mochi-rpmbuild-x86_64
rpmbuild_aarch64 = /tmp/mochi-rpmbuild-aarch64
rpmbuild_armv7hl = /tmp/mochi-rpmbuild-armv7hl

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
ldflags_docker  = -s -w -X main.build_version=$(version) -X main.build_platform=docker
ldflags_mochictl = -s -w -X main.build_version=$(version)

# Source prerequisites for the Go build targets. go.mod / go.sum are listed so
# a dependency or `toolchain` bump rebuilds the binaries on an incremental
# `make` — without them only *.go changes triggered a rebuild, so a
# go.mod-only change (e.g. a Go toolchain pin) was silently ignored until the
# next `make clean`. Simply-expanded (:=) so the find runs once at parse time.
go_sources_server   := $(shell find server -name '*.go') $(shell find common -name '*.go') go.mod go.sum
go_sources_mochictl := $(shell find mochictl -name '*.go') $(shell find common -name '*.go') go.mod go.sum

all: $(bin)/mochi-server $(bin)/mochictl

clean:
	rm -f $(bin)/mochi-server $(bin)/mochi-server.exe $(bin)/mochi-server-linux-arm64 $(bin)/mochi-server-linux-arm $(bin)/mochi-server-darwin-amd64 $(bin)/mochi-server-darwin-arm64 $(bin)/mochi-server-docker-amd64 $(bin)/mochi-server-docker-arm64
	rm -f $(bin)/mochictl $(bin)/mochictl.exe $(bin)/mochictl-linux-arm64 $(bin)/mochictl-linux-arm $(bin)/mochictl-darwin-amd64 $(bin)/mochictl-darwin-arm64 $(bin)/mochictl.1 $(bin)/mochi-server.8 $(bin)/mochi.conf.5 $(bin)/mochi.7
	rm -rf build/docker/bin

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

$(bin)/mochi-server: $(go_sources_server) | $(bin)
	CGO_ENABLED=0 go build -v -ldflags "$(ldflags_linux)" -o $(bin)/mochi-server ./server

# Phony alias for the historical name.
mochi-server: $(bin)/mochi-server

$(bin)/mochictl: $(go_sources_mochictl) | $(bin)
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -v -ldflags "$(ldflags_mochictl)" -o $(bin)/mochictl ./mochictl

mochictl: $(bin)/mochictl

# Windows mochictl: the server's admin listener is supported on windows via a
# named pipe (LocalSystem/Administrators security descriptor).
$(bin)/mochictl.exe: $(go_sources_mochictl) | $(bin)
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -v -ldflags "$(ldflags_mochictl)" -o $(bin)/mochictl.exe ./mochictl

mochictl.exe: $(bin)/mochictl.exe

# macOS mochictl: the server's admin UDS listener is supported on darwin
# (LOCAL_PEERCRED peer auth), so ship mochictl in the .pkg too.
$(bin)/mochictl-darwin-amd64: $(go_sources_mochictl) | $(bin)
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build -v -ldflags "$(ldflags_mochictl)" -o $(bin)/mochictl-darwin-amd64 ./mochictl

mochictl-darwin-amd64: $(bin)/mochictl-darwin-amd64

$(bin)/mochictl-darwin-arm64: $(go_sources_mochictl) | $(bin)
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -v -ldflags "$(ldflags_mochictl)" -o $(bin)/mochictl-darwin-arm64 ./mochictl

mochictl-darwin-arm64: $(bin)/mochictl-darwin-arm64

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

$(bin)/mochi-server-linux-arm64: $(go_sources_server) | $(bin)
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -v -ldflags "$(ldflags_linux)" -o $(bin)/mochi-server-linux-arm64 ./server

mochi-server-linux-arm64: $(bin)/mochi-server-linux-arm64

$(bin)/mochi-server-linux-arm: $(go_sources_server) | $(bin)
	CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=7 go build -v -ldflags "$(ldflags_linux)" -o $(bin)/mochi-server-linux-arm ./server

mochi-server-linux-arm: $(bin)/mochi-server-linux-arm

$(bin)/mochictl-linux-arm64: $(go_sources_mochictl) | $(bin)
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -v -ldflags "$(ldflags_mochictl)" -o $(bin)/mochictl-linux-arm64 ./mochictl

mochictl-linux-arm64: $(bin)/mochictl-linux-arm64

$(bin)/mochictl-linux-arm: $(go_sources_mochictl) | $(bin)
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
	upx -1 -qq $(build_linux_amd64)/usr/sbin/mochi-server
	mkdir -p $(build_linux_amd64)/usr/share/man/man1 $(build_linux_amd64)/usr/share/man/man5 $(build_linux_amd64)/usr/share/man/man7 $(build_linux_amd64)/usr/share/man/man8
	cp $(bin)/mochictl.1     $(build_linux_amd64)/usr/share/man/man1/
	cp $(bin)/mochi.conf.5   $(build_linux_amd64)/usr/share/man/man5/
	cp $(bin)/mochi.7        $(build_linux_amd64)/usr/share/man/man7/
	cp $(bin)/mochi-server.8 $(build_linux_amd64)/usr/share/man/man8/
	dpkg-deb -Zxz -z9 --build --root-owner-group $(build_linux_amd64)
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
	dpkg-deb -Zxz -z9 --build --root-owner-group $(build_linux_arm64)
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
	dpkg-deb -Zxz -z9 --build --root-owner-group $(build_linux_armhf)
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
	rm -rf $(rpmbuild_x86_64)
	mkdir -p $(rpmbuild_x86_64)/SOURCES $(rpmbuild_x86_64)/SPECS $(rpmbuild_x86_64)/BUILD $(rpmbuild_x86_64)/RPMS $(rpmbuild_x86_64)/SRPMS
	cp $(bin)/mochi-server $(rpmbuild_x86_64)/SOURCES/
	cp $(bin)/mochictl $(rpmbuild_x86_64)/SOURCES/
	cp $(bin)/mochictl.1 $(rpmbuild_x86_64)/SOURCES/
	cp $(bin)/mochi-server.8 $(rpmbuild_x86_64)/SOURCES/
	cp $(bin)/mochi.conf.5 $(rpmbuild_x86_64)/SOURCES/
	cp $(bin)/mochi.7 $(rpmbuild_x86_64)/SOURCES/
	cp install/usr/share/bash-completion/completions/mochictl $(rpmbuild_x86_64)/SOURCES/mochictl.bash
	cp install/usr/share/zsh/site-functions/_mochictl $(rpmbuild_x86_64)/SOURCES/_mochictl
	cp install/etc/mochi/mochi.conf $(rpmbuild_x86_64)/SOURCES/
	cp install/etc/systemd/system/mochi-server.service $(rpmbuild_x86_64)/SOURCES/
	rpmbuild -bb --define "_topdir $(rpmbuild_x86_64)" --define "_version $(version)" --target x86_64 build/rpm/mochi-server.spec
	cp $(rpmbuild_x86_64)/RPMS/x86_64/mochi-server-$(version)-1.x86_64.rpm $(rpm_x86_64)
	rm -rf $(rpmbuild_x86_64)
	ls -l $(rpm_x86_64)

rpm-x86_64: $(rpm_x86_64)

# aarch64 .rpm package
$(rpm_aarch64): $(bin)/mochi-server-linux-arm64 $(bin)/mochictl-linux-arm64 $(bin)/mochictl.1 $(bin)/mochi-server.8 $(bin)/mochi.conf.5 $(bin)/mochi.7
	rm -rf $(rpmbuild_aarch64)
	mkdir -p $(rpmbuild_aarch64)/SOURCES $(rpmbuild_aarch64)/SPECS $(rpmbuild_aarch64)/BUILD $(rpmbuild_aarch64)/RPMS $(rpmbuild_aarch64)/SRPMS
	cp $(bin)/mochi-server-linux-arm64 $(rpmbuild_aarch64)/SOURCES/mochi-server
	cp $(bin)/mochictl-linux-arm64 $(rpmbuild_aarch64)/SOURCES/mochictl
	cp $(bin)/mochictl.1 $(rpmbuild_aarch64)/SOURCES/
	cp $(bin)/mochi-server.8 $(rpmbuild_aarch64)/SOURCES/
	cp $(bin)/mochi.conf.5 $(rpmbuild_aarch64)/SOURCES/
	cp $(bin)/mochi.7 $(rpmbuild_aarch64)/SOURCES/
	cp install/usr/share/bash-completion/completions/mochictl $(rpmbuild_aarch64)/SOURCES/mochictl.bash
	cp install/usr/share/zsh/site-functions/_mochictl $(rpmbuild_aarch64)/SOURCES/_mochictl
	cp install/etc/mochi/mochi.conf $(rpmbuild_aarch64)/SOURCES/
	cp install/etc/systemd/system/mochi-server.service $(rpmbuild_aarch64)/SOURCES/
	rpmbuild -bb --define "_topdir $(rpmbuild_aarch64)" --define "_version $(version)" --target aarch64 build/rpm/mochi-server.spec
	cp $(rpmbuild_aarch64)/RPMS/aarch64/mochi-server-$(version)-1.aarch64.rpm $(rpm_aarch64)
	rm -rf $(rpmbuild_aarch64)
	ls -l $(rpm_aarch64)

rpm-aarch64: $(rpm_aarch64)

# armv7hl .rpm package
$(rpm_armv7hl): $(bin)/mochi-server-linux-arm $(bin)/mochictl-linux-arm $(bin)/mochictl.1 $(bin)/mochi-server.8 $(bin)/mochi.conf.5 $(bin)/mochi.7
	rm -rf $(rpmbuild_armv7hl)
	mkdir -p $(rpmbuild_armv7hl)/SOURCES $(rpmbuild_armv7hl)/SPECS $(rpmbuild_armv7hl)/BUILD $(rpmbuild_armv7hl)/RPMS $(rpmbuild_armv7hl)/SRPMS
	cp $(bin)/mochi-server-linux-arm $(rpmbuild_armv7hl)/SOURCES/mochi-server
	cp $(bin)/mochictl-linux-arm $(rpmbuild_armv7hl)/SOURCES/mochictl
	cp $(bin)/mochictl.1 $(rpmbuild_armv7hl)/SOURCES/
	cp $(bin)/mochi-server.8 $(rpmbuild_armv7hl)/SOURCES/
	cp $(bin)/mochi.conf.5 $(rpmbuild_armv7hl)/SOURCES/
	cp $(bin)/mochi.7 $(rpmbuild_armv7hl)/SOURCES/
	cp install/usr/share/bash-completion/completions/mochictl $(rpmbuild_armv7hl)/SOURCES/mochictl.bash
	cp install/usr/share/zsh/site-functions/_mochictl $(rpmbuild_armv7hl)/SOURCES/_mochictl
	cp install/etc/mochi/mochi.conf $(rpmbuild_armv7hl)/SOURCES/
	cp install/etc/systemd/system/mochi-server.service $(rpmbuild_armv7hl)/SOURCES/
	rpmbuild -bb --define "_topdir $(rpmbuild_armv7hl)" --define "_version $(version)" --target armv7hl build/rpm/mochi-server.spec
	cp $(rpmbuild_armv7hl)/RPMS/armv7hl/mochi-server-$(version)-1.armv7hl.rpm $(rpm_armv7hl)
	rm -rf $(rpmbuild_armv7hl)
	ls -l $(rpm_armv7hl)

rpm-armv7hl: $(rpm_armv7hl)

rpm: rpm-x86_64 rpm-aarch64 rpm-armv7hl

# --------------------------------------------------------------------------
# Windows
# --------------------------------------------------------------------------

# Windows executable (cross-compile from Linux)
$(bin)/mochi-server.exe: $(go_sources_server) | $(bin)
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -v -ldflags "$(ldflags_windows)" -o $(bin)/mochi-server.exe ./server

mochi-server.exe: $(bin)/mochi-server.exe

# Windows MSI installer (requires wixl from msitools package on Linux, or WiX on Windows)
$(msi): $(bin)/mochi-server.exe $(bin)/mochictl.exe
	mkdir -p $(build_windows)
	cp $(bin)/mochi-server.exe $(build_windows)/
	cp $(bin)/mochictl.exe $(build_windows)/
	cp build/msi/mochi.conf $(build_windows)/
	wixl -v --ext ui -a x64 -D Version=$(version) -D SourceDir=$(build_windows) -o $(msi) build/msi/mochi.wxs
	rm -rf $(build_windows)
	ls -l $(msi)

msi: $(msi)

windows: $(bin)/mochi-server.exe

# --------------------------------------------------------------------------
# macOS
# --------------------------------------------------------------------------

$(bin)/mochi-server-darwin-amd64: $(go_sources_server) | $(bin)
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -v -ldflags "$(ldflags_macos)" -o $(bin)/mochi-server-darwin-amd64 ./server

mochi-server-darwin-amd64: $(bin)/mochi-server-darwin-amd64

$(bin)/mochi-server-darwin-arm64: $(go_sources_server) | $(bin)
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -v -ldflags "$(ldflags_macos)" -o $(bin)/mochi-server-darwin-arm64 ./server

mochi-server-darwin-arm64: $(bin)/mochi-server-darwin-arm64

# macOS .pkg installers
# Requires: bomutils (/opt/bomutils), xar
$(pkg_amd64): $(bin)/mochi-server-darwin-amd64 $(bin)/mochictl-darwin-amd64
	PATH="/opt/bomutils/bin:$$PATH" ./build/scripts/build-pkg $(bin)/mochi-server-darwin-amd64 $(version) amd64 $(pkg_amd64) $(bin)/mochictl-darwin-amd64

$(pkg_arm64): $(bin)/mochi-server-darwin-arm64 $(bin)/mochictl-darwin-arm64
	PATH="/opt/bomutils/bin:$$PATH" ./build/scripts/build-pkg $(bin)/mochi-server-darwin-arm64 $(version) arm64 $(pkg_arm64) $(bin)/mochictl-darwin-arm64

pkg-amd64: $(pkg_amd64)

pkg-arm64: $(pkg_arm64)

pkg: pkg-amd64 pkg-arm64

macos: $(bin)/mochi-server-darwin-amd64 $(bin)/mochi-server-darwin-arm64

# --------------------------------------------------------------------------
# Docker
# --------------------------------------------------------------------------

docker_image = ghcr.io/mochi-os/mochi-server
docker_minor = $(word 1,$(subst ., ,$(version))).$(word 2,$(subst ., ,$(version)))

# Docker-tagged server binaries. Identical to the linux builds except for
# -X main.build_platform=docker, so a containerised server polls the docker
# versions.json (server/update.go update_url_path). Only ldflags differ, so
# these relink in well under a second against the warm Go cache. mochictl
# carries no platform tag, so the linux mochictl binaries are reused as-is.
$(bin)/mochi-server-docker-amd64: $(go_sources_server) | $(bin)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -v -ldflags "$(ldflags_docker)" -o $(bin)/mochi-server-docker-amd64 ./server

$(bin)/mochi-server-docker-arm64: $(go_sources_server) | $(bin)
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -v -ldflags "$(ldflags_docker)" -o $(bin)/mochi-server-docker-arm64 ./server

# Stage the pre-built static binaries into the Docker build context, named by
# GOARCH so the Dockerfile COPYs the right one per TARGETARCH. Reusing the
# host's warm Go cache here replaces two full in-container compiles per build.
docker-stage: $(bin)/mochi-server-docker-amd64 $(bin)/mochi-server-docker-arm64 $(bin)/mochictl $(bin)/mochictl-linux-arm64
	rm -rf build/docker/bin
	mkdir -p build/docker/bin
	cp $(bin)/mochi-server-docker-amd64 build/docker/bin/mochi-server-amd64
	cp $(bin)/mochi-server-docker-arm64 build/docker/bin/mochi-server-arm64
	cp $(bin)/mochictl                  build/docker/bin/mochictl-amd64
	cp $(bin)/mochictl-linux-arm64      build/docker/bin/mochictl-arm64

# Build for the host arch only — fast iteration during development. Tags as
# :dev so it can't be confused with a real release.
docker-local: docker-stage
	docker build -t $(docker_image):dev .

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
docker: docker-stage
	@t=$$(date +%s); docker buildx build \
	    --platform linux/amd64,linux/arm64 \
	    --sbom=false --provenance=false \
	    --tag $(docker_image):$(version) \
	    --tag $(docker_image):$(docker_minor) \
	    --tag $(docker_image):latest \
	    --tag $(docker_image):production \
	    --push \
	    . && echo ">>> docker build+push: $$(($$(date +%s)-t))s" | tee -a $(timing)

# Reclaim disk left by repeated image builds: dangling images from :dev
# retags, plus build cache from both the default daemon builder and the
# buildx container builder. The default builder's cache is already capped at
# 5GB by /etc/docker/daemon.json (builder.gc); this target is the on-demand
# sweep, mainly for the buildx builder, which daemon.json does not govern.
# Leaves tagged images (:dev, release tags) untouched.
docker-clean:
	docker image prune -f
	docker builder prune -af
	-docker buildx prune -af

# --------------------------------------------------------------------------
# Release
# --------------------------------------------------------------------------

# `release` runs a parallel build phase then a serial publish phase. `clean`
# runs first in its own sub-make so the rebuild can't race a stale binary; the
# build and publish phases are separate sub-makes so `-j$(JOBS)` applies only
# to the parallel-safe build (the publish steps must stay ordered).
# Each phase prints its wall-clock as `>>> phase ...: Ns` so a release self-
# reports where the time goes (grep the output for `>>>`). The build phase
# figure includes the docker build+push; the publish figure includes the apt /
# rpm reindex and the rsync to both hosts, each timed individually below.
release:
	@: > $(timing)
	@t=$$(date +%s); $(MAKE) clean && echo ">>> phase clean: $$(($$(date +%s)-t))s" | tee -a $(timing)
	@t=$$(date +%s); $(MAKE) -j$(JOBS) release-build && echo ">>> phase build (incl docker push): $$(($$(date +%s)-t))s" | tee -a $(timing)
	@t=$$(date +%s); $(MAKE) release-publish && echo ">>> phase publish (reindex + rsync): $$(($$(date +%s)-t))s" | tee -a $(timing)
	@echo; echo "=== release $(version) timing summary ==="; cat $(timing)

# Parallel-safe build of every release artefact. The deb/rpm/msi/pkg/docker
# branches are independent: each rpm target has its own _topdir, each deb its
# own staging dir, pkg uses mktemp, and docker stages pre-built binaries — so
# -j fans them across cores with no shared-state races. Shared binary targets
# (mochi-server*, mochictl*, man pages) are built once and reused by make.
release-build: deb rpm msi pkg docker

release-publish:
	git tag -fa $(version) -m "$(version)"
	rm -f ../packages/apt/pool/main/mochi-server_*.deb
	cp $(deb_amd64) $(deb_arm64) $(deb_armhf) ../packages/apt/pool/main
	@t=$$(date +%s); ./build/scripts/apt-repository-update ../packages/apt `cat local/gpg.txt | tr -d '\n'` && echo ">>> apt reindex (scan + gpg sign): $$(($$(date +%s)-t))s" | tee -a $(timing)
	echo '{"tracks": {"production": "$(version)"}}' > ../packages/apt/versions.json
	rm -f ../packages/rpm/Packages/mochi-server-*.rpm
	cp $(rpm_x86_64) $(rpm_aarch64) $(rpm_armv7hl) ../packages/rpm/Packages
	@t=$$(date +%s); ./build/scripts/rpm-repository-update ../packages/rpm && echo ">>> rpm reindex (createrepo): $$(($$(date +%s)-t))s" | tee -a $(timing)
	echo '{"tracks": {"production": "$(version)"}}' > ../packages/rpm/versions.json
	cp $(msi) ../packages/windows/mochi-server.msi
	echo '{"tracks": {"production": "$(version)"}}' > ../packages/windows/versions.json
	cp $(pkg_amd64) ../packages/macos/mochi-server-amd64.pkg
	cp $(pkg_arm64) ../packages/macos/mochi-server-arm64.pkg
	echo '{"tracks": {"production": "$(version)"}}' > ../packages/macos/versions.json
	mkdir -p ../packages/docker
	echo '{"tracks": {"production": "$(version)"}}' > ../packages/docker/versions.json
	# Publish to yuzu by name (not the packages.mochi-os.org alias) so the
	# target is deterministic regardless of where that record points. Wasabi is
	# frozen as a pre-decouple backup and receives no packages.
	@t0=$$(date +%s); \
	rsync -av --delete ../packages/ root@yuzu.mochi-os.org:/srv/packages/ || exit 1; \
	echo ">>> rsync local->yuzu: $$(($$(date +%s)-t0))s" | tee -a $(timing)

# Install the published version on yuzu (verified). Separate from `release`
# (which only publishes packages) so deploying stays an explicit step. Pass apt
# flags via `make deploy DEPLOY_FLAGS=--reinstall` to redeploy an identical
# version.
deploy:
	./build/scripts/deploy $(DEPLOY_FLAGS)

format:
	go fmt server/*.go

run: $(bin)/mochi-server
	$(bin)/mochi-server

# Run the server test suite. test is the fast pass (CGO disabled, no
# race detector); test-race adds the race detector at the cost of
# requiring cgo and roughly 8x slower test execution. test-race must
# pass before any commit that touches replication or other shared
# mutable state.
test:
	CGO_ENABLED=0 go test -count=1 -timeout 180s ./server

test-race:
	CGO_ENABLED=1 go test -race -count=1 -timeout 300s ./server

-include local/Makefile
