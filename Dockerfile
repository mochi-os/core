# syntax=docker/dockerfile:1.7
# Multi-stage Dockerfile for the Mochi server.
#
# Builder: golang:bookworm with gcc.
# Runtime: gcr.io/distroless/cc-debian12 — glibc only; mattn/go-sqlite3
# embeds the SQLite C source so no libsqlite3 dependency at runtime.
# mochi-server is a cgo binary (linked against glibc); mochictl is pure-Go.

ARG VERSION=dev
ARG GO_VERSION=1.25
ARG DEBIAN_RELEASE=bookworm

# ---------- Builder ---------------------------------------------------------

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-${DEBIAN_RELEASE} AS builder
ARG VERSION
ARG TARGETARCH
ENV CGO_ENABLED=1 GOOS=linux

# gcc for native, gcc-aarch64-linux-gnu for arm64 cross-compile from amd64.
# Cache IDs are per-target-arch so parallel multi-arch builds don't deadlock
# on the same /var/cache/apt + /var/lib/apt/lists locks.
RUN --mount=type=cache,id=apt-cache-${TARGETARCH},target=/var/cache/apt \
    --mount=type=cache,id=apt-lists-${TARGETARCH},target=/var/lib/apt/lists \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc libc6-dev \
        gcc-aarch64-linux-gnu libc6-dev-arm64-cross && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    case "$TARGETARCH" in \
        amd64) export GOARCH=amd64 CC=gcc ;; \
        arm64) export GOARCH=arm64 CC=aarch64-linux-gnu-gcc ;; \
        *) echo "unsupported TARGETARCH=$TARGETARCH" >&2; exit 1 ;; \
    esac && \
    go build -trimpath -ldflags "-s -w -X main.build_version=${VERSION} -X main.build_platform=docker" \
        -o /out/mochi-server ./server && \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.build_version=${VERSION}" \
        -o /out/mochictl ./mochictl

# ---------- Runtime ---------------------------------------------------------

FROM gcr.io/distroless/cc-debian12:latest AS runtime

COPY --from=builder /out/mochi-server /usr/sbin/mochi-server
COPY --from=builder /out/mochictl    /usr/bin/mochictl
COPY build/docker/mochi.conf         /etc/mochi/mochi.conf

VOLUME /var/lib/mochi
EXPOSE 8080 8443 1443/tcp 1443/udp

# Probe the admin UDS at /var/lib/mochi/run/admin.sock — works on TLS-only
# deployments and doesn't need to know the HTTP port. Public /_/health
# remains for external monitors.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD ["/usr/bin/mochictl", "health"]

# Container starts as root so directories.ensure can mkdir + chown the data
# dir; the server then drops privileges to uid/gid 1000 before serving any
# request. See core/server/directories_linux.go.
ENTRYPOINT ["/usr/sbin/mochi-server"]
