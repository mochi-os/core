# syntax=docker/dockerfile:1.7
# Multi-stage Dockerfile for the Mochi server.
#
# Builder: golang:bookworm. No cgo or cross-toolchains: SQLite is bundled
# via github.com/ncruces/go-sqlite3 (pure-Go WASM via wazero), so every
# target arch is a plain GOOS/GOARCH build.
# Runtime: gcr.io/distroless/static-debian12 — fully static binary, no libc
# dependency at runtime.

ARG VERSION=dev
ARG GO_VERSION=1.25
ARG DEBIAN_RELEASE=bookworm

# ---------- Builder ---------------------------------------------------------

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-${DEBIAN_RELEASE} AS builder
ARG VERSION
ARG TARGETARCH
ENV CGO_ENABLED=0 GOOS=linux

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    case "$TARGETARCH" in \
        amd64) export GOARCH=amd64 ;; \
        arm64) export GOARCH=arm64 ;; \
        *) echo "unsupported TARGETARCH=$TARGETARCH" >&2; exit 1 ;; \
    esac && \
    go build -trimpath -ldflags "-s -w -X main.build_version=${VERSION} -X main.build_platform=docker" \
        -o /out/mochi-server ./server && \
    go build -trimpath -ldflags "-s -w -X main.build_version=${VERSION}" \
        -o /out/mochictl ./mochictl

# ---------- Runtime ---------------------------------------------------------

FROM gcr.io/distroless/static-debian12:latest AS runtime

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
