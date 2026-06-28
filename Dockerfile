# syntax=docker/dockerfile:1.7
# Single-stage Dockerfile for the Mochi server.
#
# The server and mochictl are NOT compiled here. `make docker` stages the
# pre-built static binaries — built once on the host, reusing the warm Go
# build cache — into build/docker/bin/ named by GOARCH, and this image just
# COPYs the right one per TARGETARCH. That removes the two full in-container
# Go compiles (amd64 + arm64) that previously ran against a cold, separate
# build cache on every release.
#
# The server binaries staged here are built with -X main.build_platform=docker
# so a containerised server polls the docker versions.json for updates (see
# server/update.go update_url_path); mochictl carries no platform tag, so its
# host build is reused as-is.
#
# Runtime: gcr.io/distroless/static-debian12 — fully static binary, no libc
# dependency at runtime.

FROM gcr.io/distroless/static-debian12:latest AS runtime
ARG TARGETARCH

COPY build/docker/bin/mochi-server-${TARGETARCH} /usr/sbin/mochi-server
COPY build/docker/bin/mochictl-${TARGETARCH}     /usr/bin/mochictl
COPY build/docker/mochi.conf                     /etc/mochi/mochi.conf

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
