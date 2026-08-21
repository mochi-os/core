# syntax=docker/dockerfile:1.7 Single-stage image for the Mochi server. The
# binaries are NOT compiled here: `make docker` stages host-built static
# binaries into build/docker/bin/ named by GOARCH, and this image COPYs the one
# matching TARGETARCH. The server is built with -X main.build_platform=docker so
# it polls the docker versions.json (server/update.go); mochictl carries no
# platform tag.

# Pinned by digest, not tag, so the same commit always builds the same image.
# This is the manifest LIST digest - a per-architecture digest builds one arch
# and fails the other. `make base-digest` reports whether the tag has moved;
# bump it deliberately as part of a release.
FROM gcr.io/distroless/static-debian12:latest@sha256:a9fcaedd4c9b59e12dd65d954f0b5044f19b0647a8a3712e77205df9e7b102cd AS runtime
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
