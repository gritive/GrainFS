# Cross-platform E2E test container for GrainFS.
#
# Runs the host-side Go E2E test suite (`go test ./tests/e2e/`) inside a Linux
# container so platform-specific code paths (NBD on Linux, future O_DIRECT)
# can be exercised even when the developer's host is macOS. The companion
# `nbd-test.Dockerfile` covers the NBD-kernel-module subset; this Dockerfile
# covers everything else.
#
# Usage: make test-e2e-docker

FROM golang:1.26-alpine

RUN apk add --no-cache \
    bash \
    curl \
    docker-cli \
    git \
    make \
    nfs-utils

WORKDIR /src

# Cache go module downloads when only source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build the binary the test harness will exec.
RUN go build -o /usr/local/bin/grainfs ./cmd/grainfs/

# `go test` finds the binary via GRAINFS_BINARY (set in the Makefile target).
# Default to running the full suite; callers can override with `docker run ... -- args`.
CMD ["go", "test", "./tests/e2e/", "-count=1", "-timeout=600s", "-v"]
