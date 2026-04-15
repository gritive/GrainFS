# Docker-based NBD E2E test for GrainFS
# Usage: make test-nbd-docker

FROM golang:1.26-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    nbd-client \
    e2fsprogs \
    kmod \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /usr/local/bin/grainfs ./cmd/grainfs/

CMD ["bash", "docker/nbd-test.sh"]
