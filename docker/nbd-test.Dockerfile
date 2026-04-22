# Docker-based NBD E2E test for GrainFS
# Usage: make test-nbd-docker

FROM golang:1.26-alpine

RUN apk add --no-cache \
    bash \
    nbd-client \
    e2fsprogs \
    kmod \
    curl \
    python3

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /usr/local/bin/grainfs ./cmd/grainfs/

CMD ["bash", "docker/nbd-test.sh"]
