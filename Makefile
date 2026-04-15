BINARY := grainfs
MODULE := github.com/gritive/GrainFS
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -ldflags "-s -w -X main.version=$(VERSION)"

.PHONY: build test test-race test-e2e clean run lint

build:
	go build $(LDFLAGS) -o bin/$(BINARY) ./cmd/grainfs/

test:
	go test ./... -count=1 -cover

test-race:
	go test ./... -count=1 -race -cover

test-e2e: build
	GRAINFS_BINARY=./bin/$(BINARY) uv run pytest tests/e2e/ -v -m e2e

run: build
	./bin/$(BINARY) serve --data ./tmp --port 9000

clean:
	rm -rf bin/ tmp/

lint:
	go vet ./...
	test -z "$$(gofmt -l .)"
