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
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test ./tests/e2e/ -v -count=1 -timeout 60s

run: build
	./bin/$(BINARY) serve --data ./tmp --port 9000

clean:
	rm -rf bin/ tmp/

lint:
	go vet ./...
	test -z "$$(gofmt -l .)"
