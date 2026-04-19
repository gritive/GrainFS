BINARY := grainfs
MODULE := github.com/gritive/GrainFS
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -ldflags "-s -w -X main.version=$(VERSION)"

PROTO_SRC := $(shell find internal -name '*.proto')
PROTO_GEN := $(PROTO_SRC:.proto=.pb.go)
GO_SRC := $(shell find cmd internal -name '*.go' -not -name '*_test.go' -not -name '*.pb.go')
FBS_GEN := internal/erasure/erasurepb/ECObjectMetaFB.go \
           internal/raft/raftpb/RPCMessageFB.go

.PHONY: test test-race test-e2e test-jepsen test-smoke test-network-fault test-backup clean run lint bench test-nbd-docker update-deps fbs

bin/$(BINARY): $(GO_SRC) $(PROTO_GEN) $(FBS_GEN)
	go build $(LDFLAGS) -o $@ ./cmd/grainfs/

build: bin/$(BINARY)

%.pb.go: %.proto
	protoc --go_out=. --go_opt=paths=source_relative $<

proto: $(PROTO_GEN)

internal/erasure/erasurepb/ECObjectMetaFB.go: internal/erasure/erasurepb/ECObjectMeta.fbs
	flatc --go --gen-all -o internal/erasure/ $<

internal/raft/raftpb/RPCMessageFB.go: internal/raft/raftpb/shard.fbs
	flatc --go --gen-all -o internal/raft/ $<

fbs: $(FBS_GEN)

test:
	go test ./... -count=1 -cover

test-race:
	go test ./... -count=1 -race -cover

test-e2e: bin/$(BINARY)
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test ./tests/e2e/ -v -count=1 -timeout 300s

test-jepsen: bin/$(BINARY)
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test ./tests/e2e/ -run TestJepsen -v -timeout 5m

test-smoke: bin/$(BINARY)
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test ./tests/e2e/ -run TestSmoke -v -timeout 3m

test-network-fault:
	@./scripts/install_toxiproxy.sh
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test ./tests/e2e/ -run TestNetworkPartition -v -timeout 10m

test-backup:
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test ./tests/e2e/ -run TestBackup -v -timeout 10m

run: bin/$(BINARY)
	./bin/$(BINARY) serve --data ./tmp --port 9000

clean:
	rm -rf bin/ tmp/

lint:
	go vet ./...
	test -z "$$(gofmt -l .)"

bench: bin/$(BINARY)
	@echo "Starting GrainFS server for benchmarks..."
	@./bin/$(BINARY) serve --data /tmp/grainfs-bench --port 9100 --no-encryption --nfs-port 0 & \
		SERVER_PID=$$!; \
		sleep 2; \
		k6 run benchmarks/s3_bench.js --env BASE_URL=http://localhost:9100; \
		kill $$SERVER_PID 2>/dev/null; \
		rm -rf /tmp/grainfs-bench

test-nbd-docker:
	@echo "Running NBD E2E tests in Docker..."
	docker build -t grainfs-nbd-test -f docker/nbd-test.Dockerfile .
	docker run --rm --privileged -v /lib/modules:/lib/modules:ro grainfs-nbd-test

update-deps:
	find . -name "go.mod" -not -path "*/vendor/*" -execdir go get -u ./... \; -execdir go mod tidy \;
