BINARY := grainfs
MODULE := github.com/gritive/GrainFS
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -ldflags "-s -w -X main.version=$(VERSION)"

GO_SRC := $(shell find cmd internal -name '*.go' -not -name '*_test.go')
FBS_SRC := $(shell find internal -name '*.fbs')
FBS_STAMPS := $(FBS_SRC:.fbs=.fbs.stamp)

.PHONY: test test-race test-e2e test-e2e-docker test-jepsen test-smoke test-network-fault test-backup clean run lint bench bench-profile build-pgo test-nbd-docker update-deps fbs test-nfs4-colima

PGO_PROFILE ?= /tmp/grainfs-bench-cpu.out

bin/$(BINARY): $(GO_SRC) $(FBS_STAMPS)
	go build $(LDFLAGS) -o $@ ./cmd/grainfs/

build: bin/$(BINARY)

# build-pgo: compile with Profile-Guided Optimization using a previously collected
# pprof CPU profile (from `make bench-profile`). Typically 5-15% faster on hot paths.
# Usage: make bench-profile && make build-pgo
build-pgo: $(GO_SRC) $(FBS_STAMPS)
	@if [ ! -f "$(PGO_PROFILE)" ]; then \
		echo "No profile found at $(PGO_PROFILE). Run 'make bench-profile' first."; \
		exit 1; \
	fi
	go build $(LDFLAGS) -pgo=$(PGO_PROFILE) -o bin/$(BINARY)-pgo ./cmd/grainfs/
	@echo "PGO binary: bin/$(BINARY)-pgo (profile: $(PGO_PROFILE))"

# Each .fbs generates multiple .go files; a stamp tracks the last-run time.
# Output dir is the parent of the directory containing the .fbs file.
%.fbs.stamp: %.fbs
	flatc --go --gen-all -o $(patsubst %/,%,$(dir $(patsubst %/,%,$(dir $<))))/ $<
	touch $@

fbs: $(FBS_STAMPS)

UNIT_PKGS := $(shell go list ./... | grep -v '/tests/e2e')

test:
	go test $(UNIT_PKGS) -count=1 -cover

test-race:
	go test $(UNIT_PKGS) -count=1 -race -cover

test-e2e: bin/$(BINARY)
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test ./tests/e2e/ -v -count=1 -timeout 600s

test-nfs4-colima: build
	go test -v -tags colima -timeout 120s ./tests/nfs4_colima/ -run TestNFS4

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
	find internal -name '*.fbs.stamp' -delete

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

# bench-profile: run k6 load test while collecting pprof profiles.
# CPU profile is collected concurrently with k6 (captures real load).
# Results in /tmp/grainfs-bench-*.out
bench-profile: bin/$(BINARY)
	@echo "Starting GrainFS server with pprof..."
	@rm -rf /tmp/grainfs-bench-profile && mkdir -p /tmp/grainfs-bench-profile
	@./bin/$(BINARY) serve --data /tmp/grainfs-bench-profile --port 9100 \
		--pprof-port 6060 --no-encryption --nfs-port 0 --log-level warn & \
		SERVER_PID=$$!; \
		sleep 2; \
		echo "Collecting CPU profile during k6 run (30s)..."; \
		curl -sf "http://127.0.0.1:6060/debug/pprof/profile?seconds=30" -o /tmp/grainfs-bench-cpu.out & \
		CPU_PID=$$!; \
		k6 run benchmarks/s3_bench.js --env BASE_URL=http://localhost:9100; \
		wait $$CPU_PID && echo "pprof saved: /tmp/grainfs-bench-cpu.out"; \
		for p in mutex allocs heap goroutine; do \
			curl -sf "http://127.0.0.1:6060/debug/pprof/$$p" -o /tmp/grainfs-bench-$$p.out \
				&& echo "pprof saved: /tmp/grainfs-bench-$$p.out"; \
		done; \
		kill $$SERVER_PID 2>/dev/null; \
		rm -rf /tmp/grainfs-bench-profile; \
		echo ""; \
		echo "Analyse:"; \
		echo "  go tool pprof -top /tmp/grainfs-bench-cpu.out    # CPU hotspots"; \
		echo "  go tool pprof -top /tmp/grainfs-bench-mutex.out  # lock contention"; \
		echo "  go tool pprof -top /tmp/grainfs-bench-allocs.out # alloc hotspots"

NBD_PPROF_DIR ?= $(HOME)/tmp/grainfs-nbd-pprof

test-nbd-docker:
	@echo "Running NBD E2E tests in Docker..."
	docker build -t grainfs-nbd-test -f docker/nbd-test.Dockerfile .
	@mkdir -p $(NBD_PPROF_DIR)
	docker run --rm --privileged \
		-v /lib/modules:/lib/modules:ro \
		-v $(NBD_PPROF_DIR):/tmp \
		-e GRAINFS_PPROF=$(GRAINFS_PPROF) \
		grainfs-nbd-test

# Run the full host-side E2E suite inside a Linux container. Use this on macOS
# when you want to exercise platform-specific code paths (NBD kernel module,
# future O_DIRECT/io_uring) that don't run natively. Mounts the docker socket
# so tests that themselves invoke docker (TestNBD_*) keep working.
test-e2e-docker:
	@echo "Running cross-platform E2E tests in Linux Docker..."
	docker build -t grainfs-e2e -f docker/e2e.Dockerfile .
	docker run --rm --privileged \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v /lib/modules:/lib/modules:ro \
		-e GRAINFS_BINARY=/usr/local/bin/grainfs \
		grainfs-e2e

update-deps:
	find . -name "go.mod" -not -path "*/vendor/*" -execdir go get -u ./... \; -execdir go mod tidy \;
