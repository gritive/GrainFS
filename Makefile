BINARY := grainfs
MODULE := github.com/gritive/GrainFS
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -ldflags "-s -w -X main.version=$(VERSION)"

GO_SRC := $(shell find cmd internal -name '*.go' -not -name '*_test.go')
FBS_SRC := $(shell find internal -name '*.fbs')
FBS_STAMPS := $(FBS_SRC:.fbs=.fbs.stamp)

.PHONY: test test-race test-e2e test-e2e-iceberg test-e2e-docker test-jepsen test-smoke test-network-fault test-backup clean run lint bench bench-cluster bench-profile bench-iceberg-table bench-iceberg-table-cluster build-pgo docker-build test-nbd-docker test-nbd-interop update-deps fbs test-nfs4-colima test-nbd-colima bench-nbd bench-nbd-cluster bench-nfs bench-nfs-cluster test-fuse-s3-colima bench-fuse-s3-colima

PGO_PROFILE ?= /tmp/grainfs-bench-cpu.out
E2E_TEST_PATTERN ?= ^Test
E2E_TEST_TIMEOUT ?= 600s
E2E_TEST_PARALLEL ?= 1
E2E_TEST_P ?= 1

bin/$(BINARY): $(GO_SRC) $(FBS_STAMPS)
	go build $(LDFLAGS) -o $@ ./cmd/grainfs/

build: bin/$(BINARY)

docker-build:
	docker build --build-arg VERSION=$(VERSION) -t grainfs:$(VERSION) .

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
	@set -e; \
	list_out=$$(mktemp); \
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test -p $(E2E_TEST_P) ./tests/e2e/ -parallel $(E2E_TEST_PARALLEL) -list '$(E2E_TEST_PATTERN)' > $$list_out; \
	tests=$$(awk '/^Test/ { print $$1 }' $$list_out); \
	rm -f $$list_out; \
	for test in $$tests; do \
		echo "=== RUN SINGLE $$test ==="; \
		GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test -p $(E2E_TEST_P) ./tests/e2e/ -parallel $(E2E_TEST_PARALLEL) -run "^$$test$$" -v -count=1 -timeout $(E2E_TEST_TIMEOUT); \
	done

test-e2e-iceberg: bin/$(BINARY)
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test ./tests/e2e/ -run TestIcebergDuckDB -v -count=1 -timeout 5m

test-nfs4-colima: build
	go test -v -tags colima -timeout 120s ./tests/nfs4_colima/ -run TestNFS4

test-nbd-colima: build
	go test -v -tags colima -timeout 120s ./tests/nbd_colima/

test-nbd-interop: build
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test -v -timeout 180s ./tests/nbd_interop/

bench-nbd: build
	./benchmarks/bench_nbd_profile.sh

bench-nbd-cluster: build
	./benchmarks/bench_nbd_cluster_profile.sh

bench-nfs: build
	./benchmarks/bench_nfs_profile.sh

bench-nfs-cluster: build
	./benchmarks/bench_nfs_cluster_profile.sh

# FUSE-over-S3 e2e: macOS host runs grainfs serve, Colima Linux VM mounts via
# rclone mount and exercises common filesystem operations. Verifies that
# GrainFS's S3 API works with standard FUSE-over-S3 client tooling.
# Prereqs in the VM: rclone, fuse3 (e.g., colima ssh -- sudo apt install -y rclone fuse3)
test-fuse-s3-colima: build
	go test -v -tags colima -timeout 180s ./tests/fuse_s3_colima/ -run TestFUSE_S3

# FUSE-over-S3 throughput benchmark: compares direct S3 GET/PUT vs rclone mount
# read/write to quantify the FUSE overhead. Run after test-fuse-s3-colima.
bench-fuse-s3-colima: build
	go test -v -tags colima -timeout 300s -run '^$$' -bench BenchmarkFUSE_S3_Throughput -benchtime 1x ./tests/fuse_s3_colima/

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
	golangci-lint run --timeout=5m ./...

bench: bin/$(BINARY)
	NO_BUILD=1 ./benchmarks/run-baseline.sh

bench-cluster: bin/$(BINARY)
	NO_BUILD=1 ./benchmarks/bench_cluster.sh

# bench-profile: run k6 load test while collecting pprof profiles.
# CPU profile is collected concurrently with k6 (captures real load).
# Results in /tmp/grainfs-bench-*.out
bench-profile: bin/$(BINARY)
	NO_BUILD=1 PROFILE=1 ./benchmarks/bench_profile.sh

bench-iceberg-table: build
	./benchmarks/bench_iceberg_table.sh

bench-iceberg-table-cluster: build
	./benchmarks/bench_iceberg_table_cluster.sh

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
