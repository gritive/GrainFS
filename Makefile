BINARY := grainfs
MODULE := github.com/gritive/GrainFS
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -ldflags "-s -w -X main.version=$(VERSION)"

GO_SRC := $(shell find cmd internal -name '*.go' -not -name '*_test.go')
FBS_SRC := $(shell find internal -name '*.fbs')
FBS_STAMPS := $(FBS_SRC:.fbs=.fbs.stamp)

.PHONY: test test-unit test-colima test-race test-e2e test-e2e-iceberg test-e2e-colima test-directio-linux test-jepsen test-smoke test-network-fault test-backup clean run lint lint-keyspace bench bench-cluster bench-profile bench-topology-get bench-topology-get-matrix bench-iceberg-table bench-iceberg-table-cluster build-pgo test-nbd-interop update-deps fbs test-nfs4-colima test-pynfs-colima test-nbd-colima bench-nbd bench-nbd-cluster bench-nfs bench-nfs-cluster test-fuse-s3-colima bench-fuse-s3-colima bench-directio-s3 test-raft-v2-chaos test-compat

PGO_PROFILE ?= /tmp/grainfs-bench-cpu.out
E2E_TEST_PATTERN ?= ^Test
E2E_TEST_TIMEOUT ?= 900s
E2E_TEST_PARALLEL ?= 1
E2E_TEST_P ?= 1
E2E_TEST_JOBS ?= 2

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

test: test-unit test-colima

test-unit:
	go test $(UNIT_PKGS) -count=1 -cover

test-colima: test-directio-linux test-nbd-colima test-fuse-s3-colima test-nfs4-colima

test-race:
	go test $(UNIT_PKGS) -count=1 -race -cover

test-e2e: bin/$(BINARY)
	@set -e; \
	list_out=$$(mktemp); \
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test -p $(E2E_TEST_P) ./tests/e2e/ -parallel $(E2E_TEST_PARALLEL) -list '$(E2E_TEST_PATTERN)' > $$list_out; \
	tests=$$(awk '/^Test/ { print $$1 }' $$list_out); \
	rm -f $$list_out; \
	if [ -z "$$tests" ]; then exit 0; fi; \
	printf '%s\n' $$tests | xargs -P $(E2E_TEST_JOBS) -I {} sh -c '\
		test="$$1"; \
		echo "=== RUN SINGLE $$test ==="; \
		GRAINFS_BINARY="$(CURDIR)/bin/$(BINARY)" go test -p $(E2E_TEST_P) ./tests/e2e/ -parallel $(E2E_TEST_PARALLEL) -run "^$${test}$$" -v -count=1 -timeout $(E2E_TEST_TIMEOUT); \
	' sh {}

test-e2e-iceberg: bin/$(BINARY)
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test -tags duckdb_e2e ./tests/e2e/ -run TestIcebergDuckDB -v -count=1 -timeout 5m

test-nfs4-colima: build
	go test -v -tags colima -timeout 120s ./tests/nfs4_colima/ -run TestNFS4

test-pynfs-colima:
	@echo "Running pynfs conformance in Colima VM (advisory)"
	tests/conformance/run_pynfs.sh --colima --suite basic
	@echo "Summary: tests/conformance/results/summary.json"

test-nbd-colima: build
	go test -v -tags colima -timeout 120s ./tests/nbd_colima/

test-nbd-interop: build
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test -v -timeout 180s ./tests/nbd_interop/

# test-raft-v2-chaos: sustained chaos run with -race for raft/v2.
# Per-PR CI default: RAFT_CHAOS_DURATION=30s (runs in ~35s wall clock with -race).
# Nightly: RAFT_CHAOS_DURATION=30m make test-raft-v2-chaos.
# The timeout is set to 35m to cover the 30m nightly run plus setup overhead.
test-raft-v2-chaos:
	RAFT_CHAOS_DURATION=$${RAFT_CHAOS_DURATION:-30s} go test -race -count=1 -run TestChaos_Sustained -timeout 35m ./internal/raft/v2

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

# test-compat: rolling upgrade compatibility lane.
# Requires COMPAT_PREV_BIN=/path/to/prev-binary; tests skip if not set.
test-compat: bin/$(BINARY)
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test -tags compat -v -count=1 -timeout 10m ./tests/compat/

run: bin/$(BINARY)
	./bin/$(BINARY) serve --data ./tmp --port 9000

clean:
	rm -rf bin/ tmp/
	find internal -name '*.fbs.stamp' -delete

lint-keyspace:
	@bash scripts/lint-keyspace.sh

lint: lint-keyspace
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

bench-directio-s3:
	./benchmarks/bench_directio_s3.sh

bench-topology-get: bin/$(BINARY)
	NO_BUILD=1 PROFILE=1 ./benchmarks/bench_topology_get_profile.sh

bench-topology-get-matrix: bin/$(BINARY)
	NO_BUILD=1 PROFILE=1 ./benchmarks/bench_topology_get_matrix.sh

bench-iceberg-table: build
	./benchmarks/bench_iceberg_table.sh

bench-iceberg-table-cluster: build
	./benchmarks/bench_iceberg_table_cluster.sh

NBD_PPROF_DIR ?= $(HOME)/tmp/grainfs-nbd-pprof

# Run Linux-only tests directly inside Colima. The host cross-compiles Linux
# binaries and test binaries, copies them into the VM, then executes there.
test-e2e-colima:
	./scripts/run_colima_linux_tests.sh all

test-directio-linux:
	./scripts/run_colima_linux_tests.sh directio

update-deps:
	find . -name "go.mod" -not -path "*/vendor/*" -execdir go get -u ./... \; -execdir go mod tidy \;
