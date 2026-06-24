BINARY := grainfs
MODULE := github.com/gritive/GrainFS
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -ldflags "-s -w -X main.version=$(VERSION)"

GO_SRC := $(shell find cmd internal -name '*.go' -not -name '*_test.go')
FBS_SRC := $(shell find internal -name '*.fbs')
FBS_STAMPS := $(FBS_SRC:.fbs=.fbs.stamp)

.PHONY: test test-unit test-colima test-race test-e2e test-e2e-colima test-directio-linux test-jepsen test-smoke test-network-fault clean run lint lint-keyspace lint-storage-fixture bench bench-cluster bench-s3-compat-compare build-pgo test-nbd-interop update-deps fbs test-nbd-colima bench-nbd bench-nbd-cluster test-fuse-s3-colima test-s3-client-smoke-colima bench-fuse-s3-colima test-raft-v2-chaos test-compat test-cluster-mount-colima

PGO_PROFILE ?= /tmp/grainfs-bench-cpu.out
E2E_TEST_TIMEOUT ?= 3600s
E2E_TEST_JOBS ?= 2
GINKGO ?= go run github.com/onsi/ginkgo/v2/ginkgo
E2E_GINKGO_PROCS ?= $(E2E_TEST_JOBS)
E2E_GINKGO_REPORT_ARGS ?= --output-interceptor-mode=none --silence-skips
E2E_GINKGO_ARGS ?=

bin/$(BINARY): $(GO_SRC) $(FBS_STAMPS)
	go build $(LDFLAGS) -o $@ ./cmd/grainfs/

build: lint bin/$(BINARY)

# build-pgo: compile with Profile-Guided Optimization using a previously collected
# pprof CPU profile from a representative benchmark run. Typically 5-15% faster on hot paths.
# Usage: cp <cpu-profile> $(PGO_PROFILE) && make build-pgo
build-pgo: $(GO_SRC) $(FBS_STAMPS)
	@if [ ! -f "$(PGO_PROFILE)" ]; then \
		echo "No profile found at $(PGO_PROFILE). Copy a representative CPU profile there first."; \
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

UNIT_PKGS := $(shell go list ./... | grep -v '/tests/e2e' | grep -v '/tests/colimafixture')

test: test-unit test-colima

test-unit:
	go test $(UNIT_PKGS) -count=1 -cover

test-colima: test-directio-linux test-nbd-colima test-fuse-s3-colima test-s3-client-smoke-colima test-cluster-mount-colima

test-cluster-mount-colima: build
	go test -v -tags colima -count=1 -timeout 300s -run TestNBD_ClusterMount ./tests/nbd_colima/

test-race:
	go test $(UNIT_PKGS) -count=1 -race -cover

test-e2e: bin/$(BINARY)
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) $(GINKGO) --procs=$(E2E_GINKGO_PROCS) --timeout=$(E2E_TEST_TIMEOUT) $(E2E_GINKGO_REPORT_ARGS) $(E2E_GINKGO_ARGS) ./tests/e2e

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

test-fuse-s3-colima: build
	go test -v -tags colima -timeout 180s ./tests/fuse_s3_colima/ -run TestFUSE_S3

test-s3-client-smoke-colima:
	go test -v -tags colima -timeout 180s ./tests/fuse_s3_colima/ -run 'TestFUSE_S3_(S3FS|Goofys)' -count=1

# FUSE-over-S3 throughput benchmark: compares direct S3 GET/PUT vs rclone mount
# read/write to quantify the FUSE overhead. Run after test-fuse-s3-colima.
bench-fuse-s3-colima: build
	go test -v -tags colima -timeout 300s -run '^$$' -bench BenchmarkFUSE_S3_Throughput -benchtime 1x ./benchmarks/fuse_s3_colima/

test-jepsen: bin/$(BINARY)
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test ./tests/e2e/ -run TestJepsen -v -timeout 5m

test-smoke: bin/$(BINARY)
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test ./tests/e2e/ -run TestSmoke -v -timeout 3m

test-network-fault:
	@./scripts/install_toxiproxy.sh
	GRAINFS_BINARY=$(CURDIR)/bin/$(BINARY) go test ./tests/e2e/ -run TestNetworkPartition -v -timeout 10m

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

# lint-storage-fixture asserts that no production code imports the LocalBackend
# test fixture. See ADR-0015. This guards the marker comment's truthfulness —
# if a future PR adds storage.NewLocalBackend (or any LocalBackend-named symbol)
# under cmd/ or internal/ (excluding _test.go and internal/storage/ itself),
# the build fails fast instead of silently lying.
lint-storage-fixture:
	@test -d cmd && test -d internal || { echo "FAIL: source layout changed — cmd/ or internal/ missing"; exit 2; }
	@hits=$$(grep -rE 'storage\.[A-Za-z_]*LocalBackend' --include='*.go' cmd/ internal/ 2>/dev/null | { grep -v '_test.go' || true; } | { grep -v 'internal/storage/' || true; }); \
	if [ -n "$$hits" ]; then \
		echo "FAIL: production caller of LocalBackend found (see ADR-0015):"; \
		echo "$$hits"; \
		exit 1; \
	fi; \
	echo "lint-storage-fixture: OK (no production caller of LocalBackend)"

lint: lint-keyspace lint-storage-fixture
	go vet ./...
	test -z "$$(gofmt -l .)"
	golangci-lint run --timeout=5m ./...

bench: bin/$(BINARY)
	NO_BUILD=1 TARGETS=grainfs-single ./benchmarks/bench_s3_compat_compare.sh

bench-cluster: bin/$(BINARY)
	NO_BUILD=1 TARGETS=grainfs-cluster ./benchmarks/bench_s3_compat_compare.sh

bench-s3-compat-compare: bin/$(BINARY)
	NO_BUILD=1 ./benchmarks/bench_s3_compat_compare.sh

NBD_PPROF_DIR ?= $(HOME)/tmp/grainfs-nbd-pprof

# Run Linux-only tests directly inside Colima. The host cross-compiles Linux
# binaries and test binaries, copies them into the VM, then executes there.
test-e2e-colima:
	./scripts/run_colima_linux_tests.sh all

test-directio-linux:
	./scripts/run_colima_linux_tests.sh directio

update-deps:
	find . -name "go.mod" -not -path "*/vendor/*" -execdir go get -u ./... \; -execdir go mod tidy \;
