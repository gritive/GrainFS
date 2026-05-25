# Repository Layout

GrainFS follows the standard Go layout: `cmd/` contains thin command entry points and `internal/` contains private implementation packages.

Important roots:

- `cmd/grainfs`: Cobra command definitions.
- `internal/`: server, storage, cluster, protocol, metrics, and admin implementation.
- `tests/`: e2e and interop tests.
- `benchmarks/`: benchmark scripts and methodology support.
- `docs/`: customer-facing and contributor-facing documentation.
