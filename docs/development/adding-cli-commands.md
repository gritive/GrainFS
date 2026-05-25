# Adding CLI Commands

Keep `cmd/grainfs` thin. Command files should define Cobra wiring, flags, and a short `RunE` that calls an internal package. Business logic belongs under `internal/`.

Use tests to guard help output and command boundaries.
