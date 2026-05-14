# NFSv4 Conformance Tests

External RFC 8881 conformance uses pynfs from upstream `linux-nfs.org`.

These tests are advisory. They do not run in the default PR test target, but each run writes pass/fail counts to `results/summary.json` so failures are visible and comparable over time.

## License Note

pynfs is an external GPL tool. GrainFS does not vendor or redistribute it. `run_pynfs.sh` clones pynfs into `.pynfs/` on the operator machine and checks out the pinned commit in `PYNFS_SHA`.

## Usage

```sh
make build
tests/conformance/run_pynfs.sh --suite basic
```

Outputs:

```text
tests/conformance/results/pynfs-<timestamp>.log
tests/conformance/results/summary.json
```

On macOS, use a Linux client through Colima:

```sh
make test-pynfs-colima
```

The default suite is `basic`. Use `--suite all` for the full pynfs suite.

## Updating pynfs

Update `PYNFS_SHA` to a reviewed upstream commit and remove `.pynfs/` before rerunning.

## Bit Rot Prevention

`results/summary.json` is overwritten on every run. A nightly or weekly owner should review the counts and open follow-up issues for new failures. Owner: TBD.
