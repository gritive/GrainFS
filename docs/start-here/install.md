# Install

GrainFS is distributed as a single binary. The repository also supports local source builds for development.

## Build from source

```bash
make build
```

The binary is written to `bin/grainfs`.

## Development prerequisites

- Go 1.26+
- `golangci-lint` for `make lint` and `make build`
- AWS CLI for S3 examples
- Linux client tools for NFS, NBD, 9P, and FUSE-over-S3 integration tests
- `warp` for S3-compatible benchmark comparisons

## Verify the binary

```bash
bin/grainfs --help
bin/grainfs serve --help
```

## Next steps

Run the [quickstart](quickstart.md), then read the [CLI reference](../reference/cli.md).
