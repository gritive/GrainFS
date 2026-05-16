# `GrainFS` NBD Compatibility Matrix

This document summarizes user-facing Network Block Device compatibility.

`Supported` means the behavior has e2e, conformance, or real client integration
coverage. Unit-test-only coverage is not enough.

## Status Definitions

| Status        | Meaning                                                                                              |
| ------------- | ---------------------------------------------------------------------------------------------------- |
| Supported     | Covered by e2e, conformance, or real client integration tests.                                       |
| Partial       | Integration-tested, but with known semantic or scope limits.                                         |
| Not supported | `GrainFS` does not implement or claim this compatibility surface.                                      |
| Not planned   | Intentionally outside the product scope.                                                             |

## Protocol Surface

| Area            | Surface                        | Status        | Notes                                                                                    |
| --------------- | ------------------------------ | ------------- | ---------------------------------------------------------------------------------------- |
| Client platform | Linux kernel NBD client        | Supported     |                                                                                          |
| Client platform | macOS native NBD client        | Not supported | macOS has no native kernel NBD client path.                                              |
| Negotiation     | Fixed newstyle                 | Supported     |                                                                                          |
| Negotiation     | Export-name selection          | Supported     | Volumes are selected by name with `nbd-client -N <volume>`.                              |
| Negotiation     | `OPT_INFO` / `OPT_GO`          | Partial       | qemu/libnbd interop is not claimed.                                                      |
| Discovery       | `NBD_INFO_BLOCK_SIZE`          | Partial       | qemu/libnbd interop is not claimed.                                                      |
| Commands        | READ                           | Supported     |                                                                                          |
| Commands        | WRITE                          | Supported     |                                                                                          |
| Commands        | FLUSH                          | Supported     |                                                                                          |
| Commands        | WRITE_ZEROES                   | Partial       | qemu/libnbd interop is not claimed.                                                      |
| Replies         | Structured read reply          | Partial       | qemu/libnbd interop is not claimed.                                                      |
| Metadata        | `base:allocation` block status | Partial       | qemu/libnbd interop is not claimed.                                                      |
| Headers         | Extended headers               | Not supported | Parser exists, but basic negotiation returns `NBD_REP_ERR_UNSUP` until interop is fixed. |
