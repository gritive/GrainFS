# `GrainFS` 9P Compatibility Matrix

This matrix summarizes user-facing 9P2000.L compatibility. `GrainFS` disables 9P
by default; use it only on trusted networks.

`Supported` means the behavior has e2e, conformance, or real client integration
coverage. Unit-test-only coverage is not enough.

## Status Definitions

| Status | Meaning |
| --- | --- |
| Supported | Covered by e2e, conformance, or real client integration tests. |
| Partial | Integration-tested, but with known semantic or scope limits. |
| Not tested    | Implemented or expected behavior exists, but no e2e, conformance, or real client test covers it yet. |
| Not supported | `GrainFS` does not implement or claim this compatibility surface. |
| Not planned | Intentionally outside the product scope. |

## Protocol Surface

| Area | Surface | Status | Notes |
| --- | --- | --- | --- |
| Protocol | 9P2000.L over TCP | Supported |  |
| Client platform | Linux kernel 9P client | Supported |  |
| Client platform | macOS native 9P mount | Not supported |  |
| Mount root | `aname=/` bucket listing | Supported | Root mount lists buckets as directories. |
| Mount root | `aname=/bucket` direct bucket mount | Supported | Bucket mount exposes objects at the mount root. |
| Security | Per-client 9P authentication | Not supported | The server is unauthenticated; restrict access with bind address, firewall, or VM networking. |

## File Operation Semantics

| Area | Surface | Status | Notes |
| --- | --- | --- | --- |
| Directory listing | List buckets and object names | Supported |  |
| File I/O | Read/write regular files | Supported |  |
| File I/O | Overwrite existing file | Supported |  |
| File I/O | Truncate file | Supported |  |
| Metadata | `chmod`, `touch`, `stat` mode/mtime | Supported | 9P metadata sidecars are hidden from directory listings. |
| POSIX-like behavior | Rename | Supported |  |
| POSIX-like behavior | Unlink | Supported |  |
| POSIX-like behavior | Directories | Partial | Directory markers and nested object prefixes are modeled; full filesystem semantics are not claimed. |
| POSIX-like behavior | Advisory lock enforcement | Not supported | Lock requests are not a compatibility guarantee for multi-client coordination. |
| POSIX-like behavior | Hard links | Not supported |  |
| POSIX-like behavior | Symlinks | Not supported |  |
| POSIX-like behavior | Special files and xattrs | Not supported |  |

## Related

- [User guide](../users/guide.md#9p)
- [Benchmark methodology](benchmarks.md)
- [NFSv4 compatibility](nfs-compatibility.md)
