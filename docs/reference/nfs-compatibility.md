# `GrainFS` NFSv4 Compatibility Matrix

This matrix summarizes user-facing NFSv4 compatibility. NFSv4.2 is specified by
RFC 7862, with its XDR in RFC 7863. Use
[`nfsv4-attribute-audit.md`](nfsv4-attribute-audit.md) for the detailed RFC 8881
attribute audit.

`Supported` means the behavior has e2e, conformance, or real client integration
coverage. Unit-test-only coverage is not enough.

## Status Definitions

| Status        | Meaning                                                                 |
| ------------- | ----------------------------------------------------------------------- |
| Supported     | Covered by e2e, conformance, or real client integration tests.          |
| Partial       | Integration-tested, but with known semantic or scope limits.            |
| Not supported | `GrainFS` does not implement or claim this compatibility surface. |
| Not planned   | Intentionally outside the product scope.                                |

## Protocol Surface

| Area           | Surface                                       | Status    | Notes                                                                                                                                    |
| -------------- | --------------------------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| Protocol       | NFSv4 server                                  | Supported |                                                                                                                                          |
| Protocol       | NFSv4.0 mount path                            | Supported |                                                                                                                                        |
| Protocol       | NFSv4.1 mount path                            | Supported |                                                                                                                                        |
| Protocol       | NFSv4.2 mount path                            | Supported |                                                                                                                                        |
| Protocol       | NFSv4 attribute model                         | Partial   | Several optional/recommended attributes are not implemented; see the attribute audit. |
| Protocol       | NFSv4.2 feature parity                        | Partial   | RFC 7862 feature parity is not claimed; only listed NFSv4.2 operations are claimed.                                                     |
| Security       | Per-client NFS authentication                 | Not supported | NFSv4 exports do not use S3 IAM; restrict access with bind address, firewall, or private network. |
| Exports        | Multi-bucket pseudo-root                      | Supported | Pseudo-root lists exported buckets as child directories.                                                                                 |
| Exports        | Read-only exports                             | Supported |                                                                                                                                        |

## File Operation Semantics

| Area                | Surface                     | Status        | Notes                                                                                                                             |
| ------------------- | --------------------------- | ------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| File I/O            | Read/write regular files    | Supported     |                                                                                                                                   |
| File I/O            | Large file read/write       | Supported     |                                                                                                                                   |
| File I/O            | COMMIT                      | Supported     |                                                                                                                                   |
| File I/O            | SEEK                        | Supported     | NFSv4.2 operation.                                                                                                                |
| File I/O            | ALLOCATE/DEALLOCATE         | Supported     | NFSv4.2 operation.                                                                                                                |
| File I/O            | COPY                        | Supported     | NFSv4.2 operation.                                                                                                                |
| Metadata            | GETATTR required attributes | Partial       | Advertised attributes are kept consistent with the RFC matrix.                                                                    |
| Metadata            | SETATTR size/mode/mtime     | Supported     |                                                                                                                                   |
| Metadata            | Owner/group mapping         | Partial       | Owner and group are hardcoded as `root`; no idmap/authenticated user mapping.                                                     |
| POSIX-like behavior | Atomic rename               | Supported     |                                                                                                                                   |
| POSIX-like behavior | File locking                | Partial       | Validate full lock-manager behavior per client workload.                                                                          |
| POSIX-like behavior | Hard links                  | Not supported |                                                                                                                                   |
| POSIX-like behavior | Symlinks                    | Partial       | Protocol-unit coverage for `CREATE` with `NF4LNK`, `READLINK`, `GETATTR type` as `NF4LNK`, and `symlink_support`. Real-client integration coverage and server-side target resolution are not implemented. |
| POSIX-like behavior | ACLs                        | Not supported |                                                                                                                                   |
| POSIX-like behavior | chown/chgrp                 | Not supported |                                                                                                                                   |
| Capacity/quota      | Quota attributes            | Not supported |                                                                                                                                   |
| pNFS                | Layout attributes and pNFS  | Not supported |                                                                                                                                   |
