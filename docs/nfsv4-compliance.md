# NFSv4.1 Compliance Matrix

RFC 8881 Section 5.8 attribute coverage audit for the GrainFS NFSv4.1 server.

Status legend: **Done** means implemented and advertised truthfully; **Partial** means implemented with documented caveats; **Skipped** means not implemented or not advertised. Source references prefer symbols over line numbers to avoid drift.

Last audit: 2026-05-14, NFS multi-export Phase 6.

## REQUIRED Attributes

| Bit | Name | Status | Source | Notes |
|-----|------|--------|--------|-------|
| 0 | supported_attrs | Done | `internal/nfs4server/compound.go` `encodeAttrs` | Advertises the corrected implemented bitmap, including word 2 for bit 75. |
| 1 | type | Done | `internal/nfs4server/compound.go` `encodeAttrs` | Returns `NF4DIR` for pseudo/export roots and directories, `NF4REG` for files. |
| 2 | fh_expire_type | Done | `internal/nfs4server/compound.go` `encodeAttrs`, `opPutFH` | Returns `FH4_VOLATILE_ANY`; `opPutFH` checks export generation for stale/revoked handles. |
| 3 | change | Done | `internal/nfs4server/compound.go` `encodeAttrs` | Uses object last-modified or directory mtime. |
| 4 | size | Done | `internal/nfs4server/compound.go` `encodeAttrs`, `opSetAttr` | GETATTR returns object size; SETATTR truncates via backend support. |
| 5 | link_support | Done | `internal/nfs4server/compound.go` `encodeAttrs` | Returns false; GrainFS does not implement NFS LINK. |
| 6 | symlink_support | Done | `internal/nfs4server/compound.go` `encodeAttrs` | Returns false; GrainFS does not implement NFS SYMLINK. |
| 7 | named_attr | Done | `internal/nfs4server/compound.go` `encodeAttrs` | Returns false; named attributes are not exposed. |
| 8 | fsid | Done | `internal/nfs4server/compound.go` `encodeAttrs`, `exportFSID` | Pseudo-root uses reserved `{1,0}`; bucket exports use per-export major/minor. |
| 9 | unique_handles | Done | `internal/nfs4server/compound.go` `encodeAttrs` | Returns true; filehandles are path-derived and generation-bound. |
| 10 | lease_time | Done | `internal/nfs4server/compound.go` `encodeAttrs` | Static 90-second lease policy. |
| 11 | rdattr_error | Skipped | `internal/nfs4server/compound.go` `opReadDir` | READDIR does not emit per-entry `rdattr_error`; failures are handled at operation level. |
| 19 | filehandle | Done | `internal/nfs4server/compound.go` `opGetFH`, `opReadDir` | Filehandles are returned by GETFH and path traversal; READDIR attrs can include encoded object attributes. |
| 75 | suppattr_exclcreat | Done | `internal/nfs4server/compound.go` `readAttrBitmap`, `encodeAttrs` | Third bitmap word is supported; returns an empty EXCLUSIVE4_1 create-attribute bitmap. |

## RECOMMENDED Attributes

| Bit | Name | Status | Source | Notes |
|-----|------|--------|--------|-------|
| 12 | acl | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | ACL payloads are not implemented. |
| 13 | aclsupport | Done | `internal/nfs4server/compound.go` `encodeAttrs` | Returns `0`, meaning no ACL support. |
| 14 | archive | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | Deprecated archive flag is not tracked or advertised. |
| 15 | cansettime | Done | `internal/nfs4server/compound.go` `encodeAttrs`, `opSetAttr` | Returns true; `time_modify_set` is persisted, `time_access_set` is consumed but not stored. |
| 16 | case_insensitive | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | S3 keys are case-sensitive; this boolean is not currently returned. |
| 17 | case_preserving | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | S3 preserves key case, but the attribute is not currently returned. |
| 18 | chown_restricted | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | POSIX ownership changes are not modeled. |
| 20 | fileid | Done | `internal/nfs4server/compound.go` `encodeAttrs`, `pathToFileID` | Stable FNV-1a path-derived file IDs. |
| 21 | files_avail | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | No inode-style available-file accounting. |
| 22 | files_free | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | No inode-style free-file accounting. |
| 23 | files_total | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | No inode-style total-file accounting. |
| 24 | fs_locations | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | NFS referrals and moved filesystems are not implemented. |
| 25 | hidden | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | No hidden-file mapping is defined for S3 keys. |
| 26 | homogeneous | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | GrainFS exports are homogeneous, but this attribute is not returned. |
| 27 | maxfilesize | Done | `internal/nfs4server/compound.go` `encodeAttrs` | Returns `1 << 53`. |
| 28 | maxlink | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | Hard links are unsupported. |
| 29 | maxname | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | S3 key component/name limits are not surfaced. |
| 30 | maxread | Done | `internal/nfs4server/compound.go` `encodeAttrs` | Returns 1 MiB. |
| 31 | maxwrite | Done | `internal/nfs4server/compound.go` `encodeAttrs`, `opWrite` | Returns and enforces 1 MiB writes. |
| 32 | mimetype | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | S3 Content-Type is not mapped into NFS attributes. |
| 33 | mode | Done | `internal/nfs4server/compound.go` `encodeAttrs`, `opSetAttr` | Files use sidecar mode fallback `0644`; directories return `0755`; SETATTR persists mode sidecar. |
| 34 | no_trunc | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | Name truncation policy is not surfaced. |
| 35 | numlinks | Done | `internal/nfs4server/compound.go` `encodeAttrs` | Returns 1 for files and 2 for directories. |
| 36 | owner | Partial | `internal/nfs4server/compound.go` `encodeAttrs` | Returns hardcoded `root`; no idmap or authenticated user mapping. |
| 37 | owner_group | Partial | `internal/nfs4server/compound.go` `encodeAttrs` | Returns hardcoded `root`; no idmap or group mapping. |
| 38 | quota_avail_hard | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | NFS quota accounting is not implemented. |
| 39 | quota_avail_soft | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | NFS quota accounting is not implemented. |
| 40 | quota_used | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | NFS quota accounting is not implemented. |
| 41 | rawdev | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | Device nodes are not represented. |
| 42 | space_avail | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | Filesystem capacity is not surfaced through NFS yet. |
| 43 | space_free | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | Filesystem capacity is not surfaced through NFS yet. |
| 44 | space_total | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | Filesystem capacity is not surfaced through NFS yet. |
| 45 | space_used | Done | `internal/nfs4server/compound.go` `encodeAttrs` | Returns file size as used space. |
| 46 | system | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | System-file flag is not modeled. |
| 47 | time_access | Done | `internal/nfs4server/compound.go` `encodeAttrs` | Returns last-modified timestamp as access time fallback. |
| 48 | time_access_set | Partial | `internal/nfs4server/compound.go` `opSetAttr`, `supported_attrs` | SETATTR consumes the value for reader alignment but does not persist atime. |
| 49 | time_backup | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | Backup time is not tracked. |
| 50 | time_create | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | Creation time is not tracked separately. |
| 51 | time_delta | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | Timestamp precision is not surfaced. |
| 52 | time_metadata | Done | `internal/nfs4server/compound.go` `encodeAttrs` | Returns last-modified/sidecar mtime. |
| 53 | time_modify | Done | `internal/nfs4server/compound.go` `encodeAttrs` | Returns object last-modified or sidecar mtime. |
| 54 | time_modify_set | Done | `internal/nfs4server/compound.go` `opSetAttr` | SETATTR persists mtime sidecar. |
| 55 | mounted_on_fileid | Done | `internal/nfs4server/compound.go` `encodeAttrs` | Export roots report pseudo-root fileid; other paths report their own fileid. |
| 56 | dir_notif_delay | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | Directory notification delay is not implemented. |
| 57 | dirent_notif_delay | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | Directory-entry notification delay is not implemented. |
| 58 | dacl | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | NFS ACLs are not implemented. |
| 59 | sacl | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | NFS ACLs are not implemented. |
| 60 | change_policy | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | Change policy is not surfaced. |
| 61 | fs_status | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | Filesystem status is not surfaced. |
| 62 | fs_layout_type | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | pNFS is not implemented. |
| 63 | layout_hint | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | pNFS is not implemented. |
| 64 | layout_type | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | pNFS is not implemented. |
| 65 | layout_blksize | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | pNFS is not implemented. |
| 66 | layout_alignment | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | pNFS is not implemented. |
| 67 | fs_locations_info | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | Advanced filesystem location metadata is not implemented. |
| 68 | mdsthreshold | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | pNFS metadata server thresholds are not implemented. |
| 69 | retention_get | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | NFS retention attributes are not mapped to object-lock metadata. |
| 70 | retention_set | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | NFS retention attributes are not mapped to object-lock metadata. |
| 71 | retentevt_get | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | NFS retention event attributes are not mapped. |
| 72 | retentevt_set | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | NFS retention event attributes are not mapped. |
| 73 | retention_hold | Skipped | `internal/nfs4server/compound.go` `encodeAttrs` | NFS retention hold is not mapped. |
| 74 | mode_set_masked | Skipped | `internal/nfs4server/compound.go` `opSetAttr` | Masked mode setting is not implemented. |
| 76 | fs_charset_cap | Skipped | `internal/nfs4server/compound.go` `readAttrBitmap`, `encodeAttrs` | Third bitmap word is representable, but charset capability flags are not surfaced. |

## SETATTR-Writable Attributes

| Bit | Name | Status | Source | Notes |
|-----|------|--------|--------|-------|
| 4 | size | Done | `internal/nfs4server/compound.go` `opSetAttr` | Truncates or extends file contents through the backend. |
| 33 | mode | Done | `internal/nfs4server/compound.go` `opSetAttr` | Persists mode in NFS sidecar metadata. |
| 36 | owner | Skipped | `internal/nfs4server/compound.go` `opSetAttr` | Ownership changes are not implemented. |
| 37 | owner_group | Skipped | `internal/nfs4server/compound.go` `opSetAttr` | Group changes are not implemented. |
| 48 | time_access_set | Partial | `internal/nfs4server/compound.go` `opSetAttr` | Value is consumed, but atime is not persisted. |
| 54 | time_modify_set | Done | `internal/nfs4server/compound.go` `opSetAttr` | Persists mtime in NFS sidecar metadata. |
| 58 | dacl | Skipped | `internal/nfs4server/compound.go` `opSetAttr` | ACL SETATTR is not implemented. |
| 59 | sacl | Skipped | `internal/nfs4server/compound.go` `opSetAttr` | ACL SETATTR is not implemented. |
| 70 | retention_set | Skipped | `internal/nfs4server/compound.go` `opSetAttr` | Retention SETATTR is not implemented. |
| 72 | retentevt_set | Skipped | `internal/nfs4server/compound.go` `opSetAttr` | Retention event SETATTR is not implemented. |
| 73 | retention_hold | Skipped | `internal/nfs4server/compound.go` `opSetAttr` | Retention hold SETATTR is not implemented. |
| 74 | mode_set_masked | Skipped | `internal/nfs4server/compound.go` `opSetAttr` | Masked mode SETATTR is not implemented. |

## Supported-Attrs Consistency Gate

The corrected `supported_attrs` bitmap advertises bits 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13, 15, 20, 27, 30, 31, 33, 35, 36, 37, 45, 47, 48, 52, 53, 54, 55, and 75.

Every advertised bit is marked **Done** or **Partial** above. Every **Skipped** bit is not advertised. This is the audit gate for future updates: changing `supported_attrs` requires updating this matrix in the same PR.

## Conformance Test Results

- Last pynfs run: not run in this audit PR.
- Runner: `tests/conformance/run_pynfs.sh`
- Coverage gaps tracked in: `TODOS.md`
