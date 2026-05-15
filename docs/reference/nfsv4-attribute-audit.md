# NFSv4 Attribute Audit

RFC 8881 (NFSv4.1) Section 5.8 attribute coverage audit for the `GrainFS` NFSv4
server. `GrainFS` also accepts NFSv4.0 and NFSv4.2 clients. NFSv4.2 is specified
by RFC 7862, but this audit covers the shared NFSv4 attribute model.

Status legend for attribute reads: **Returned** means the attribute is
returned truthfully, including explicit false/zero capability values.
**Partial** means returned with documented caveats. **Not returned** means the
attribute is not in `supported_attrs` or not returned.

SETATTR uses its own writable-status legend below.

Last audit: 2026-05-14, NFS multi-export Phase 6.

## REQUIRED Attributes

| Bit | Name | Status | Notes |
|-----|------|--------|-------|
| 0 | supported_attrs | Returned |  |
| 1 | type | Returned |  |
| 2 | fh_expire_type | Returned | Filehandles are volatile across export generation changes. |
| 3 | change | Returned |  |
| 4 | size | Returned |  |
| 5 | link_support | Returned |  |
| 6 | symlink_support | Returned |  |
| 7 | named_attr | Returned |  |
| 8 | fsid | Returned |  |
| 9 | unique_handles | Returned |  |
| 10 | lease_time | Returned | 90 seconds. |
| 11 | rdattr_error | Not returned |  |
| 19 | filehandle | Returned |  |
| 75 | suppattr_exclcreat | Returned |  |

## RECOMMENDED Attributes

| Bit | Name | Status | Notes |
|-----|------|--------|-------|
| 12 | acl | Not returned |  |
| 13 | aclsupport | Returned |  |
| 14 | archive | Not returned |  |
| 15 | cansettime | Returned | `time_access_set` is consumed but not persisted. |
| 16 | case_insensitive | Not returned |  |
| 17 | case_preserving | Not returned |  |
| 18 | chown_restricted | Not returned |  |
| 20 | fileid | Returned |  |
| 21 | files_avail | Not returned |  |
| 22 | files_free | Not returned |  |
| 23 | files_total | Not returned |  |
| 24 | fs_locations | Not returned |  |
| 25 | hidden | Not returned |  |
| 26 | homogeneous | Not returned |  |
| 27 | maxfilesize | Returned |  |
| 28 | maxlink | Not returned |  |
| 29 | maxname | Not returned |  |
| 30 | maxread | Returned | 1 MiB. |
| 31 | maxwrite | Returned | 1 MiB. |
| 32 | mimetype | Not returned |  |
| 33 | mode | Returned |  |
| 34 | no_trunc | Not returned |  |
| 35 | numlinks | Returned |  |
| 36 | owner | Partial | Hardcoded `root`; no idmap or authenticated user mapping. |
| 37 | owner_group | Partial | Hardcoded `root`; no idmap or group mapping. |
| 38 | quota_avail_hard | Not returned |  |
| 39 | quota_avail_soft | Not returned |  |
| 40 | quota_used | Not returned |  |
| 41 | rawdev | Not returned |  |
| 42 | space_avail | Not returned |  |
| 43 | space_free | Not returned |  |
| 44 | space_total | Not returned |  |
| 45 | space_used | Returned |  |
| 46 | system | Not returned |  |
| 47 | time_access | Returned | Falls back to last-modified time. |
| 48 | time_access_set | Partial | Consumed but atime is not persisted. |
| 49 | time_backup | Not returned |  |
| 50 | time_create | Not returned |  |
| 51 | time_delta | Not returned |  |
| 52 | time_metadata | Returned | Falls back to last-modified time. |
| 53 | time_modify | Returned | Falls back to last-modified time. |
| 54 | time_modify_set | Returned |  |
| 55 | mounted_on_fileid | Returned |  |
| 56 | dir_notif_delay | Not returned |  |
| 57 | dirent_notif_delay | Not returned |  |
| 58 | dacl | Not returned |  |
| 59 | sacl | Not returned |  |
| 60 | change_policy | Not returned |  |
| 61 | fs_status | Not returned |  |
| 62 | fs_layout_type | Not returned |  |
| 63 | layout_hint | Not returned |  |
| 64 | layout_type | Not returned |  |
| 65 | layout_blksize | Not returned |  |
| 66 | layout_alignment | Not returned |  |
| 67 | fs_locations_info | Not returned |  |
| 68 | mdsthreshold | Not returned |  |
| 69 | retention_get | Not returned |  |
| 70 | retention_set | Not returned |  |
| 71 | retentevt_get | Not returned |  |
| 72 | retentevt_set | Not returned |  |
| 73 | retention_hold | Not returned |  |
| 74 | mode_set_masked | Not returned |  |
| 76 | fs_charset_cap | Not returned |  |

## SETATTR-Writable Attributes

| Bit | Name | Status | Notes |
|-----|------|--------|-------|
| 4 | size | Writable |  |
| 33 | mode | Writable |  |
| 36 | owner | Not writable |  |
| 37 | owner_group | Not writable |  |
| 48 | time_access_set | Partial | Consumed but atime is not persisted. |
| 54 | time_modify_set | Writable |  |
| 58 | dacl | Not writable |  |
| 59 | sacl | Not writable |  |
| 70 | retention_set | Not writable |  |
| 72 | retentevt_set | Not writable |  |
| 73 | retention_hold | Not writable |  |
| 74 | mode_set_masked | Not writable |  |

## Supported-Attrs Consistency Gate

The `supported_attrs` bitmap advertises bits 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13, 15, 20, 27, 30, 31, 33, 35, 36, 37, 45, 47, 48, 52, 53, 54, 55, and 75.

The table marks each advertised bit as **Returned** or **Partial**.
`GrainFS` does not advertise **Not returned** bits. Future PRs that change
`supported_attrs` must update this matrix.
