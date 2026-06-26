# AppendObject Incremental Metadata Design

Date: 2026-06-26

## Problem

`AppendObject` still rewrites metadata proportional to the current segment count.
The v0.0.741.0 bounded fix removed the single-node previous-record decode and
chunkref remove/add churn for already chunk-referenced appendable objects, but
both single-node and cluster append still rebuild the full object manifest and
the composite ETag history on every append.

Today the append state is embedded in the object record:

- `Object.Segments[]` / `PutObjectMetaCmd.Segments[]` stores every append segment.
- `AppendCallMD5s[]` stores every per-call digest so coalesce can preserve ETag.
- `CompositeETag` hashes the full digest list every append.
- cluster append publishes the whole updated `PutObjectMetaCmd` through
  quorum-meta CAS.

So N appends still write and hash O(N) metadata per append. True O(1) append
requires splitting the mutable append tail out of the object summary.

## Goals

- O(1) metadata write per append in steady state.
- Preserve existing S3-visible behavior: offset checks, `MaxAppendSegments`,
  object size, ETag, coalesced reads, and versioning semantics.
- Keep crash recovery deterministic for both single-node Badger metadata and
  cluster quorum-meta blobs.
- Allow brownfield objects with embedded `Segments[]` / `AppendCallMD5s[]` to
  keep reading and appending during rollout.

## Non-Goals

- Changing AppendObject payload digest semantics. The current checksum-as-MD5
  proxy remains until the separate API-boundary MD5 capture work.
- Solving append failover fencing. That is tracked separately as conditional
  lease work.
- Making appendable/coalesced objects eligible for EC redundancy upgrade.

## Format

Introduce an append-state side record per object version:

```text
append-summary:{bucket}/{key}/{versionID}
  size
  segment_count
  etag_digest_state
  etag_part_count
  compacted_prefix_ref
  tail_epoch

append-segment:{bucket}/{key}/{versionID}/{seq}
  SegmentRef
  call_digest
```

For single-node, these are Badger keys in the same transaction namespace as the
object record and chunkref membership. For cluster, they are quorum-meta sibling
blobs colocated with the object metadata placement set:

```text
.quorum_meta_append/{bucket}/{key}/{versionID}/summary
.quorum_meta_append/{bucket}/{key}/{versionID}/segments/{seq}
```

The object record remains the list/read surface. Appendable objects move to a
summary form:

- `IsAppendable=true`
- `Size`, `ETag`, `ModTime`, placement, ACL/tags/user metadata remain on the
  object record.
- `Segments[]` may contain only a compacted prefix/coalesced boundary, not the
  full append tail.
- `AppendCallMD5s[]` is empty once side-record mode is active.

Readers reconstruct appendable bodies as:

1. read object summary,
2. read compacted/coalesced prefix refs from the object record,
3. stream ordered append side segments from `seq=prefix_count+1`.

## Running ETag State

Store the running MD5 context state for the composite ETag input plus the count
of append calls. Each append updates:

```text
etag_digest_state = MD5StateAppend(old_state, call_digest)
etag_part_count++
ETag = hex(MD5StateSum(etag_digest_state)) + "-" + etag_part_count
```

The single-node implementation stores Go's `crypto/md5` binary-marshaled state
for the composite ETag accumulator, covered by tests against `CompositeETag`.
Cluster side records must use the same state representation or add an explicit
portable codec before cross-version quorum-meta rollout.

## Append Algorithm

Single-node transaction:

1. Load object summary and append summary.
2. Validate `expectedOffset == summary.size`.
3. Write the segment blob before metadata commit, as today.
4. In one Badger transaction:
   - CAS the summary generation / tail epoch.
   - write `append-segment seq=N+1`,
   - update append summary size/count/running ETag state,
   - update object record size/ETag/ModTime only,
   - add chunkref membership for the new segment.

Cluster owner CAS:

1. Owner reads quorum-meta object summary and append summary quorum.
2. Validate offset and placement.
3. Write segment shards/blobs as today.
4. Publish one new append segment blob and one updated append summary blob with
   `tail_epoch+1` CAS.
5. Publish the small object-summary update with the new size/ETag/ModTime.

The append summary CAS is the linearization point for the mutable tail. The
object-summary publish can be retried idempotently from the append summary if a
crash lands between side-record commit and summary publish.

## Brownfield Migration

No eager migration.

On read:

- If no append side summary exists, use the embedded `Segments[]` /
  `AppendCallMD5s[]` path.
- If a side summary exists, merge compacted object refs plus side segments.

On first append to an embedded appendable object:

1. Build side summary from embedded `Segments[]` and `AppendCallMD5s[]`.
2. Write side segment records for the embedded tail or mark them as a compacted
   prefix if coalesced.
3. Switch object record to side-record mode in the same CAS/transaction as the
   new append.

If that conversion is too large for the first implementation slice, land it as
read-only side-record support plus new-object side-record writes, then convert
brownfield appendables in a second slice.

## Chunkref and GC

Chunkref truth remains manifest membership:

- single-node append adds only the new segment membership, as v0.0.741.0 does.
- side-record scan must contribute append segment locators to chunkref rebuild.
- object delete/version hard-delete must delete side records and remove their
  chunkrefs, or tombstone them for the existing orphan walker to reclaim.

Cluster orphan walkers must treat `.quorum_meta_append` as part of object
metadata liveness. A live object summary without its append summary is corrupt
and should fail closed on read.

## Coalesce

Coalesce consumes a prefix of append side segments and writes a coalesced ref.
It must not drop digest history; the running ETag state stays in append summary
and is not recomputed from the surviving segment list.

After coalesce:

- object record gains/updates the coalesced prefix ref,
- append summary advances `compacted_prefix_ref` / prefix count,
- consumed side segment records become deletable after the coalesced ref is
  durable and referenced.

## Implementation Slices

1. Single-node side-record format, read path, writer path, brownfield appendable
   conversion, delete cleanup, append-base summary validation, and running ETag
   state are shipped in v0.0.743.0/v0.0.745.0/v0.0.746.0.
2. Cluster quorum-meta append side records.
   Add `.quorum_meta_append` local/peer primitives, owner CAS, read merge, and
   orphan walker coverage.
3. Coalesce integration.
   Consume side-record prefixes without ETag recompute and keep crash recovery
   idempotent.
4. Benchmark gate.
   `BenchmarkS3Append` and `BenchmarkClusterAppend` must show allocs/op per
   append flattening across 4/8/16 segment sweeps, with no GET/range/read
   regression.

## Test Plan

- Unit: key codecs, summary CAS, MD5 accumulator vs `CompositeETag`, brownfield
  conversion, delete cleanup, chunkref rebuild.
- Single-node integration: append/read/range/coalesce across embedded and
  side-record objects.
- Cluster integration: owner append, forwarded append, owner crash after side
  segment write, quorum read with a missing side summary fails closed.
- Benchmarks: `BenchmarkS3Append`, `BenchmarkClusterAppend`,
  `BenchmarkClusterCopy`, `BenchmarkClusterMultipart_Complete`.

## Open Constraints

- The MD5 accumulator must not depend on unexported stdlib state layouts.
- The cluster summary/object publish split needs an idempotent repair path before
  enabling brownfield conversion in cluster mode.
- Side records count toward the same `MaxAppendSegments` limit; compaction does
  not reset the S3 append count.
