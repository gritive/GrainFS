# ADR 0005: Volume Block I/O Belongs Behind a Private Module

## Status

Accepted

## Context

`internal/volume.Manager` currently owns public volume orchestration and the
block-level implementation details for reads, writes, deferred writes, and
discards. Understanding one byte-range operation requires reasoning about
logical offsets, physical block keys, dedup references, live-map updates, block
cache behavior, partial I/O, pool quota checks, read amplification metrics, and
`AllocatedBlocks` accounting in the same large module.

That makes the volume layer hard to test at the policy level. Manager behavior
tests can observe end results, but the branch matrix for block merge rules,
cache hit and invalidation behavior, direct-block write selection, allocation
delta calculation, and dedup/live-map interactions is spread across public
method setup instead of concentrated behind a small interface.

## Decision

Volume block I/O is deepened behind a private module in `internal/volume`.
The module owns block I/O planning and execution for `ReadAt`, `WriteAt`,
`WriteAtDeferred`, and `Discard` once `Manager` has loaded the relevant volume
metadata and live map under the existing manager lock.

`Manager` remains responsible for public method semantics: lock ownership,
volume lookup, bounds checks, metadata load and persistence, live-map
load/persistence, applying allocation deltas, and high-level deferred-write
fallback decisions. Snapshot, clone, rollback, `RecordFreedBytes`, and
`Recalculate` stay outside the first slice.

The private module has explicit methods for read, write, deferred write, and
discard rather than one generic operation enum. It uses private adapter
interfaces for the block object store, dedup index, block cache, read
amplification meter, and deferred block writer. The concrete adapters may call
existing storage backends, `dedup.DedupIndex`, `blockcache.Cache`,
`readamp.RecordVolumeBlock`, and async put support, but tests can use fakes at
the module interface.

The module owns physical block key selection, block merge rules, cache
read-through and invalidation policy, quota checks for normal writes, direct
block write selection, dedup/live-map mutation, and cache side-effect
execution. Its result shape records explicit side effects such as bytes read or
written, allocation byte delta, invalidated keys, live-map dirtiness, deferred
commit functions, and metadata flags needed by `Manager`.

For deferred writes, `Manager` keeps the high-level fallback rule: if the
volume state or manager configuration means async deferred writing is not a
valid public path, `Manager` calls the synchronous write path instead of
invoking the deferred block I/O method.

## Consequences

The volume layer gains a private test surface where block I/O policy can be
tested without constructing every public manager scenario. Manager tests remain
as behavior coverage for public volume semantics, while module tests cover the
branch matrix behind the private seam.

The first implementation should add `internal/volume/block_io.go` and
`internal/volume/block_io_test.go`, then move `ReadAt`, `WriteAt`, `Discard`,
and finally `WriteAtDeferred` policy into the module in that order.

The existing `AllocatedBlocks` metadata name is not changed by this decision.
The module result should use byte-oriented names such as `AllocationBytesDelta`
to reflect the current quota/accounting behavior without widening the first
slice into a metadata migration.
