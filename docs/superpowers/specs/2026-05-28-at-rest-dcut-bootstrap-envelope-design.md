# At-Rest D-Cut Bootstrap Envelope Design

**Date:** 2026-05-28
**Status:** Draft for D-cut implementation planning

## Problem

The static `encryption.key` is no longer just a data-at-rest key. The remaining
D-cut blockers use it as boot glue:

- `Config.RawEncryptionKey` carries raw static-key bytes so the invite bootstrap
  provider can seal them to a joiner.
- `BootstrapSecretsPayload.encryption_key` sends those bytes over the zero-CA
  sealed bootstrap envelope.
- `stageInviteSecrets` writes `<dataDir>/encryption.key` on invite join.
- `ensureNodeIdentity` and `loadAndMigrateInviteNodeKey` re-seal
  `keys.d/node.key.enc` under that static key so a later KEK prune cannot remove
  the key needed to restart a node.

Removing the static key without replacing those roles would either make invite
joiners unable to boot or make restarted nodes depend on a pruned KEK
generation.

## Goal

Remove static `encryption.key` from production protection paths while preserving:

- zero-CA invite join for fresh nodes,
- restartable per-node transport identity,
- KEK prune safety,
- the existing greenfield at-rest format boundary,
- no plaintext private key after Phase-1 crash recovery completes.

## Non-Goals

- Do not introduce another cluster-wide static data-at-rest key.
- Do not deliver DEK plaintext in the bootstrap envelope.
- Do not re-enable data-DEK rotation.
- Do not solve raft log store encryption; that remains a separate design.

## Current Flow

Leader-side bootstrap provider:

1. Reads `state.cfg.RawEncryptionKey`.
2. Reads every KEK generation from `state.kekStore`.
3. Reads the transport PSK.
4. Encodes all three into `BootstrapSecretsPayload`.
5. Seals the FlatBuffer to the joiner's node identity.

Joiner-side Phase 1:

1. Opens the sealed bootstrap.
2. Decodes `encryption_key`, KEKs, transport PSK, peer SPKIs, and dropped bit.
3. Writes `encryption.key`, `keys/<gen>.key`, `cluster.id`, and
   `keys.d/current.key`.
4. Seals `node.key.enc` under the highest KEK pre-drop, or under
   `encryption.key` post-drop.

Joiner-side Phase 2:

1. Loads `node.key.enc`.
2. Migrates it to static `encryption.key`.
3. Sends Phase-2 ACK.

Normal member boot:

1. Loads static `encryption.key`.
2. Wires KEK store and DEK keeper.
3. Ensures `node.key.enc` exists and is sealed under static `encryption.key`.

## Target Flow

### Bootstrap Envelope

`BootstrapSecretsPayload` becomes a KEK/bootstrap envelope, not a static-key
carrier:

- keep `kek_generations`,
- keep `transport_psk` until the cluster-key drop is complete,
- keep `peer_spkis`,
- keep `cluster_key_dropped`,
- stop setting `encryption_key` in new encoders while keeping the FlatBuffers
  field readable for legacy payloads until the format-7 cut,
- add no DEK plaintext field.

The joiner obtains DEK material through normal meta-raft replay after Phase-2
membership. That keeps the DEK under the existing replicated FSM discipline and
avoids creating a second DEK delivery path.

### Node Identity Sealing

`keys.d/node.key.enc` is sealed under a KEK generation again, but with an
explicit prune-safety contract:

- Persist `keys.d/node.key.gen` beside `node.key.enc`.
- Load `node.key.enc` using the recorded KEK generation from the local KEK store.
- On successful boot, re-seal `node.key.enc` under the active local KEK
  generation when the recorded generation is not active.
- Publish per-node evidence `(node_id, spki, node_key_kek_gen)` through the
  existing peer registry / presence path.
- Reject KEK prune while any current voter has not presented node identity
  evidence at a generation that will survive the prune.

This removes the static-key dependency while closing the historical bug: a KEK
generation cannot be pruned while it is still the only key that can open a
voter's `node.key.enc`.

### Boot Ordering

The node identity key can be loaded after the local KEK store is opened and
before the node needs to present a per-node cert. It must not depend on the
DEK keeper, because DEK readiness depends on meta-raft replay and may happen
after transport starts.

The ordering remains:

1. Load cluster ID and KEK store.
2. Load or generate `node.key.enc` using KEK-generation sidecar.
3. Wire transport identity / accept-set. This includes the post-drop invite
   join path that currently calls `transport.LoadNodeKey` before QUIC `Listen`;
   that path must use the same KEK-generation sidecar and must not fall back to
   `RawEncryptionKey`.
4. Start meta-raft.
5. Rebuild DEK keeper through normal raft snapshot/replay.

## Compatibility Boundary

This is greenfield-only, matching the existing D-track approach. The final code
slice that stops writing/reading static `encryption.key` must bump
`encryption.format` from `6` to `7` and loud-fail older directories.

Read compatibility for existing tracked tests can be temporary during the
transition, but production boot in format `7` must not require
`Config.RawEncryptionKey`.

## Implementation Slices

### Slice A: Wire-Format and Provider Split

- Change `BootstrapSecretProvider.BootstrapSecrets` to return only
  `(kekGens, transportPSK)`.
- Stop encoding `BootstrapSecretsPayload.encryption_key` in new payloads.
- Keep decode compatibility for legacy payloads until the format-7 cut.
- Update `meta_join_invite` and bootstrap codec tests to assert new payloads
  have no encryption key while old payloads still decode.

### Slice B: KEK-Gen Node Identity Seal

- Change `ensureNodeIdentity` to use the active KEK generation and persist
  `node.key.gen`.
- Change `loadAndMigrateInviteNodeKey` to load using the recorded KEK generation
  and re-seal to active KEK, not static `encryption.key`.
- Change post-drop invite transport present-flip loading to use the recorded
  KEK generation before `Listen`.
- Add crash-window tests for missing sidecar, stale sidecar, and pruned gen.

### Slice C: Prune Gate Evidence

- Extend per-node presence evidence with `node_key_kek_gen`.
- Thread that evidence through the existing member self-register / peer registry
  path, not a new side channel.
- Reject KEK prune if any current voter lacks fresh evidence that its
  `node.key.enc` is sealed under a non-pruned generation.
- Add a multi-node unit/integration test for prune refusal and re-seal success.

### Slice D: Static-Key Removal

- Remove `RawEncryptionKey` from `serveruntime.Config`.
- Remove `LoadOrCreateEncryptionKeyWithRaw`.
- Stop writing `<dataDir>/encryption.key` in invite join.
- Remove `--encryption-key-file` after every remaining `cfg.Encryptor` consumer
  is gone.
- Bump `encryption.format` to `7`.

Implementation note: Slice D shipped the Zero-CA boot-glue cut. Static-key
data-path consumers remain tracked under R3, but invite bootstrap and node
identity no longer depend on raw static key material.

## Acceptance Criteria

- A fresh invite joiner stages no `encryption.key`.
- `node.key.enc` survives restart after KEK rotation and allowed prune.
- KEK prune is refused while a voter still depends on the prune target.
- `Config.RawEncryptionKey` has no production caller.
- A format-7 cluster boots with KEK files but without `encryption.key`.
- Existing D-track tests remain green, with legacy compatibility tests scoped to
  pre-format-7 behavior only.

## Follow-Ups

- Data-DEK rotation remains blocked until all ciphertext-bearing formats persist
  non-zero `dek_gen`.
- Raft log store at-rest encryption remains separate from static-key retirement.
