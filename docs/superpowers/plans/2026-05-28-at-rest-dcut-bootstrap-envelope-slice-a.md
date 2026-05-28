# At-Rest D-Cut Bootstrap Envelope Slice A Plan

> **For agentic workers:** This plan records the completed Slice A implementation
> from `docs/superpowers/specs/2026-05-28-at-rest-dcut-bootstrap-envelope-design.md`.

**Goal:** Stop new zero-CA invite bootstrap payloads from carrying the static
`encryption.key`, while keeping legacy payload decode support until the
format-7 cut.

**Scope:** Wire-format producer, bootstrap secret provider split, and the
post-drop pre-`Listen` node identity load needed to keep the slice runnable
without sending a static key. This does not change Phase-2 node identity
migration, KEK prune evidence, or final static-key removal.

## Tasks

- [x] **Task 1: Provider split**
  - Removed `RawEncryptionKey` from `bootstrapSecretProvider`.
  - Changed `BootstrapSecretProvider.BootstrapSecrets` to return only
    `(kekGens, transportPSK)`.
  - Updated provider tests to assert static key material is no longer returned.

- [x] **Task 2: New payloads omit static key**
  - Changed `MetaJoinReceiver` to encode Phase-1 bootstrap payloads without
    `encryption_key`.
  - Changed `EncodeBootstrapSecretsPayloadWithCutover` so new payloads cannot
    populate the legacy `encryption_key` field.
  - Kept `DecodeBootstrapSecretsPayloadWithCutover` legacy-readable.

- [x] **Task 3: Tests**
  - Updated bootstrap codec tests so new cutover payloads decode an empty static
    key and old payloads still decode the legacy key.
  - Updated meta join invite tests to assert Phase-1 sealed bootstrap payloads
    no longer carry `encryption.key`.
  - Added post-drop coverage proving the joiner can load a KEK-sealed
    `node.key.enc` before QUIC `Listen` without a static key.

## Verification

- [x] `go test ./internal/cluster ./internal/serveruntime -run 'BootstrapSecrets|HandleJoin_Phase1|BootstrapSecretsPayload' -count=1`
- [x] `go test ./internal/cluster -run 'Invite|BootstrapSecretsPayload|MetaJoin' -count=1`
- [x] `go test ./internal/serveruntime -run 'Invite|BootstrapSecret|StageInvite|NodeKey|PostDrop|Boot' -count=1`
- [x] `go test ./internal/cluster ./internal/serveruntime -count=1`

## Follow-Ups

- Slice B must finish moving Phase-2 `node.key.enc` migration off the static
  encryption key while preserving crash/restart safety.
- Slice C must publish `node_key_kek_gen` evidence through member registration
  and block KEK prune when a voter depends on the retiring generation.

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| Engineering self-review | feature-pipeline | Confirm Slice A stopped sending static key without breaking pre-`Listen` post-drop identity load | 1 | clean | Fixed the post-drop gap by loading the KEK-sealed node key from staged disk KEK before `wireDEKKeeper`; deferred Phase-2 migration and prune evidence to Slices B/C. |
