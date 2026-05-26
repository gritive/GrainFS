package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/migration"
)

func (f *MetaFSM) applyMigrationJobStart(payload []byte) error {
	if f.migrationStore == nil {
		return fmt.Errorf("meta_fsm: migration store not wired")
	}
	bucket, startedAt, err := migration.DecodeJobStartPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: MigrationJobStart: %w", err)
	}
	ts := time.Unix(0, startedAt)
	return f.migrationStore.SaveJob(&migration.JobState{
		Bucket:    bucket,
		Status:    migration.StatusRunning,
		StartedAt: ts,
		UpdatedAt: ts,
	})
}

func (f *MetaFSM) applyMigrationJobDone(payload []byte) error {
	if f.migrationStore == nil {
		return fmt.Errorf("meta_fsm: migration store not wired")
	}
	bucket, copied, errors, updatedAt, err := migration.DecodeJobDonePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: MigrationJobDone: %w", err)
	}
	job, err := f.migrationStore.GetJob(bucket)
	if err != nil {
		return fmt.Errorf("meta_fsm: MigrationJobDone: get job: %w", err)
	}
	if job == nil {
		job = &migration.JobState{Bucket: bucket}
	}
	job.Status = migration.StatusComplete
	job.Copied = copied
	job.Errors = errors
	job.UpdatedAt = time.Unix(0, updatedAt)
	return f.migrationStore.SaveJob(job)
}

func (f *MetaFSM) applyMigrationJobFailed(payload []byte) error {
	if f.migrationStore == nil {
		return fmt.Errorf("meta_fsm: migration store not wired")
	}
	bucket, reason, errors, updatedAt, err := migration.DecodeJobFailedPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: MigrationJobFailed: %w", err)
	}
	job, err := f.migrationStore.GetJob(bucket)
	if err != nil {
		return fmt.Errorf("meta_fsm: MigrationJobFailed: get job: %w", err)
	}
	if job == nil {
		job = &migration.JobState{Bucket: bucket}
	}
	job.Status = migration.StatusFailed
	job.Reason = reason
	job.Errors = errors
	job.UpdatedAt = time.Unix(0, updatedAt)
	return f.migrationStore.SaveJob(job)
}

func (f *MetaFSM) ActiveFeatures() compat.ActiveFeatures {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.activeFeatures
}

func (f *MetaFSM) CapabilityEvidence(nodeID string, now time.Time) compat.Evidence {
	f.mu.RLock()
	defer f.mu.RUnlock()
	caps := map[string]bool{}
	if f.iamApplier != nil && f.migrationStore != nil {
		caps[compat.CapabilityMigrationCutoverV1] = true
	}
	if f.exportStore != nil {
		caps[compat.CapabilityNfsExportCreateV1] = true
	}
	caps[compat.CapabilityMultipartListingV1] = true
	// Advertise the KEK envelope lifecycle capability once the cluster KEK
	// store is wired (same readiness signal MetaKEKRotateCmd Apply checks).
	// Without this no node advertises kek_envelope_v1, so the capability gate
	// rejects every rotate/retire/prune with "missing=[<peer>]". (Caught by
	// the Task 15 e2e lifecycle suite; plan line 111's claimed auto-advertise
	// was never wired into the evidence source.)
	if f.keystore != nil {
		caps[compat.CapabilityKEKEnvelopeV1] = true
		caps[compat.CapabilityDEKReplicatedV1] = true
	}
	return compat.Evidence{
		NodeID:       compat.NodeID(nodeID),
		Capabilities: caps,
		LastSeen:     now,
		Ready:        true,
	}
}

func (f *MetaFSM) applyCapabilityActivate(payload []byte) error {
	cmd := clusterpb.GetRootAsMetaCapabilityActivateCmd(payload, 0)
	capability := string(cmd.Capability())
	if capability == "" {
		return fmt.Errorf("meta_fsm: CapabilityActivate missing capability")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.activeFeatures = f.activeFeatures.With(capability)
	return nil
}

func (f *MetaFSM) applyMigrationCutover(payload []byte) error {
	if f.migrationStore == nil {
		return fmt.Errorf("meta_fsm: migration store not wired")
	}
	if f.iamApplier == nil {
		return fmt.Errorf("meta_fsm: IAM applier not configured")
	}
	cmd := clusterpb.GetRootAsMetaMigrationCutoverCmd(payload, 0)
	bucket := string(cmd.Bucket())
	if bucket == "" {
		return fmt.Errorf("meta_fsm: MigrationCutover missing bucket")
	}
	if err := f.iamApplier.ApplyBucketUpstreamStatusSet(bucket, iam.BucketUpstreamStatusCutover); err != nil {
		return fmt.Errorf("meta_fsm: MigrationCutover upstream status: %w", err)
	}
	f.mu.Lock()
	f.activeFeatures = f.activeFeatures.With(compat.CapabilityMigrationCutoverV1)
	f.mu.Unlock()
	return f.migrationStore.SaveJob(&migration.JobState{
		Bucket:    bucket,
		Status:    migration.StatusComplete,
		UpdatedAt: time.Unix(0, cmd.UpdatedAtUnixNs()),
	})
}

func (f *MetaFSM) applyConfigPut(payload []byte) error {
	if f.cfgStore == nil {
		return nil // safe no-op until wired
	}
	key, value, err := decodeMetaConfigPutCmd(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: ConfigPut: %w", err)
	}
	return f.cfgStore.Set(context.Background(), key, value)
}

func (f *MetaFSM) applyConfigDelete(payload []byte) error {
	if f.cfgStore == nil {
		return nil // safe no-op until wired
	}
	key, err := decodeMetaConfigDeleteCmd(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: ConfigDelete: %w", err)
	}
	return f.cfgStore.Unset(context.Background(), key)
}
