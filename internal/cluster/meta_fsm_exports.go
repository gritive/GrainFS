package cluster

import (
	"fmt"
	"sort"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/nfsexport"
)

// SetLifecycle wires the lifecycle store into the MetaFSM. Must be called
// before raft Start so apply does not race with replay.
func (f *MetaFSM) SetLifecycle(store *lifecycle.Store) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lifecycleStore = store
}

// SetExportStore wires the NFS export registry store into the MetaFSM. Must be
// called before raft Start so apply does not race with replay.
func (f *MetaFSM) SetExportStore(store *nfsexport.Store) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exportStore = store
	if f.exportFsidMajor == 0 {
		f.exportFsidMajor = 1
	}
}

// SetExportFsidMajor sets the cluster-wide fsid namespace used when the
// MetaFSM assigns fsid minors during NFS export upsert apply.
func (f *MetaFSM) SetExportFsidMajor(v uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exportFsidMajor = v
}

func (f *MetaFSM) applyBucketLifecyclePut(payload []byte) error {
	if f.lifecycleStore == nil {
		return fmt.Errorf("meta_fsm: lifecycle store not wired")
	}
	bucket, raw, err := lifecycle.DecodePutPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: BucketLifecyclePut: %w", err)
	}
	return f.lifecycleStore.PutRaw(bucket, raw)
}

func (f *MetaFSM) applyBucketLifecycleDelete(payload []byte) error {
	if f.lifecycleStore == nil {
		return fmt.Errorf("meta_fsm: lifecycle store not wired")
	}
	bucket, err := lifecycle.DecodeDeletePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: BucketLifecycleDelete: %w", err)
	}
	return f.lifecycleStore.Delete(bucket)
}

func (f *MetaFSM) applyNfsExportUpsert(payload []byte) error {
	if f.exportStore == nil {
		return fmt.Errorf("meta_fsm: NFS export store not wired")
	}
	bucket, cfg, err := nfsexport.DecodeUpsertPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: NfsExportUpsert: %w", err)
	}
	if cfg.FsidMinor != 0 || cfg.Generation != 0 {
		if cfg.FsidMajor == 0 {
			cfg.FsidMajor = f.exportFsidMajor
		}
		if err := f.exportStore.Put(bucket, cfg); err != nil {
			return err
		}
	} else {
		if _, err := f.exportStore.ApplyUpsert(bucket, cfg.ReadOnly, f.exportFsidMajor); err != nil {
			return err
		}
	}
	f.publishNfsExportChange()
	return nil
}

func (f *MetaFSM) applyNfsExportCreate(payload []byte) error {
	if f.exportStore == nil {
		return fmt.Errorf("meta_fsm: NFS export store not wired")
	}
	bucket, cfg, err := nfsexport.DecodeUpsertPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: NfsExportCreate: %w", err)
	}
	if _, err := f.exportStore.ApplyCreate(bucket, cfg.ReadOnly, f.exportFsidMajor); err != nil {
		return err
	}
	f.publishNfsExportChange()
	return nil
}

func (f *MetaFSM) applyNfsExportDelete(payload []byte) error {
	if f.exportStore == nil {
		return fmt.Errorf("meta_fsm: NFS export store not wired")
	}
	bucket, err := nfsexport.DecodeDeletePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: NfsExportDelete: %w", err)
	}
	if err := f.exportStore.Delete(bucket); err != nil {
		return err
	}
	f.publishNfsExportChange()
	return nil
}

func (f *MetaFSM) applyNfsExportBucketDeleteCascade(payload []byte) error {
	if f.exportStore == nil {
		return fmt.Errorf("meta_fsm: NFS export store not wired")
	}
	bucket, _, err := nfsexport.DecodeBucketDeleteCascadePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: NfsExportBucketDeleteCascade: %w", err)
	}
	if err := f.exportStore.Delete(bucket); err != nil {
		return err
	}
	f.publishNfsExportChange()
	return nil
}

// SetOnNfsExportChange registers a callback fired after each NFS export
// registry upsert/delete is applied. The callback must not block.
func (f *MetaFSM) SetOnNfsExportChange(fn func()) {
	f.mu.Lock()
	f.onNfsExportChange = fn
	f.mu.Unlock()
}

func (f *MetaFSM) publishNfsExportChange() {
	f.mu.RLock()
	cb := f.onNfsExportChange
	f.mu.RUnlock()
	if cb != nil {
		cb()
	}
}

func buildNfsExportEntriesVector(b *flatbuffers.Builder, entries map[string]nfsexport.Config) flatbuffers.UOffsetT {
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	offsets := make([]flatbuffers.UOffsetT, len(names))
	for i := len(names) - 1; i >= 0; i-- {
		bucket := names[i]
		cfg := entries[bucket]
		bucketOff := b.CreateString(bucket)
		clusterpb.NfsExportConfigStart(b)
		clusterpb.NfsExportConfigAddReadOnly(b, cfg.ReadOnly)
		clusterpb.NfsExportConfigAddFsidMajor(b, cfg.FsidMajor)
		clusterpb.NfsExportConfigAddFsidMinor(b, cfg.FsidMinor)
		clusterpb.NfsExportConfigAddGeneration(b, cfg.Generation)
		cfgOff := clusterpb.NfsExportConfigEnd(b)
		clusterpb.NfsExportUpsertCmdStart(b)
		clusterpb.NfsExportUpsertCmdAddBucket(b, bucketOff)
		clusterpb.NfsExportUpsertCmdAddConfig(b, cfgOff)
		offsets[i] = clusterpb.NfsExportUpsertCmdEnd(b)
	}
	clusterpb.MetaStateSnapshotStartNfsExportsVector(b, len(offsets))
	for i := len(offsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(len(offsets))
}
