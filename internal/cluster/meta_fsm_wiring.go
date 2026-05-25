package cluster

import (
	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/iam/bucketpolicy"
	"github.com/gritive/GrainFS/internal/iam/group"
	"github.com/gritive/GrainFS/internal/iam/mountsastore"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iam/policyattach"
	"github.com/gritive/GrainFS/internal/iam/policystore"
	"github.com/gritive/GrainFS/internal/migration"
)

// ClusterConfig returns the cluster-wide policy snapshot. Read-only; consumers
// call its getters at use-time. Safe for concurrent reads.
func (f *MetaFSM) ClusterConfig() *ClusterConfig { return f.clusterCfg }

// SetIAM wires the IAM Applier into the MetaFSM. Must be called before the
// raft log starts replaying; set alongside the Encryptor used to decrypt
// secret_key_enc payloads. iamApplier nil = IAM commands return "not configured".
func (f *MetaFSM) SetIAM(store *iam.Store, applier *iam.Applier) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.iamStore = store
	f.iamApplier = applier
}

// IAMStore returns the IAM Store for read access (auth checks, bootstrap shim).
func (f *MetaFSM) IAMStore() *iam.Store { return f.iamStore }

// SetConfigStore wires the cluster-wide config registry into the MetaFSM.
// Must be called before the raft log starts replaying. nil means
// ConfigPut/ConfigDelete commands are safe no-ops (not configured yet).
func (f *MetaFSM) SetConfigStore(s *config.Store) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cfgStore = s
}

// SetPolicyStore wires the IAM policy store into the MetaFSM. Must be called
// before the raft log starts replaying. nil means PolicyPut/PolicyDelete
// commands are safe no-ops (not configured yet).
func (f *MetaFSM) SetPolicyStore(s *policystore.InMemoryStore) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.policyStore = s
}

// SetPolicyResolver wires the effective-policy resolver into the MetaFSM.
// When non-nil, its cache is invalidated on every PolicyPut/PolicyDelete apply.
// nil is safe (no-op invalidation).
func (f *MetaFSM) SetPolicyResolver(r *policy.Resolver) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.policyResolver = r
}

// SetGroupStore wires the IAM group store into the MetaFSM. Must be called
// before the raft log starts replaying. nil means Group* commands are safe
// no-ops (not configured yet).
func (f *MetaFSM) SetGroupStore(s *group.InMemoryStore) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.groupStore = s
}

// SetPolicyAttachStore wires the SA/group→policy attachment store into the
// MetaFSM. Must be called before the raft log starts replaying. nil means
// PolicyAttach* commands are safe no-ops.
func (f *MetaFSM) SetPolicyAttachStore(s *policyattach.InMemoryStore) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.policyAttachStore = s
}

// SetBucketPolicyStore wires the per-bucket policy document store into the
// MetaFSM. Must be called before the raft log starts replaying. nil means
// BucketPolicy* commands are safe no-ops.
func (f *MetaFSM) SetBucketPolicyStore(s *bucketpolicy.InMemoryStore) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bucketPolicyStore = s
}

// SetMountSAStore wires the NFS/9P mount service account store into the
// MetaFSM. Must be called before the raft log starts replaying. nil means
// MountSA* commands return an error.
func (f *MetaFSM) SetMountSAStore(s *mountsastore.Store) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mountSAStore = s
}

// SetMigration wires the migration job store into the MetaFSM. Must be called
// before raft Start so apply does not race with replay.
func (f *MetaFSM) SetMigration(store *migration.JobStore) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.migrationStore = store
}

func (f *MetaFSM) SetOnIcebergApplyResult(fn func(requestID string, err error)) {
	f.mu.Lock()
	f.onIcebergResult = fn
	f.mu.Unlock()
}

func (f *MetaFSM) publishIcebergResult(requestID string, err error) {
	f.mu.RLock()
	cb := f.onIcebergResult
	f.mu.RUnlock()
	if cb != nil && requestID != "" {
		cb(requestID, err)
	}
}
