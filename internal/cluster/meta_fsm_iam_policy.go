package cluster

import (
	"context"
	"fmt"

	"github.com/gritive/GrainFS/internal/iam"
	iambuiltin "github.com/gritive/GrainFS/internal/iam/builtin"
	"github.com/gritive/GrainFS/internal/iam/mountsastore"
	"github.com/gritive/GrainFS/internal/reservedname"
)

// applyIAM dispatches an IAM command to the configured iam.Applier. Returns
// an error if IAM was not wired (Phase 1: IAM defaults nil, set via SetIAM).
func (f *MetaFSM) applyIAM(payload []byte, fn func(*iam.Applier, []byte) error) error {
	if f.iamApplier == nil {
		return fmt.Errorf("meta_fsm: IAM applier not configured")
	}
	return fn(f.iamApplier, payload)
}

func (f *MetaFSM) applyPolicyPut(payload []byte) error {
	if f.policyStore == nil {
		return nil // safe no-op until wired
	}
	name, docJSON, isBuiltin, err := DecodePolicyPutPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: PolicyPut: %w", err)
	}
	// FSM-side guard: refuse any non-bootstrap payload that claims a built-in
	// name (isBuiltin=false means it came from the CLI/handler path, not seed).
	if !isBuiltin && iambuiltin.IsBuiltinName(name) {
		return fmt.Errorf("meta_fsm: PolicyPut: refusing to overwrite built-in policy %q", name)
	}
	if err := f.policyStore.Put(context.Background(), name, docJSON, isBuiltin); err != nil {
		return fmt.Errorf("meta_fsm: PolicyPut store: %w", err)
	}
	if f.policyResolver != nil {
		// A policy doc body change can affect any cached entry that references it;
		// invalidate the entire cache (passing both nil slices nukes all entries).
		f.policyResolver.Invalidate(nil, nil)
	}
	return nil
}

func (f *MetaFSM) applyPolicyDelete(payload []byte) error {
	if f.policyStore == nil {
		return nil // safe no-op until wired
	}
	name, err := DecodePolicyDeletePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: PolicyDelete: %w", err)
	}
	if err := f.policyStore.Delete(context.Background(), name); err != nil {
		return fmt.Errorf("meta_fsm: PolicyDelete store: %w", err)
	}
	if f.policyResolver != nil {
		f.policyResolver.Invalidate(nil, nil)
	}
	return nil
}

func (f *MetaFSM) applyGroupPut(payload []byte) error {
	if f.groupStore == nil {
		return nil // safe no-op until wired
	}
	name, policies, err := DecodeGroupPutPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: GroupPut: %w", err)
	}
	if err := f.groupStore.Put(context.Background(), name, policies); err != nil {
		return fmt.Errorf("meta_fsm: GroupPut store: %w", err)
	}
	if f.policyResolver != nil {
		// Group policy attachment changes can affect any cached entry; nuke all.
		f.policyResolver.Invalidate(nil, nil)
	}
	return nil
}

func (f *MetaFSM) applyGroupDelete(payload []byte) error {
	if f.groupStore == nil {
		return nil // safe no-op until wired
	}
	name, err := DecodeGroupDeletePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: GroupDelete: %w", err)
	}
	if err := f.groupStore.Delete(context.Background(), name); err != nil {
		return fmt.Errorf("meta_fsm: GroupDelete store: %w", err)
	}
	if f.policyResolver != nil {
		f.policyResolver.Invalidate(nil, nil)
	}
	return nil
}

func (f *MetaFSM) applyGroupMemberPut(payload []byte) error {
	if f.groupStore == nil {
		return nil // safe no-op until wired
	}
	grp, saID, err := DecodeGroupMemberPutPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: GroupMemberPut: %w", err)
	}
	if err := f.groupStore.AddMember(context.Background(), grp, saID); err != nil {
		return fmt.Errorf("meta_fsm: GroupMemberPut store: %w", err)
	}
	if f.policyResolver != nil {
		// Only the affected SA's cached entries need to be dropped.
		f.policyResolver.Invalidate([]string{saID}, nil)
	}
	return nil
}

func (f *MetaFSM) applyGroupMemberDelete(payload []byte) error {
	if f.groupStore == nil {
		return nil // safe no-op until wired
	}
	grp, saID, err := DecodeGroupMemberDeletePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: GroupMemberDelete: %w", err)
	}
	if err := f.groupStore.RemoveMember(context.Background(), grp, saID); err != nil {
		return fmt.Errorf("meta_fsm: GroupMemberDelete store: %w", err)
	}
	if f.policyResolver != nil {
		f.policyResolver.Invalidate([]string{saID}, nil)
	}
	return nil
}

func (f *MetaFSM) applyPolicyAttachToSAPut(payload []byte) error {
	if f.policyAttachStore == nil {
		return nil // safe no-op until wired
	}
	saID, pol, err := DecodePolicyAttachToSAPutPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: PolicyAttachToSAPut: %w", err)
	}
	if err := f.policyAttachStore.AttachToSA(context.Background(), saID, pol); err != nil {
		return fmt.Errorf("meta_fsm: PolicyAttachToSAPut store: %w", err)
	}
	if f.policyResolver != nil {
		// Only the affected SA's cached entries need to be dropped.
		f.policyResolver.Invalidate([]string{saID}, nil)
	}
	return nil
}

func (f *MetaFSM) applyPolicyAttachToSADelete(payload []byte) error {
	if f.policyAttachStore == nil {
		return nil // safe no-op until wired
	}
	saID, pol, err := DecodePolicyAttachToSADeletePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: PolicyAttachToSADelete: %w", err)
	}
	if err := f.policyAttachStore.DetachFromSA(context.Background(), saID, pol); err != nil {
		return fmt.Errorf("meta_fsm: PolicyAttachToSADelete store: %w", err)
	}
	if f.policyResolver != nil {
		// Only the affected SA's cached entries need to be dropped.
		f.policyResolver.Invalidate([]string{saID}, nil)
	}
	return nil
}

func (f *MetaFSM) applyPolicyAttachToGroupPut(payload []byte) error {
	if f.policyAttachStore == nil {
		return nil // safe no-op until wired
	}
	grp, pol, err := DecodePolicyAttachToGroupPutPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: PolicyAttachToGroupPut: %w", err)
	}
	if err := f.policyAttachStore.AttachToGroup(context.Background(), grp, pol); err != nil {
		return fmt.Errorf("meta_fsm: PolicyAttachToGroupPut store: %w", err)
	}
	if f.policyResolver != nil {
		// TODO(opt): nuke only SAs that are members of grp once we can enumerate
		// them cheaply from this apply path. For now a nuclear invalidate is safe
		// and cache rebuild is cheap.
		f.policyResolver.Invalidate(nil, nil)
	}
	return nil
}

func (f *MetaFSM) applyPolicyAttachToGroupDelete(payload []byte) error {
	if f.policyAttachStore == nil {
		return nil // safe no-op until wired
	}
	grp, pol, err := DecodePolicyAttachToGroupDeletePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: PolicyAttachToGroupDelete: %w", err)
	}
	if err := f.policyAttachStore.DetachFromGroup(context.Background(), grp, pol); err != nil {
		return fmt.Errorf("meta_fsm: PolicyAttachToGroupDelete store: %w", err)
	}
	if f.policyResolver != nil {
		// TODO(opt): nuke only SAs that are members of grp once we can enumerate
		// them cheaply from this apply path. For now a nuclear invalidate is safe
		// and cache rebuild is cheap.
		f.policyResolver.Invalidate(nil, nil)
	}
	return nil
}

func (f *MetaFSM) applyBucketPolicyPut(payload []byte) error {
	if f.bucketPolicyStore == nil {
		return nil // safe no-op until wired
	}
	bucket, docJSON, err := DecodeBucketPolicyPutPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: BucketPolicyPut: %w", err)
	}
	if reservedname.IsInternalBucket(bucket) {
		return fmt.Errorf("meta_fsm: BucketPolicyPut: bucket %q is internal and cannot receive policy mutations via public API", bucket)
	}
	if err := f.bucketPolicyStore.Put(context.Background(), bucket, docJSON); err != nil {
		return fmt.Errorf("meta_fsm: BucketPolicyPut store: %w", err)
	}
	if f.policyResolver != nil {
		// Only cache entries for this bucket are stale.
		f.policyResolver.Invalidate(nil, []string{bucket})
	}
	return nil
}

func (f *MetaFSM) applyBucketPolicyDelete(payload []byte) error {
	if f.bucketPolicyStore == nil {
		return nil // safe no-op until wired
	}
	bucket, err := DecodeBucketPolicyDeletePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: BucketPolicyDelete: %w", err)
	}
	if reservedname.IsInternalBucket(bucket) {
		return fmt.Errorf("meta_fsm: BucketPolicyDelete: bucket %q is internal and cannot receive policy mutations via public API", bucket)
	}
	if err := f.bucketPolicyStore.Delete(context.Background(), bucket); err != nil {
		return fmt.Errorf("meta_fsm: BucketPolicyDelete store: %w", err)
	}
	if f.policyResolver != nil {
		// Only cache entries for this bucket are stale.
		f.policyResolver.Invalidate(nil, []string{bucket})
	}
	return nil
}

// applyMountSACreate handles MetaCmdTypeMountSACreate - creates a NFS/9P mount SA.
func (f *MetaFSM) applyMountSACreate(payload []byte) error {
	if f.mountSAStore == nil {
		return fmt.Errorf("meta_fsm: MountSACreate: store not wired")
	}
	sa, err := mountsastore.DecodeCreatePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: MountSACreate: %w", err)
	}
	if err := f.mountSAStore.ApplyCreate(sa); err != nil {
		return fmt.Errorf("meta_fsm: MountSACreate store: %w", err)
	}
	if f.policyResolver != nil {
		// T4 will narrow this to mount-SA namespace; broad invalidation is safe.
		f.policyResolver.Invalidate(nil, nil)
	}
	return nil
}

// applyMountSADelete handles MetaCmdTypeMountSADelete - deletes a NFS/9P mount SA.
func (f *MetaFSM) applyMountSADelete(payload []byte) error {
	if f.mountSAStore == nil {
		return fmt.Errorf("meta_fsm: MountSADelete: store not wired")
	}
	name, err := mountsastore.DecodeDeletePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: MountSADelete: %w", err)
	}
	if err := f.mountSAStore.ApplyDelete(name); err != nil {
		return fmt.Errorf("meta_fsm: MountSADelete store: %w", err)
	}
	if f.policyResolver != nil {
		f.policyResolver.Invalidate(nil, nil)
	}
	return nil
}

// applyMountSAAttachPolicy handles MetaCmdTypeMountSAAttachPolicy.
func (f *MetaFSM) applyMountSAAttachPolicy(payload []byte) error {
	if f.policyAttachStore == nil {
		return fmt.Errorf("meta_fsm: MountSAAttachPolicy: policyAttachStore not wired")
	}
	mountSA, pol, err := mountsastore.DecodeAttachPolicyPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: MountSAAttachPolicy: %w", err)
	}
	if err := f.policyAttachStore.AttachToMountSA(context.Background(), mountSA, pol); err != nil {
		return fmt.Errorf("meta_fsm: MountSAAttachPolicy store: %w", err)
	}
	if f.policyResolver != nil {
		// T4 will narrow cache invalidation to mount-SA namespace.
		f.policyResolver.Invalidate(nil, nil)
	}
	return nil
}

// applyMountSADetachPolicy handles MetaCmdTypeMountSADetachPolicy.
func (f *MetaFSM) applyMountSADetachPolicy(payload []byte) error {
	if f.policyAttachStore == nil {
		return fmt.Errorf("meta_fsm: MountSADetachPolicy: policyAttachStore not wired")
	}
	mountSA, pol, err := mountsastore.DecodeDetachPolicyPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: MountSADetachPolicy: %w", err)
	}
	if err := f.policyAttachStore.DetachFromMountSA(context.Background(), mountSA, pol); err != nil {
		return fmt.Errorf("meta_fsm: MountSADetachPolicy store: %w", err)
	}
	if f.policyResolver != nil {
		f.policyResolver.Invalidate(nil, nil)
	}
	return nil
}

// applyCreateBucketWithPolicyAttach handles MetaCmd 62 - the IAM half of the
// sequenced bucket-create + policy-attach operation (D#13, F#2).
//
// Approach: sequenced (not cross-FSM atomic). The bucket itself is created by
// the data-plane FSM via the existing CreateBucket path; this MetaCmd only
// handles the IAM side: validate SA + policy existence, then attach the policy
// to the SA. The admin handler is responsible for rolling back via DeleteBucket
// if this propose fails.
//
// If both attach_sa and attach_policy are empty, this is a no-op (create-only
// caller path; the bucket was already created by the prior CreateBucket propose).
func (f *MetaFSM) applyCreateBucketWithPolicyAttach(payload []byte) error {
	bucket, sa, pol, err := decodeMetaCreateBucketWithPolicyAttachCmd(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: CreateBucketWithPolicyAttach decode: %w", err)
	}
	if sa == "" {
		// create-only path: no IAM half to apply.
		return nil
	}
	// F#2: validate SA existence before any mutation.
	if f.iamApplier == nil {
		return fmt.Errorf("meta_fsm: CreateBucketWithPolicyAttach: iam applier not configured")
	}
	if !f.iamApplier.SAExists(sa) {
		return fmt.Errorf("meta_fsm: CreateBucketWithPolicyAttach: SA %q does not exist (F#2)", sa)
	}
	// F#2: validate policy existence before any mutation.
	if f.policyStore == nil {
		return fmt.Errorf("meta_fsm: CreateBucketWithPolicyAttach: policy store not configured")
	}
	if _, perr := f.policyStore.GetRaw(context.Background(), pol); perr != nil {
		return fmt.Errorf("meta_fsm: CreateBucketWithPolicyAttach: policy %q does not exist: %w", pol, perr)
	}
	// Attach policy to SA.
	if f.policyAttachStore == nil {
		return fmt.Errorf("meta_fsm: CreateBucketWithPolicyAttach: policy attach store not configured")
	}
	if attachErr := f.policyAttachStore.AttachToSA(context.Background(), sa, pol); attachErr != nil {
		return fmt.Errorf("meta_fsm: CreateBucketWithPolicyAttach: attach: %w", attachErr)
	}
	if f.policyResolver != nil {
		f.policyResolver.Invalidate([]string{sa}, []string{bucket})
	}
	return nil
}
