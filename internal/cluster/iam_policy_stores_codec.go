package cluster

import (
	"fmt"
	"sort"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/iam/bucketpolicy"
	"github.com/gritive/GrainFS/internal/iam/group"
	"github.com/gritive/GrainFS/internal/iam/mountsastore"
	"github.com/gritive/GrainFS/internal/iam/policyattach"
	"github.com/gritive/GrainFS/internal/iam/policystore"
)

// encodeMetaIAMPolicyStoresSnapshot serializes all §2 + §A IAM policy stores into
// a MetaIAMPolicyStoresSnapshot FlatBuffers buffer used as the IPST trailer payload.
//
// Sorted-key serialization throughout (names/ids sorted alphabetically before encoding;
// attached_policies and members sorted within each entry) preserves byte determinism.
func encodeMetaIAMPolicyStoresSnapshot(
	policies []policystore.PolicyEntry,
	groups []group.GroupEntry,
	attach policyattach.AttachSnapshot,
	bucketPols []bucketpolicy.BucketPolicyEntry,
	mountSAs []mountsastore.MountSA,
) ([]byte, error) {
	b := clusterBuilderPool.Get()

	// ── Policies ──────────────────────────────────────────────────────────────
	sort.Slice(policies, func(i, j int) bool { return policies[i].Name < policies[j].Name })
	polOffs := make([]flatbuffers.UOffsetT, len(policies))
	for i := len(policies) - 1; i >= 0; i-- {
		e := policies[i]
		nameOff := b.CreateString(e.Name)
		docOff := b.CreateByteVector(e.Doc)
		clusterpb.MetaIAMPolicyEntryStart(b)
		clusterpb.MetaIAMPolicyEntryAddName(b, nameOff)
		clusterpb.MetaIAMPolicyEntryAddDocJson(b, docOff)
		clusterpb.MetaIAMPolicyEntryAddBuiltin(b, e.Builtin)
		polOffs[i] = clusterpb.MetaIAMPolicyEntryEnd(b)
	}
	clusterpb.MetaIAMPolicyStoresSnapshotStartPoliciesVector(b, len(polOffs))
	for i := len(polOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(polOffs[i])
	}
	polVec := b.EndVector(len(polOffs))

	// ── Groups ────────────────────────────────────────────────────────────────
	sort.Slice(groups, func(i, j int) bool { return groups[i].Name < groups[j].Name })
	grpOffs := make([]flatbuffers.UOffsetT, len(groups))
	for i := len(groups) - 1; i >= 0; i-- {
		g := groups[i]
		nameOff := b.CreateString(g.Name)

		aps := append([]string(nil), g.AttachedPolicies...)
		sort.Strings(aps)
		apOffs := make([]flatbuffers.UOffsetT, len(aps))
		for j, p := range aps {
			apOffs[j] = b.CreateString(p)
		}
		clusterpb.MetaIAMGroupEntryStartAttachedPoliciesVector(b, len(apOffs))
		for j := len(apOffs) - 1; j >= 0; j-- {
			b.PrependUOffsetT(apOffs[j])
		}
		apVec := b.EndVector(len(apOffs))

		ms := append([]string(nil), g.Members...)
		sort.Strings(ms)
		mOffs := make([]flatbuffers.UOffsetT, len(ms))
		for j, m := range ms {
			mOffs[j] = b.CreateString(m)
		}
		clusterpb.MetaIAMGroupEntryStartMembersVector(b, len(mOffs))
		for j := len(mOffs) - 1; j >= 0; j-- {
			b.PrependUOffsetT(mOffs[j])
		}
		mVec := b.EndVector(len(mOffs))

		clusterpb.MetaIAMGroupEntryStart(b)
		clusterpb.MetaIAMGroupEntryAddName(b, nameOff)
		clusterpb.MetaIAMGroupEntryAddAttachedPolicies(b, apVec)
		clusterpb.MetaIAMGroupEntryAddMembers(b, mVec)
		grpOffs[i] = clusterpb.MetaIAMGroupEntryEnd(b)
	}
	clusterpb.MetaIAMPolicyStoresSnapshotStartGroupsVector(b, len(grpOffs))
	for i := len(grpOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(grpOffs[i])
	}
	grpVec := b.EndVector(len(grpOffs))

	// ── SA attachments ────────────────────────────────────────────────────────
	saEntries := attach.SAAttachments
	sort.Slice(saEntries, func(i, j int) bool { return saEntries[i].SAID < saEntries[j].SAID })
	saOffs := make([]flatbuffers.UOffsetT, len(saEntries))
	for i := len(saEntries) - 1; i >= 0; i-- {
		e := saEntries[i]
		saIDOff := b.CreateString(e.SAID)

		ps := append([]string(nil), e.Policies...)
		sort.Strings(ps)
		pOffs := make([]flatbuffers.UOffsetT, len(ps))
		for j, p := range ps {
			pOffs[j] = b.CreateString(p)
		}
		clusterpb.MetaIAMPolicyAttachSAEntryStartPoliciesVector(b, len(pOffs))
		for j := len(pOffs) - 1; j >= 0; j-- {
			b.PrependUOffsetT(pOffs[j])
		}
		pVec := b.EndVector(len(pOffs))

		clusterpb.MetaIAMPolicyAttachSAEntryStart(b)
		clusterpb.MetaIAMPolicyAttachSAEntryAddSaId(b, saIDOff)
		clusterpb.MetaIAMPolicyAttachSAEntryAddPolicies(b, pVec)
		saOffs[i] = clusterpb.MetaIAMPolicyAttachSAEntryEnd(b)
	}
	clusterpb.MetaIAMPolicyStoresSnapshotStartSaAttachmentsVector(b, len(saOffs))
	for i := len(saOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(saOffs[i])
	}
	saVec := b.EndVector(len(saOffs))

	// ── Group attachments ─────────────────────────────────────────────────────
	grpAtt := attach.GroupAttachments
	sort.Slice(grpAtt, func(i, j int) bool { return grpAtt[i].Group < grpAtt[j].Group })
	gaOffs := make([]flatbuffers.UOffsetT, len(grpAtt))
	for i := len(grpAtt) - 1; i >= 0; i-- {
		e := grpAtt[i]
		grpOff := b.CreateString(e.Group)

		ps := append([]string(nil), e.Policies...)
		sort.Strings(ps)
		pOffs := make([]flatbuffers.UOffsetT, len(ps))
		for j, p := range ps {
			pOffs[j] = b.CreateString(p)
		}
		clusterpb.MetaIAMPolicyAttachGroupEntryStartPoliciesVector(b, len(pOffs))
		for j := len(pOffs) - 1; j >= 0; j-- {
			b.PrependUOffsetT(pOffs[j])
		}
		pVec := b.EndVector(len(pOffs))

		clusterpb.MetaIAMPolicyAttachGroupEntryStart(b)
		clusterpb.MetaIAMPolicyAttachGroupEntryAddGroup(b, grpOff)
		clusterpb.MetaIAMPolicyAttachGroupEntryAddPolicies(b, pVec)
		gaOffs[i] = clusterpb.MetaIAMPolicyAttachGroupEntryEnd(b)
	}
	clusterpb.MetaIAMPolicyStoresSnapshotStartGroupAttachmentsVector(b, len(gaOffs))
	for i := len(gaOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(gaOffs[i])
	}
	gaVec := b.EndVector(len(gaOffs))

	// ── Bucket policies ───────────────────────────────────────────────────────
	sort.Slice(bucketPols, func(i, j int) bool { return bucketPols[i].Bucket < bucketPols[j].Bucket })
	bpOffs := make([]flatbuffers.UOffsetT, len(bucketPols))
	for i := len(bucketPols) - 1; i >= 0; i-- {
		e := bucketPols[i]
		bucketOff := b.CreateString(e.Bucket)
		docOff := b.CreateByteVector(e.Doc)
		clusterpb.MetaIAMBucketPolicyEntryStart(b)
		clusterpb.MetaIAMBucketPolicyEntryAddBucket(b, bucketOff)
		clusterpb.MetaIAMBucketPolicyEntryAddDocJson(b, docOff)
		bpOffs[i] = clusterpb.MetaIAMBucketPolicyEntryEnd(b)
	}
	clusterpb.MetaIAMPolicyStoresSnapshotStartBucketPoliciesVector(b, len(bpOffs))
	for i := len(bpOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(bpOffs[i])
	}
	bpVec := b.EndVector(len(bpOffs))

	// ── MountSA entries ───────────────────────────────────────────────────────
	sort.Slice(mountSAs, func(i, j int) bool { return mountSAs[i].Name < mountSAs[j].Name })
	msaOffs := make([]flatbuffers.UOffsetT, len(mountSAs))
	for i := len(mountSAs) - 1; i >= 0; i-- {
		sa := mountSAs[i]
		nameOff := b.CreateString(sa.Name)
		createdByOff := b.CreateString(sa.CreatedBy)
		clusterpb.MetaMountSAEntryStart(b)
		clusterpb.MetaMountSAEntryAddName(b, nameOff)
		clusterpb.MetaMountSAEntryAddNumericUid(b, sa.NumericUID)
		clusterpb.MetaMountSAEntryAddCreatedAt(b, sa.CreatedAt)
		clusterpb.MetaMountSAEntryAddCreatedBy(b, createdByOff)
		msaOffs[i] = clusterpb.MetaMountSAEntryEnd(b)
	}
	clusterpb.MetaIAMPolicyStoresSnapshotStartMountSasVector(b, len(msaOffs))
	for i := len(msaOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(msaOffs[i])
	}
	msaVec := b.EndVector(len(msaOffs))

	// ── MountSA attachments ───────────────────────────────────────────────────
	msaAtt := attach.MountSAAttachments
	sort.Slice(msaAtt, func(i, j int) bool { return msaAtt[i].MountSA < msaAtt[j].MountSA })
	msaAttOffs := make([]flatbuffers.UOffsetT, len(msaAtt))
	for i := len(msaAtt) - 1; i >= 0; i-- {
		e := msaAtt[i]
		msaNameOff := b.CreateString(e.MountSA)

		ps := append([]string(nil), e.Policies...)
		sort.Strings(ps)
		pOffs := make([]flatbuffers.UOffsetT, len(ps))
		for j, p := range ps {
			pOffs[j] = b.CreateString(p)
		}
		clusterpb.MetaIAMPolicyAttachMountSAEntryStartPoliciesVector(b, len(pOffs))
		for j := len(pOffs) - 1; j >= 0; j-- {
			b.PrependUOffsetT(pOffs[j])
		}
		pVec := b.EndVector(len(pOffs))

		clusterpb.MetaIAMPolicyAttachMountSAEntryStart(b)
		clusterpb.MetaIAMPolicyAttachMountSAEntryAddMountSa(b, msaNameOff)
		clusterpb.MetaIAMPolicyAttachMountSAEntryAddPolicies(b, pVec)
		msaAttOffs[i] = clusterpb.MetaIAMPolicyAttachMountSAEntryEnd(b)
	}
	clusterpb.MetaIAMPolicyStoresSnapshotStartMountSaAttachmentsVector(b, len(msaAttOffs))
	for i := len(msaAttOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(msaAttOffs[i])
	}
	msaAttVec := b.EndVector(len(msaAttOffs))

	// ── Assemble root ─────────────────────────────────────────────────────────
	clusterpb.MetaIAMPolicyStoresSnapshotStart(b)
	clusterpb.MetaIAMPolicyStoresSnapshotAddPolicies(b, polVec)
	clusterpb.MetaIAMPolicyStoresSnapshotAddGroups(b, grpVec)
	clusterpb.MetaIAMPolicyStoresSnapshotAddSaAttachments(b, saVec)
	clusterpb.MetaIAMPolicyStoresSnapshotAddGroupAttachments(b, gaVec)
	clusterpb.MetaIAMPolicyStoresSnapshotAddBucketPolicies(b, bpVec)
	clusterpb.MetaIAMPolicyStoresSnapshotAddMountSas(b, msaVec)
	clusterpb.MetaIAMPolicyStoresSnapshotAddMountSaAttachments(b, msaAttVec)
	return fbFinish(b, clusterpb.MetaIAMPolicyStoresSnapshotEnd(b)), nil
}

// decodeMetaIAMPolicyStoresSnapshot parses an IPST trailer payload and returns
// the store snapshots needed to ReplaceAll on each in-memory/Badger store.
func decodeMetaIAMPolicyStoresSnapshot(data []byte) (
	policies []policystore.PolicyEntry,
	groups []group.GroupEntry,
	attach policyattach.AttachSnapshot,
	bucketPols []bucketpolicy.BucketPolicyEntry,
	mountSAs []mountsastore.MountSA,
	err error,
) {
	snap, err := fbSafe(data, func(d []byte) *clusterpb.MetaIAMPolicyStoresSnapshot {
		return clusterpb.GetRootAsMetaIAMPolicyStoresSnapshot(d, 0)
	})
	if err != nil {
		return nil, nil, policyattach.AttachSnapshot{}, nil, nil,
			fmt.Errorf("iam_policy_stores_codec: MetaIAMPolicyStoresSnapshot: %w", err)
	}

	// Policies
	policies = make([]policystore.PolicyEntry, snap.PoliciesLength())
	var pFB clusterpb.MetaIAMPolicyEntry
	for i := 0; i < snap.PoliciesLength(); i++ {
		if !snap.Policies(&pFB, i) {
			return nil, nil, policyattach.AttachSnapshot{}, nil, nil,
				fmt.Errorf("iam_policy_stores_codec: policy %d decode failed", i)
		}
		policies[i] = policystore.PolicyEntry{
			Name:    string(pFB.Name()),
			Doc:     append([]byte(nil), pFB.DocJsonBytes()...),
			Builtin: pFB.Builtin(),
		}
	}

	// Groups
	groups = make([]group.GroupEntry, snap.GroupsLength())
	var gFB clusterpb.MetaIAMGroupEntry
	for i := 0; i < snap.GroupsLength(); i++ {
		if !snap.Groups(&gFB, i) {
			return nil, nil, policyattach.AttachSnapshot{}, nil, nil,
				fmt.Errorf("iam_policy_stores_codec: group %d decode failed", i)
		}
		aps := make([]string, gFB.AttachedPoliciesLength())
		for j := range aps {
			aps[j] = string(gFB.AttachedPolicies(j))
		}
		ms := make([]string, gFB.MembersLength())
		for j := range ms {
			ms[j] = string(gFB.Members(j))
		}
		groups[i] = group.GroupEntry{
			Name:             string(gFB.Name()),
			AttachedPolicies: aps,
			Members:          ms,
		}
	}

	// SA attachments
	saEntries := make([]policyattach.SAAttachEntry, snap.SaAttachmentsLength())
	var saFB clusterpb.MetaIAMPolicyAttachSAEntry
	for i := 0; i < snap.SaAttachmentsLength(); i++ {
		if !snap.SaAttachments(&saFB, i) {
			return nil, nil, policyattach.AttachSnapshot{}, nil, nil,
				fmt.Errorf("iam_policy_stores_codec: sa_attachment %d decode failed", i)
		}
		ps := make([]string, saFB.PoliciesLength())
		for j := range ps {
			ps[j] = string(saFB.Policies(j))
		}
		saEntries[i] = policyattach.SAAttachEntry{SAID: string(saFB.SaId()), Policies: ps}
	}

	// Group attachments
	gaEntries := make([]policyattach.GroupAttachEntry, snap.GroupAttachmentsLength())
	var gaFB clusterpb.MetaIAMPolicyAttachGroupEntry
	for i := 0; i < snap.GroupAttachmentsLength(); i++ {
		if !snap.GroupAttachments(&gaFB, i) {
			return nil, nil, policyattach.AttachSnapshot{}, nil, nil,
				fmt.Errorf("iam_policy_stores_codec: group_attachment %d decode failed", i)
		}
		ps := make([]string, gaFB.PoliciesLength())
		for j := range ps {
			ps[j] = string(gaFB.Policies(j))
		}
		gaEntries[i] = policyattach.GroupAttachEntry{Group: string(gaFB.Group()), Policies: ps}
	}

	// Bucket policies
	bucketPols = make([]bucketpolicy.BucketPolicyEntry, snap.BucketPoliciesLength())
	var bpFB clusterpb.MetaIAMBucketPolicyEntry
	for i := 0; i < snap.BucketPoliciesLength(); i++ {
		if !snap.BucketPolicies(&bpFB, i) {
			return nil, nil, policyattach.AttachSnapshot{}, nil, nil,
				fmt.Errorf("iam_policy_stores_codec: bucket_policy %d decode failed", i)
		}
		bucketPols[i] = bucketpolicy.BucketPolicyEntry{
			Bucket: string(bpFB.Bucket()),
			Doc:    append([]byte(nil), bpFB.DocJsonBytes()...),
		}
	}

	// MountSA entries (§A, optional — pre-§A snapshots have 0 entries here)
	mountSAs = make([]mountsastore.MountSA, snap.MountSasLength())
	var msaFB clusterpb.MetaMountSAEntry
	for i := 0; i < snap.MountSasLength(); i++ {
		if !snap.MountSas(&msaFB, i) {
			return nil, nil, policyattach.AttachSnapshot{}, nil, nil,
				fmt.Errorf("iam_policy_stores_codec: mount_sa %d decode failed", i)
		}
		mountSAs[i] = mountsastore.MountSA{
			Name:       string(msaFB.Name()),
			NumericUID: msaFB.NumericUid(),
			CreatedAt:  msaFB.CreatedAt(),
			CreatedBy:  string(msaFB.CreatedBy()),
		}
	}

	// MountSA attachments (§A, optional)
	msaAttEntries := make([]policyattach.MountSAAttachEntry, snap.MountSaAttachmentsLength())
	var msaAttFB clusterpb.MetaIAMPolicyAttachMountSAEntry
	for i := 0; i < snap.MountSaAttachmentsLength(); i++ {
		if !snap.MountSaAttachments(&msaAttFB, i) {
			return nil, nil, policyattach.AttachSnapshot{}, nil, nil,
				fmt.Errorf("iam_policy_stores_codec: mount_sa_attachment %d decode failed", i)
		}
		ps := make([]string, msaAttFB.PoliciesLength())
		for j := range ps {
			ps[j] = string(msaAttFB.Policies(j))
		}
		msaAttEntries[i] = policyattach.MountSAAttachEntry{MountSA: string(msaAttFB.MountSa()), Policies: ps}
	}
	attach = policyattach.AttachSnapshot{SAAttachments: saEntries, GroupAttachments: gaEntries, MountSAAttachments: msaAttEntries}

	return policies, groups, attach, bucketPols, mountSAs, nil
}
