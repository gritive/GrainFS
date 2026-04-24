package cluster

import (
	"fmt"
	"sort"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// objectMeta is a local struct for serializing object metadata to BadgerDB.
type objectMeta struct {
	Key          string
	Size         int64
	ContentType  string
	ETag         string
	LastModified int64
	ACL          uint8 // s3auth.ACLGrant bitmask; 0 = private (backward compat)
}

// clusterMultipartMeta holds metadata about an in-progress multipart upload
// as stored in BadgerDB.
type clusterMultipartMeta struct {
	ContentType string
}

// --- helpers ---

func fbFinish(b *flatbuffers.Builder, root flatbuffers.UOffsetT) []byte {
	b.Finish(root)
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	return out
}

// fbSafe wraps a FlatBuffers decode call with a panic→error conversion.
// FlatBuffers panics on malformed data; this makes those panics into errors.
func fbSafe[T any](data []byte, fn func([]byte) T) (t T, err error) {
	if len(data) == 0 {
		return t, fmt.Errorf("empty data")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("invalid flatbuffer: %v", r)
		}
	}()
	return fn(data), nil
}

// --- Command encode/decode ---

func encodeCreateBucketCmd(c CreateBucketCmd) ([]byte, error) {
	b := flatbuffers.NewBuilder(64)
	bucketOff := b.CreateString(c.Bucket)
	clusterpb.CreateBucketCmdStart(b)
	clusterpb.CreateBucketCmdAddBucket(b, bucketOff)
	return fbFinish(b, clusterpb.CreateBucketCmdEnd(b)), nil
}

func decodeCreateBucketCmd(data []byte) (CreateBucketCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.CreateBucketCmd {
		return clusterpb.GetRootAsCreateBucketCmd(d, 0)
	})
	if err != nil {
		return CreateBucketCmd{}, err
	}
	return CreateBucketCmd{Bucket: string(t.Bucket())}, nil
}

func encodeDeleteBucketCmd(c DeleteBucketCmd) ([]byte, error) {
	b := flatbuffers.NewBuilder(64)
	bucketOff := b.CreateString(c.Bucket)
	clusterpb.DeleteBucketCmdStart(b)
	clusterpb.DeleteBucketCmdAddBucket(b, bucketOff)
	return fbFinish(b, clusterpb.DeleteBucketCmdEnd(b)), nil
}

func decodeDeleteBucketCmd(data []byte) (DeleteBucketCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.DeleteBucketCmd {
		return clusterpb.GetRootAsDeleteBucketCmd(d, 0)
	})
	if err != nil {
		return DeleteBucketCmd{}, err
	}
	return DeleteBucketCmd{Bucket: string(t.Bucket())}, nil
}

func encodePutObjectMetaCmd(c PutObjectMetaCmd) ([]byte, error) {
	b := flatbuffers.NewBuilder(128)
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	ctOff := b.CreateString(c.ContentType)
	etagOff := b.CreateString(c.ETag)
	vidOff := b.CreateString(c.VersionID)
	clusterpb.PutObjectMetaCmdStart(b)
	clusterpb.PutObjectMetaCmdAddBucket(b, bucketOff)
	clusterpb.PutObjectMetaCmdAddKey(b, keyOff)
	clusterpb.PutObjectMetaCmdAddSize(b, c.Size)
	clusterpb.PutObjectMetaCmdAddContentType(b, ctOff)
	clusterpb.PutObjectMetaCmdAddEtag(b, etagOff)
	clusterpb.PutObjectMetaCmdAddModTime(b, c.ModTime)
	clusterpb.PutObjectMetaCmdAddVersionId(b, vidOff)
	return fbFinish(b, clusterpb.PutObjectMetaCmdEnd(b)), nil
}

func decodePutObjectMetaCmd(data []byte) (PutObjectMetaCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.PutObjectMetaCmd {
		return clusterpb.GetRootAsPutObjectMetaCmd(d, 0)
	})
	if err != nil {
		return PutObjectMetaCmd{}, err
	}
	return PutObjectMetaCmd{
		Bucket:      string(t.Bucket()),
		Key:         string(t.Key()),
		Size:        t.Size(),
		ContentType: string(t.ContentType()),
		ETag:        string(t.Etag()),
		ModTime:     t.ModTime(),
		VersionID:   string(t.VersionId()),
	}, nil
}

func encodeDeleteObjectCmd(c DeleteObjectCmd) ([]byte, error) {
	b := flatbuffers.NewBuilder(64)
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	vidOff := b.CreateString(c.VersionID)
	clusterpb.DeleteObjectCmdStart(b)
	clusterpb.DeleteObjectCmdAddBucket(b, bucketOff)
	clusterpb.DeleteObjectCmdAddKey(b, keyOff)
	clusterpb.DeleteObjectCmdAddVersionId(b, vidOff)
	return fbFinish(b, clusterpb.DeleteObjectCmdEnd(b)), nil
}

func decodeDeleteObjectCmd(data []byte) (DeleteObjectCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.DeleteObjectCmd {
		return clusterpb.GetRootAsDeleteObjectCmd(d, 0)
	})
	if err != nil {
		return DeleteObjectCmd{}, err
	}
	return DeleteObjectCmd{
		Bucket:    string(t.Bucket()),
		Key:       string(t.Key()),
		VersionID: string(t.VersionId()),
	}, nil
}

func encodeDeleteObjectVersionCmd(c DeleteObjectVersionCmd) ([]byte, error) {
	b := flatbuffers.NewBuilder(64)
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	vidOff := b.CreateString(c.VersionID)
	clusterpb.DeleteObjectVersionCmdStart(b)
	clusterpb.DeleteObjectVersionCmdAddBucket(b, bucketOff)
	clusterpb.DeleteObjectVersionCmdAddKey(b, keyOff)
	clusterpb.DeleteObjectVersionCmdAddVersionId(b, vidOff)
	return fbFinish(b, clusterpb.DeleteObjectVersionCmdEnd(b)), nil
}

func decodeDeleteObjectVersionCmd(data []byte) (DeleteObjectVersionCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.DeleteObjectVersionCmd {
		return clusterpb.GetRootAsDeleteObjectVersionCmd(d, 0)
	})
	if err != nil {
		return DeleteObjectVersionCmd{}, err
	}
	return DeleteObjectVersionCmd{
		Bucket:    string(t.Bucket()),
		Key:       string(t.Key()),
		VersionID: string(t.VersionId()),
	}, nil
}

func encodeCreateMultipartUploadCmd(c CreateMultipartUploadCmd) ([]byte, error) {
	b := flatbuffers.NewBuilder(128)
	uidOff := b.CreateString(c.UploadID)
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	ctOff := b.CreateString(c.ContentType)
	clusterpb.CreateMultipartUploadCmdStart(b)
	clusterpb.CreateMultipartUploadCmdAddUploadId(b, uidOff)
	clusterpb.CreateMultipartUploadCmdAddBucket(b, bucketOff)
	clusterpb.CreateMultipartUploadCmdAddKey(b, keyOff)
	clusterpb.CreateMultipartUploadCmdAddContentType(b, ctOff)
	clusterpb.CreateMultipartUploadCmdAddCreatedAt(b, c.CreatedAt)
	return fbFinish(b, clusterpb.CreateMultipartUploadCmdEnd(b)), nil
}

func decodeCreateMultipartUploadCmd(data []byte) (CreateMultipartUploadCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.CreateMultipartUploadCmd {
		return clusterpb.GetRootAsCreateMultipartUploadCmd(d, 0)
	})
	if err != nil {
		return CreateMultipartUploadCmd{}, err
	}
	return CreateMultipartUploadCmd{
		UploadID:    string(t.UploadId()),
		Bucket:      string(t.Bucket()),
		Key:         string(t.Key()),
		ContentType: string(t.ContentType()),
		CreatedAt:   t.CreatedAt(),
	}, nil
}

func encodeCompleteMultipartCmd(c CompleteMultipartCmd) ([]byte, error) {
	b := flatbuffers.NewBuilder(128)
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	uidOff := b.CreateString(c.UploadID)
	ctOff := b.CreateString(c.ContentType)
	etagOff := b.CreateString(c.ETag)
	vidOff := b.CreateString(c.VersionID)
	clusterpb.CompleteMultipartCmdStart(b)
	clusterpb.CompleteMultipartCmdAddBucket(b, bucketOff)
	clusterpb.CompleteMultipartCmdAddKey(b, keyOff)
	clusterpb.CompleteMultipartCmdAddUploadId(b, uidOff)
	clusterpb.CompleteMultipartCmdAddSize(b, c.Size)
	clusterpb.CompleteMultipartCmdAddContentType(b, ctOff)
	clusterpb.CompleteMultipartCmdAddEtag(b, etagOff)
	clusterpb.CompleteMultipartCmdAddModTime(b, c.ModTime)
	clusterpb.CompleteMultipartCmdAddVersionId(b, vidOff)
	return fbFinish(b, clusterpb.CompleteMultipartCmdEnd(b)), nil
}

func decodeCompleteMultipartCmd(data []byte) (CompleteMultipartCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.CompleteMultipartCmd {
		return clusterpb.GetRootAsCompleteMultipartCmd(d, 0)
	})
	if err != nil {
		return CompleteMultipartCmd{}, err
	}
	return CompleteMultipartCmd{
		Bucket:      string(t.Bucket()),
		Key:         string(t.Key()),
		UploadID:    string(t.UploadId()),
		Size:        t.Size(),
		ContentType: string(t.ContentType()),
		ETag:        string(t.Etag()),
		ModTime:     t.ModTime(),
		VersionID:   string(t.VersionId()),
	}, nil
}

func encodeAbortMultipartCmd(c AbortMultipartCmd) ([]byte, error) {
	b := flatbuffers.NewBuilder(64)
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	uidOff := b.CreateString(c.UploadID)
	clusterpb.AbortMultipartCmdStart(b)
	clusterpb.AbortMultipartCmdAddBucket(b, bucketOff)
	clusterpb.AbortMultipartCmdAddKey(b, keyOff)
	clusterpb.AbortMultipartCmdAddUploadId(b, uidOff)
	return fbFinish(b, clusterpb.AbortMultipartCmdEnd(b)), nil
}

func decodeAbortMultipartCmd(data []byte) (AbortMultipartCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.AbortMultipartCmd {
		return clusterpb.GetRootAsAbortMultipartCmd(d, 0)
	})
	if err != nil {
		return AbortMultipartCmd{}, err
	}
	return AbortMultipartCmd{
		Bucket:   string(t.Bucket()),
		Key:      string(t.Key()),
		UploadID: string(t.UploadId()),
	}, nil
}

func encodeSetBucketPolicyCmd(c SetBucketPolicyCmd) ([]byte, error) {
	b := flatbuffers.NewBuilder(128)
	bucketOff := b.CreateString(c.Bucket)
	var policyOff flatbuffers.UOffsetT
	if len(c.PolicyJSON) > 0 {
		policyOff = b.CreateByteVector(c.PolicyJSON)
	}
	clusterpb.SetBucketPolicyCmdStart(b)
	clusterpb.SetBucketPolicyCmdAddBucket(b, bucketOff)
	if len(c.PolicyJSON) > 0 {
		clusterpb.SetBucketPolicyCmdAddPolicyJson(b, policyOff)
	}
	return fbFinish(b, clusterpb.SetBucketPolicyCmdEnd(b)), nil
}

func decodeSetBucketPolicyCmd(data []byte) (SetBucketPolicyCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.SetBucketPolicyCmd {
		return clusterpb.GetRootAsSetBucketPolicyCmd(d, 0)
	})
	if err != nil {
		return SetBucketPolicyCmd{}, err
	}
	return SetBucketPolicyCmd{Bucket: string(t.Bucket()), PolicyJSON: t.PolicyJsonBytes()}, nil
}

func encodeDeleteBucketPolicyCmd(c DeleteBucketPolicyCmd) ([]byte, error) {
	b := flatbuffers.NewBuilder(64)
	bucketOff := b.CreateString(c.Bucket)
	clusterpb.DeleteBucketPolicyCmdStart(b)
	clusterpb.DeleteBucketPolicyCmdAddBucket(b, bucketOff)
	return fbFinish(b, clusterpb.DeleteBucketPolicyCmdEnd(b)), nil
}

func decodeDeleteBucketPolicyCmd(data []byte) (DeleteBucketPolicyCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.DeleteBucketPolicyCmd {
		return clusterpb.GetRootAsDeleteBucketPolicyCmd(d, 0)
	})
	if err != nil {
		return DeleteBucketPolicyCmd{}, err
	}
	return DeleteBucketPolicyCmd{Bucket: string(t.Bucket())}, nil
}

// --- ObjectMeta codec ---

func marshalObjectMeta(m objectMeta) ([]byte, error) {
	b := flatbuffers.NewBuilder(128)
	keyOff := b.CreateString(m.Key)
	ctOff := b.CreateString(m.ContentType)
	etagOff := b.CreateString(m.ETag)
	clusterpb.ObjectMetaStart(b)
	clusterpb.ObjectMetaAddKey(b, keyOff)
	clusterpb.ObjectMetaAddSize(b, m.Size)
	clusterpb.ObjectMetaAddContentType(b, ctOff)
	clusterpb.ObjectMetaAddEtag(b, etagOff)
	clusterpb.ObjectMetaAddLastModified(b, m.LastModified)
	clusterpb.ObjectMetaAddAcl(b, m.ACL)
	return fbFinish(b, clusterpb.ObjectMetaEnd(b)), nil
}

func unmarshalObjectMeta(data []byte) (objectMeta, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.ObjectMeta {
		return clusterpb.GetRootAsObjectMeta(d, 0)
	})
	if err != nil {
		return objectMeta{}, fmt.Errorf("unmarshal ObjectMeta: %w", err)
	}
	return objectMeta{
		Key:          string(t.Key()),
		Size:         t.Size(),
		ContentType:  string(t.ContentType()),
		ETag:         string(t.Etag()),
		LastModified: t.LastModified(),
		ACL:          t.Acl(),
	}, nil
}

// --- SnapshotState codec ---

func marshalSnapshotState(state map[string][]byte) ([]byte, error) {
	b := flatbuffers.NewBuilder(len(state) * 64)

	// Sort keys for deterministic output.
	keys := make([]string, 0, len(state))
	for k := range state {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build each KeyValue in reverse order (FlatBuffers vectors are prepended).
	kvOffsets := make([]flatbuffers.UOffsetT, len(keys))
	for i := len(keys) - 1; i >= 0; i-- {
		k := keys[i]
		v := state[k]
		keyOff := b.CreateString(k)
		var valOff flatbuffers.UOffsetT
		if len(v) > 0 {
			valOff = b.CreateByteVector(v)
		}
		clusterpb.KeyValueStart(b)
		clusterpb.KeyValueAddKey(b, keyOff)
		if len(v) > 0 {
			clusterpb.KeyValueAddValue(b, valOff)
		}
		kvOffsets[i] = clusterpb.KeyValueEnd(b)
	}

	// Build entries vector.
	clusterpb.SnapshotStateStartEntriesVector(b, len(kvOffsets))
	for i := len(kvOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(kvOffsets[i])
	}
	entriesVec := b.EndVector(len(kvOffsets))

	clusterpb.SnapshotStateStart(b)
	clusterpb.SnapshotStateAddEntries(b, entriesVec)
	return fbFinish(b, clusterpb.SnapshotStateEnd(b)), nil
}

func unmarshalSnapshotState(data []byte) (result map[string][]byte, err error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("unmarshal SnapshotState: empty data")
	}
	defer func() {
		if r := recover(); r != nil {
			result = nil
			err = fmt.Errorf("unmarshal SnapshotState: invalid flatbuffer: %v", r)
		}
	}()
	ss := clusterpb.GetRootAsSnapshotState(data, 0)
	result = make(map[string][]byte, ss.EntriesLength())
	var kv clusterpb.KeyValue
	for i := 0; i < ss.EntriesLength(); i++ {
		if !ss.Entries(&kv, i) {
			continue
		}
		result[string(kv.Key())] = kv.ValueBytes()
	}
	return result, nil
}

// --- ClusterMultipartMeta codec ---

func marshalClusterMultipartMeta(m clusterMultipartMeta) ([]byte, error) {
	b := flatbuffers.NewBuilder(64)
	ctOff := b.CreateString(m.ContentType)
	clusterpb.MultipartMetaStart(b)
	clusterpb.MultipartMetaAddContentType(b, ctOff)
	return fbFinish(b, clusterpb.MultipartMetaEnd(b)), nil
}

func unmarshalClusterMultipartMeta(data []byte) (clusterMultipartMeta, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MultipartMeta {
		return clusterpb.GetRootAsMultipartMeta(d, 0)
	})
	if err != nil {
		return clusterMultipartMeta{}, fmt.Errorf("unmarshal MultipartMeta: %w", err)
	}
	return clusterMultipartMeta{ContentType: string(t.ContentType())}, nil
}

// --- MigrateShard / MigrationDone codec ---

func encodeMigrateShardCmd(c MigrateShardFSMCmd) ([]byte, error) {
	b := flatbuffers.NewBuilder(128)
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	vidOff := b.CreateString(c.VersionID)
	srcOff := b.CreateString(c.SrcNode)
	dstOff := b.CreateString(c.DstNode)
	clusterpb.MigrateShardCmdStart(b)
	clusterpb.MigrateShardCmdAddBucket(b, bucketOff)
	clusterpb.MigrateShardCmdAddKey(b, keyOff)
	clusterpb.MigrateShardCmdAddVersionId(b, vidOff)
	clusterpb.MigrateShardCmdAddSrcNode(b, srcOff)
	clusterpb.MigrateShardCmdAddDstNode(b, dstOff)
	return fbFinish(b, clusterpb.MigrateShardCmdEnd(b)), nil
}

func decodeMigrateShardCmd(data []byte) (MigrateShardFSMCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MigrateShardCmd {
		return clusterpb.GetRootAsMigrateShardCmd(d, 0)
	})
	if err != nil {
		return MigrateShardFSMCmd{}, err
	}
	return MigrateShardFSMCmd{
		Bucket:    string(t.Bucket()),
		Key:       string(t.Key()),
		VersionID: string(t.VersionId()),
		SrcNode:   string(t.SrcNode()),
		DstNode:   string(t.DstNode()),
	}, nil
}

func decodeMigrationDoneCmd(data []byte) (MigrationDoneFSMCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MigrationDoneCmd {
		return clusterpb.GetRootAsMigrationDoneCmd(d, 0)
	})
	if err != nil {
		return MigrationDoneFSMCmd{}, err
	}
	return MigrationDoneFSMCmd{
		Bucket:    string(t.Bucket()),
		Key:       string(t.Key()),
		VersionID: string(t.VersionId()),
		SrcNode:   string(t.SrcNode()),
		DstNode:   string(t.DstNode()),
	}, nil
}

func encodeMigrationDoneCmd(c MigrationDoneFSMCmd) ([]byte, error) {
	b := flatbuffers.NewBuilder(128)
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	vidOff := b.CreateString(c.VersionID)
	srcOff := b.CreateString(c.SrcNode)
	dstOff := b.CreateString(c.DstNode)
	clusterpb.MigrationDoneCmdStart(b)
	clusterpb.MigrationDoneCmdAddBucket(b, bucketOff)
	clusterpb.MigrationDoneCmdAddKey(b, keyOff)
	clusterpb.MigrationDoneCmdAddVersionId(b, vidOff)
	clusterpb.MigrationDoneCmdAddSrcNode(b, srcOff)
	clusterpb.MigrationDoneCmdAddDstNode(b, dstOff)
	return fbFinish(b, clusterpb.MigrationDoneCmdEnd(b)), nil
}

func encodeSetBucketVersioningCmd(c SetBucketVersioningCmd) ([]byte, error) {
	b := flatbuffers.NewBuilder(64)
	bucketOff := b.CreateString(c.Bucket)
	stateOff := b.CreateString(c.State)
	clusterpb.SetBucketVersioningCmdStart(b)
	clusterpb.SetBucketVersioningCmdAddBucket(b, bucketOff)
	clusterpb.SetBucketVersioningCmdAddState(b, stateOff)
	return fbFinish(b, clusterpb.SetBucketVersioningCmdEnd(b)), nil
}

func decodeSetBucketVersioningCmd(data []byte) (SetBucketVersioningCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.SetBucketVersioningCmd {
		return clusterpb.GetRootAsSetBucketVersioningCmd(d, 0)
	})
	if err != nil {
		return SetBucketVersioningCmd{}, err
	}
	return SetBucketVersioningCmd{
		Bucket: string(t.Bucket()),
		State:  string(t.State()),
	}, nil
}

func encodeSetObjectACLCmd(c SetObjectACLCmd) ([]byte, error) {
	b := flatbuffers.NewBuilder(64)
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	clusterpb.SetObjectACLCmdStart(b)
	clusterpb.SetObjectACLCmdAddBucket(b, bucketOff)
	clusterpb.SetObjectACLCmdAddKey(b, keyOff)
	clusterpb.SetObjectACLCmdAddAcl(b, c.ACL)
	return fbFinish(b, clusterpb.SetObjectACLCmdEnd(b)), nil
}

func decodeSetObjectACLCmd(data []byte) (SetObjectACLCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.SetObjectACLCmd {
		return clusterpb.GetRootAsSetObjectACLCmd(d, 0)
	})
	if err != nil {
		return SetObjectACLCmd{}, err
	}
	return SetObjectACLCmd{
		Bucket: string(t.Bucket()),
		Key:    string(t.Key()),
		ACL:    t.Acl(),
	}, nil
}

// --- Payload encoding dispatch ---

func encodePayload(cmdType CommandType, payload any) ([]byte, error) {
	switch cmdType {
	case CmdNoOp:
		return nil, nil
	case CmdCreateBucket:
		return encodeCreateBucketCmd(payload.(CreateBucketCmd))
	case CmdDeleteBucket:
		return encodeDeleteBucketCmd(payload.(DeleteBucketCmd))
	case CmdPutObjectMeta:
		return encodePutObjectMetaCmd(payload.(PutObjectMetaCmd))
	case CmdDeleteObject:
		return encodeDeleteObjectCmd(payload.(DeleteObjectCmd))
	case CmdCreateMultipartUpload:
		return encodeCreateMultipartUploadCmd(payload.(CreateMultipartUploadCmd))
	case CmdCompleteMultipart:
		return encodeCompleteMultipartCmd(payload.(CompleteMultipartCmd))
	case CmdAbortMultipart:
		return encodeAbortMultipartCmd(payload.(AbortMultipartCmd))
	case CmdSetBucketPolicy:
		return encodeSetBucketPolicyCmd(payload.(SetBucketPolicyCmd))
	case CmdDeleteBucketPolicy:
		return encodeDeleteBucketPolicyCmd(payload.(DeleteBucketPolicyCmd))
	case CmdMigrateShard:
		return encodeMigrateShardCmd(payload.(MigrateShardFSMCmd))
	case CmdMigrationDone:
		return encodeMigrationDoneCmd(payload.(MigrationDoneFSMCmd))
	case CmdPutShardPlacement:
		return encodePutShardPlacementCmd(payload.(PutShardPlacementCmd))
	case CmdDeleteShardPlacement:
		return encodeDeleteShardPlacementCmd(payload.(DeleteShardPlacementCmd))
	case CmdDeleteObjectVersion:
		return encodeDeleteObjectVersionCmd(payload.(DeleteObjectVersionCmd))
	case CmdSetBucketVersioning:
		return encodeSetBucketVersioningCmd(payload.(SetBucketVersioningCmd))
	case CmdSetObjectACL:
		return encodeSetObjectACLCmd(payload.(SetObjectACLCmd))
	default:
		return nil, fmt.Errorf("unknown command type: %d", cmdType)
	}
}

func encodePutShardPlacementCmd(c PutShardPlacementCmd) ([]byte, error) {
	b := flatbuffers.NewBuilder(128)
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)

	nodeOffs := make([]flatbuffers.UOffsetT, len(c.NodeIDs))
	for i, n := range c.NodeIDs {
		nodeOffs[i] = b.CreateString(n)
	}
	clusterpb.PutShardPlacementCmdStartNodeIdsVector(b, len(c.NodeIDs))
	for i := len(c.NodeIDs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(nodeOffs[i])
	}
	nodesVec := b.EndVector(len(c.NodeIDs))

	clusterpb.PutShardPlacementCmdStart(b)
	clusterpb.PutShardPlacementCmdAddBucket(b, bucketOff)
	clusterpb.PutShardPlacementCmdAddKey(b, keyOff)
	clusterpb.PutShardPlacementCmdAddNodeIds(b, nodesVec)
	clusterpb.PutShardPlacementCmdAddK(b, int32(c.K))
	clusterpb.PutShardPlacementCmdAddM(b, int32(c.M))
	return fbFinish(b, clusterpb.PutShardPlacementCmdEnd(b)), nil
}

func decodePutShardPlacementCmd(data []byte) (PutShardPlacementCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.PutShardPlacementCmd {
		return clusterpb.GetRootAsPutShardPlacementCmd(d, 0)
	})
	if err != nil {
		return PutShardPlacementCmd{}, err
	}
	n := t.NodeIdsLength()
	nodes := make([]string, n)
	for i := 0; i < n; i++ {
		nodes[i] = string(t.NodeIds(i))
	}
	return PutShardPlacementCmd{
		Bucket:  string(t.Bucket()),
		Key:     string(t.Key()),
		NodeIDs: nodes,
		K:       int(t.K()),
		M:       int(t.M()),
	}, nil
}

func encodeDeleteShardPlacementCmd(c DeleteShardPlacementCmd) ([]byte, error) {
	b := flatbuffers.NewBuilder(64)
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	clusterpb.DeleteShardPlacementCmdStart(b)
	clusterpb.DeleteShardPlacementCmdAddBucket(b, bucketOff)
	clusterpb.DeleteShardPlacementCmdAddKey(b, keyOff)
	return fbFinish(b, clusterpb.DeleteShardPlacementCmdEnd(b)), nil
}

func decodeDeleteShardPlacementCmd(data []byte) (DeleteShardPlacementCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.DeleteShardPlacementCmd {
		return clusterpb.GetRootAsDeleteShardPlacementCmd(d, 0)
	})
	if err != nil {
		return DeleteShardPlacementCmd{}, err
	}
	return DeleteShardPlacementCmd{
		Bucket: string(t.Bucket()),
		Key:    string(t.Key()),
	}, nil
}
