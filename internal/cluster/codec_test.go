package cluster

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestEncodeDecodeCommand_CreateBucket(t *testing.T) {
	orig := CreateBucketCmd{Bucket: "my-bucket"}

	encoded, err := EncodeCommand(CmdCreateBucket, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	assert.Equal(t, CmdCreateBucket, cmd.Type)

	decoded, err := decodeCreateBucketCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "my-bucket", decoded.Bucket)
	assert.False(t, decoded.BypassReserved, "default bypass_reserved must be false")
}

func TestEncodeDecodeCommand_CreateBucket_BypassReserved(t *testing.T) {
	// Roundtrip with BypassReserved=true (bootstrap bypass path).
	orig := CreateBucketCmd{Bucket: "_grainfs", BypassReserved: true}

	encoded, err := EncodeCommand(CmdCreateBucket, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)

	decoded, err := decodeCreateBucketCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "_grainfs", decoded.Bucket)
	assert.True(t, decoded.BypassReserved, "bypass_reserved must round-trip as true")
}

func TestEncodeDecodeCommand_CreateBucket_BypassHelper(t *testing.T) {
	// encodeCreateBucketCmdBypass must produce a decodable bypass=true payload.
	data, err := encodeCreateBucketCmdBypass("default")
	require.NoError(t, err)

	decoded, err := decodeCreateBucketCmd(data)
	require.NoError(t, err)
	assert.Equal(t, "default", decoded.Bucket)
	assert.True(t, decoded.BypassReserved)
}

func TestEncodeDecodeCommand_PutObjectMeta(t *testing.T) {
	orig := PutObjectMetaCmd{
		Bucket:           "test-bucket",
		Key:              "photos/sunset.jpg",
		Size:             1048576,
		ContentType:      "image/jpeg",
		ETag:             "d41d8cd98f00b204e9800998ecf8427e",
		ModTime:          1700000000,
		PlacementGroupID: "group-2",
		UserMetadata:     map[string]string{"x-amz-meta-mtime": "1710000000"},
		SSEAlgorithm:     "AES256",
	}

	encoded, err := EncodeCommand(CmdPutObjectMeta, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	assert.Equal(t, CmdPutObjectMeta, cmd.Type)

	decoded, err := decodePutObjectMetaCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "test-bucket", decoded.Bucket)
	assert.Equal(t, "photos/sunset.jpg", decoded.Key)
	assert.Equal(t, int64(1048576), decoded.Size)
	assert.Equal(t, "image/jpeg", decoded.ContentType)
	assert.Equal(t, "d41d8cd98f00b204e9800998ecf8427e", decoded.ETag)
	assert.Equal(t, int64(1700000000), decoded.ModTime)
	assert.Equal(t, "group-2", decoded.PlacementGroupID)
	assert.Equal(t, map[string]string{"x-amz-meta-mtime": "1710000000"}, decoded.UserMetadata)
	assert.Equal(t, "AES256", decoded.SSEAlgorithm)
}

// Phase 2 Task 2.3: PutObjectMetaCmd.Segments[] survives encode/decode and
// preserves per-segment placement metadata.
func TestPutObjectMetaCmd_SegmentsRoundTrip(t *testing.T) {
	orig := PutObjectMetaCmd{
		Bucket:           "test-bucket",
		Key:              "large.bin",
		Size:             64 << 20,
		ContentType:      "application/octet-stream",
		ETag:             "etag-md5-of-whole",
		ModTime:          1700000000,
		VersionID:        "v1",
		ECData:           4,
		ECParity:         2,
		NodeIDs:          []string{"n1", "n2", "n3", "n4", "n5", "n6"},
		PlacementGroupID: "group-a",
		Segments: []SegmentMetaEntry{
			{
				BlobID:           "blob-0",
				Size:             16 << 20,
				Checksum:         []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
				PlacementGroupID: "group-a",
				ShardSize:        4 << 20,
				SegmentIdx:       0,
				NodeIDs:          []string{"n1", "n2", "n3", "n4", "n5", "n6"},
				ECData:           4,
				ECParity:         2,
				RingVersion:      0,
			},
			{
				BlobID:           "blob-1",
				Size:             16 << 20,
				Checksum:         []byte{0x10, 0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 0x09, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01},
				PlacementGroupID: "group-b",
				ShardSize:        4 << 20,
				SegmentIdx:       1,
				NodeIDs:          []string{"n2", "n3", "n4", "n5", "n6", "n7"},
				ECData:           4,
				ECParity:         2,
				RingVersion:      0,
			},
		},
	}

	encoded, err := EncodeCommand(CmdPutObjectMeta, orig)
	require.NoError(t, err)
	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	decoded, err := decodePutObjectMetaCmd(cmd.Data)
	require.NoError(t, err)

	require.Len(t, decoded.Segments, 2)
	for i, seg := range decoded.Segments {
		assert.Equal(t, orig.Segments[i].BlobID, seg.BlobID, "segment %d BlobID", i)
		assert.Equal(t, orig.Segments[i].Size, seg.Size, "segment %d Size", i)
		assert.Equal(t, orig.Segments[i].Checksum, seg.Checksum, "segment %d Checksum", i)
		assert.Equal(t, orig.Segments[i].PlacementGroupID, seg.PlacementGroupID, "segment %d PG", i)
		assert.Equal(t, orig.Segments[i].ShardSize, seg.ShardSize, "segment %d ShardSize", i)
		assert.Equal(t, orig.Segments[i].SegmentIdx, seg.SegmentIdx, "segment %d SegmentIdx", i)
		assert.Equal(t, orig.Segments[i].NodeIDs, seg.NodeIDs, "segment %d NodeIDs", i)
		assert.Equal(t, orig.Segments[i].ECData, seg.ECData, "segment %d ECData", i)
		assert.Equal(t, orig.Segments[i].ECParity, seg.ECParity, "segment %d ECParity", i)
	}
}

// Legacy PutObjectMetaCmd payloads (no Segments) decode unchanged.
func TestPutObjectMetaCmd_EmptySegmentsLegacyCompatible(t *testing.T) {
	orig := PutObjectMetaCmd{
		Bucket:           "test-bucket",
		Key:              "small.txt",
		Size:             1024,
		ContentType:      "text/plain",
		ETag:             "abc",
		ModTime:          1700000000,
		PlacementGroupID: "group-0",
	}
	encoded, err := EncodeCommand(CmdPutObjectMeta, orig)
	require.NoError(t, err)
	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	decoded, err := decodePutObjectMetaCmd(cmd.Data)
	require.NoError(t, err)
	assert.Empty(t, decoded.Segments)
	assert.Equal(t, "test-bucket", decoded.Bucket)
	assert.Equal(t, "small.txt", decoded.Key)
}

func TestEncodeDecodeCommand_CompleteMultipart(t *testing.T) {
	orig := CompleteMultipartCmd{
		Bucket:           "uploads",
		Key:              "video.mp4",
		UploadID:         "upload-abc-123",
		Size:             5242880,
		ContentType:      "video/mp4",
		ETag:             "abc123def456",
		ModTime:          1700001000,
		PlacementGroupID: "group-3",
		ECData:           2,
		ECParity:         1,
		NodeIDs:          []string{"n1", "n2", "n3"},
	}

	encoded, err := EncodeCommand(CmdCompleteMultipart, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	assert.Equal(t, CmdCompleteMultipart, cmd.Type)

	decoded, err := decodeCompleteMultipartCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "uploads", decoded.Bucket)
	assert.Equal(t, "video.mp4", decoded.Key)
	assert.Equal(t, "upload-abc-123", decoded.UploadID)
	assert.Equal(t, int64(5242880), decoded.Size)
	assert.Equal(t, "video/mp4", decoded.ContentType)
	assert.Equal(t, "abc123def456", decoded.ETag)
	assert.Equal(t, int64(1700001000), decoded.ModTime)
	assert.Equal(t, "group-3", decoded.PlacementGroupID)
	assert.Equal(t, uint8(2), decoded.ECData)
	assert.Equal(t, uint8(1), decoded.ECParity)
	assert.Equal(t, []string{"n1", "n2", "n3"}, decoded.NodeIDs)
}

func TestObjectMetaCodecRoundTrip(t *testing.T) {
	orig := objectMeta{
		Key:              "docs/readme.md",
		Size:             4096,
		ContentType:      "text/markdown",
		ETag:             "etag-xyz",
		LastModified:     1700002000,
		PlacementGroupID: "group-2",
		UserMetadata:     map[string]string{"x-amz-meta-owner": "me"},
		SSEAlgorithm:     "AES256",
	}

	data, err := marshalObjectMeta(orig)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	decoded, err := unmarshalObjectMeta(data)
	require.NoError(t, err)
	assert.Equal(t, orig.Key, decoded.Key)
	assert.Equal(t, orig.Size, decoded.Size)
	assert.Equal(t, orig.ContentType, decoded.ContentType)
	assert.Equal(t, orig.ETag, decoded.ETag)
	assert.Equal(t, orig.LastModified, decoded.LastModified)
	assert.Equal(t, orig.PlacementGroupID, decoded.PlacementGroupID)
	assert.Equal(t, orig.UserMetadata, decoded.UserMetadata)
	assert.Equal(t, orig.SSEAlgorithm, decoded.SSEAlgorithm)
}

func TestSnapshotStateCodecRoundTrip(t *testing.T) {
	orig := map[string][]byte{
		"bucket:docs":       []byte("{}"),
		"obj:docs/file.txt": []byte("meta-bytes"),
		"mpu:upload-1":      []byte("mpu-bytes"),
	}

	data, err := marshalSnapshotState(orig)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	decoded, err := unmarshalSnapshotState(data)
	require.NoError(t, err)
	assert.Len(t, decoded, 3)
	assert.Equal(t, []byte("{}"), decoded["bucket:docs"])
	assert.Equal(t, []byte("meta-bytes"), decoded["obj:docs/file.txt"])
	assert.Equal(t, []byte("mpu-bytes"), decoded["mpu:upload-1"])
}

func TestClusterMultipartMetaCodecRoundTrip(t *testing.T) {
	orig := clusterMultipartMeta{
		Bucket:           "photos",
		Key:              "raw/img.bin",
		CreatedAt:        12345,
		ContentType:      "application/octet-stream",
		PlacementGroupID: "group-5",
	}

	data, err := marshalClusterMultipartMeta(orig)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	decoded, err := unmarshalClusterMultipartMeta(data)
	require.NoError(t, err)
	assert.Equal(t, "photos", decoded.Bucket)
	assert.Equal(t, "raw/img.bin", decoded.Key)
	assert.Equal(t, int64(12345), decoded.CreatedAt)
	assert.Equal(t, "application/octet-stream", decoded.ContentType)
	assert.Equal(t, "group-5", decoded.PlacementGroupID)
}

func TestClusterMultipartMetaCodecLegacyDecodeZeroValues(t *testing.T) {
	legacy := clusterMultipartMeta{
		ContentType:      "application/octet-stream",
		PlacementGroupID: "group-legacy",
	}
	data, err := marshalClusterMultipartMeta(legacy)
	require.NoError(t, err)

	decoded, err := unmarshalClusterMultipartMeta(data)
	require.NoError(t, err)
	assert.Empty(t, decoded.Bucket)
	assert.Empty(t, decoded.Key)
	assert.Zero(t, decoded.CreatedAt)
	assert.Equal(t, "application/octet-stream", decoded.ContentType)
	assert.Equal(t, "group-legacy", decoded.PlacementGroupID)
}

func TestEncodeDecodeCommand_DeleteBucket(t *testing.T) {
	orig := DeleteBucketCmd{Bucket: "remove-me"}

	encoded, err := EncodeCommand(CmdDeleteBucket, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	assert.Equal(t, CmdDeleteBucket, cmd.Type)

	decoded, err := decodeDeleteBucketCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "remove-me", decoded.Bucket)
}

func TestEncodeDecodeCommand_DeleteObject(t *testing.T) {
	orig := DeleteObjectCmd{Bucket: "b", Key: "file.txt"}

	encoded, err := EncodeCommand(CmdDeleteObject, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	assert.Equal(t, CmdDeleteObject, cmd.Type)

	decoded, err := decodeDeleteObjectCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "b", decoded.Bucket)
	assert.Equal(t, "file.txt", decoded.Key)
}

func TestEncodeDecodeCommand_CreateMultipartUpload(t *testing.T) {
	orig := CreateMultipartUploadCmd{
		UploadID: "mpu-123", Bucket: "b", Key: "big.bin",
		ContentType: "application/octet-stream", CreatedAt: 1700000000,
		PlacementGroupID: "group-4",
	}

	encoded, err := EncodeCommand(CmdCreateMultipartUpload, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	assert.Equal(t, CmdCreateMultipartUpload, cmd.Type)

	decoded, err := decodeCreateMultipartUploadCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "mpu-123", decoded.UploadID)
	assert.Equal(t, "b", decoded.Bucket)
	assert.Equal(t, "big.bin", decoded.Key)
	assert.Equal(t, "application/octet-stream", decoded.ContentType)
	assert.Equal(t, int64(1700000000), decoded.CreatedAt)
	assert.Equal(t, "group-4", decoded.PlacementGroupID)
}

func TestEncodeDecodeCommand_AbortMultipart(t *testing.T) {
	orig := AbortMultipartCmd{Bucket: "b", Key: "abort.bin", UploadID: "mpu-abort"}

	encoded, err := EncodeCommand(CmdAbortMultipart, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	assert.Equal(t, CmdAbortMultipart, cmd.Type)

	decoded, err := decodeAbortMultipartCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "b", decoded.Bucket)
	assert.Equal(t, "abort.bin", decoded.Key)
	assert.Equal(t, "mpu-abort", decoded.UploadID)
}

func TestEncodeDecodeCommand_SetBucketPolicy(t *testing.T) {
	policyJSON := []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}`)
	orig := SetBucketPolicyCmd{Bucket: "my-bucket", PolicyJSON: policyJSON}

	encoded, err := EncodeCommand(CmdSetBucketPolicy, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	assert.Equal(t, CmdSetBucketPolicy, cmd.Type)

	decoded, err := decodeSetBucketPolicyCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "my-bucket", decoded.Bucket)
	assert.Equal(t, policyJSON, decoded.PolicyJSON)
}

func TestEncodeDecodeCommand_DeleteBucketPolicy(t *testing.T) {
	orig := DeleteBucketPolicyCmd{Bucket: "policy-bucket"}

	encoded, err := EncodeCommand(CmdDeleteBucketPolicy, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	assert.Equal(t, CmdDeleteBucketPolicy, cmd.Type)

	decoded, err := decodeDeleteBucketPolicyCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "policy-bucket", decoded.Bucket)
}

func TestDecodeCommands_InvalidData(t *testing.T) {
	// FlatBuffers panics on malformed data; test that the top-level entry
	// points (DecodeCommand, unmarshalSnapshotState) convert panics to errors.
	// Inner decode functions are only called with already-validated data.
	_, err := DecodeCommand([]byte("not valid flatbuffer data"))
	assert.Error(t, err, "DecodeCommand should fail on invalid data")

	_, err = unmarshalSnapshotState([]byte("not valid flatbuffer data"))
	assert.Error(t, err, "unmarshalSnapshotState should fail on invalid data")

	_, err = DecodeCommand(nil)
	assert.Error(t, err, "DecodeCommand should fail on nil data")

	_, err = unmarshalSnapshotState(nil)
	assert.Error(t, err, "unmarshalSnapshotState should fail on nil data")
}

func TestEncodePayload_UnknownCommandType(t *testing.T) {
	_, err := encodePayload(CommandType(99), CreateBucketCmd{Bucket: "x"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown command type")
}

func TestClusterCodecOutputIsNotJSON(t *testing.T) {
	// Encode a command and verify the output is not valid JSON
	encoded, err := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "test"})
	require.NoError(t, err)

	var js json.RawMessage
	err = json.Unmarshal(encoded, &js)
	assert.Error(t, err, "protobuf output should not parse as valid JSON")

	// Also verify objectMeta encoding is not JSON
	metaBytes, err := marshalObjectMeta(objectMeta{
		Key: "key", Size: 10, ContentType: "text/plain", ETag: "e", LastModified: 1,
	})
	require.NoError(t, err)
	err = json.Unmarshal(metaBytes, &js)
	assert.Error(t, err, "protobuf objectMeta output should not parse as valid JSON")

	// Snapshot state encoding is not JSON
	snapBytes, err := marshalSnapshotState(map[string][]byte{"k": []byte("v")})
	require.NoError(t, err)
	err = json.Unmarshal(snapBytes, &js)
	assert.Error(t, err, "protobuf snapshot output should not parse as valid JSON")
}

func TestCodec_SetBucketVersioningCmd_RoundTrip(t *testing.T) {
	cmd := SetBucketVersioningCmd{Bucket: "mybucket", State: "Enabled"}
	raw, err := encodeSetBucketVersioningCmd(cmd)
	require.NoError(t, err)
	got, err := decodeSetBucketVersioningCmd(raw)
	require.NoError(t, err)
	assert.Equal(t, cmd, got)
}

func TestCodec_SetObjectACLCmd_RoundTrip(t *testing.T) {
	cmd := SetObjectACLCmd{Bucket: "b", Key: "file.txt", ACL: 2}
	raw, err := encodeSetObjectACLCmd(cmd)
	require.NoError(t, err)
	got, err := decodeSetObjectACLCmd(raw)
	require.NoError(t, err)
	assert.Equal(t, cmd, got)
}

func TestCodec_ObjectMeta_ACL_RoundTrip(t *testing.T) {
	m := objectMeta{Key: "f", Size: 5, ContentType: "text/plain", ETag: "e", LastModified: 1, ACL: 2}
	raw, err := marshalObjectMeta(m)
	require.NoError(t, err)
	got, err := unmarshalObjectMeta(raw)
	require.NoError(t, err)
	assert.Equal(t, m, got)
}

func TestCodec_ObjectMeta_ACL_BackwardCompat(t *testing.T) {
	// Old record without ACL field (ACL=0) should unmarshal cleanly as ACL=0 (private).
	m := objectMeta{Key: "f", Size: 5, ContentType: "text/plain", ETag: "e", LastModified: 1}
	raw, err := marshalObjectMeta(m)
	require.NoError(t, err)
	got, err := unmarshalObjectMeta(raw)
	require.NoError(t, err)
	assert.Equal(t, uint8(0), got.ACL, "legacy records should have ACL=0 (private)")
}

func TestEncodeObjectMeta_AllocsBounded(t *testing.T) {
	meta := objectMeta{
		Key: "test-key", Size: 1024, ContentType: "application/octet-stream",
		ETag: "abc123", LastModified: 1714000000,
	}
	_, _ = marshalObjectMeta(meta)
	allocs := testing.AllocsPerRun(100, func() {
		_, _ = marshalObjectMeta(meta)
	})
	assert.LessOrEqual(t, allocs, 2.0, "marshalObjectMeta should allocate ≤2 (output slice + copy)")
}

func TestCoalescedShardRefRoundTrip(t *testing.T) {
	in := objectMeta{
		Key: "a", Size: 1024, IsAppendable: true,
		Segments: []storage.SegmentRef{{BlobID: "s1", Size: 512, Checksum: []byte("e1")}},
		Coalesced: []CoalescedShardRef{{
			CoalescedID: "c1", Size: 1024, ETag: "etag-c1",
			ShardKey: "a/coalesced/c1", Version: 1,
		}},
	}
	raw, err := marshalObjectMeta(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := unmarshalObjectMeta(raw)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Coalesced) != 1 || got.Coalesced[0].CoalescedID != "c1" ||
		got.Coalesced[0].ShardKey != "a/coalesced/c1" || got.Coalesced[0].Size != 1024 {
		t.Fatalf("Coalesced round-trip mismatch: %+v", got.Coalesced)
	}
}

func TestCoalesceSegmentsCmdRoundTrip(t *testing.T) {
	in := CoalesceSegmentsCmd{
		Bucket: "b", Key: "a", CoalescedID: "c1",
		ShardKey: "a/coalesced/c1", Size: 1024, ETag: "etag",
		ConsumedSegmentIDs: []string{"s1", "s2"},
	}
	raw, err := encodeCoalesceSegmentsCmd(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodeCoalesceSegmentsCmd(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.CoalescedID != "c1" || got.ShardKey != "a/coalesced/c1" || got.Size != 1024 ||
		len(got.ConsumedSegmentIDs) != 2 || got.ConsumedSegmentIDs[0] != "s1" {
		t.Fatalf("CoalesceSegmentsCmd round-trip mismatch: %+v", got)
	}
}

// TestCoalescedShardRefECParamsRoundTrip ensures the Phase B3 EC placement
// params (RingVersion / ECData / ECParity / NodeIDs) survive marshal+unmarshal.
// These fields are required by appendableReader to reconstruct the coalesced
// blob via the EC reader (PutObject GET path).
func TestCoalescedShardRefECParamsRoundTrip(t *testing.T) {
	in := objectMeta{
		Key: "a", Size: 4096, IsAppendable: true,
		Coalesced: []CoalescedShardRef{{
			CoalescedID: "c1", Size: 4096, ETag: "etag-c1",
			ShardKey:    "a/coalesced/c1",
			Version:     1,
			RingVersion: 7,
			ECData:      4, ECParity: 2,
			NodeIDs: []string{"n1", "n2", "n3", "n4", "n5", "n6"},
		}},
	}
	raw, err := marshalObjectMeta(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := unmarshalObjectMeta(raw)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Coalesced) != 1 {
		t.Fatalf("Coalesced length = %d, want 1", len(got.Coalesced))
	}
	c := got.Coalesced[0]
	if c.RingVersion != 7 || c.ECData != 4 || c.ECParity != 2 {
		t.Fatalf("EC params mismatch: ringVer=%d ecData=%d ecParity=%d", c.RingVersion, c.ECData, c.ECParity)
	}
	if len(c.NodeIDs) != 6 || c.NodeIDs[0] != "n1" || c.NodeIDs[5] != "n6" {
		t.Fatalf("NodeIDs mismatch: %v", c.NodeIDs)
	}
}

// TestCoalesceSegmentsCmdECParamsRoundTrip ensures the Raft command carries
// the EC placement decision so the FSM apply can store it on the
// CoalescedShardRef. Without these fields the read path cannot reconstruct
// the coalesced blob.
func TestCoalesceSegmentsCmdECParamsRoundTrip(t *testing.T) {
	in := CoalesceSegmentsCmd{
		Bucket: "b", Key: "a", CoalescedID: "c1",
		ShardKey: "a/coalesced/c1", Size: 4096, ETag: "etag",
		ConsumedSegmentIDs: []string{"s1", "s2"},
		Placement:          []string{"n1", "n2", "n3", "n4", "n5", "n6"},
		ECData:             4, ECParity: 2,
		RingVersion: 9,
	}
	raw, err := encodeCoalesceSegmentsCmd(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodeCoalesceSegmentsCmd(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ECData != 4 || got.ECParity != 2 || got.RingVersion != 9 {
		t.Fatalf("EC params mismatch: ecData=%d ecParity=%d ringVer=%d", got.ECData, got.ECParity, got.RingVersion)
	}
	if len(got.Placement) != 6 || got.Placement[0] != "n1" || got.Placement[5] != "n6" {
		t.Fatalf("Placement mismatch: %v", got.Placement)
	}
}

func TestPutObjectMetaCmd_PartsRoundtrip(t *testing.T) {
	cmd := PutObjectMetaCmd{
		Bucket: "b", Key: "k", VersionID: "v", PlacementGroupID: "g",
		Size: 12, ETag: "etag-full",
		Parts: []storage.MultipartPartEntry{
			{PartNumber: 1, Size: 5, ETag: "p1"},
			{PartNumber: 2, Size: 7, ETag: "p2"},
		},
	}
	raw, err := encodePutObjectMetaCmd(cmd)
	require.NoError(t, err)
	got, err := decodePutObjectMetaCmd(raw)
	require.NoError(t, err)
	require.Equal(t, cmd.Parts, got.Parts)
}

func TestObjectMeta_PartsRoundtrip(t *testing.T) {
	m := objectMeta{
		Key: "k", Size: 12, ETag: "etag-full", PlacementGroupID: "g",
		Parts: []storage.MultipartPartEntry{
			{PartNumber: 1, Size: 5, ETag: "p1"},
			{PartNumber: 2, Size: 7, ETag: "p2"},
		},
	}
	raw, err := marshalObjectMeta(m)
	require.NoError(t, err)
	got, err := unmarshalObjectMeta(raw)
	require.NoError(t, err)
	require.Equal(t, m.Parts, got.Parts)
}

func TestObjectMeta_PartsLegacyDecode(t *testing.T) {
	m := objectMeta{Key: "k", Size: 1, ETag: "e", PlacementGroupID: "g"}
	raw, err := marshalObjectMeta(m)
	require.NoError(t, err)
	got, err := unmarshalObjectMeta(raw)
	require.NoError(t, err)
	require.Nil(t, got.Parts)
}

func TestCodec_ObjectMeta_TagsRoundTrip(t *testing.T) {
	original := objectMeta{
		Key:          "k",
		Size:         5,
		ContentType:  "text/plain",
		ETag:         "etag",
		LastModified: 1700000000,
		Tags: []storage.Tag{
			{Key: "env", Value: "prod"},
			{Key: "owner", Value: "alice"},
		},
	}
	raw, err := marshalObjectMeta(original)
	require.NoError(t, err)
	got, err := unmarshalObjectMeta(raw)
	require.NoError(t, err)
	require.Equal(t, original.Tags, got.Tags)
}

func TestCodec_SetObjectTagsCmd_RoundTrip(t *testing.T) {
	cmd := SetObjectTagsCmd{
		Bucket:    "b",
		Key:       "k",
		VersionID: "v1",
		Tags:      []storage.Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "alice"}},
	}
	raw, err := encodeSetObjectTagsCmd(cmd)
	require.NoError(t, err)
	got, err := decodeSetObjectTagsCmd(raw)
	require.NoError(t, err)
	require.Equal(t, cmd, got)
}

func TestCodec_SetObjectTagsCmd_EmptyTags_RoundTrip(t *testing.T) {
	cmd := SetObjectTagsCmd{Bucket: "b", Key: "k", VersionID: ""}
	raw, err := encodeSetObjectTagsCmd(cmd)
	require.NoError(t, err)
	got, err := decodeSetObjectTagsCmd(raw)
	require.NoError(t, err)
	require.Equal(t, cmd, got)
}
