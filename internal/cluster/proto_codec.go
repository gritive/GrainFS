package cluster

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// objectMeta is a local struct for serializing object metadata to BadgerDB.
type objectMeta struct {
	Key          string
	Size         int64
	ContentType  string
	ETag         string
	LastModified int64
}

// clusterMultipartMeta holds metadata about an in-progress multipart upload
// as stored in BadgerDB.
type clusterMultipartMeta struct {
	ContentType string
}

// --- Command encode/decode ---

// encodeCreateBucketCmd marshals a CreateBucketCmd to protobuf bytes.
func encodeCreateBucketCmd(c CreateBucketCmd) ([]byte, error) {
	pb := &clusterpb.CreateBucketCmd{Bucket: c.Bucket}
	return proto.Marshal(pb)
}

// decodeCreateBucketCmd unmarshals protobuf bytes into a CreateBucketCmd.
func decodeCreateBucketCmd(data []byte) (CreateBucketCmd, error) {
	var pb clusterpb.CreateBucketCmd
	if err := proto.Unmarshal(data, &pb); err != nil {
		return CreateBucketCmd{}, fmt.Errorf("unmarshal CreateBucketCmd: %w", err)
	}
	return CreateBucketCmd{Bucket: pb.Bucket}, nil
}

// encodeDeleteBucketCmd marshals a DeleteBucketCmd to protobuf bytes.
func encodeDeleteBucketCmd(c DeleteBucketCmd) ([]byte, error) {
	pb := &clusterpb.DeleteBucketCmd{Bucket: c.Bucket}
	return proto.Marshal(pb)
}

// decodeDeleteBucketCmd unmarshals protobuf bytes into a DeleteBucketCmd.
func decodeDeleteBucketCmd(data []byte) (DeleteBucketCmd, error) {
	var pb clusterpb.DeleteBucketCmd
	if err := proto.Unmarshal(data, &pb); err != nil {
		return DeleteBucketCmd{}, fmt.Errorf("unmarshal DeleteBucketCmd: %w", err)
	}
	return DeleteBucketCmd{Bucket: pb.Bucket}, nil
}

// encodePutObjectMetaCmd marshals a PutObjectMetaCmd to protobuf bytes.
func encodePutObjectMetaCmd(c PutObjectMetaCmd) ([]byte, error) {
	pb := &clusterpb.PutObjectMetaCmd{
		Bucket:      c.Bucket,
		Key:         c.Key,
		Size:        c.Size,
		ContentType: c.ContentType,
		Etag:        c.ETag,
		ModTime:     c.ModTime,
	}
	return proto.Marshal(pb)
}

// decodePutObjectMetaCmd unmarshals protobuf bytes into a PutObjectMetaCmd.
func decodePutObjectMetaCmd(data []byte) (PutObjectMetaCmd, error) {
	var pb clusterpb.PutObjectMetaCmd
	if err := proto.Unmarshal(data, &pb); err != nil {
		return PutObjectMetaCmd{}, fmt.Errorf("unmarshal PutObjectMetaCmd: %w", err)
	}
	return PutObjectMetaCmd{
		Bucket:      pb.Bucket,
		Key:         pb.Key,
		Size:        pb.Size,
		ContentType: pb.ContentType,
		ETag:        pb.Etag,
		ModTime:     pb.ModTime,
	}, nil
}

// encodeDeleteObjectCmd marshals a DeleteObjectCmd to protobuf bytes.
func encodeDeleteObjectCmd(c DeleteObjectCmd) ([]byte, error) {
	pb := &clusterpb.DeleteObjectCmd{Bucket: c.Bucket, Key: c.Key}
	return proto.Marshal(pb)
}

// decodeDeleteObjectCmd unmarshals protobuf bytes into a DeleteObjectCmd.
func decodeDeleteObjectCmd(data []byte) (DeleteObjectCmd, error) {
	var pb clusterpb.DeleteObjectCmd
	if err := proto.Unmarshal(data, &pb); err != nil {
		return DeleteObjectCmd{}, fmt.Errorf("unmarshal DeleteObjectCmd: %w", err)
	}
	return DeleteObjectCmd{Bucket: pb.Bucket, Key: pb.Key}, nil
}

// encodeCreateMultipartUploadCmd marshals a CreateMultipartUploadCmd to protobuf bytes.
func encodeCreateMultipartUploadCmd(c CreateMultipartUploadCmd) ([]byte, error) {
	pb := &clusterpb.CreateMultipartUploadCmd{
		UploadId:    c.UploadID,
		Bucket:      c.Bucket,
		Key:         c.Key,
		ContentType: c.ContentType,
		CreatedAt:   c.CreatedAt,
	}
	return proto.Marshal(pb)
}

// decodeCreateMultipartUploadCmd unmarshals protobuf bytes into a CreateMultipartUploadCmd.
func decodeCreateMultipartUploadCmd(data []byte) (CreateMultipartUploadCmd, error) {
	var pb clusterpb.CreateMultipartUploadCmd
	if err := proto.Unmarshal(data, &pb); err != nil {
		return CreateMultipartUploadCmd{}, fmt.Errorf("unmarshal CreateMultipartUploadCmd: %w", err)
	}
	return CreateMultipartUploadCmd{
		UploadID:    pb.UploadId,
		Bucket:      pb.Bucket,
		Key:         pb.Key,
		ContentType: pb.ContentType,
		CreatedAt:   pb.CreatedAt,
	}, nil
}

// encodeCompleteMultipartCmd marshals a CompleteMultipartCmd to protobuf bytes.
func encodeCompleteMultipartCmd(c CompleteMultipartCmd) ([]byte, error) {
	pb := &clusterpb.CompleteMultipartCmd{
		Bucket:      c.Bucket,
		Key:         c.Key,
		UploadId:    c.UploadID,
		Size:        c.Size,
		ContentType: c.ContentType,
		Etag:        c.ETag,
		ModTime:     c.ModTime,
	}
	return proto.Marshal(pb)
}

// decodeCompleteMultipartCmd unmarshals protobuf bytes into a CompleteMultipartCmd.
func decodeCompleteMultipartCmd(data []byte) (CompleteMultipartCmd, error) {
	var pb clusterpb.CompleteMultipartCmd
	if err := proto.Unmarshal(data, &pb); err != nil {
		return CompleteMultipartCmd{}, fmt.Errorf("unmarshal CompleteMultipartCmd: %w", err)
	}
	return CompleteMultipartCmd{
		Bucket:      pb.Bucket,
		Key:         pb.Key,
		UploadID:    pb.UploadId,
		Size:        pb.Size,
		ContentType: pb.ContentType,
		ETag:        pb.Etag,
		ModTime:     pb.ModTime,
	}, nil
}

// encodeAbortMultipartCmd marshals an AbortMultipartCmd to protobuf bytes.
func encodeAbortMultipartCmd(c AbortMultipartCmd) ([]byte, error) {
	pb := &clusterpb.AbortMultipartCmd{
		Bucket:   c.Bucket,
		Key:      c.Key,
		UploadId: c.UploadID,
	}
	return proto.Marshal(pb)
}

// decodeAbortMultipartCmd unmarshals protobuf bytes into an AbortMultipartCmd.
func decodeAbortMultipartCmd(data []byte) (AbortMultipartCmd, error) {
	var pb clusterpb.AbortMultipartCmd
	if err := proto.Unmarshal(data, &pb); err != nil {
		return AbortMultipartCmd{}, fmt.Errorf("unmarshal AbortMultipartCmd: %w", err)
	}
	return AbortMultipartCmd{
		Bucket:   pb.Bucket,
		Key:      pb.Key,
		UploadID: pb.UploadId,
	}, nil
}

// --- BucketPolicy codec ---

func encodeSetBucketPolicyCmd(c SetBucketPolicyCmd) ([]byte, error) {
	pb := &clusterpb.SetBucketPolicyCmd{Bucket: c.Bucket, PolicyJson: c.PolicyJSON}
	return proto.Marshal(pb)
}

func decodeSetBucketPolicyCmd(data []byte) (SetBucketPolicyCmd, error) {
	var pb clusterpb.SetBucketPolicyCmd
	if err := proto.Unmarshal(data, &pb); err != nil {
		return SetBucketPolicyCmd{}, fmt.Errorf("unmarshal SetBucketPolicyCmd: %w", err)
	}
	return SetBucketPolicyCmd{Bucket: pb.Bucket, PolicyJSON: pb.PolicyJson}, nil
}

func encodeDeleteBucketPolicyCmd(c DeleteBucketPolicyCmd) ([]byte, error) {
	pb := &clusterpb.DeleteBucketPolicyCmd{Bucket: c.Bucket}
	return proto.Marshal(pb)
}

func decodeDeleteBucketPolicyCmd(data []byte) (DeleteBucketPolicyCmd, error) {
	var pb clusterpb.DeleteBucketPolicyCmd
	if err := proto.Unmarshal(data, &pb); err != nil {
		return DeleteBucketPolicyCmd{}, fmt.Errorf("unmarshal DeleteBucketPolicyCmd: %w", err)
	}
	return DeleteBucketPolicyCmd{Bucket: pb.Bucket}, nil
}

// --- ObjectMeta codec ---

// marshalObjectMeta serializes an objectMeta to protobuf bytes.
func marshalObjectMeta(m objectMeta) ([]byte, error) {
	pb := &clusterpb.ObjectMeta{
		Key:          m.Key,
		Size:         m.Size,
		ContentType:  m.ContentType,
		Etag:         m.ETag,
		LastModified: m.LastModified,
	}
	return proto.Marshal(pb)
}

// unmarshalObjectMeta deserializes protobuf bytes into an objectMeta.
func unmarshalObjectMeta(data []byte) (objectMeta, error) {
	var pb clusterpb.ObjectMeta
	if err := proto.Unmarshal(data, &pb); err != nil {
		return objectMeta{}, fmt.Errorf("unmarshal ObjectMeta: %w", err)
	}
	return objectMeta{
		Key:          pb.Key,
		Size:         pb.Size,
		ContentType:  pb.ContentType,
		ETag:         pb.Etag,
		LastModified: pb.LastModified,
	}, nil
}

// --- SnapshotState codec ---

// marshalSnapshotState serializes a map[string][]byte to protobuf bytes.
func marshalSnapshotState(state map[string][]byte) ([]byte, error) {
	pb := &clusterpb.SnapshotState{Entries: state}
	return proto.Marshal(pb)
}

// unmarshalSnapshotState deserializes protobuf bytes into a map[string][]byte.
func unmarshalSnapshotState(data []byte) (map[string][]byte, error) {
	var pb clusterpb.SnapshotState
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, fmt.Errorf("unmarshal SnapshotState: %w", err)
	}
	return pb.Entries, nil
}

// --- ClusterMultipartMeta codec ---

// marshalClusterMultipartMeta serializes a clusterMultipartMeta to protobuf bytes.
func marshalClusterMultipartMeta(m clusterMultipartMeta) ([]byte, error) {
	pb := &clusterpb.MultipartMeta{ContentType: m.ContentType}
	return proto.Marshal(pb)
}

// unmarshalClusterMultipartMeta deserializes protobuf bytes into a clusterMultipartMeta.
func unmarshalClusterMultipartMeta(data []byte) (clusterMultipartMeta, error) {
	var pb clusterpb.MultipartMeta
	if err := proto.Unmarshal(data, &pb); err != nil {
		return clusterMultipartMeta{}, fmt.Errorf("unmarshal MultipartMeta: %w", err)
	}
	return clusterMultipartMeta{ContentType: pb.ContentType}, nil
}

// --- Payload encoding dispatch ---

// encodePayload marshals a command payload to protobuf bytes based on command type.
func encodePayload(cmdType CommandType, payload any) ([]byte, error) {
	switch cmdType {
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
	default:
		return nil, fmt.Errorf("unknown command type: %d", cmdType)
	}
}
