package cluster

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// EncodeBucketPolicyPutPayload serializes a BucketPolicyPut inner payload (the
// data bytes of a MetaCmd envelope).
func EncodeBucketPolicyPutPayload(bucket string, docJSON []byte) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(bucket)
	docOff := b.CreateByteVector(docJSON)
	clusterpb.MetaBucketPolicyPutCmdStart(b)
	clusterpb.MetaBucketPolicyPutCmdAddBucket(b, bucketOff)
	clusterpb.MetaBucketPolicyPutCmdAddDocJson(b, docOff)
	return fbFinish(b, clusterpb.MetaBucketPolicyPutCmdEnd(b)), nil
}

// DecodeBucketPolicyPutPayload parses a BucketPolicyPut inner payload and
// returns the bucket name and doc JSON bytes. The returned doc bytes are a copy
// independent of the input buffer.
func DecodeBucketPolicyPutPayload(data []byte) (bucket string, docJSON []byte, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaBucketPolicyPutCmd {
		return clusterpb.GetRootAsMetaBucketPolicyPutCmd(d, 0)
	})
	if err != nil {
		return "", nil, fmt.Errorf("bucketpolicy_codec: BucketPolicyPut: %w", err)
	}
	rawDoc := t.DocJsonBytes()
	return string(t.Bucket()), append([]byte(nil), rawDoc...), nil
}

// EncodeBucketPolicyDeletePayload serializes a BucketPolicyDelete inner
// payload.
func EncodeBucketPolicyDeletePayload(bucket string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(bucket)
	clusterpb.MetaBucketPolicyDeleteCmdStart(b)
	clusterpb.MetaBucketPolicyDeleteCmdAddBucket(b, bucketOff)
	return fbFinish(b, clusterpb.MetaBucketPolicyDeleteCmdEnd(b)), nil
}

// DecodeBucketPolicyDeletePayload parses a BucketPolicyDelete inner payload
// and returns the bucket name.
func DecodeBucketPolicyDeletePayload(data []byte) (bucket string, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaBucketPolicyDeleteCmd {
		return clusterpb.GetRootAsMetaBucketPolicyDeleteCmd(d, 0)
	})
	if err != nil {
		return "", fmt.Errorf("bucketpolicy_codec: BucketPolicyDelete: %w", err)
	}
	return string(t.Bucket()), nil
}
