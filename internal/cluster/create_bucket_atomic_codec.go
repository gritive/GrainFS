package cluster

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// EncodeMetaCreateBucketWithPolicyAttachCmd serializes a
// CreateBucketWithPolicyAttach inner payload (the data bytes of a MetaCmd
// envelope). bucket is always required; attachSA and attachPolicy may be empty
// (create-only, no IAM half).
func EncodeMetaCreateBucketWithPolicyAttachCmd(bucket, attachSA, attachPolicy string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(bucket)
	saOff := b.CreateString(attachSA)
	polOff := b.CreateString(attachPolicy)
	clusterpb.MetaCreateBucketWithPolicyAttachCmdStart(b)
	clusterpb.MetaCreateBucketWithPolicyAttachCmdAddBucket(b, bucketOff)
	clusterpb.MetaCreateBucketWithPolicyAttachCmdAddAttachSa(b, saOff)
	clusterpb.MetaCreateBucketWithPolicyAttachCmdAddAttachPolicy(b, polOff)
	return fbFinish(b, clusterpb.MetaCreateBucketWithPolicyAttachCmdEnd(b)), nil
}

// decodeMetaCreateBucketWithPolicyAttachCmd parses the inner payload and
// returns bucket, SA, and policy name. Returned strings are independent copies
// of the input buffer.
func decodeMetaCreateBucketWithPolicyAttachCmd(data []byte) (bucket, sa, policy string, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaCreateBucketWithPolicyAttachCmd {
		return clusterpb.GetRootAsMetaCreateBucketWithPolicyAttachCmd(d, 0)
	})
	if err != nil {
		return "", "", "", fmt.Errorf("create_bucket_atomic_codec: decode: %w", err)
	}
	return string(t.Bucket()), string(t.AttachSa()), string(t.AttachPolicy()), nil
}
