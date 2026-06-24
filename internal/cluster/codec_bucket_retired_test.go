package cluster

// codec_bucket_retired_test.go holds the FlatBuffer decode helpers for the
// retired group-0 bucket commands. These are test-only: the commands have no
// production proposer (Task 12: bucket control-plane moved to meta-raft) and
// their applies are retired no-ops. The encode path (encodeCreate/DeleteBucket*,
// encodeSetBucketPolicy*, encodeSetBucketVersioning) remains in codec.go because
// EncodeCommand's dispatch still accepts these command types for codec round-trip
// and replay-safe log tests. The decode functions are here because they are only
// called from codec_test.go tests.

import "github.com/gritive/GrainFS/internal/cluster/clusterpb"

func decodeCreateBucketCmd(data []byte) (CreateBucketCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.CreateBucketCmd {
		return clusterpb.GetRootAsCreateBucketCmd(d, 0)
	})
	if err != nil {
		return CreateBucketCmd{}, err
	}
	return CreateBucketCmd{Bucket: string(t.Bucket()), BypassReserved: t.BypassReserved()}, nil
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

func decodeSetBucketPolicyCmd(data []byte) (SetBucketPolicyCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.SetBucketPolicyCmd {
		return clusterpb.GetRootAsSetBucketPolicyCmd(d, 0)
	})
	if err != nil {
		return SetBucketPolicyCmd{}, err
	}
	return SetBucketPolicyCmd{Bucket: string(t.Bucket()), PolicyJSON: t.PolicyJsonBytes()}, nil
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
