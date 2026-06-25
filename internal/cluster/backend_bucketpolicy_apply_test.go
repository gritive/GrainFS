package cluster

// backend_bucketpolicy_apply_test.go — Task 12: TestNotifyOnApplyFiresBucketPolicyInvalidate
// was removed. The onBucketPolicyApply field and SetOnBucketPolicyApply setter on
// DistributedBackend were retired: bucket control-plane moved to meta-raft, and
// policy invalidation is now driven solely by the meta post-commit hook. There
// is nothing left to test here.
