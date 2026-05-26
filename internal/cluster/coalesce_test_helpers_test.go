package cluster

import "context"

// currentSize returns obj.Size when present, 0 otherwise.
func currentSize(t clusterTestTB, b *DistributedBackend, bucket, key string) int64 {
	t.Helper()
	obj, err := b.HeadObject(context.Background(), bucket, key)
	if err != nil {
		return 0
	}
	return obj.Size
}
