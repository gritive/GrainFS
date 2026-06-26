package cluster

// Test-only Badger key builders. Production code uses the FSM's KeySpace
// (f.keys.*, which applies the per-group prefix); these package-level helpers
// build the equivalent UNPREFIXED keys so unit tests can assert against an
// empty-keyspace FSM without instantiating a full keyspace. They live in a
// _test.go file precisely because nothing in production references them.

func bucketKey(bucket string) []byte { return []byte("bucket:" + bucket) }

func objectMetaKey(bucket, key string) []byte { return []byte("obj:" + bucket + "/" + key) }

func shardPlacementKey(bucket, key string) []byte {
	return []byte("placement:" + bucket + "/" + key)
}
