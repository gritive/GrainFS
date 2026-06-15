package cluster

// Test-only Badger key builders. Production code uses the FSM's KeySpace
// (f.keys.*, which applies the per-group prefix); these package-level helpers
// build the equivalent UNPREFIXED keys so unit tests can assert against an
// empty-keyspace FSM without instantiating a full keyspace. They live in a
// _test.go file precisely because nothing in production references them.

func bucketKey(bucket string) []byte { return []byte("bucket:" + bucket) }

func bucketPolicyKey(bucket string) []byte { return []byte("policy:" + bucket) }

func bucketVerKey(bucket string) []byte { return []byte("bucketver:" + bucket) }

func objectMetaKey(bucket, key string) []byte { return []byte("obj:" + bucket + "/" + key) }

// objectMetaKeyV returns the per-version metadata key:
//
//	obj:{bucket}/{key}/{versionID}
func objectMetaKeyV(bucket, key, versionID string) []byte {
	return []byte("obj:" + bucket + "/" + key + "/" + versionID)
}

// latestKey points to the current latest version id for an object, or the
// literal "DEL" marker when the object has been tombstoned (soft-deleted).
func latestKey(bucket, key string) []byte {
	return []byte("lat:" + bucket + "/" + key)
}

func multipartKey(uploadID string) []byte { return []byte("mpu:" + uploadID) }

func shardPlacementKey(bucket, key string) []byte {
	return []byte("placement:" + bucket + "/" + key)
}
