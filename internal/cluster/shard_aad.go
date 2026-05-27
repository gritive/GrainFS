package cluster

import "github.com/gritive/GrainFS/internal/encrypt"

// ShardAADFields binds an EC shard's chunked at-rest encryption to its position
// (bucket, key, shardIdx). eccodec appends the intra-shard chunk ordinal. Domain
// DomainShard + clusterID are added by the DataEncryptor seam. This is the AAD
// shape CAS 4c's TransitionReseal reconstructs as oldAAD — do not change it
// without re-writing every shard. Exported so internal/cluster/putpipeline's
// cpupool shares the one definition (DRY: the shard AAD shape is load-bearing).
func ShardAADFields(bucket, key string, shardIdx int) []encrypt.AADField {
	return []encrypt.AADField{
		encrypt.FieldString(bucket),
		encrypt.FieldString(key),
		encrypt.FieldUint32(uint32(shardIdx)),
	}
}
