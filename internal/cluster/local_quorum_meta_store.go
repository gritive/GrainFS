package cluster

import (
	"github.com/gritive/GrainFS/internal/storage"
)

// LocalQuorumMetaStore owns node-local quorum-meta filesystem operations.
// It is semantic, not raw KV: local writes decode candidate and existing blobs
// and apply the package-level quorum-meta CAS/LWW decision before publishing.
type LocalQuorumMetaStore struct {
	dataDirs              []string
	quorumMetaTargetLocks keyedRWMutex
	syncDirHook           func(string) error
}

func NewLocalQuorumMetaStore(dataDirs []string) *LocalQuorumMetaStore {
	return &LocalQuorumMetaStore{dataDirs: dataDirs}
}

// quorumMetaTargetLock takes the per-target write lock that serializes the
// (LWW guard-read + os.Rename) critical section in the leaf writers and returns
// an unlock closure. Concurrent callers converge on a single lock per target
// path, and the lock is reclaimed once the last holder releases.
func (m *LocalQuorumMetaStore) quorumMetaTargetLock(target string) func() {
	return m.quorumMetaTargetLocks.lockWrite(target)
}

func (m *LocalQuorumMetaStore) fsyncDir(dir string) error {
	if m.syncDirHook != nil {
		return m.syncDirHook(dir)
	}
	return syncDir(dir)
}

func (s *ShardService) writeQuorumMetaLocal(bucket, key string, data []byte) error {
	return s.qmeta.writeQuorumMetaLocal(bucket, key, data)
}

func (s *ShardService) writeQuorumMetaLocalWithResult(bucket, key string, data []byte) (quorumMetaLocalWriteResult, error) {
	return s.qmeta.writeQuorumMetaLocalWithResult(bucket, key, data)
}

func (s *ShardService) writeQuorumMetaVersionLocal(bucket, versionSubpath string, data []byte) error {
	return s.qmeta.writeQuorumMetaVersionLocal(bucket, versionSubpath, data)
}

func (s *ShardService) readQuorumMetaRaw(bucket, key string) ([]byte, error) {
	return s.qmeta.readQuorumMetaRaw(bucket, key)
}

func (s *ShardService) readQuorumMetaVersionsLocal(bucket, key string) ([]PutObjectMetaCmd, error) {
	return s.qmeta.readQuorumMetaVersionsLocal(bucket, key)
}

func (s *ShardService) readQuorumMetaVersionsRawLocal(bucket, key string) ([][]byte, error) {
	return s.qmeta.readQuorumMetaVersionsRawLocal(bucket, key)
}

func (s *ShardService) ScanQuorumMetaBucket(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	return s.qmeta.ScanQuorumMetaBucket(bucket, prefix)
}

func (s *ShardService) scanQuorumMetaBucketStrict(bucket string) ([]PutObjectMetaCmd, error) {
	return s.qmeta.scanQuorumMetaBucketStrict(bucket)
}

func (s *ShardService) ScanQuorumMetaVersionsBucket(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	return s.qmeta.ScanQuorumMetaVersionsBucket(bucket, prefix)
}

func (s *ShardService) ScanQuorumMetaVersionsBucketAll(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	return s.qmeta.ScanQuorumMetaVersionsBucketAll(bucket, prefix)
}

func (s *ShardService) scanQuorumMetaVersionsBucketAllStrict(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	return s.qmeta.scanQuorumMetaVersionsBucketAllStrict(bucket, prefix)
}

func (s *ShardService) deleteQuorumMetaLocal(bucket, key string) error {
	return s.qmeta.deleteQuorumMetaLocal(bucket, key)
}

func (s *ShardService) deleteQuorumMetaVersionLocal(bucket, key, versionID string) error {
	return s.qmeta.deleteQuorumMetaVersionLocal(bucket, key, versionID)
}

func (s *ShardService) rollbackQuorumMetaLocalIfMatch(bucket, key string, expected []byte, previous []byte, hadPrevious bool) error {
	return s.qmeta.rollbackQuorumMetaLocalIfMatch(bucket, key, expected, previous, hadPrevious)
}

func (s *ShardService) decodeQuorumMetaBlob(data []byte) (*storage.Object, PlacementMeta, error) {
	return s.qmeta.decodeQuorumMetaBlob(data)
}

func (s *ShardService) decodeQuorumMetaCmdBlob(data []byte) (PutObjectMetaCmd, error) {
	return s.qmeta.decodeQuorumMetaCmdBlob(data)
}

func (s *ShardService) IterQuorumMetaECShardTargets(fn func(ECShardScanTarget) error) error {
	return s.qmeta.IterQuorumMetaECShardTargets(fn)
}
