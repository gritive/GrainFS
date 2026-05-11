package dedup

import (
	"fmt"
)

const (
	snapBlockPrefix   = "vd:s:"  // vd:s:{vol}:{snapID}:{blkNum:012d} → canonicalKey
	snapStatePrefix   = "vd:ss:" // vd:ss:{vol}:{snapID}              → byte(SnapshotState)
	snapMetaPrefix    = "vd:sm:" // vd:sm:{vol}:{snapID}              → binary SnapshotMeta
	rollbackStatePref = "vd:rb:" // vd:rb:{vol}:{snapID}              → byte (1=in-progress)
	clonePrefix       = "vd:cl:" // vd:cl:{dstVol}                    → byte (1=in-progress)
)

func snapBlockKey(vol, snapID string, blkNum int64) []byte {
	return []byte(fmt.Sprintf("%s%s:%s:%012d", snapBlockPrefix, vol, snapID, blkNum))
}

func snapBlockPrefixKey(vol, snapID string) []byte {
	return []byte(snapBlockPrefix + vol + ":" + snapID + ":")
}

func snapStateKey(vol, snapID string) []byte {
	return []byte(snapStatePrefix + vol + ":" + snapID)
}

func snapStatePrefixKey() []byte { return []byte(snapStatePrefix) }

func snapMetaKey(vol, snapID string) []byte {
	return []byte(snapMetaPrefix + vol + ":" + snapID)
}

func snapMetaPrefixKey(vol string) []byte {
	return []byte(snapMetaPrefix + vol + ":")
}

func rollbackStateKey(vol, snapID string) []byte {
	return []byte(rollbackStatePref + vol + ":" + snapID)
}

func rollbackStatePrefixKey() []byte { return []byte(rollbackStatePref) }

func cloneStateKey(dstVol string) []byte {
	return []byte(clonePrefix + dstVol)
}

func cloneStatePrefixKey() []byte { return []byte(clonePrefix) }

func errNotImpl(name string) error { return fmt.Errorf("dedup: %s not implemented", name) }

// Stub implementations — real logic comes in subsequent tasks (B3-B7).

func (b *badgerIndex) SnapshotBegin(vol, snapID string) error {
	return errNotImpl("SnapshotBegin")
}

func (b *badgerIndex) SnapshotAppendChunk(vol, snapID string, entries []SnapshotBlockEntry) error {
	return errNotImpl("SnapshotAppendChunk")
}

func (b *badgerIndex) SnapshotCommit(vol, snapID string, meta SnapshotMeta) error {
	return errNotImpl("SnapshotCommit")
}

func (b *badgerIndex) SnapshotAbort(vol, snapID string) ([]string, error) {
	return nil, errNotImpl("SnapshotAbort")
}

func (b *badgerIndex) SnapshotIter(vol, snapID string, fn func(blkNum int64, canonical string) error) error {
	return errNotImpl("SnapshotIter")
}

func (b *badgerIndex) SnapshotReadBlock(vol, snapID string, blkNum int64) (string, bool, error) {
	return "", false, errNotImpl("SnapshotReadBlock")
}

func (b *badgerIndex) SnapshotDelete(vol, snapID string) ([]string, error) {
	return nil, errNotImpl("SnapshotDelete")
}

func (b *badgerIndex) SnapshotListInProgress() ([]struct{ Vol, SnapID string }, error) {
	return nil, errNotImpl("SnapshotListInProgress")
}

func (b *badgerIndex) SnapshotListPendingRollbacks() ([]struct{ Vol, SnapID string }, error) {
	return nil, errNotImpl("SnapshotListPendingRollbacks")
}

func (b *badgerIndex) SnapshotListPendingClones() ([]string, error) {
	return nil, errNotImpl("SnapshotListPendingClones")
}

func (b *badgerIndex) SnapshotRollback(vol, snapID string) ([]string, error) {
	return nil, errNotImpl("SnapshotRollback")
}

func (b *badgerIndex) SnapshotClone(srcVol, dstVol string) error {
	return errNotImpl("SnapshotClone")
}

func (b *badgerIndex) IterLiveBlocks(vol string, fn func(blkNum int64, canonical string) error) error {
	return errNotImpl("IterLiveBlocks")
}

func (b *badgerIndex) SnapshotPutMeta(vol string, meta SnapshotMeta) error {
	return errNotImpl("SnapshotPutMeta")
}

func (b *badgerIndex) SnapshotList(vol string) ([]SnapshotMeta, error) {
	return nil, errNotImpl("SnapshotList")
}
