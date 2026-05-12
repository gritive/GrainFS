package volume

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/volume/dedup"
	"github.com/stretchr/testify/require"
)

// plannerForTest builds a blockIOPlanner with fake dependencies.
func plannerForTest(store *fakeBlockStore, di blockDedupIndex) blockIOPlanner {
	return blockIOPlanner{objects: store, dedup: di}
}

func TestPlanWrite_DirectNewBlock(t *testing.T) {
	store := newFakeBlockStore() // empty — HeadObject returns not-found
	pl := plannerForTest(store, nil)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := make([]byte, DefaultBlockSize)

	actions, err := pl.planWrite("v", vol, p, 0, nil, 0, 0, false)

	require.NoError(t, err)
	require.Len(t, actions, 1)
	a := actions[0]
	require.Equal(t, ActionDirect, a.Kind)
	require.Equal(t, int64(0), a.BlkNum)
	require.True(t, a.IsNew)
	require.False(t, a.Async)
	require.Equal(t, blockKey("v", 0), a.Key)
}

func TestPlanWrite_DirectExistingBlock(t *testing.T) {
	store := newFakeBlockStore()
	store.objects[blockKey("v", 0)] = make([]byte, DefaultBlockSize) // block exists
	pl := plannerForTest(store, nil)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := make([]byte, DefaultBlockSize)

	actions, err := pl.planWrite("v", vol, p, 0, nil, 0, 0, false)

	require.NoError(t, err)
	require.Len(t, actions, 1)
	require.False(t, actions[0].IsNew)
}

func TestPlanWrite_AsyncEligibleWhenFlagSet(t *testing.T) {
	store := newFakeBlockStore()
	pl := plannerForTest(store, nil)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := make([]byte, DefaultBlockSize)

	actions, err := pl.planWrite("v", vol, p, 0, nil, 0, 0, true /*asyncEligible*/)

	require.NoError(t, err)
	require.Len(t, actions, 1)
	require.True(t, actions[0].Async)
}

func TestPlanWrite_CowModeWhenSnapshotsExist(t *testing.T) {
	store := newFakeBlockStore()
	pl := plannerForTest(store, nil)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize, SnapshotCount: 1}
	liveMap := map[int64]string{0: physicalKey("v", 0, nil)}
	p := make([]byte, DefaultBlockSize)

	actions, err := pl.planWrite("v", vol, p, 0, liveMap, 0, 0, false)

	require.NoError(t, err)
	require.Len(t, actions, 1)
	a := actions[0]
	require.Equal(t, ActionCow, a.Kind)
	// cowBlockKey generates a UUID each call; verify format prefix instead of exact match
	require.True(t, strings.HasPrefix(a.Key, fmt.Sprintf("%sv/blk_%012d_v", metaPrefix, 0)), "unexpected cow key: %s", a.Key)
}

func TestPlanWrite_DedupMode(t *testing.T) {
	store := newFakeBlockStore()
	di := &fakeDedupIndex{blocks: map[int64]string{}}
	pl := plannerForTest(store, di)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := make([]byte, DefaultBlockSize)

	actions, err := pl.planWrite("v", vol, p, 0, nil, 0, 0, false)

	require.NoError(t, err)
	require.Len(t, actions, 1)
	a := actions[0]
	require.Equal(t, ActionDedup, a.Kind)
	require.True(t, a.IsNew)
	require.NotEmpty(t, a.Key) // proposed UUID key
}

func TestPlanWrite_QuotaExceededWhenNewBlocksExceedLimit(t *testing.T) {
	store := newFakeBlockStore() // all blocks new
	pl := plannerForTest(store, nil)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 4), BlockSize: DefaultBlockSize}
	p := make([]byte, DefaultBlockSize*2) // 2 new blocks needed

	// quota allows only 1 block
	_, err := pl.planWrite("v", vol, p, 0, nil, 0, int64(DefaultBlockSize), false)

	require.ErrorIs(t, err, ErrPoolQuotaExceeded)
}

func TestPlanWrite_QuotaOkWhenBlocksAlreadyAllocated(t *testing.T) {
	store := newFakeBlockStore()
	// block 0 already exists — no new allocation
	store.objects[blockKey("v", 0)] = make([]byte, DefaultBlockSize)
	pl := plannerForTest(store, nil)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 4), BlockSize: DefaultBlockSize}
	p := make([]byte, DefaultBlockSize)

	// quota only allows 0 new blocks; since the block exists this should pass
	actions, err := pl.planWrite("v", vol, p, 0, nil, int64(DefaultBlockSize), int64(DefaultBlockSize), false)

	require.NoError(t, err)
	require.Len(t, actions, 1)
	require.False(t, actions[0].IsNew)
}

func TestPlanWrite_MultipleBlocks(t *testing.T) {
	store := newFakeBlockStore()
	pl := plannerForTest(store, nil)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 4), BlockSize: DefaultBlockSize}
	p := make([]byte, DefaultBlockSize*3)

	actions, err := pl.planWrite("v", vol, p, 0, nil, 0, 0, false)

	require.NoError(t, err)
	require.Len(t, actions, 3)
	for i, a := range actions {
		require.Equal(t, ActionDirect, a.Kind)
		require.Equal(t, int64(i), a.BlkNum)
		require.Equal(t, i*DefaultBlockSize, a.DataStart)
		require.Equal(t, DefaultBlockSize, a.CanWrite)
	}
}

func TestPlanWrite_PartialBlockAtOffset(t *testing.T) {
	store := newFakeBlockStore()
	pl := plannerForTest(store, nil)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := make([]byte, 100) // 100 bytes into block 0

	actions, err := pl.planWrite("v", vol, p, 512, nil, 0, 0, false)

	require.NoError(t, err)
	require.Len(t, actions, 1)
	a := actions[0]
	require.Equal(t, int64(512), a.BlkOff)
	require.Equal(t, 100, a.CanWrite)
	require.Equal(t, 0, a.DataStart)
}

// fakeDedupIndex is a test double for blockDedupIndex.
type fakeDedupIndex struct {
	blocks   map[int64]string // blkNum → canonical key; absent = not found
	writeErr error
}

func (d *fakeDedupIndex) ReadBlock(_ string, blkNum int64) (string, bool, error) {
	k, ok := d.blocks[blkNum]
	return k, ok, nil
}

func (d *fakeDedupIndex) WriteBlock(_ string, blkNum int64, _ [32]byte, newKey string) (dedup.WriteResult, error) {
	if d.writeErr != nil {
		return dedup.WriteResult{}, d.writeErr
	}
	canonical, exists := d.blocks[blkNum]
	if !exists {
		d.blocks[blkNum] = newKey
		canonical = newKey
	}
	return dedup.WriteResult{Canonical: canonical, IsNew: !exists}, nil
}

func (d *fakeDedupIndex) FreeBlock(_ string, blkNum int64) (string, bool, error) {
	key, ok := d.blocks[blkNum]
	if ok {
		delete(d.blocks, blkNum)
	}
	return key, ok, nil
}
