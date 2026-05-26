package volume

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// plannerForTest builds a blockIOPlanner with fake dependencies.
func plannerForTest(store *fakeBlockStore) blockIOPlanner {
	return blockIOPlanner{objects: store}
}

func TestPlanWrite_DirectNewBlock(t *testing.T) {
	store := newFakeBlockStore() // empty — HeadObject returns not-found
	pl := plannerForTest(store)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := make([]byte, DefaultBlockSize)

	actions, err := pl.planWrite("v", vol, p, 0, 0, 0, false)

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
	pl := plannerForTest(store)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := make([]byte, DefaultBlockSize)

	actions, err := pl.planWrite("v", vol, p, 0, 0, 0, false)

	require.NoError(t, err)
	require.Len(t, actions, 1)
	require.False(t, actions[0].IsNew)
}

func TestPlanWrite_AsyncEligibleWhenFlagSet(t *testing.T) {
	store := newFakeBlockStore()
	pl := plannerForTest(store)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := make([]byte, DefaultBlockSize)

	actions, err := pl.planWrite("v", vol, p, 0, 0, 0, true /*asyncEligible*/)

	require.NoError(t, err)
	require.Len(t, actions, 1)
	require.True(t, actions[0].Async)
}

func TestPlanWrite_QuotaExceededWhenNewBlocksExceedLimit(t *testing.T) {
	store := newFakeBlockStore() // all blocks new
	pl := plannerForTest(store)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 4), BlockSize: DefaultBlockSize}
	p := make([]byte, DefaultBlockSize*2) // 2 new blocks needed

	// quota allows only 1 block
	_, err := pl.planWrite("v", vol, p, 0, 0, int64(DefaultBlockSize), false)

	require.ErrorIs(t, err, ErrPoolQuotaExceeded)
}

func TestPlanWrite_QuotaOkWhenBlocksAlreadyAllocated(t *testing.T) {
	store := newFakeBlockStore()
	// block 0 already exists — no new allocation
	store.objects[blockKey("v", 0)] = make([]byte, DefaultBlockSize)
	pl := plannerForTest(store)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 4), BlockSize: DefaultBlockSize}
	p := make([]byte, DefaultBlockSize)

	// quota only allows 0 new blocks; since the block exists this should pass
	actions, err := pl.planWrite("v", vol, p, 0, int64(DefaultBlockSize), int64(DefaultBlockSize), false)

	require.NoError(t, err)
	require.Len(t, actions, 1)
	require.False(t, actions[0].IsNew)
}

func TestPlanWrite_MultipleBlocks(t *testing.T) {
	store := newFakeBlockStore()
	pl := plannerForTest(store)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 4), BlockSize: DefaultBlockSize}
	p := make([]byte, DefaultBlockSize*3)

	actions, err := pl.planWrite("v", vol, p, 0, 0, 0, false)

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
	pl := plannerForTest(store)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := make([]byte, 100) // 100 bytes into block 0

	actions, err := pl.planWrite("v", vol, p, 512, 0, 0, false)

	require.NoError(t, err)
	require.Len(t, actions, 1)
	a := actions[0]
	require.Equal(t, int64(512), a.BlkOff)
	require.Equal(t, 100, a.CanWrite)
	require.Equal(t, 0, a.DataStart)
}

func TestPlanWrite_PartialDirectNoHeadObject(t *testing.T) {
	store := newFakeBlockStore()
	pl := plannerForTest(store)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := make([]byte, 100) // partial block

	_, err := pl.planWrite("v", vol, p, 512, 0, 0, false)

	require.NoError(t, err)
	require.Empty(t, store.heads, "partial block must not call HeadObject")
}
