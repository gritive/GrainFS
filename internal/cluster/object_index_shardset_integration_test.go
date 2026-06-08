package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// At N=1 the façade over a real *MetaFSM must yield identical lookups to
// calling the MetaFSM directly — the behavior-neutral seam proof for PUT→GET
// routing semantics.
func TestObjectIndexShardSet_N1_IdenticalToDirectMetaFSM(t *testing.T) {
	fsm := NewMetaFSM()
	entry := ObjectIndexEntry{Bucket: "b", Key: "k", VersionID: "v1", PlacementGroupID: "g0", Size: 7}
	payload, err := encodeMetaPutObjectIndexCmd(entry, false)
	require.NoError(t, err)
	require.NoError(t, fsm.applyPutObjectIndex(payload))

	set, err := NewObjectIndexShardSet([]ObjectIndexShard{{Reader: fsm, Writer: nil}})
	require.NoError(t, err)

	directLatest, directOK := fsm.ObjectIndexLatest("b", "k")
	setLatest, setOK := set.ObjectIndexLatest("b", "k")
	assert.Equal(t, directOK, setOK)
	assert.Equal(t, directLatest, setLatest)

	directVer, directVOK := fsm.ObjectIndexVersion("b", "k", "v1")
	setVer, setVOK := set.ObjectIndexVersion("b", "k", "v1")
	assert.Equal(t, directVOK, setVOK)
	assert.Equal(t, directVer, setVer)
}
