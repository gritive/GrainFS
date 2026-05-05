package cluster

import (
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/scrubber"
)

func encodeScrubTriggerForTest(t *testing.T, sid, bucket, prefix string, scope int, dryRun bool, requestedAt int64, origNode string) []byte {
	t.Helper()
	b := flatbuffers.NewBuilder(64)
	sidOff := b.CreateString(sid)
	bktOff := b.CreateString(bucket)
	pfxOff := b.CreateString(prefix)
	nodeOff := b.CreateString(origNode)
	clusterpb.MetaScrubTriggerCmdStart(b)
	clusterpb.MetaScrubTriggerCmdAddSessionId(b, sidOff)
	clusterpb.MetaScrubTriggerCmdAddBucket(b, bktOff)
	clusterpb.MetaScrubTriggerCmdAddKeyPrefix(b, pfxOff)
	clusterpb.MetaScrubTriggerCmdAddScope(b, int32(scope))
	clusterpb.MetaScrubTriggerCmdAddDryRun(b, dryRun)
	clusterpb.MetaScrubTriggerCmdAddRequestedAt(b, requestedAt)
	clusterpb.MetaScrubTriggerCmdAddOriginatorNodeId(b, nodeOff)
	b.Finish(clusterpb.MetaScrubTriggerCmdEnd(b))
	return b.FinishedBytes()
}

func TestMetaFSM_ApplyScrubTrigger_RecentEntry_FiresCallback(t *testing.T) {
	f := NewMetaFSM()
	var got scrubber.ScrubTriggerEntry
	fired := false
	f.SetOnScrubTrigger(func(e scrubber.ScrubTriggerEntry) {
		got = e
		fired = true
	})

	data := encodeScrubTriggerForTest(t, "sid-1", "bk", "pfx", 0, false, time.Now().Unix(), "n1")
	require.NoError(t, f.applyScrubTrigger(data))

	require.True(t, fired)
	require.Equal(t, "sid-1", got.SessionID)
	require.Equal(t, "bk", got.Bucket)
	require.Equal(t, "pfx", got.KeyPrefix)
	require.Equal(t, scrubber.ScopeFull, got.Scope)
	require.Equal(t, "n1", got.OriginatorNodeID)
}

func TestMetaFSM_ApplyScrubTrigger_StaleEntry_Skipped(t *testing.T) {
	f := NewMetaFSM()
	fired := false
	f.SetOnScrubTrigger(func(scrubber.ScrubTriggerEntry) { fired = true })

	staleAt := time.Now().Add(-2 * time.Hour).Unix()
	data := encodeScrubTriggerForTest(t, "sid-2", "bk", "", 0, false, staleAt, "n1")
	require.NoError(t, f.applyScrubTrigger(data))

	require.False(t, fired, "stale entry must skip callback")
}

func TestMetaFSM_ApplyScrubTrigger_NoCallback_Noop(t *testing.T) {
	f := NewMetaFSM()
	data := encodeScrubTriggerForTest(t, "sid-3", "bk", "", 0, false, time.Now().Unix(), "n1")
	require.NoError(t, f.applyScrubTrigger(data))
}

func TestMetaFSM_ApplyScrubTrigger_EmptyPayload_Errors(t *testing.T) {
	f := NewMetaFSM()
	err := f.applyScrubTrigger(nil)
	require.Error(t, err)
}
