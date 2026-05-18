package eventstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEvent_RoundTripFB(t *testing.T) {
	e := Event{
		Timestamp:     1700000000_123_456_789,
		Type:          EventTypeS3,
		Action:        EventActionPut,
		Bucket:        "b",
		Key:           "k",
		Size:          1024,
		ID:            "evt-1",
		Phase:         "startup",
		Outcome:       "success",
		ShardID:       3,
		PeerID:        "peer-A",
		BytesRepaired: 4096,
		DurationMs:    250,
		ErrCode:       "orphan_tmp",
		CorrelationID: "corr-xyz",
		VersionID:     "v7",
		RemovedID:     "peer-B",
		Force:         true,
	}
	data, err := encodeEvent(e)
	require.NoError(t, err)
	got, err := decodeEventStorage(data)
	require.NoError(t, err)
	require.Equal(t, e, got)
}
