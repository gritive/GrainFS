package eventstore

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEvent_RoundTripFB(t *testing.T) {
	e := Event{
		Timestamp: 1700000000_123_456_789,
		Type:      EventTypeS3,
		Action:    EventActionPut,
		Bucket:    "b",
		Key:       "k",
		Size:      1024,
	}
	data, err := encodeEvent(e)
	require.NoError(t, err)
	got, err := decodeEventStorage(data)
	require.NoError(t, err)
	require.Equal(t, e, got)
}

func TestEvent_DecodeRejectsLegacyJSON(t *testing.T) {
	legacy := []byte(`{"ts":1700000000,"type":"s3"}`)
	_, err := decodeEventStorage(legacy)
	require.True(t, errors.Is(err, ErrLegacyStorageFormat))
}
