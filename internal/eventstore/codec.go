package eventstore

import (
	"errors"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/eventstore/eventstorepb"
)

var ErrLegacyStorageFormat = errors.New("eventstore: legacy storage format detected (wipe-and-restart required)")

func encodeEvent(e Event) ([]byte, error) {
	b := flatbuffers.NewBuilder(128)
	typOff := b.CreateString(e.Type)
	actOff := b.CreateString(e.Action)
	bucketOff := b.CreateString(e.Bucket)
	keyOff := b.CreateString(e.Key)
	eventstorepb.EventStart(b)
	eventstorepb.EventAddTimestampUnixNs(b, e.Timestamp)
	eventstorepb.EventAddType(b, typOff)
	eventstorepb.EventAddAction(b, actOff)
	eventstorepb.EventAddBucket(b, bucketOff)
	eventstorepb.EventAddKey(b, keyOff)
	eventstorepb.EventAddSize(b, e.Size)
	b.Finish(eventstorepb.EventEnd(b))
	return b.FinishedBytes(), nil
}

func decodeEventStorage(data []byte) (out Event, err error) {
	if len(data) > 0 && data[0] == '{' {
		return Event{}, ErrLegacyStorageFormat
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode event storage: malformed FB: %v", r)
		}
	}()
	t := eventstorepb.GetRootAsEvent(data, 0)
	return Event{
		Timestamp: t.TimestampUnixNs(),
		Type:      string(t.Type()),
		Action:    string(t.Action()),
		Bucket:    string(t.Bucket()),
		Key:       string(t.Key()),
		Size:      t.Size(),
	}, nil
}
