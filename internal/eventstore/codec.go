package eventstore

import (
	"errors"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/eventstore/eventstorepb"
)

var ErrLegacyStorageFormat = errors.New("eventstore: legacy storage format detected (wipe-and-restart required)")

func encodeEvent(e Event) ([]byte, error) {
	b := flatbuffers.NewBuilder(256)
	typOff := b.CreateString(e.Type)
	actOff := b.CreateString(e.Action)
	bucketOff := b.CreateString(e.Bucket)
	keyOff := b.CreateString(e.Key)
	idOff := b.CreateString(e.ID)
	phaseOff := b.CreateString(e.Phase)
	outcomeOff := b.CreateString(e.Outcome)
	peerOff := b.CreateString(e.PeerID)
	errCodeOff := b.CreateString(e.ErrCode)
	corrOff := b.CreateString(e.CorrelationID)
	versionOff := b.CreateString(e.VersionID)
	removedOff := b.CreateString(e.RemovedID)

	eventstorepb.EventStart(b)
	eventstorepb.EventAddTimestampUnixNs(b, e.Timestamp)
	eventstorepb.EventAddType(b, typOff)
	eventstorepb.EventAddAction(b, actOff)
	eventstorepb.EventAddBucket(b, bucketOff)
	eventstorepb.EventAddKey(b, keyOff)
	eventstorepb.EventAddSize(b, e.Size)
	eventstorepb.EventAddId(b, idOff)
	eventstorepb.EventAddPhase(b, phaseOff)
	eventstorepb.EventAddOutcome(b, outcomeOff)
	eventstorepb.EventAddShardId(b, e.ShardID)
	eventstorepb.EventAddPeerId(b, peerOff)
	eventstorepb.EventAddBytesRepaired(b, e.BytesRepaired)
	eventstorepb.EventAddDurationMs(b, e.DurationMs)
	eventstorepb.EventAddErrCode(b, errCodeOff)
	eventstorepb.EventAddCorrelationId(b, corrOff)
	eventstorepb.EventAddVersionId(b, versionOff)
	eventstorepb.EventAddRemovedId(b, removedOff)
	eventstorepb.EventAddForce(b, e.Force)
	b.Finish(eventstorepb.EventEnd(b))
	return b.FinishedBytes(), nil
}

func decodeEventStorage(data []byte) (out Event, err error) {
	// Whitespace-tolerant legacy JSON guard — matches the other 3 storage
	// decoders (packblob, cluster/quarantine, receipt). encoding/json accepts
	// leading space/tab/LF/CR, so legacy files that pass through a formatter
	// would otherwise slip past a strict `data[0] == '{'` check and surface
	// as a confusing "malformed FB" panic-message instead of the actionable
	// ErrLegacyStorageFormat sentinel.
	trimmed := data
	for len(trimmed) > 0 && (trimmed[0] == ' ' || trimmed[0] == '\t' || trimmed[0] == '\n' || trimmed[0] == '\r') {
		trimmed = trimmed[1:]
	}
	if len(trimmed) > 0 && trimmed[0] == '{' {
		return Event{}, ErrLegacyStorageFormat
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode event storage: malformed FB: %v", r)
		}
	}()
	t := eventstorepb.GetRootAsEvent(data, 0)
	return Event{
		Timestamp:     t.TimestampUnixNs(),
		Type:          string(t.Type()),
		Action:        string(t.Action()),
		Bucket:        string(t.Bucket()),
		Key:           string(t.Key()),
		Size:          t.Size(),
		ID:            string(t.Id()),
		Phase:         string(t.Phase()),
		Outcome:       string(t.Outcome()),
		ShardID:       t.ShardId(),
		PeerID:        string(t.PeerId()),
		BytesRepaired: t.BytesRepaired(),
		DurationMs:    t.DurationMs(),
		ErrCode:       string(t.ErrCode()),
		CorrelationID: string(t.CorrelationId()),
		VersionID:     string(t.VersionId()),
		RemovedID:     string(t.RemovedId()),
		Force:         t.Force(),
	}, nil
}
