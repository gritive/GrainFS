package raft

import "testing"

func TestDecodeHeartbeatBatchOwnsDecodedStrings(t *testing.T) {
	items := []hbItem{{
		groupID: "group-A",
		args: &AppendEntriesArgs{
			Term:         7,
			LeaderID:     "node-A",
			PrevLogIndex: 41,
			PrevLogTerm:  6,
			LeaderCommit: 40,
		},
	}}
	payload, err := encodeHeartbeatBatch(items)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := decodeHeartbeatBatch(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 1 {
		t.Fatalf("decoded %d items, want 1", len(decoded))
	}

	for i := range payload {
		payload[i] = 0xff
	}

	if decoded[0].groupID != "group-A" {
		t.Fatalf("groupID = %q, want group-A", decoded[0].groupID)
	}
	if decoded[0].args.LeaderID != "node-A" {
		t.Fatalf("LeaderID = %q, want node-A", decoded[0].args.LeaderID)
	}
}

func TestEncodeHeartbeatBatchLargeBatchRoundTrip(t *testing.T) {
	items := benchmarkHeartbeatItems(17)
	payload, err := encodeHeartbeatBatch(items)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := decodeHeartbeatBatch(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != len(items) {
		t.Fatalf("decoded %d items, want %d", len(decoded), len(items))
	}
	for i := range items {
		if decoded[i].groupID != items[i].groupID {
			t.Fatalf("decoded[%d].groupID = %q, want %q", i, decoded[i].groupID, items[i].groupID)
		}
		if decoded[i].args.PrevLogIndex != items[i].args.PrevLogIndex {
			t.Fatalf("decoded[%d].PrevLogIndex = %d, want %d", i, decoded[i].args.PrevLogIndex, items[i].args.PrevLogIndex)
		}
	}
}
