package ecspike

import (
	"testing"
)

func TestPlacement_Distinct(t *testing.T) {
	cfg := &Config{
		Nodes:   []string{"a", "b", "c", "d", "e", "f"},
		DataK:   4,
		ParityM: 2,
	}
	// Every (key, shardIdx) for 0..5 should map to 6 distinct nodes when
	// shardCount == nodeCount.
	keys := []string{"foo", "bar", "baz/biz", "", "x"}
	for _, k := range keys {
		seen := make(map[int]bool)
		for i := 0; i < cfg.numShards(); i++ {
			nodeIdx := cfg.Placement(k, i)
			if seen[nodeIdx] {
				t.Fatalf("key=%q shardIdx=%d landed on dup node %d", k, i, nodeIdx)
			}
			seen[nodeIdx] = true
		}
	}
}

func TestHeader_Roundtrip(t *testing.T) {
	sizes := []int64{0, 1, 1 << 20, 16 * 1024 * 1024, 1 << 40}
	for _, s := range sizes {
		h := encodeHeader(s)
		got, body, err := decodeHeader(append(h, 0xab, 0xcd))
		if err != nil {
			t.Fatalf("decode(%d): %v", s, err)
		}
		if got != s {
			t.Fatalf("size %d: got %d", s, got)
		}
		if len(body) != 2 || body[0] != 0xab || body[1] != 0xcd {
			t.Fatalf("body corruption: %x", body)
		}
	}
}

func TestHeader_TooSmall(t *testing.T) {
	_, _, err := decodeHeader([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for short input")
	}
}
