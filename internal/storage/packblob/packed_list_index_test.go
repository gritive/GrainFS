package packblob

import (
	"reflect"
	"testing"
)

func TestPackedListIndexPageUsesBucketPrefixAndMarker(t *testing.T) {
	var idx packedListIndex
	idx.add(packedKey{bucket: "other", key: "docs/000"})
	idx.add(packedKey{bucket: "bench", key: "img/000"})
	idx.add(packedKey{bucket: "bench", key: "docs/001"})
	idx.add(packedKey{bucket: "bench", key: "docs/003"})
	idx.add(packedKey{bucket: "bench", key: "docs/002"})

	keys, truncated := idx.page("bench", "docs/", "docs/001", 2)
	if truncated {
		t.Fatalf("truncated = true, want false")
	}
	if want := []string{"docs/002", "docs/003"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("keys = %v, want %v", keys, want)
	}
}

func TestPackedListIndexAddRemoveIsIdempotent(t *testing.T) {
	var idx packedListIndex
	pk := packedKey{bucket: "bench", key: "docs/001"}

	idx.add(pk)
	idx.add(pk)
	idx.remove(pk)
	idx.remove(pk)

	keys, truncated := idx.page("bench", "docs/", "", 10)
	if truncated {
		t.Fatalf("truncated = true, want false")
	}
	if len(keys) != 0 {
		t.Fatalf("keys = %v, want empty", keys)
	}
}
