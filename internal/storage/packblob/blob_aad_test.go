package packblob

import (
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
)

func aadFor(blobID, offset uint64, key string, flags byte) string {
	cid := make([]byte, 16)
	return string(encrypt.BuildAAD(encrypt.DomainShard, cid, blobEntryAADFields(blobID, offset, key, flags)...))
}

func TestBlobEntryAAD_DistinctPerField(t *testing.T) {
	base := aadFor(1, 100, "k", 0x02)
	cases := map[string]string{
		"blobID": aadFor(2, 100, "k", 0x02),
		"offset": aadFor(1, 200, "k", 0x02),
		"key":    aadFor(1, 100, "k2", 0x02),
		"flags":  aadFor(1, 100, "k", 0x03),
	}
	for name, got := range cases {
		if got == base {
			t.Fatalf("AAD must differ when %s changes", name)
		}
	}
}
