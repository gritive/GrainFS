package cluster

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildShardPackRecordEncodesAppendRecord(t *testing.T) {
	key := "bucket\x00object/v1\x001"
	data := []byte("payload")

	record := appendShardPackRecord(nil, shardPackFlagPut, key, data)

	require.Len(t, record, 4+1+4+len(key)+len(data)+4)
	require.Equal(t, uint32(len(key)), binary.BigEndian.Uint32(record[0:4]))
	require.Equal(t, shardPackFlagPut, record[4])
	require.Equal(t, uint32(len(data)), binary.BigEndian.Uint32(record[5:9]))
	require.Equal(t, key, string(record[9:9+len(key)]))
	require.Equal(t, data, record[9+len(key):9+len(key)+len(data)])
	require.Equal(t, shardPackCRC(shardPackFlagPut, key, data), binary.BigEndian.Uint32(record[len(record)-4:]))
}

func BenchmarkShardPackStore_Put64KiB(b *testing.B) {
	store, err := newShardPackStore(b.TempDir(), nil)
	if err != nil {
		b.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), 64<<10)

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.put("bucket", fmt.Sprintf("object/%08d/v1", i), 1, payload); err != nil {
			b.Fatal(err)
		}
	}
}
