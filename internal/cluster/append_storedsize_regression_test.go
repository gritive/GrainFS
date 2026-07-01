package cluster

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWriteSegmentBlobForAppend_NeverSetsStoredSize pins the premise the
// append-segment wire codec's forward-compatibility argument depends on: the
// append write path stores segments uncompressed, so StoredSize stays 0 and the
// EncodeAppendSegment output stays byte-identical to the pre-StoredSize format
// (no rolling-upgrade window). If append-side segments ever start carrying a
// compressed StoredSize, this test flips — a deliberate signal to revisit the
// forward-compat window before shipping it.
func TestWriteSegmentBlobForAppend_NeverSetsStoredSize(t *testing.T) {
	b := NewSingletonBackendForTest(t)

	body := "hello append segment"
	seg, err := b.writeSegmentBlobForAppend("bkt", "key", strings.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, int64(0), seg.StoredSize,
		"append-side segments must stay uncompressed (StoredSize==0); the append_summary codec forward-compat argument depends on it")
	require.Equal(t, int64(len(body)), seg.Size)
}
