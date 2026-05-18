package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestForwardReadAtBufferSizeClasses(t *testing.T) {
	tests := []struct {
		name      string
		length    int64
		wantLen   int
		wantCap   int
		wantClass int
	}{
		{name: "zero", length: 0, wantLen: 0, wantCap: 0, wantClass: 0},
		{name: "small", length: 1, wantLen: 1, wantCap: 4 * 1024, wantClass: 4 * 1024},
		{name: "small boundary", length: 4 * 1024, wantLen: 4 * 1024, wantCap: 4 * 1024, wantClass: 4 * 1024},
		{name: "medium", length: 4*1024 + 1, wantLen: 4*1024 + 1, wantCap: 16 * 1024, wantClass: 16 * 1024},
		{name: "medium boundary", length: 16 * 1024, wantLen: 16 * 1024, wantCap: 16 * 1024, wantClass: 16 * 1024},
		{name: "large pooled", length: 16*1024 + 1, wantLen: 16*1024 + 1, wantCap: 64 * 1024, wantClass: 64 * 1024},
		{name: "large pooled boundary", length: 64 * 1024, wantLen: 64 * 1024, wantCap: 64 * 1024, wantClass: 64 * 1024},
		{name: "unpooled", length: 64*1024 + 1, wantLen: 64*1024 + 1, wantCap: 64*1024 + 1, wantClass: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf, class := getForwardReadAtBuffer(tt.length)
			require.Len(t, buf, tt.wantLen)
			require.Equal(t, tt.wantCap, cap(buf))
			require.Equal(t, tt.wantClass, class)
			putForwardReadAtBuffer(buf, class)
		})
	}
}

func TestPutForwardReadAtBufferZerosFullClass(t *testing.T) {
	buf, class := getForwardReadAtBuffer(1)
	require.Equal(t, 4*1024, class)

	full := buf[:class]
	for i := range full {
		full[i] = 0xff
	}
	putForwardReadAtBuffer(buf, class)

	got, gotClass := getForwardReadAtBuffer(1)
	require.Equal(t, class, gotClass)
	require.Equal(t, 4*1024, cap(got))
	require.Equal(t, make([]byte, cap(got)), got[:cap(got)])
	putForwardReadAtBuffer(got, gotClass)
}
