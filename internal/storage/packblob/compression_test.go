package packblob

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompress_RoundTrip(t *testing.T) {
	data := bytes.Repeat([]byte("hello world "), 100)

	compressed, err := compress(data)
	require.NoError(t, err)
	require.Less(t, len(compressed), len(data), "compressed should be smaller than original")

	got, err := decompress(compressed)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestCompress_Incompressible_RoundTrip(t *testing.T) {
	data := make([]byte, 1024)
	rand.Read(data)

	compressed, err := compress(data)
	require.NoError(t, err)

	got, err := decompress(compressed)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestCompress_Empty(t *testing.T) {
	compressed, err := compress([]byte{})
	require.NoError(t, err)

	got, err := decompress(compressed)
	require.NoError(t, err)
	require.Equal(t, []byte{}, got)
}
