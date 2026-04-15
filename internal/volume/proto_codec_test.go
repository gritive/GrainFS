package volume

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVolumeCodecRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		vol  Volume
	}{
		{
			name: "basic volume",
			vol: Volume{
				Name:      "test-vol",
				Size:      1024 * 1024,
				BlockSize: DefaultBlockSize,
			},
		},
		{
			name: "empty name",
			vol: Volume{
				Name:      "",
				Size:      0,
				BlockSize: 0,
			},
		},
		{
			name: "large values",
			vol: Volume{
				Name:      "large-volume-with-long-name",
				Size:      1 << 40, // 1 TiB
				BlockSize: 65536,
			},
		},
		{
			name: "non-default block size to verify int32 conversion",
			vol: Volume{
				Name:      "custom-bs",
				Size:      8192,
				BlockSize: 8192,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			original := &tt.vol

			// Act
			data, err := marshalVolume(original)
			require.NoError(t, err)

			got, err := unmarshalVolume(data)
			require.NoError(t, err)

			// Assert
			assert.Equal(t, original.Name, got.Name)
			assert.Equal(t, original.Size, got.Size)
			assert.Equal(t, original.BlockSize, got.BlockSize)
		})
	}
}

func TestVolumeCodecOutputIsNotJSON(t *testing.T) {
	// Arrange
	vol := &Volume{
		Name:      "json-check",
		Size:      4096,
		BlockSize: DefaultBlockSize,
	}

	// Act
	data, err := marshalVolume(vol)
	require.NoError(t, err)

	// Assert: protobuf output should not contain JSON field name strings
	dataStr := string(data)
	assert.NotContains(t, dataStr, `"name"`, "protobuf output should not contain JSON field name")
	assert.NotContains(t, dataStr, `"size"`, "protobuf output should not contain JSON field name")
	assert.NotContains(t, dataStr, `"block_size"`, "protobuf output should not contain JSON field name")

	// protobuf binary never starts with '{' (JSON object opener)
	assert.NotEqual(t, byte('{'), data[0], "protobuf output should not start with '{'")
}
