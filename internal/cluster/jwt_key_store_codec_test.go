package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/stretchr/testify/require"
)

func TestJWTKeyStoreCodec_Roundtrip(t *testing.T) {
	current := &jwt.KeySeed{
		Kid:           "k_current",
		WrappedSecret: []byte{0x01, 0x02, 0x03, 0x04},
		DekGen:        7,
		Role:          "current",
		DemotedAt:     0,
	}
	previous := &jwt.KeySeed{
		Kid:           "k_previous",
		WrappedSecret: []byte{0xAA, 0xBB, 0xCC},
		DekGen:        5,
		Role:          "previous",
		DemotedAt:     1700000000,
	}

	t.Run("both populated", func(t *testing.T) {
		encoded := encodeJWTKeyStore(current, previous)
		require.NotEmpty(t, encoded)

		gotCurrent, gotPrevious, err := decodeJWTKeyStore(encoded)
		require.NoError(t, err)
		require.NotNil(t, gotCurrent)
		require.NotNil(t, gotPrevious)

		require.Equal(t, current.Kid, gotCurrent.Kid)
		require.Equal(t, current.WrappedSecret, gotCurrent.WrappedSecret)
		require.Equal(t, current.DekGen, gotCurrent.DekGen)
		require.Equal(t, current.DemotedAt, gotCurrent.DemotedAt)
		require.Equal(t, "current", gotCurrent.Role)

		require.Equal(t, previous.Kid, gotPrevious.Kid)
		require.Equal(t, previous.WrappedSecret, gotPrevious.WrappedSecret)
		require.Equal(t, previous.DekGen, gotPrevious.DekGen)
		require.Equal(t, previous.DemotedAt, gotPrevious.DemotedAt)
		require.Equal(t, "previous", gotPrevious.Role)
	})

	t.Run("current only", func(t *testing.T) {
		encoded := encodeJWTKeyStore(current, nil)
		gotCurrent, gotPrevious, err := decodeJWTKeyStore(encoded)
		require.NoError(t, err)
		require.NotNil(t, gotCurrent)
		require.Nil(t, gotPrevious)
		require.Equal(t, current.Kid, gotCurrent.Kid)
	})

	t.Run("both nil", func(t *testing.T) {
		encoded := encodeJWTKeyStore(nil, nil)
		gotCurrent, gotPrevious, err := decodeJWTKeyStore(encoded)
		require.NoError(t, err)
		require.Nil(t, gotCurrent)
		require.Nil(t, gotPrevious)
	})
}
