package encrypt

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanonicalIKM_DeterministicOrderIndependent(t *testing.T) {
	a, err := CanonicalIKM([]string{"machine-id:abc", "mac:00:11:22", "cpu:x"})
	require.NoError(t, err)
	b, err := CanonicalIKM([]string{"cpu:x", "machine-id:abc", "mac:00:11:22"})
	require.NoError(t, err)
	require.True(t, bytes.Equal(a, b), "IKM must be order-independent")
}

func TestCanonicalIKM_FramingUnambiguous(t *testing.T) {
	x, err := CanonicalIKM([]string{"a", "bc"})
	require.NoError(t, err)
	y, err := CanonicalIKM([]string{"ab", "c"})
	require.NoError(t, err)
	require.False(t, bytes.Equal(x, y), "length-prefixed framing must disambiguate")
}

func TestCanonicalIKM_EmptyErrors(t *testing.T) {
	_, err := CanonicalIKM(nil)
	require.Error(t, err)
	_, err = CanonicalIKM([]string{"", "  "})
	require.Error(t, err, "all-empty factors must error (never derive from empty IKM)")
}

func TestCanonicalIKM_DropsEmptyButKeepsNonEmpty(t *testing.T) {
	a, err := CanonicalIKM([]string{"", "machine-id:abc", "   "})
	require.NoError(t, err)
	b, err := CanonicalIKM([]string{"machine-id:abc"})
	require.NoError(t, err)
	require.True(t, bytes.Equal(a, b))
}

func TestDefaultFactorSource_HasAtLeastOne(t *testing.T) {
	f, err := DefaultFactorSource{}.Factors()
	require.NoError(t, err)
	var nonEmpty int
	for _, s := range f {
		if s != "" {
			nonEmpty++
		}
	}
	require.GreaterOrEqual(t, nonEmpty, 1, "default source should resolve >=1 factor on this host")
}
