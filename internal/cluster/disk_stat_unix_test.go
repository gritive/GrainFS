//go:build unix

package cluster

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSysDiskStat_ReturnsReasonableValues(t *testing.T) {
	usedPct, availBytes := sysDiskStat(os.TempDir())

	assert.GreaterOrEqual(t, usedPct, 0.0)
	assert.LessOrEqual(t, usedPct, 100.0)
	assert.Greater(t, availBytes, uint64(0))
}

func TestSysDiskStat_InvalidDirReturnsZero(t *testing.T) {
	usedPct, availBytes := sysDiskStat("/nonexistent/xyz/abc")

	assert.Equal(t, 0.0, usedPct)
	assert.Equal(t, uint64(0), availBytes)
}
