package cluster

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDoctor_CheckDiskSpace_ValidDir(t *testing.T) {
	d := &Doctor{dataDir: os.TempDir()}
	result := d.checkDiskSpace()

	assert.Contains(t, []string{"pass", "warn", "fail"}, result.Status)
	assert.NotEmpty(t, result.Message)
	assert.NotEmpty(t, result.Duration)
}

func TestDoctor_CheckDiskSpace_InvalidDir(t *testing.T) {
	d := &Doctor{dataDir: "/nonexistent/xyz/abc"}
	result := d.checkDiskSpace()

	assert.Equal(t, "warn", result.Status)
	assert.Equal(t, "Unable to check disk space", result.Message)
}
