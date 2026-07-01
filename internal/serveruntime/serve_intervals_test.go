package serveruntime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestValidateRequiredIntervals_ZeroResetsToDefault(t *testing.T) {
	cfg := &Config{
		ScrubInterval: 0,
	}
	ValidateRequiredIntervals(cfg)
	assert.Equal(t, 24*time.Hour, cfg.ScrubInterval)
}

func TestValidateRequiredIntervals_NonZeroUnchanged(t *testing.T) {
	cfg := &Config{
		ScrubInterval: 12 * time.Hour,
	}
	ValidateRequiredIntervals(cfg)
	assert.Equal(t, 12*time.Hour, cfg.ScrubInterval)
}
