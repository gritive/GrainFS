package serveruntime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestValidateRequiredIntervals_ZeroResetsToDefault(t *testing.T) {
	cfg := &Config{
		ScrubInterval:       0,
		ReshardInterval:     0,
		RingReshardInterval: 0,
	}
	ValidateRequiredIntervals(cfg)
	assert.Equal(t, 24*time.Hour, cfg.ScrubInterval)
	assert.Equal(t, 24*time.Hour, cfg.ReshardInterval)
	assert.Equal(t, time.Hour, cfg.RingReshardInterval)
}

func TestValidateRequiredIntervals_NonZeroUnchanged(t *testing.T) {
	cfg := &Config{
		ScrubInterval:       12 * time.Hour,
		ReshardInterval:     6 * time.Hour,
		RingReshardInterval: 30 * time.Minute,
	}
	ValidateRequiredIntervals(cfg)
	assert.Equal(t, 12*time.Hour, cfg.ScrubInterval)
	assert.Equal(t, 6*time.Hour, cfg.ReshardInterval)
	assert.Equal(t, 30*time.Minute, cfg.RingReshardInterval)
}
