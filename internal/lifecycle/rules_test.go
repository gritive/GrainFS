package lifecycle

import (
	"encoding/xml"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate_ValidConfig(t *testing.T) {
	cfg := &LifecycleConfiguration{
		Rules: []Rule{
			{
				ID:         "expire-logs",
				Status:     "Enabled",
				Filter:     &Filter{Prefix: "logs/"},
				Expiration: &Expiration{Days: 30},
			},
		},
	}
	assert.NoError(t, Validate(cfg))
}

func TestValidate_EmptyRules(t *testing.T) {
	cfg := &LifecycleConfiguration{}
	assert.NoError(t, Validate(cfg))
}

func TestValidate_DuplicateID(t *testing.T) {
	cfg := &LifecycleConfiguration{
		Rules: []Rule{
			{ID: "rule1", Status: "Enabled"},
			{ID: "rule1", Status: "Disabled"},
		},
	}
	assert.Error(t, Validate(cfg))
}

func TestValidate_EmptyID(t *testing.T) {
	cfg := &LifecycleConfiguration{
		Rules: []Rule{{ID: "", Status: "Enabled"}},
	}
	assert.Error(t, Validate(cfg))
}

func TestValidate_InvalidStatus(t *testing.T) {
	cfg := &LifecycleConfiguration{
		Rules: []Rule{{ID: "r", Status: "Active"}},
	}
	assert.Error(t, Validate(cfg))
}

func TestValidate_NegativeDays(t *testing.T) {
	cfg := &LifecycleConfiguration{
		Rules: []Rule{
			{ID: "r", Status: "Enabled", Expiration: &Expiration{Days: -1}},
		},
	}
	assert.Error(t, Validate(cfg))
}

func TestValidate_ZeroDays(t *testing.T) {
	cfg := &LifecycleConfiguration{
		Rules: []Rule{
			{ID: "r", Status: "Enabled", Expiration: &Expiration{Days: 0}},
		},
	}
	assert.Error(t, Validate(cfg))
}

func TestXMLRoundTrip(t *testing.T) {
	original := &LifecycleConfiguration{
		Rules: []Rule{
			{
				ID:         "expire-old",
				Status:     "Enabled",
				Filter:     &Filter{Prefix: "tmp/"},
				Expiration: &Expiration{Days: 7},
				NoncurrentVersionExpiration: &NoncurrentVersionExpiration{
					NoncurrentDays:          14,
					NewerNoncurrentVersions: 3,
				},
			},
		},
	}

	data, err := xml.Marshal(original)
	require.NoError(t, err)

	var decoded LifecycleConfiguration
	require.NoError(t, xml.Unmarshal(data, &decoded))
	require.Len(t, decoded.Rules, 1)
	assert.Equal(t, "expire-old", decoded.Rules[0].ID)
	assert.Equal(t, "Enabled", decoded.Rules[0].Status)
	assert.Equal(t, 7, decoded.Rules[0].Expiration.Days)
	assert.Equal(t, 14, decoded.Rules[0].NoncurrentVersionExpiration.NoncurrentDays)
	assert.Equal(t, 3, decoded.Rules[0].NoncurrentVersionExpiration.NewerNoncurrentVersions)
}
