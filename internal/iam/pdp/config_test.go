package pdp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name            string
		input           string
		want            Config
		wantErrContains string
	}{
		{
			name:  "default empty is disabled",
			input: `{}`,
			want:  Config{Enabled: false, Timeout: 2 * time.Second, FailurePolicy: FailureClosed},
		},
		{
			name:  "valid enabled unix endpoint",
			input: `{"enabled":true,"endpoint":"unix:///run/grainfs/pdp.sock","timeout":"3s","failure_policy":"open"}`,
			want:  Config{Enabled: true, SocketPath: "/run/grainfs/pdp.sock", Timeout: 3 * time.Second, FailurePolicy: FailureOpen},
		},
		{
			name:  "empty bytes parses as disabled",
			input: "",
			want:  Config{Enabled: false, Timeout: 2 * time.Second, FailurePolicy: FailureClosed},
		},
		{
			name:  "disabled with empty endpoint still parses (disabled = inert)",
			input: `{"enabled":false,"endpoint":""}`,
			want:  Config{Enabled: false, Timeout: 2 * time.Second, FailurePolicy: FailureClosed},
		},
		{
			name:            "invalid JSON",
			input:           `{`,
			wantErrContains: "invalid JSON",
		},
		{
			name:            "enabled rejects missing endpoint",
			input:           `{"enabled":true}`,
			wantErrContains: "endpoint must be a unix:// socket",
		},
		{
			name:            "enabled rejects non-unix scheme",
			input:           `{"enabled":true,"endpoint":"https://pdp.example.com/authorize"}`,
			wantErrContains: "endpoint must be a unix:// socket",
		},
		{
			name:            "rejects bad timeout",
			input:           `{"enabled":true,"endpoint":"unix:///s.sock","timeout":"nope"}`,
			wantErrContains: "invalid timeout",
		},
		{
			name:            "rejects over-cap timeout",
			input:           `{"enabled":true,"endpoint":"unix:///s.sock","timeout":"11s"}`,
			wantErrContains: "timeout must be >0",
		},
		{
			name:            "rejects bad failure_policy",
			input:           `{"enabled":true,"endpoint":"unix:///s.sock","failure_policy":"maybe"}`,
			wantErrContains: "failure_policy must be",
		},
		{
			name:            "rejects relative unix socket path",
			input:           `{"enabled":true,"endpoint":"unix://relative.sock"}`,
			wantErrContains: "absolute socket path",
		},
		{
			name:            "rejects non-positive timeout (zero)",
			input:           `{"enabled":true,"endpoint":"unix:///s.sock","timeout":"0s"}`,
			wantErrContains: "timeout must be >0",
		},
		{
			name:            "rejects non-positive timeout (negative)",
			input:           `{"enabled":true,"endpoint":"unix:///s.sock","timeout":"-1s"}`,
			wantErrContains: "timeout must be >0",
		},
		{
			name:  "valid full cache block",
			input: `{"cache":{"ttl_allow":"30s","ttl_deny":"10s","max_entries":4096,"grace_ttl":"5m"}}`,
			want: Config{
				Enabled: false, Timeout: 2 * time.Second, FailurePolicy: FailureClosed,
				Cache: CacheConfig{Active: true, TTLAllow: 30 * time.Second, TTLDeny: 10 * time.Second, MaxEntries: 4096, GraceTTL: 5 * time.Minute},
			},
		},
		{
			name:  "absent cache is inactive",
			input: `{}`,
			want:  Config{Enabled: false, Timeout: 2 * time.Second, FailurePolicy: FailureClosed},
		},
		{
			name:  "both ttls zero is inactive",
			input: `{"cache":{"ttl_allow":"0s","ttl_deny":"0s"}}`,
			want:  Config{Enabled: false, Timeout: 2 * time.Second, FailurePolicy: FailureClosed},
		},
		{
			name:  "ttl_deny only is active",
			input: `{"cache":{"ttl_allow":"0s","ttl_deny":"30s"}}`,
			want: Config{
				Enabled: false, Timeout: 2 * time.Second, FailurePolicy: FailureClosed,
				Cache: CacheConfig{Active: true, TTLDeny: 30 * time.Second, MaxEntries: defaultMaxEntries},
			},
		},
		{
			name:  "max_entries omitted with active defaults",
			input: `{"cache":{"ttl_allow":"30s"}}`,
			want: Config{
				Enabled: false, Timeout: 2 * time.Second, FailurePolicy: FailureClosed,
				Cache: CacheConfig{Active: true, TTLAllow: 30 * time.Second, MaxEntries: defaultMaxEntries},
			},
		},
		{
			name:            "grace_ttl without caching errors",
			input:           `{"cache":{"grace_ttl":"5m"}}`,
			wantErrContains: "grace_ttl requires caching",
		},
		{
			name:            "rejects bad cache duration",
			input:           `{"cache":{"ttl_allow":"nope"}}`,
			wantErrContains: "iam.pdp",
		},
		{
			name:            "rejects over-cap ttl_allow",
			input:           `{"cache":{"ttl_allow":"11m"}}`,
			wantErrContains: "ttl_allow",
		},
		{
			name:            "rejects over-cap grace_ttl",
			input:           `{"cache":{"ttl_allow":"30s","grace_ttl":"2h"}}`,
			wantErrContains: "grace_ttl",
		},
		{
			name:            "rejects negative max_entries",
			input:           `{"cache":{"ttl_allow":"30s","max_entries":-1}}`,
			wantErrContains: "max_entries",
		},
		{
			name:            "rejects negative cache duration",
			input:           `{"cache":{"ttl_allow":"-1s"}}`,
			wantErrContains: "ttl_allow",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := ParseConfig([]byte(tc.input))
			if tc.wantErrContains != "" {
				require.ErrorContains(t, err, tc.wantErrContains)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, c)
		})
	}
}
