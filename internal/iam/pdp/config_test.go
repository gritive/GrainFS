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
