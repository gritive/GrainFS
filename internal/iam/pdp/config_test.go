package pdp

import (
	"crypto/tls"
	"strings"
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
			want:  Config{Enabled: false, Timeout: 2 * time.Second, FailurePolicy: FailureClosed, TLS: TLSConfig{MinVersion: tls.VersionTLS12}},
		},
		{
			name:  "valid enabled https endpoint",
			input: `{"enabled":true,"endpoint":"https://pdp.example:8443","timeout":"3s","failure_policy":"open"}`,
			want: Config{
				Enabled: true, Scheme: "https", RemoteURL: "https://pdp.example:8443", Host: "pdp.example:8443",
				Timeout: 3 * time.Second, FailurePolicy: FailureOpen,
				TLS: TLSConfig{MinVersion: tls.VersionTLS12},
			},
		},
		{
			name:  "empty bytes parses as disabled",
			input: "",
			want:  Config{Enabled: false, Timeout: 2 * time.Second, FailurePolicy: FailureClosed, TLS: TLSConfig{MinVersion: tls.VersionTLS12}},
		},
		{
			name:  "disabled with empty endpoint still parses (disabled = inert)",
			input: `{"enabled":false,"endpoint":""}`,
			want:  Config{Enabled: false, Timeout: 2 * time.Second, FailurePolicy: FailureClosed, TLS: TLSConfig{MinVersion: tls.VersionTLS12}},
		},
		{
			name:            "invalid JSON",
			input:           `{`,
			wantErrContains: "invalid JSON",
		},
		{
			name:            "enabled rejects missing endpoint",
			input:           `{"enabled":true}`,
			wantErrContains: "endpoint must be http",
		},
		{
			name:            "enabled rejects unix scheme",
			input:           `{"enabled":true,"endpoint":"unix:///run/pdp.sock"}`,
			wantErrContains: "endpoint must be http",
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
				TLS:   TLSConfig{MinVersion: tls.VersionTLS12},
				Cache: CacheConfig{Active: true, TTLAllow: 30 * time.Second, TTLDeny: 10 * time.Second, MaxEntries: 4096, GraceTTL: 5 * time.Minute},
			},
		},
		{
			name:  "absent cache is inactive",
			input: `{}`,
			want:  Config{Enabled: false, Timeout: 2 * time.Second, FailurePolicy: FailureClosed, TLS: TLSConfig{MinVersion: tls.VersionTLS12}},
		},
		{
			name:  "both ttls zero is inactive",
			input: `{"cache":{"ttl_allow":"0s","ttl_deny":"0s"}}`,
			want:  Config{Enabled: false, Timeout: 2 * time.Second, FailurePolicy: FailureClosed, TLS: TLSConfig{MinVersion: tls.VersionTLS12}},
		},
		{
			name:  "ttl_deny only is active",
			input: `{"cache":{"ttl_allow":"0s","ttl_deny":"30s"}}`,
			want: Config{
				Enabled: false, Timeout: 2 * time.Second, FailurePolicy: FailureClosed,
				TLS:   TLSConfig{MinVersion: tls.VersionTLS12},
				Cache: CacheConfig{Active: true, TTLDeny: 30 * time.Second, MaxEntries: defaultMaxEntries},
			},
		},
		{
			name:  "max_entries omitted with active defaults",
			input: `{"cache":{"ttl_allow":"30s"}}`,
			want: Config{
				Enabled: false, Timeout: 2 * time.Second, FailurePolicy: FailureClosed,
				TLS:   TLSConfig{MinVersion: tls.VersionTLS12},
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

func TestParseConfig_RemoteTransport(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr string // substring; "" = ok
		check   func(t *testing.T, c Config)
	}{
		{name: "https remote ok", raw: `{"enabled":true,"endpoint":"https://pdp.example:8443"}`,
			check: func(t *testing.T, c Config) {
				if c.Scheme != "https" || c.RemoteURL != "https://pdp.example:8443" {
					t.Fatalf("got scheme=%q url=%q", c.Scheme, c.RemoteURL)
				}
			}},
		{name: "http loopback ok", raw: `{"enabled":true,"endpoint":"http://127.0.0.1:8181"}`,
			check: func(t *testing.T, c Config) {
				if c.Scheme != "http" {
					t.Fatalf("got scheme=%q", c.Scheme)
				}
			}},
		{name: "unix rejected", raw: `{"enabled":true,"endpoint":"unix:///run/pdp.sock"}`, wantErr: "endpoint must be http"},
		{name: "ftp rejected", raw: `{"enabled":true,"endpoint":"ftp://x"}`, wantErr: "endpoint must be http"},
		{name: "missing host", raw: `{"enabled":true,"endpoint":"https://"}`, wantErr: "host"},
		{name: "endpoint with path rejected", raw: `{"enabled":true,"endpoint":"https://pdp.example/base"}`, wantErr: "no path"},
		{name: "endpoint with trailing slash rejected", raw: `{"enabled":true,"endpoint":"https://pdp.example/"}`, wantErr: "no path"},
		{name: "endpoint with query rejected", raw: `{"enabled":true,"endpoint":"https://pdp.example?x=y"}`, wantErr: "no path"},
		{name: "endpoint with userinfo rejected", raw: `{"enabled":true,"endpoint":"https://user:pass@pdp.example"}`, wantErr: "no path"},
		{name: "http non-loopback literal rejected", raw: `{"enabled":true,"endpoint":"http://10.0.0.5:8181"}`, wantErr: "loopback"},
		{name: "https literal-private rejected", raw: `{"enabled":true,"endpoint":"https://10.0.0.5:8443"}`, wantErr: "private"},
		{name: "https literal-private allowed with allow_private", raw: `{"enabled":true,"endpoint":"https://10.0.0.5:8443","ssrf":{"allow_private":true}}`},
		{name: "https loopback literal rejected", raw: `{"enabled":true,"endpoint":"https://127.0.0.1:8443"}`, wantErr: "loopback"},
		{name: "obfuscated octal IP rejected", raw: `{"enabled":true,"endpoint":"https://0177.0.0.1:8443"}`, wantErr: "not a valid host"},
		{name: "obfuscated integer IP rejected", raw: `{"enabled":true,"endpoint":"https://2130706433:8443"}`, wantErr: "not a valid host"},
		{name: "tls min_version floor", raw: `{"enabled":true,"endpoint":"https://pdp.example:8443","tls":{"min_version":"1.1"}}`, wantErr: "min_version"},
		{name: "malformed ca_pem rejected", raw: `{"enabled":true,"endpoint":"https://pdp.example:8443","tls":{"ca_pem":"not a cert"}}`, wantErr: "ca_pem"},
		{name: "disabled skips endpoint validation", raw: `{"enabled":false,"endpoint":"ftp://whatever"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := ParseConfig([]byte(tt.raw))
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				if tt.check != nil {
					tt.check(t, c)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("want err containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}
