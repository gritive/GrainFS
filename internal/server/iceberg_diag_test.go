package server

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestParseIcebergDiagEnv(t *testing.T) {
	tests := []struct {
		name          string
		accessLogEnv  string // "" = unset
		traceMsEnv    string // "" = unset
		wantAccessLog bool
		wantTraceNs   int64
		wantWarn      bool // D5: parse 실패 시 stderr warn emit
	}{
		{"both unset", "", "", false, 0, false},
		{"access log on", "1", "", true, 0, false},
		{"access log true", "true", "", true, 0, false},
		{"trace 2000ms", "", "2000", false, 2 * int64(time.Second), false},
		{"both set", "1", "2000", true, 2 * int64(time.Second), false},
		{"trace abc invalid", "", "abc", false, 0, true},
		{"trace negative", "", "-5", false, 0, false},
		{"trace zero", "", "0", false, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GRAINFS_ICEBERG_ACCESS_LOG", tc.accessLogEnv)
			t.Setenv("GRAINFS_ICEBERG_COMMIT_TRACE_MS", tc.traceMsEnv)

			var buf bytes.Buffer
			old := log.Logger
			log.Logger = zerolog.New(&buf)
			t.Cleanup(func() { log.Logger = old })

			gotAccess, gotTrace := parseIcebergDiagEnv()
			if gotAccess != tc.wantAccessLog {
				t.Errorf("accessLog: got %v want %v", gotAccess, tc.wantAccessLog)
			}
			if gotTrace != tc.wantTraceNs {
				t.Errorf("traceNs: got %d want %d", gotTrace, tc.wantTraceNs)
			}
			gotWarn := strings.Contains(buf.String(), `"level":"warn"`) &&
				strings.Contains(buf.String(), "iceberg_diag")
			if gotWarn != tc.wantWarn {
				t.Errorf("warn emitted: got %v want %v (log=%q)", gotWarn, tc.wantWarn, buf.String())
			}
		})
	}
}

func TestLogIcebergAccessFormat(t *testing.T) {
	var buf bytes.Buffer
	old := log.Logger
	log.Logger = zerolog.New(&buf)
	t.Cleanup(func() { log.Logger = old })

	logIcebergAccess("POST", "/iceberg/v1/namespaces/n1/tables/t1", 409, 12345*time.Microsecond)

	line := buf.String()
	for _, key := range []string{
		`"method":"POST"`,
		`"path":"/iceberg/v1/namespaces/n1/tables/t1"`,
		`"status":409`,
		`"elapsed_ms":12.345`,
		`"message":"iceberg_access"`,
	} {
		if !strings.Contains(line, key) {
			t.Errorf("access log line missing %q: %s", key, line)
		}
	}
}

func TestIcebergAccessLogDisabledZeroAlloc(t *testing.T) {
	s := &Server{} // access log OFF (default)
	handler := s.icebergAccessLog(func(ctx context.Context, c *app.RequestContext) {})

	ctx := context.Background()
	c := &app.RequestContext{} // OFF path doesn't read from c — safe

	allocs := testing.AllocsPerRun(100, func() {
		handler(ctx, c)
	})
	if allocs > 0 {
		t.Fatalf("disabled middleware should be zero-alloc, got %.2f allocs/run", allocs)
	}
}
