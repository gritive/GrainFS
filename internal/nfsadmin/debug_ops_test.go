package nfsadmin

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/adminapi"
)

func TestRunDebug_TextMode(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "GET",
		path:   "/v1/nfs/exports/b1/debug",
		body: ExportDebugResp{
			Bucket:     "b1",
			Registered: true,
			ReadOnly:   false,
			FsidMajor:  1,
			FsidMinor:  2,
			Generation: 3,
			BackendBucket: adminapi.ExportDebugBackend{
				Exists:      true,
				ObjectCount: 5,
			},
		},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	require.NoError(t, RunDebug(context.Background(), DebugExportOptions{BaseOptions: base, Bucket: "b1"}))
	require.Contains(t, out.String(), "Bucket: b1")
	require.Contains(t, out.String(), "Backend:")
}

func TestRunDebug_JSONMode(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "GET",
		path:   "/v1/nfs/exports/b1/debug",
		body: ExportDebugResp{
			Bucket:     "b1",
			Registered: true,
			BackendBucket: adminapi.ExportDebugBackend{
				Exists:      true,
				ObjectCount: 5,
			},
		},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	base.JSONOut = true
	require.NoError(t, RunDebug(context.Background(), DebugExportOptions{BaseOptions: base, Bucket: "b1"}))
	var got map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &got))
	require.Equal(t, "b1", got["bucket"])
}

func TestRunDebug_ErrorWritesToStderr(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method:  "GET",
		path:    "/v1/nfs/exports/missing/debug",
		status:  404,
		errResp: &Error{Code: "bucket_not_found", Message: "missing", Help: "create it", DocsURL: "https://example.test"},
	}})
	defer srv.Close()
	base, _, errBuf := optsForServer(srv)
	err := RunDebug(context.Background(), DebugExportOptions{BaseOptions: base, Bucket: "missing"})
	require.Error(t, err)
	require.Contains(t, errBuf.String(), "Error:")
	require.Contains(t, errBuf.String(), "Hint:")
}
