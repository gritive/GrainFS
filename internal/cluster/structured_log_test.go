package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureLogger returns a zerolog.Logger that writes JSON to buf.
func captureLogger(buf *bytes.Buffer) zerolog.Logger {
	return zerolog.New(buf).With().Timestamp().Logger().Level(zerolog.DebugLevel)
}

// logLines parses all JSON log lines from buf.
func logLines(buf *bytes.Buffer) []map[string]any {
	var result []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(line, &m); err == nil {
			result = append(result, m)
		}
	}
	return result
}

// autoNotifyRaft calls NotifyCommit on the executor after each Propose.
type autoNotifyRaft struct {
	nodeID string
	exec   *MigrationExecutor
}

func (r *autoNotifyRaft) NodeID() string { return r.nodeID }
func (r *autoNotifyRaft) Propose(data []byte) error {
	// Parse the proposal to extract bucket/key/versionID and notify commit.
	// Simpler: notify for any pending task after a brief yield.
	go func() {
		r.exec.mu.Lock()
		for id := range r.exec.pending {
			// Parse id = "bucket/key/versionID"
			r.exec.mu.Unlock()
			// Use split on "/" — id format is bucket+"/"+key+"/"+versionID
			// Just close the channel directly by calling NotifyCommit with empty strings
			// and look for exact id match.
			_ = id
			return
		}
		r.exec.mu.Unlock()
	}()
	// Simpler: unblock by closing all pending channels directly.
	go func() {
		r.exec.mu.Lock()
		ids := make([]string, 0, len(r.exec.pending))
		for id := range r.exec.pending {
			ids = append(ids, id)
		}
		r.exec.mu.Unlock()
		for _, id := range ids {
			// Parse bucket/key/version from id
			// id = bucket + "/" + key + "/" + versionID
			// versionID is always "" in these tests
			// Find last two slashes
			last := len(id) - 1 // strip trailing "/"
			mid := 0
			for i := last - 1; i >= 0; i-- {
				if id[i] == '/' {
					mid = i
					break
				}
			}
			bucket := id[:mid]
			key := id[mid+1 : last]
			r.exec.NotifyCommit(bucket, key, "")
		}
	}()
	return nil
}

// makeExecutorWithLogger creates a MigrationExecutor wired to an auto-notifying raft.
func makeExecutorWithLogger(buf *bytes.Buffer) *MigrationExecutor {
	e := NewMigrationExecutor(nil, nil, 2) // placeholders; replaced below
	raft := &autoNotifyRaft{nodeID: "self", exec: e}
	e.mover = &noopMover{}
	e.node = raft
	e.logger = captureLogger(buf).With().Str("component", "migration").Logger()
	return e
}

// TestStructuredLogging_ComponentField: all migration log lines must have component="migration".
func TestStructuredLogging_ComponentField(t *testing.T) {
	var buf bytes.Buffer
	e := makeExecutorWithLogger(&buf)

	task := MigrationTask{Bucket: "b", Key: "k", SrcNode: "src", DstNode: "dst"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := e.Execute(ctx, task)
	require.NoError(t, err)

	lines := logLines(&buf)
	require.NotEmpty(t, lines, "must emit at least one log line")
	for _, line := range lines {
		assert.Equal(t, "migration", line["component"], "all log lines must have component=migration")
	}
}

// TestStructuredLogging_PhaseField: Execute() must log at each phase with a phase= field.
func TestStructuredLogging_PhaseField(t *testing.T) {
	var buf bytes.Buffer
	e := makeExecutorWithLogger(&buf)

	task := MigrationTask{Bucket: "b", Key: "k", SrcNode: "src", DstNode: "dst"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := e.Execute(ctx, task)
	require.NoError(t, err)

	lines := logLines(&buf)
	phases := make(map[string]bool)
	for _, line := range lines {
		if p, ok := line["phase"].(string); ok {
			phases[p] = true
		}
	}
	assert.True(t, phases["1"], "must log phase=1 (copy shards)")
	assert.True(t, phases["2"], "must log phase=2 (propose done)")
	assert.True(t, phases["4"], "must log phase=4 (delete src)")
}
