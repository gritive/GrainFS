package alerts

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSendCmd_Apply_ClaimsDedupAndSpawnsWorker(t *testing.T) {
	var spawnCount int
	env := &dispatchEnv{
		lastSent: map[string]time.Time{},
		inFlight: map[string]struct{}{},
		opts:     Options{Clock: time.Now, DedupWindow: time.Minute},
		url:      "http://test",
	}
	env.spawnTestHook = func(a Alert, _, _ string) { spawnCount++ }

	cmd := sendCmd{alert: Alert{Type: "t", Resource: "r"}}
	cmd.apply(env)

	require.Equal(t, 1, spawnCount)
	_, ok := env.inFlight["t|r"]
	require.True(t, ok, "inFlight must hold key during worker run")
}

func TestSendCmd_Apply_DedupSuppresses(t *testing.T) {
	var spawnCount int
	now := time.Now()
	env := &dispatchEnv{
		lastSent: map[string]time.Time{"t|r": now.Add(-30 * time.Second)},
		inFlight: map[string]struct{}{},
		opts:     Options{Clock: func() time.Time { return now }, DedupWindow: time.Minute},
		url:      "http://test",
	}
	env.spawnTestHook = func(a Alert, _, _ string) { spawnCount++ }

	cmd := sendCmd{alert: Alert{Type: "t", Resource: "r"}}
	cmd.apply(env)
	require.Equal(t, 0, spawnCount, "dedup window must suppress within 1 minute")
}

func TestReleaseCmd_Apply_UpdatesLastSentAndCallsOnResult(t *testing.T) {
	var got Alert
	var gotErr error
	env := &dispatchEnv{
		lastSent: map[string]time.Time{},
		inFlight: map[string]struct{}{"t|r": {}},
		opts:     Options{Clock: time.Now, DedupWindow: time.Minute},
		onResult: func(a Alert, err error) { got = a; gotErr = err },
	}
	cmd := releaseCmd{
		key:   "t|r",
		alert: Alert{Type: "t", Resource: "r"},
		err:   nil,
	}
	cmd.apply(env)
	_, stillInFlight := env.inFlight["t|r"]
	require.False(t, stillInFlight)
	_, ok := env.lastSent["t|r"]
	require.True(t, ok)
	require.Equal(t, "t", got.Type)
	require.NoError(t, gotErr)
}
