package admin

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/scrubber"
)

type mockDirector struct {
	triggered scrubber.TriggerReq
	sessionID string
	created   bool
	sessions  []scrubber.Session
	cancelled []string
	getOK     bool
	getReturn scrubber.Session
}

func (m *mockDirector) Trigger(req scrubber.TriggerReq) (string, bool) {
	m.triggered = req
	return m.sessionID, m.created
}
func (m *mockDirector) Sessions() []scrubber.Session { return m.sessions }
func (m *mockDirector) GetSession(id string) (scrubber.Session, bool) {
	return m.getReturn, m.getOK
}
func (m *mockDirector) CancelSession(id string) error {
	m.cancelled = append(m.cancelled, id)
	return nil
}
func (m *mockDirector) ApplyFromFSM(_ scrubber.ScrubTriggerEntry) {}

func TestScrubVolume_TriggersWithFullScopeByDefault(t *testing.T) {
	d := &mockDirector{sessionID: "sess-1", created: true}
	deps := &Deps{Director: d}
	resp, err := ScrubVolume(context.Background(), deps, ScrubVolumeReq{Name: "myvol"})
	require.NoError(t, err)
	require.Equal(t, "sess-1", resp.SessionID)
	require.True(t, resp.Created)
	require.Equal(t, "__grainfs_volumes", d.triggered.Bucket)
	require.Equal(t, "__vol/myvol/blk_", d.triggered.KeyPrefix)
	require.Equal(t, scrubber.ScopeFull, d.triggered.Scope)
}

func TestScrubVolume_DryRun(t *testing.T) {
	d := &mockDirector{sessionID: "sess-2", created: true}
	deps := &Deps{Director: d}
	_, err := ScrubVolume(context.Background(), deps, ScrubVolumeReq{Name: "myvol", DryRun: true})
	require.NoError(t, err)
	require.True(t, d.triggered.DryRun)
}

func TestScrubVolume_LiveScope(t *testing.T) {
	d := &mockDirector{sessionID: "sess-3", created: true}
	deps := &Deps{Director: d}
	_, err := ScrubVolume(context.Background(), deps, ScrubVolumeReq{Name: "v", Scope: "live"})
	require.NoError(t, err)
	require.Equal(t, scrubber.ScopeLive, d.triggered.Scope)
}

func TestScrubVolume_InvalidScope(t *testing.T) {
	d := &mockDirector{sessionID: "x", created: true}
	deps := &Deps{Director: d}
	_, err := ScrubVolume(context.Background(), deps, ScrubVolumeReq{Name: "v", Scope: "bogus"})
	require.Error(t, err)
}

func TestScrubVolume_NoName(t *testing.T) {
	d := &mockDirector{}
	_, err := ScrubVolume(context.Background(), &Deps{Director: d}, ScrubVolumeReq{})
	require.Error(t, err)
}

func TestScrubVolume_NoDirector(t *testing.T) {
	_, err := ScrubVolume(context.Background(), &Deps{}, ScrubVolumeReq{Name: "v"})
	require.Error(t, err)
}

func TestListScrubJobs_EmptyOK(t *testing.T) {
	resp, err := ListScrubJobs(context.Background(), &Deps{Director: &mockDirector{}})
	require.NoError(t, err)
	require.Empty(t, resp.Jobs)
}

func TestGetScrubJob_NotFound(t *testing.T) {
	_, err := GetScrubJob(context.Background(), &Deps{Director: &mockDirector{}}, "missing")
	require.Error(t, err)
}

func TestCancelScrubJob_DelegatesToDirector(t *testing.T) {
	d := &mockDirector{}
	require.NoError(t, CancelScrubJob(context.Background(), &Deps{Director: d}, "sess-x"))
	require.Equal(t, []string{"sess-x"}, d.cancelled)
}
