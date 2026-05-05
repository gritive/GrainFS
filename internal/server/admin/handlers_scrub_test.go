package admin

import (
	"context"
	"testing"
	"time"

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

type mockScrubProposer struct {
	gotReq scrubber.TriggerReq
	entry  scrubber.ScrubTriggerEntry
	err    error
}

func (m *mockScrubProposer) Propose(_ context.Context, req scrubber.TriggerReq) (scrubber.ScrubTriggerEntry, error) {
	m.gotReq = req
	return m.entry, m.err
}

func TestTriggerScrub_HappyPath(t *testing.T) {
	p := &mockScrubProposer{entry: scrubber.ScrubTriggerEntry{SessionID: "sid-1"}}
	resp, err := TriggerScrub(context.Background(), &Deps{ScrubProposer: p}, ScrubReq{Bucket: "ec1", Scope: "full"})
	require.NoError(t, err)
	require.Equal(t, "sid-1", resp.SessionID)
	require.True(t, resp.Created)
	require.Equal(t, "ec1", p.gotReq.Bucket)
	require.Equal(t, scrubber.ScopeFull, p.gotReq.Scope)
}

func TestTriggerScrub_BucketRequired(t *testing.T) {
	p := &mockScrubProposer{}
	_, err := TriggerScrub(context.Background(), &Deps{ScrubProposer: p}, ScrubReq{Bucket: ""})
	var ae *Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "invalid", ae.Code)
}

func TestTriggerScrub_InvalidScope(t *testing.T) {
	p := &mockScrubProposer{}
	_, err := TriggerScrub(context.Background(), &Deps{ScrubProposer: p}, ScrubReq{Bucket: "ec1", Scope: "weird"})
	var ae *Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "invalid", ae.Code)
}

func TestTriggerScrub_ProposerNotConfigured(t *testing.T) {
	_, err := TriggerScrub(context.Background(), &Deps{}, ScrubReq{Bucket: "ec1"})
	var ae *Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "internal", ae.Code)
}

func TestTriggerScrub_ProposerError(t *testing.T) {
	p := &mockScrubProposer{err: errAggTest("raft timeout")}
	_, err := TriggerScrub(context.Background(), &Deps{ScrubProposer: p}, ScrubReq{Bucket: "ec1"})
	var ae *Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "internal", ae.Code)
}

type errAggTest string

func (e errAggTest) Error() string { return string(e) }

type mockScrubAggregator struct {
	infos    []ScrubJobInfo
	failures []string
	err      error
}

func (m *mockScrubAggregator) Peers(_ context.Context, _ string) ([]ScrubJobInfo, []string, error) {
	return m.infos, m.failures, m.err
}

func TestGetScrubJob_AggregatesAcrossPeers(t *testing.T) {
	d := &mockDirector{getOK: true, getReturn: scrubber.Session{
		ID: "sid-1", Bucket: "ec1", Status: "running",
		StartedAt: time.Unix(100, 0),
		Stats:     scrubber.SessionStats{Checked: 3, Healthy: 3},
	}}
	agg := &mockScrubAggregator{infos: []ScrubJobInfo{
		{SessionID: "sid-1", Checked: 5, Healthy: 4, Detected: 1, OwnedHere: true, Bucket: "ec1"},
	}}
	info, err := GetScrubJob(context.Background(), &Deps{Director: d, ScrubAggregator: agg}, "sid-1")
	require.NoError(t, err)
	require.Equal(t, int64(8), info.Checked)
	require.Equal(t, int64(7), info.Healthy)
	require.Equal(t, int64(1), info.Detected)
	require.False(t, info.Partial)
}

func TestGetScrubJob_PeerTimeout_Partial(t *testing.T) {
	d := &mockDirector{getOK: true, getReturn: scrubber.Session{
		ID: "sid-2", Bucket: "ec1", Status: "done", StartedAt: time.Unix(100, 0),
	}}
	agg := &mockScrubAggregator{failures: []string{"peer-2"}}
	info, err := GetScrubJob(context.Background(), &Deps{Director: d, ScrubAggregator: agg}, "sid-2")
	require.NoError(t, err)
	require.True(t, info.Partial)
	require.Equal(t, []string{"peer-2"}, info.PeerFailures)
}
