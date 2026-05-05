package admin_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gritive/GrainFS/internal/server/admin"
	"github.com/stretchr/testify/require"
)

type stubVlogAPI struct {
	resp admin.VlogBreakdownResp
	err  error
}

func (s *stubVlogAPI) Breakdown() (admin.VlogBreakdownResp, error) { return s.resp, s.err }

func TestGetVlogBreakdown_NotConfigured(t *testing.T) {
	d := &admin.Deps{}
	_, err := admin.GetVlogBreakdown(context.Background(), d)
	var ae *admin.Error
	require.True(t, errors.As(err, &ae))
	require.Equal(t, "not_found", ae.Code)
}

func TestGetVlogBreakdown_DelegatesToAdapter(t *testing.T) {
	want := admin.VlogBreakdownResp{
		TotalVlogBytes: 12345,
		LimitBytes:     67890,
		Ratio:          0.18,
		Level:          "ok",
		Categories: []admin.VlogCategoryBytes{
			{Category: "meta", VlogBytes: 9000},
			{Category: "receipts", VlogBytes: 3000},
			{Category: "dedup", VlogBytes: 345},
		},
		GCFailures: map[string]int32{"meta": 0, "receipts": 0, "dedup": 0},
		SmokeReport: admin.VlogSmokeReport{
			Live:  []string{},
			Stale: []string{},
		},
	}
	d := &admin.Deps{VlogBreakdown: &stubVlogAPI{resp: want}}
	got, err := admin.GetVlogBreakdown(context.Background(), d)
	require.NoError(t, err)
	require.Equal(t, want, got)
	// Spec contract: caller (adapter) is responsible for sort-desc on
	// Categories. Verify the test fixture itself is sorted so a regression in
	// the adapter is caught downstream.
	for i := 1; i < len(got.Categories); i++ {
		require.GreaterOrEqual(t, got.Categories[i-1].VlogBytes, got.Categories[i].VlogBytes,
			"breakdown contract: categories must be sorted desc by vlog_bytes")
	}
}

func TestGetVlogBreakdown_PropagatesAdapterError(t *testing.T) {
	stub := &stubVlogAPI{err: errors.New("statfs failed")}
	d := &admin.Deps{VlogBreakdown: stub}
	_, err := admin.GetVlogBreakdown(context.Background(), d)
	require.Error(t, err)
	require.Contains(t, err.Error(), "statfs failed")
}
