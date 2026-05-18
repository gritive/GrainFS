package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/stretchr/testify/require"
)

type fakeCapabilityCluster struct {
	*fakeClusterInfo
	evidence map[string]map[string]bool
}

func (f *fakeCapabilityCluster) CapabilityEvidence() map[string]map[string]bool {
	return f.evidence
}

func TestCapabilitiesStatus_ReportsEvidence(t *testing.T) {
	s := &Server{cluster: &fakeCapabilityCluster{
		fakeClusterInfo: &fakeClusterInfo{},
		evidence: map[string]map[string]bool{
			"node-1": {"multipart_listing_v1": true},
			"node-2": {"multipart_listing_v1": false},
		},
	}}

	c := app.NewContext(0)
	s.capabilitiesStatus(context.Background(), c)

	var body struct {
		Peers map[string]map[string]bool `json:"peers"`
	}
	require.NoError(t, json.Unmarshal(c.Response.Body(), &body))
	require.True(t, body.Peers["node-1"]["multipart_listing_v1"])
	require.False(t, body.Peers["node-2"]["multipart_listing_v1"])
}

func TestCapabilitiesStatus_EmptyWhenNoCluster(t *testing.T) {
	s := &Server{}
	c := app.NewContext(0)
	s.capabilitiesStatus(context.Background(), c)

	var body struct {
		Peers map[string]map[string]bool `json:"peers"`
	}
	require.NoError(t, json.Unmarshal(c.Response.Body(), &body))
	require.Empty(t, body.Peers)
}
