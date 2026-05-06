package clusteradmin

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

// PeerRow is the per-row payload that RenderPeers emits. Exposed so callers
// that want a non-table format (JSON / structured logging) reuse the same
// derivation logic.
type PeerRow struct {
	ID       string
	RaftAddr string
	Role     string // "leader" | "follower"
	State    string // "configured" | "down" | "unresolved_legacy"
}

// PeersFromStatus derives the rendered peer list from a Status, filtering
// down_nodes and peer_states into State and tagging the leader.
func PeersFromStatus(s *Status) []PeerRow {
	peers := append([]string(nil), s.Peers...)
	sort.Strings(peers)
	down := make(map[string]struct{}, len(s.DownNodes))
	for _, d := range s.DownNodes {
		down[d] = struct{}{}
	}
	out := make([]PeerRow, 0, len(peers))
	for _, p := range peers {
		row := PeerRow{ID: p, RaftAddr: s.PeerAddrs[p], Role: "follower", State: "configured"}
		if p == s.LeaderID {
			row.Role = "leader"
		}
		if _, isDown := down[p]; isDown {
			row.State = "down"
		}
		if state := s.PeerStates[p]; state != "" {
			row.State = state
		}
		out = append(out, row)
	}
	return out
}

// RenderPeersTable writes a tab-aligned peer table to w.
func RenderPeersTable(w io.Writer, rows []PeerRow) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE_ID\tRAFT_ADDR\tROLE\tSTATE")
	for _, r := range rows {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", r.ID, r.RaftAddr, r.Role, r.State)
	}
	return tw.Flush()
}

// FilterEventsByAction keeps only events whose Action is in the actions set.
// Empty actions returns events unchanged so callers can pass user-supplied
// flags directly without branching.
func FilterEventsByAction(events []Event, actions []string) []Event {
	if len(actions) == 0 {
		return events
	}
	keep := make(map[string]struct{}, len(actions))
	for _, a := range actions {
		keep[a] = struct{}{}
	}
	out := make([]Event, 0, len(events))
	for _, e := range events {
		if _, ok := keep[e.Action]; ok {
			out = append(out, e)
		}
	}
	return out
}

// RenderEventsTable writes audit events as a tab-aligned table.
func RenderEventsTable(w io.Writer, events []Event) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "TIME\tTYPE\tACTION\tDETAIL")
	for _, e := range events {
		ts := time.Unix(e.Timestamp, 0).Format(time.RFC3339)
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", ts, e.Type, e.Action, EventDetail(e))
	}
	return tw.Flush()
}

// EventDetail compacts the structured fields of an Event into one line.
func EventDetail(e Event) string {
	parts := []string{}
	if e.Bucket != "" {
		parts = append(parts, "bucket="+e.Bucket)
	}
	if e.Key != "" {
		parts = append(parts, "key="+e.Key)
	}
	if e.User != "" {
		parts = append(parts, "user="+e.User)
	}
	if len(e.Metadata) > 0 {
		keys := make([]string, 0, len(e.Metadata))
		for k := range e.Metadata {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s=%v", k, e.Metadata[k]))
		}
	}
	return strings.Join(parts, " ")
}
