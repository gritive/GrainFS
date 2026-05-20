package clusteradmin

import (
	"encoding/json"
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
	State    string // "unknown_configured" | "down" | "unresolved_legacy"
}

// PeersFromStatus derives the rendered peer list from a Status, filtering
// down_nodes and peer_states into State and tagging the leader. The wire
// state "configured" is rendered as unknown_configured so operators do not
// read identity resolution as fresh liveness.
func PeersFromStatus(s *Status) []PeerRow {
	peers := append([]string(nil), s.Peers...)
	sort.Strings(peers)
	down := make(map[string]struct{}, len(s.DownNodes))
	for _, d := range s.DownNodes {
		down[d] = struct{}{}
	}
	out := make([]PeerRow, 0, len(peers))
	for _, p := range peers {
		row := PeerRow{ID: p, RaftAddr: s.PeerAddrs[p], Role: "follower", State: renderPeerState("configured")}
		if p == s.LeaderID {
			row.Role = "leader"
		}
		if _, isDown := down[p]; isDown {
			row.State = "down"
		}
		if state := s.PeerStates[p]; state != "" {
			row.State = renderPeerState(state)
		}
		out = append(out, row)
	}
	return out
}

func renderPeerState(state string) string {
	if state == "configured" {
		return "unknown_configured"
	}
	return state
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
	return strings.Join(parts, " ")
}

// RenderClusterConfigShow prints the REV header + a KEY/EFFECTIVE/SOURCE
// tabwriter table. With asJSON, emits the raw response verbatim.
func RenderClusterConfigShow(w io.Writer, resp *ClusterConfigResponse, asJSON bool) error {
	if asJSON {
		return json.NewEncoder(w).Encode(resp)
	}
	fmt.Fprintf(w, "REV: %d\n\n", resp.Rev)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "KEY\tEFFECTIVE\tSOURCE")
	keys := make([]string, 0, len(resp.Source))
	for k := range resp.Source {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(tw, "%s\t%v\t%s\n", k, resp.Effective[k], resp.Source[k])
	}
	return tw.Flush()
}

// RenderClusterConfigGet prints "<key> = <value>  (<source>)" or the JSON
// envelope used by `cluster config get --json`.
func RenderClusterConfigGet(w io.Writer, resp *ClusterConfigResponse, key string, asJSON bool) error {
	if asJSON {
		return json.NewEncoder(w).Encode(map[string]any{
			"key":    key,
			"value":  resp.Effective[key],
			"source": resp.Source[key],
		})
	}
	fmt.Fprintf(w, "%s = %v  (%s)\n", key, resp.Effective[key], resp.Source[key])
	return nil
}

// RenderClusterConfigDiff prints only rows whose source == "explicit".
func RenderClusterConfigDiff(w io.Writer, resp *ClusterConfigResponse) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "KEY\tVALUE")
	keys := make([]string, 0)
	for k, src := range resp.Source {
		if src == "explicit" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(tw, "%s\t%v\n", k, resp.Effective[k])
	}
	return tw.Flush()
}

// ParseClusterConfigKVs turns ["k=v",...] into a map. Values parse as JSON
// literals first (so 30, true, "x" work); non-JSON values like https://…
// URLs fall back to a plain string.
func ParseClusterConfigKVs(kvs []string) (map[string]any, error) {
	out := map[string]any{}
	for _, kv := range kvs {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return nil, fmt.Errorf("invalid <key>=<value>: %q", kv)
		}
		var parsed any
		if err := json.Unmarshal([]byte(v), &parsed); err != nil {
			parsed = v
		}
		out[k] = parsed
	}
	return out, nil
}
