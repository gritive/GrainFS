package clusteradmin

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
)

// RemovePeerOptions carries the inputs the operator-facing remove-peer
// flow needs. The caller (CLI) populates this from flags + std streams.
type RemovePeerOptions struct {
	Endpoint  string
	ID        string
	Force     bool
	AssumeYes bool
	Timeout   time.Duration

	// Streams: Stderr is used for the pre-flight summary + prompt. Stdin is
	// read for the y/N confirmation when AssumeYes is false. Stdout is where
	// the success line lands so it composes cleanly in pipelines.
	Stdout io.Writer
	Stderr io.Writer
	Stdin  io.Reader
}

// errInvalidTimeout reports a non-positive Timeout. Returned eagerly so the
// caller sees a clear message instead of a "context deadline exceeded" from
// context.WithTimeout(0).
var errInvalidTimeout = errors.New("timeout must be positive")

// RemovePeer drives the full operator workflow for removing a meta-Raft
// voter:
//
//  1. fetch /api/cluster/status
//  2. validate cluster mode, target-in-peers, leader-locality
//  3. compute pre-flight; refuse if it would break quorum unless Force is set
//  4. prompt y/N unless AssumeYes is set
//  5. POST /api/cluster/remove-peer; surface server errors verbatim
//
// Errors returned here are operator-readable; the CLI layer just prints them.
func RemovePeer(ctx context.Context, opts RemovePeerOptions) error {
	if opts.Stdout == nil || opts.Stderr == nil {
		return errors.New("clusteradmin.RemovePeer: Stdout and Stderr are required")
	}
	if opts.Timeout <= 0 {
		return errInvalidTimeout
	}
	client := NewClient(opts.Endpoint)
	callCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	status, err := client.Status(callCtx)
	if err != nil {
		return fmt.Errorf("fetch cluster status: %w", err)
	}
	if status.Mode != "cluster" {
		return fmt.Errorf("node is not in cluster mode (mode=%q); remove-peer requires a Raft cluster", status.Mode)
	}
	if !Contains(status.Peers, opts.ID) {
		return fmt.Errorf("peer %q is not a current voter; current peers: %s",
			opts.ID, strings.Join(status.Peers, ", "))
	}
	if status.State != "Leader" {
		return fmt.Errorf("connected node is not the leader (state=%s); rerun on leader %q",
			status.State, status.LeaderID)
	}

	result, ok := removePeerPreflightFromStatus(status, opts.ID)
	if !ok {
		return errors.New("pre-flight: peer snapshot unavailable; cluster liveness snapshot is required before membership mutation")
	}

	fmt.Fprintf(opts.Stderr, "Removing %s from cluster:\n  %s\n", opts.ID, removePeerPreflightSummary(result))
	if !result.Allowed {
		switch result.Reason {
		case cluster.RemovePeerPreflightQuorumWouldBreak:
			if !opts.Force {
				return fmt.Errorf("pre-flight: alive_after (%d) < new_quorum (%d); rerun with --force to override",
					result.AliveAfter, result.NewQuorum)
			}
		case cluster.RemovePeerPreflightIdentityUnresolved:
			return fmt.Errorf("pre-flight: identity unresolved; remove blocking peers first: %s",
				strings.Join(result.BlockingPeers, ", "))
		default:
			return fmt.Errorf("pre-flight: %s", result.Reason)
		}
	}

	if !opts.AssumeYes {
		if opts.Stdin == nil {
			return errors.New("interactive confirmation requires Stdin or AssumeYes=true")
		}
		fmt.Fprint(opts.Stderr, "Proceed? [y/N]: ")
		ans, _ := bufio.NewReader(opts.Stdin).ReadString('\n')
		ans = strings.TrimSpace(strings.ToLower(ans))
		if ans != "y" && ans != "yes" {
			return errors.New("aborted")
		}
	}

	if err := client.RemovePeer(callCtx, opts.ID, opts.Force); err != nil {
		return err
	}
	fmt.Fprintf(opts.Stdout, "removed %s\n", opts.ID)
	return nil
}

func removePeerPreflightFromStatus(status *Status, target string) (cluster.RemovePeerPreflightResult, bool) {
	if len(status.PeerSnapshot) == 0 {
		return cluster.RemovePeerPreflightResult{}, false
	}
	return cluster.EvaluateRemovePeerPreflight(cluster.RemovePeerPreflightInput{
		TargetID: target,
		Voters:   status.Peers,
		Snapshot: status.PeerSnapshot,
	}), true
}

func removePeerPreflightSummary(result cluster.RemovePeerPreflightResult) string {
	return fmt.Sprintf("voters: %d -> %d   alive_after: %d   new_quorum: %d",
		result.VotersAfter+1, result.VotersAfter, result.AliveAfter, result.NewQuorum)
}

// PeersOptions describes how the peers listing should be fetched and rendered.
type PeersOptions struct {
	Endpoint string
	Format   string // "text" or "json"
	Timeout  time.Duration
	Stdout   io.Writer
}

// Peers fetches /api/cluster/status and renders either a tab-aligned voter
// table (default) or the raw JSON payload.
func Peers(ctx context.Context, opts PeersOptions) error {
	if opts.Stdout == nil {
		return errors.New("clusteradmin.Peers: Stdout is required")
	}
	if opts.Timeout <= 0 {
		return errInvalidTimeout
	}
	client := NewClient(opts.Endpoint)
	callCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	status, err := client.Status(callCtx)
	if err != nil {
		return err
	}
	if opts.Format == "json" {
		raw, _ := json.MarshalIndent(status, "", "  ")
		fmt.Fprintln(opts.Stdout, string(raw))
		return nil
	}
	if status.Mode != "cluster" {
		fmt.Fprintf(opts.Stdout, "mode: %s (no peers)\n", status.Mode)
		return nil
	}
	return RenderPeersTable(opts.Stdout, PeersFromStatus(status))
}

// EventsOptions describes how the audit-log listing should be fetched,
// filtered, and rendered.
type EventsOptions struct {
	Endpoint    string
	Since       time.Duration
	Limit       int
	TypeFilters []string
	Format      string // "text" or "json"
	Timeout     time.Duration
	Stdout      io.Writer
}

// Events fetches /api/eventlog, applies the client-side action filter, and
// renders either a tab-aligned table (default) or the raw JSON array.
func Events(ctx context.Context, opts EventsOptions) error {
	if opts.Stdout == nil {
		return errors.New("clusteradmin.Events: Stdout is required")
	}
	if opts.Timeout <= 0 {
		return errInvalidTimeout
	}
	client := NewClient(opts.Endpoint)
	callCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	events, err := client.EventLog(callCtx, opts.Since, opts.Limit)
	if err != nil {
		return err
	}
	events = FilterEventsByAction(events, opts.TypeFilters)

	if opts.Format == "json" {
		raw, _ := json.MarshalIndent(events, "", "  ")
		fmt.Fprintln(opts.Stdout, string(raw))
		return nil
	}
	return RenderEventsTable(opts.Stdout, events)
}
