// Package cluster: production implementations of PeerKEKProbe and
// RaftConfigReader for the KEK rotation leader (Task 11 wiring).
//
// The leader-side orchestrator (KEKRotationLeader) consumes these interfaces;
// tests inject fakes. Production injection happens at serve-runtime boot
// once MetaRaft + QUICTransport are constructed.
package cluster

import (
	"context"
	"fmt"
	"sort"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// PeerProbeDialer abstracts a QUIC stream call: caller hands the leader a
// concrete impl bound to a *transport.QUICTransport, the leader uses it to
// reach voter QUIC addresses. Keeping this an interface rather than the raw
// *QUICTransport keeps the cluster package independent of QUIC connection
// pooling and the boot-time dialer plumbing.
//
// Both dialer variants delegate to QUICTransport.Call. Production uses the
// same instance for both — the StreamType byte routes to the right handler.
type PeerProbeDialer interface {
	CallKEKDiskSpace(ctx context.Context, peer string, payload []byte) ([]byte, error)
	CallKEKLeaseSnapshot(ctx context.Context, peer string, payload []byte) ([]byte, error)
}

// QUICPeerProbeDialer is the production PeerProbeDialer backed by a
// *transport.QUICTransport. Built once at boot.
type QUICPeerProbeDialer struct{ T *transport.QUICTransport }

// CallKEKDiskSpace dispatches a StreamKEKDiskSpaceProbe request to peer and
// returns the response payload (or an error). On non-OK status the underlying
// QUIC layer surfaces an error.
func (d *QUICPeerProbeDialer) CallKEKDiskSpace(ctx context.Context, peer string, payload []byte) ([]byte, error) {
	resp, err := d.T.Call(ctx, peer, &transport.Message{
		Type:    transport.StreamKEKDiskSpaceProbe,
		Payload: payload,
	})
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// CallKEKLeaseSnapshot dispatches a StreamKEKLeaseSnapshotProbe request.
func (d *QUICPeerProbeDialer) CallKEKLeaseSnapshot(ctx context.Context, peer string, payload []byte) ([]byte, error) {
	resp, err := d.T.Call(ctx, peer, &transport.Message{
		Type:    transport.StreamKEKLeaseSnapshotProbe,
		Payload: payload,
	})
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// peerKEKProbeImpl is the production PeerKEKProbe. It fans out
// GetKEKDiskSpace / GetKEKLeaseSnapshot to every voter via the QUIC dialer,
// with a self-shortcut that invokes the local handlers directly so the
// leader's own disk + lease count counts toward the cluster check without
// taking a network roundtrip.
type peerKEKProbeImpl struct {
	dialer       PeerProbeDialer
	raftConfig   RaftConfigReader
	selfID       string
	localDiskFn  func() (KEKDiskSpaceResp, error)
	localLeaseFn func(version uint32) (KEKLeaseSnapshotResp, error)
}

// NewPeerKEKProbe constructs a PeerKEKProbe wired against a QUIC dialer for
// remote voters and a pair of local-self closures for the leader's own
// keystore-dir free bytes + in-flight lease snapshot.
//
// raftConfig is the same RaftConfigReader the leader uses elsewhere; the
// probe consults it to enumerate the voter set.
//
// localDiskFn / localLeaseFn must be non-nil — they short-circuit the
// self-call. localDiskFn typically wraps statfsDiskSpace bound to the
// keystore directory; localLeaseFn wraps the local KEKLeaseTracker.Count.
func NewPeerKEKProbe(
	dialer PeerProbeDialer,
	raftConfig RaftConfigReader,
	selfID string,
	localDiskFn func() (KEKDiskSpaceResp, error),
	localLeaseFn func(version uint32) (KEKLeaseSnapshotResp, error),
) PeerKEKProbe {
	return &peerKEKProbeImpl{
		dialer:       dialer,
		raftConfig:   raftConfig,
		selfID:       selfID,
		localDiskFn:  localDiskFn,
		localLeaseFn: localLeaseFn,
	}
}

// ProbeAllKEKDiskSpace fans out a disk-space probe to every voter, including
// the local node (self-shortcut). Returns one entry per voter on success;
// returns the first error encountered.
func (p *peerKEKProbeImpl) ProbeAllKEKDiskSpace(ctx context.Context) ([]KEKDiskSpaceResp, error) {
	voters, _ := p.raftConfig.EffectiveConfiguration()
	out := make([]KEKDiskSpaceResp, 0, len(voters))
	for _, v := range voters {
		if v == p.selfID {
			resp, err := p.localDiskFn()
			if err != nil {
				return nil, fmt.Errorf("KEKDiskSpace self: %w", err)
			}
			if resp.NodeID == "" {
				resp.NodeID = p.selfID
			}
			out = append(out, resp)
			continue
		}
		resp, err := GetKEKDiskSpace(ctx, v, p.dialer.CallKEKDiskSpace)
		if err != nil {
			return nil, fmt.Errorf("KEKDiskSpace %s: %w", v, err)
		}
		out = append(out, resp)
	}
	return out, nil
}

// ProbeKEKLeaseSnapshot fans out a lease-snapshot probe to every voter in
// `voters`. Returns one sample per voter on success (in input order).
func (p *peerKEKProbeImpl) ProbeKEKLeaseSnapshot(ctx context.Context, voters []string, version uint32) ([]LeaseAttestationSample, error) {
	out := make([]LeaseAttestationSample, 0, len(voters))
	for _, v := range voters {
		if v == p.selfID {
			resp, err := p.localLeaseFn(version)
			if err != nil {
				return nil, fmt.Errorf("KEKLeaseSnapshot self: %w", err)
			}
			if resp.NodeID == "" {
				resp.NodeID = p.selfID
			}
			out = append(out, LeaseAttestationSample{
				NodeID:          resp.NodeID,
				ObservedAtIndex: resp.ObservedAtRaftCommitIndex,
				LeaseCount:      resp.LeaseCount,
			})
			continue
		}
		resp, err := GetKEKLeaseSnapshot(ctx, v, version, p.dialer.CallKEKLeaseSnapshot)
		if err != nil {
			return nil, fmt.Errorf("KEKLeaseSnapshot %s: %w", v, err)
		}
		out = append(out, LeaseAttestationSample{
			NodeID:          resp.NodeID,
			ObservedAtIndex: resp.ObservedAtRaftCommitIndex,
			LeaseCount:      resp.LeaseCount,
		})
	}
	return out, nil
}

// metaRaftConfigReader implements RaftConfigReader by wrapping
// MetaRaft.Node().Configuration(). The voter IDs are sorted ascending unique
// per the RaftConfigReader contract; learners are filtered out (only Voters
// participate in lease attestation and disk-space gating).
type metaRaftConfigReader struct{ mr *MetaRaft }

// NewMetaRaftConfigReader wraps a *MetaRaft as a RaftConfigReader.
func NewMetaRaftConfigReader(mr *MetaRaft) RaftConfigReader {
	return &metaRaftConfigReader{mr: mr}
}

// EffectiveConfiguration returns the current voter list (sorted) + the
// underlying raft committed index. Phase B doesn't have a stable
// "configuration index" accessor on the v2 raft.Node API; we use the
// committed index as a monotonically advancing config-change watermark
// (every voter-set change is committed as an entry, so a divergent committed
// index implies a possible config change — the race-detect path is then
// pessimistic but never silently misses a real change).
func (r *metaRaftConfigReader) EffectiveConfiguration() ([]string, uint64) {
	cfg := r.mr.Node().Configuration()
	voters := make([]string, 0, len(cfg.Servers))
	for _, srv := range cfg.Servers {
		if srv.Suffrage == raft.Voter {
			voters = append(voters, string(srv.ID))
		}
	}
	sort.Strings(voters)
	return voters, r.mr.Node().CommittedIndex()
}
