package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gritive/GrainFS/internal/transport"
)

// indexGroupNotReadyMsg is the on-wire marker for the boot-race case: the
// forwarded group ID is not yet in the leader's IndexGroupManager map. The
// sender string-matches it (mirroring backend.go's raft.ErrNotLeader recovery
// at the propose-forward reply boundary, since encodeProposeForwardReply
// flattens unknown sentinels to applyErrCodeInternal and errors.Is identity
// does not survive the wire). The marker must be unique enough that a genuine
// hard error never collides with it.
const indexGroupNotReadyMsg = "index group: not ready (group not yet registered)"

// errIndexGroupNotReady is the local sentinel; its Error() string IS the wire
// marker, so encode→decode round-trips back to a string the sender recognizes.
var errIndexGroupNotReady = errors.New(indexGroupNotReadyMsg)

// isIndexGroupNotReady reports whether a decoded propose-forward reply error is
// the retryable boot-race not-ready signal (as opposed to a terminal failure).
func isIndexGroupNotReady(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, errIndexGroupNotReady) || strings.Contains(err.Error(), indexGroupNotReadyMsg)
}

// indexGroupForwardRetryBackoff is the wait between not-ready retries during the
// boot race. Small so a follower that beats its leader's group registration only
// pauses briefly.
const indexGroupForwardRetryBackoff = 50 * time.Millisecond

// IndexGroupProposeForwardReceiver handles StreamIndexGroupProposeForward on the
// leader side: it resolves the forwarded group ID to a local index group via the
// IndexGroupManager and proposes the command on that group's raft node. Mirrors
// ForwardReceiver.HandleGroupPropose, but keyed on index groups and kept
// independent of ForwardReceiver (which boot constructs with the data-group
// manager). Boot wiring (calling Register with live groups) is Task 5.
type IndexGroupProposeForwardReceiver struct {
	groups *IndexGroupManager
}

func NewIndexGroupProposeForwardReceiver(groups *IndexGroupManager) *IndexGroupProposeForwardReceiver {
	return &IndexGroupProposeForwardReceiver{groups: groups}
}

// Register installs this receiver as the handler for StreamIndexGroupProposeForward.
func (r *IndexGroupProposeForwardReceiver) Register(shardSvc *ShardService) {
	shardSvc.RegisterHandler(transport.StreamIndexGroupProposeForward, r.Handle)
}

// Handle decodes {groupID, data}, looks up the local index group, and proposes.
// Boot race (group not in the map) → retryable not-ready reply. Otherwise it
// proposes on the group's node, waits for local apply, and harvests any FSM
// apply error so it round-trips to the follower as a terminal failure (mirrors
// HandleGroupPropose's ProposeWait→WaitApplied→ApplyError harvest).
func (r *IndexGroupProposeForwardReceiver) Handle(req *transport.Message) *transport.Message {
	groupID, data, err := decodeGroupForwardPayload(req.Payload)
	if err != nil {
		return indexGroupProposeReply(0, err)
	}
	g, ok := r.groups.Lookup(groupID)
	if !ok || g.node == nil {
		// Boot race: the group is not yet registered. Retryable.
		return indexGroupProposeReply(0, errIndexGroupNotReady)
	}
	ctx, cancel := context.WithTimeout(context.Background(), proposeForwardTimeout)
	defer cancel()
	idx, err := g.node.ProposeWait(ctx, data)
	if err == nil {
		err = g.waitAppliedResult(ctx, idx)
	}
	if err != nil {
		return indexGroupProposeReply(0, err)
	}
	return indexGroupProposeReply(idx, nil)
}

// indexGroupProposeReply builds the leader→follower reply on the same wire
// format as groupProposeReply (encodeProposeForwardReply), so the existing
// decodeProposeForwardReply parses it unchanged.
func indexGroupProposeReply(index uint64, err error) *transport.Message {
	return &transport.Message{
		Type:    transport.StreamIndexGroupProposeForward,
		Payload: encodeProposeForwardReply(index, err),
	}
}

// IndexGroupProposeForwardDialer dials the given peer with a forward payload and
// returns the raw reply payload. Injected so boot can supply CallPooled and
// tests can supply an in-proc route. Mirrors MetaForwardDialer.
type IndexGroupProposeForwardDialer func(ctx context.Context, peer string, payload []byte) ([]byte, error)

// IndexGroupProposeForwardSender forwards an index-group proposal from a
// follower to the group's current leader. The leader hint is resolved by an
// optional resolver (LeaderID() → dial target); on not-ready (boot race) it
// retries with a short backoff against the refreshed hint.
type IndexGroupProposeForwardSender struct {
	dialer   IndexGroupProposeForwardDialer
	resolve  func(hint string) string
	maxTries int
}

func NewIndexGroupProposeForwardSender(d IndexGroupProposeForwardDialer) *IndexGroupProposeForwardSender {
	return &IndexGroupProposeForwardSender{
		dialer:   d,
		maxTries: defaultIndexGroupForwardTries,
	}
}

// defaultIndexGroupForwardTries bounds boot-race retries. The deadline-less
// caller is additionally bounded by ctx; this caps the loop when ctx is generous.
const defaultIndexGroupForwardTries = 64

// WithLeaderHintResolver wires a resolver that maps a raft LeaderID hint to a
// dial target (mirrors ForwardSender.WithLeaderHintResolver). Without it, the
// hint is dialed verbatim.
func (s *IndexGroupProposeForwardSender) WithLeaderHintResolver(resolve func(hint string) string) *IndexGroupProposeForwardSender {
	s.resolve = resolve
	return s
}

// Send forwards data for groupID to the leader identified by leaderHint and
// returns the leader's committed log index. The signature matches the hook
// Task 5 installs on each indexGroup:
//
//	func(ctx, data) (uint64, error) { return sender.Send(ctx, g.node.LeaderID(), groupID, data) }
//
// On not-ready (boot race) it retries with backoff; on any other error it
// returns immediately (terminal). With no leader hint there is no peer to dial.
func (s *IndexGroupProposeForwardSender) Send(ctx context.Context, leaderHint, groupID string, data []byte) (uint64, error) {
	payload := encodeGroupForwardPayload(groupID, data)
	var lastErr error
	for try := 0; try < s.maxTries; try++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return 0, fmt.Errorf("index group forward: %w (last: %v)", err, lastErr)
			}
			return 0, err
		}
		target := s.target(leaderHint)
		if target == "" {
			return 0, fmt.Errorf("index group forward: no leader to forward to")
		}
		reply, err := s.dialer(ctx, target, payload)
		if err != nil {
			lastErr = err
			if !sleepCtx(ctx, indexGroupForwardRetryBackoff) {
				return 0, ctx.Err()
			}
			continue
		}
		idx, applyErr, transportErr := decodeProposeForwardReply(reply)
		if transportErr != nil {
			return 0, fmt.Errorf("index group forward: %w", transportErr)
		}
		if applyErr != nil {
			if isIndexGroupNotReady(applyErr) {
				// Boot race: leader's group not yet registered. Retry.
				lastErr = applyErr
				if !sleepCtx(ctx, indexGroupForwardRetryBackoff) {
					return 0, ctx.Err()
				}
				continue
			}
			// Terminal: FSM apply error or other hard failure.
			return 0, applyErr
		}
		return idx, nil
	}
	if lastErr != nil {
		return 0, fmt.Errorf("index group forward: retries exhausted: %w", lastErr)
	}
	return 0, fmt.Errorf("index group forward: retries exhausted")
}

func (s *IndexGroupProposeForwardSender) target(hint string) string {
	if s.resolve != nil {
		return s.resolve(hint)
	}
	return hint
}

// sleepCtx waits d or until ctx is done; returns false if ctx was cancelled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
