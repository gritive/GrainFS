package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/transport"
)

// Phase B1: forward-on-read for append-segment blobs.
//
// AppendObject writes segment blobs to the owner-node's per-group local disk
// (<groupRoot>/data/<bucket>/<key>_segments/<blobID>). After commit, GETs that
// land on a non-owner node otherwise ENOENT on the segment path. This file
// adds a tiny peer-fetch RPC so non-owners can stream a segment blob from
// whichever peer holds it. Phase B2/B3 will replace this with proactive
// replication; B1 keeps a single owner-local copy and reads through.
//
// Dispatch: each node hosts multiple group backends (one per placement
// group it participates in). The request payload carries the groupID so the
// receiver can look up the right *DistributedBackend via DataGroupManager
// and resolve the segment path against THAT backend's root. Registering one
// handler per backend would not work — QUIC's per-StreamType router keeps
// a single handler, and only the last group to register would win.

// Wire format for the request payload:
//   [2B BE groupIDLen][groupID][2B BE bucketLen][bucket][2B BE keyLen][key][2B BE blobIDLen][blobID]
//   (optional, B2+) [1B kind]
//
// The kind byte (segment=0, coalesced=1) is appended after blobID for
// callers that need to fetch coalesced blobs over the same stream. Legacy
// payloads (no trailing kind byte) decode as kind=segment for backward
// compatibility — the decoder checks remaining bytes after blobID.
//
// Response: framed metadata message followed by raw segment bytes on the
// same bidirectional stream. The metadata payload is a single status byte:
//   0x00 = OK    (body follows)
//   0x01 = ENOENT (no body; segment not on this node)
//   0x02 = ERROR (body is the error string)

const (
	appendSegStatusOK     byte = 0x00
	appendSegStatusNoEnt  byte = 0x01
	appendSegStatusError  byte = 0x02
	maxAppendSegFieldSize      = 4096

	// appendSegKind* identify which on-disk blob the peer should open.
	appendSegKindSegment   byte = 0
	appendSegKindCoalesced byte = 1
)

// errPeerAppendSegmentNotFound is returned by the client when a peer
// answers ENOENT for the segment blob. The caller iterates the next peer.
var errPeerAppendSegmentNotFound = errors.New("append segment not on peer")

// appendSegmentGroupLookup is the minimal interface the node-level handler
// needs to resolve a groupID to a *DistributedBackend. DataGroupManager
// satisfies this via *DataGroupManager.Get(...).Backend(); the
// boot-wiring adapter unwraps that to the embedded *DistributedBackend.
type appendSegmentGroupLookup interface {
	Backend(groupID string) *DistributedBackend
}

// encodeAppendSegmentRequestKind is the kind-aware encoder. Phase B1 callers
// pass appendSegKindSegment; Phase B2 coalesced reads pass
// appendSegKindCoalesced so the peer resolves coalescedBlobPath instead of
// segmentBlobPath.
func encodeAppendSegmentRequestKind(groupID, bucket, key, blobID string, kind byte) ([]byte, error) {
	for _, s := range []string{groupID, bucket, key, blobID} {
		if len(s) > maxAppendSegFieldSize {
			return nil, fmt.Errorf("append segment request field too large: %d", len(s))
		}
	}
	out := make([]byte, 0, 8+len(groupID)+len(bucket)+len(key)+len(blobID)+1)
	var hdr [2]byte
	for _, s := range []string{groupID, bucket, key, blobID} {
		binary.BigEndian.PutUint16(hdr[:], uint16(len(s)))
		out = append(out, hdr[:]...)
		out = append(out, s...)
	}
	// Append kind byte unconditionally; legacy decoders without the kind
	// branch ignore trailing bytes (the current decoder already does).
	out = append(out, kind)
	return out, nil
}

// decodeAppendSegmentRequest decodes a request; kind defaults to
// appendSegKindSegment when the trailing byte is absent (legacy payloads).
func decodeAppendSegmentRequest(buf []byte) (groupID, bucket, key, blobID string, kind byte, err error) {
	read := func(off *int) (string, error) {
		if *off+2 > len(buf) {
			return "", fmt.Errorf("short header")
		}
		n := int(binary.BigEndian.Uint16(buf[*off:]))
		*off += 2
		if n > maxAppendSegFieldSize {
			return "", fmt.Errorf("field too large: %d", n)
		}
		if *off+n > len(buf) {
			return "", fmt.Errorf("short field body")
		}
		s := string(buf[*off : *off+n])
		*off += n
		return s, nil
	}
	off := 0
	kind = appendSegKindSegment // backward-compat default
	if groupID, err = read(&off); err != nil {
		return
	}
	if bucket, err = read(&off); err != nil {
		return
	}
	if key, err = read(&off); err != nil {
		return
	}
	if blobID, err = read(&off); err != nil {
		return
	}
	if off < len(buf) {
		kind = buf[off]
	}
	return
}

// RegisterAppendSegmentHandler installs the node-level handler for
// StreamReadAppendSegment. Called from the boot path AFTER both the
// data-group manager and the QUIC transport are available. lookup may
// return nil for unknown groupIDs (peer doesn't host that group) — the
// handler then answers ENOENT so the client iterates to the next peer.
func RegisterAppendSegmentHandler(tr *transport.QUICTransport, lookup appendSegmentGroupLookup) {
	if tr == nil || lookup == nil {
		return
	}
	tr.HandleRead(transport.StreamReadAppendSegment, func(req *transport.Message) (*transport.Message, io.ReadCloser) {
		groupID, bucket, key, blobID, kind, err := decodeAppendSegmentRequest(req.Payload)
		if err != nil {
			return errorAppendSegmentMeta(req, err.Error()), nil
		}
		b := lookup.Backend(groupID)
		if b == nil {
			// Peer doesn't host this group, so it never has the segment.
			return &transport.Message{Type: req.Type, ID: req.ID, Status: transport.StatusOK, Payload: []byte{appendSegStatusNoEnt}}, nil
		}
		var path string
		switch kind {
		case appendSegKindCoalesced:
			path = b.coalescedBlobPath(bucket, key, blobID)
		default:
			path = b.SegmentBlobPath(bucket, key, blobID)
		}
		f, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				return &transport.Message{Type: req.Type, ID: req.ID, Status: transport.StatusOK, Payload: []byte{appendSegStatusNoEnt}}, nil
			}
			return errorAppendSegmentMeta(req, err.Error()), nil
		}
		return &transport.Message{Type: req.Type, ID: req.ID, Status: transport.StatusOK, Payload: []byte{appendSegStatusOK}}, f
	})
}

func errorAppendSegmentMeta(req *transport.Message, msg string) *transport.Message {
	payload := make([]byte, 1+len(msg))
	payload[0] = appendSegStatusError
	copy(payload[1:], msg)
	return &transport.Message{Type: req.Type, ID: req.ID, Status: transport.StatusOK, Payload: payload}
}

// readAppendSegmentFromPeerKind issues a single-peer fetch for either a raw
// segment or a coalesced blob (selected via kind). Returns
// errPeerAppendSegmentNotFound when the peer answers ENOENT so the caller
// can iterate to the next peer.
func (b *DistributedBackend) readAppendSegmentFromPeerKind(ctx context.Context, peer, bucket, key, blobID string, kind byte) (io.ReadCloser, error) {
	if b.shardSvc == nil || b.shardSvc.transport == nil {
		return nil, fmt.Errorf("append segment peer fetch: no transport")
	}
	addr, err := b.shardSvc.resolvePeerAddress(peer)
	if err != nil {
		return nil, err
	}
	payload, err := encodeAppendSegmentRequestKind(b.groupID, bucket, key, blobID, kind)
	if err != nil {
		return nil, err
	}
	req := &transport.Message{Type: transport.StreamReadAppendSegment, Payload: payload}
	resp, body, err := b.shardSvc.transport.CallRead(ctx, addr, req)
	if err != nil {
		return nil, fmt.Errorf("call peer %s: %w", peer, err)
	}
	if len(resp.Payload) == 0 {
		if body != nil {
			_ = body.Close()
		}
		return nil, fmt.Errorf("empty append-segment response from %s", peer)
	}
	switch resp.Payload[0] {
	case appendSegStatusOK:
		if body == nil {
			return nil, fmt.Errorf("peer %s ok but no body", peer)
		}
		return body, nil
	case appendSegStatusNoEnt:
		if body != nil {
			_ = body.Close()
		}
		return nil, errPeerAppendSegmentNotFound
	case appendSegStatusError:
		if body != nil {
			_ = body.Close()
		}
		return nil, fmt.Errorf("peer %s error: %s", peer, string(resp.Payload[1:]))
	default:
		if body != nil {
			_ = body.Close()
		}
		return nil, fmt.Errorf("peer %s: unknown status byte %d", peer, resp.Payload[0])
	}
}

// fetchAppendBlobFromAnyPeer iterates over live peers (excluding self) and
// returns the first peer that holds the blob (raw segment or coalesced,
// selected via kind). Returns an ENOENT-style error only when every peer
// reports missing.
//
// Phase B1: we do not pre-resolve the owner address; the owner of an
// append group is the one peer that actually has the blob, but threading
// that resolution into the GET path needs cross-package wiring we are
// deferring. With 4 nodes this is at most 3 cheap RPCs in the worst case.
func (b *DistributedBackend) fetchAppendBlobFromAnyPeer(ctx context.Context, bucket, key, blobID string, kind byte) (io.ReadCloser, error) {
	if b.shardSvc == nil {
		return nil, fmt.Errorf("no shard service")
	}
	self := b.currentSelfAddr()
	peers := b.liveNodes()
	tried := 0
	var firstNonENOENT error
	for _, peer := range peers {
		if peer == "" || peer == self {
			continue
		}
		tried++
		rc, err := b.readAppendSegmentFromPeerKind(ctx, peer, bucket, key, blobID, kind)
		if err == nil {
			metrics.AppendForwardOnReadTotal.Inc()
			return rc, nil
		}
		if !errors.Is(err, errPeerAppendSegmentNotFound) && firstNonENOENT == nil {
			firstNonENOENT = err
		}
	}
	if tried == 0 {
		return nil, fmt.Errorf("no peers to fetch append blob %s", blobID)
	}
	if firstNonENOENT != nil {
		return nil, firstNonENOENT
	}
	return nil, fmt.Errorf("append blob %s missing on all peers", blobID)
}
