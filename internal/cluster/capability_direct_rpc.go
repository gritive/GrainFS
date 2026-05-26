// Package cluster: direct capability probe RPC (Task 1b, Option A).
//
// RPC mechanism: QUICTransport.Call / Handle with StreamCapabilityProbe (0x17).
// Production wires with an injected capabilityProbeDialer (same pattern as
// forwardDialer in forward_sender.go). Tests pass a fake function — no QUIC
// needed in unit tests.
//
// Wire layout for requests and responses uses a simple length-prefixed binary
// encoding (no FlatBuffers) because this RPC has no FBS schema.
package cluster

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
)

// capProbeRequestMagic guards against misrouted payloads on StreamCapabilityProbe.
var capProbeRequestMagic = []byte("CAPREQ\x01")

// capProbeReplyMagic guards against misrouted payloads.
var capProbeReplyMagic = []byte("CAPREP\x01")

// capabilityProbeDialer abstracts the transport Call for testability.
// Production wires it to: func(ctx, peer, payload) { return quicTransport.Call(ctx, peer, &transport.Message{Type: transport.StreamCapabilityProbe, Payload: payload}) }
type capabilityProbeDialer func(ctx context.Context, peer string, payload []byte) ([]byte, error)

// capabilityProbeRequest is the client-side request.
type capabilityProbeRequest struct {
	ExpectedServerID string
	Nonce            [32]byte
}

// capabilityProbeResponse is the server-side response.
type capabilityProbeResponse struct {
	ServerID        string
	Capabilities    []string // sorted ascending
	BinaryVersion   string
	SignedAssertion []byte
}

// canonicalEncodeCapAssertion produces the canonical plaintext that the server
// signs and the client verifies. Layout:
//
//	[magic "CAP\x01" 4B]
//	[server_id_len uint16-BE][server_id bytes]
//	[num_caps uint16-BE][for each cap: len uint16-BE + bytes]
//	[nonce 32B]
//
// sortedCaps MUST already be sorted ascending (caller responsibility).
// nonce MUST be exactly 32 bytes.
func canonicalEncodeCapAssertion(serverID string, sortedCaps []string, nonce []byte) []byte {
	if len(nonce) != 32 {
		panic(fmt.Sprintf("canonicalEncodeCapAssertion: nonce must be 32 bytes, got %d", len(nonce)))
	}
	size := 4 + 2 + len(serverID) + 2
	for _, c := range sortedCaps {
		size += 2 + len(c)
	}
	size += 32
	out := make([]byte, 0, size)
	out = append(out, "CAP\x01"...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(serverID)))
	out = append(out, serverID...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(sortedCaps)))
	for _, c := range sortedCaps {
		out = binary.BigEndian.AppendUint16(out, uint16(len(c)))
		out = append(out, c...)
	}
	out = append(out, nonce...)
	return out
}

// encodeCapProbeRequest serialises a request into a wire payload.
func encodeCapProbeRequest(req capabilityProbeRequest) []byte {
	idLen := len(req.ExpectedServerID)
	out := make([]byte, 0, len(capProbeRequestMagic)+2+idLen+32)
	out = append(out, capProbeRequestMagic...)
	out = binary.BigEndian.AppendUint16(out, uint16(idLen))
	out = append(out, req.ExpectedServerID...)
	out = append(out, req.Nonce[:]...)
	return out
}

// decodeCapProbeRequest deserialises a wire payload produced by encodeCapProbeRequest.
func decodeCapProbeRequest(data []byte) (capabilityProbeRequest, error) {
	if len(data) < len(capProbeRequestMagic) || string(data[:len(capProbeRequestMagic)]) != string(capProbeRequestMagic) {
		return capabilityProbeRequest{}, errors.New("capability_probe: bad request magic")
	}
	rest := data[len(capProbeRequestMagic):]
	if len(rest) < 2 {
		return capabilityProbeRequest{}, errors.New("capability_probe: request too short for server_id length")
	}
	idLen := int(binary.BigEndian.Uint16(rest))
	rest = rest[2:]
	if len(rest) < idLen+32 {
		return capabilityProbeRequest{}, fmt.Errorf("capability_probe: request truncated (id_len=%d, remaining=%d)", idLen, len(rest))
	}
	var req capabilityProbeRequest
	req.ExpectedServerID = string(rest[:idLen])
	copy(req.Nonce[:], rest[idLen:idLen+32])
	return req, nil
}

// encodeCapProbeResponse serialises a response.
func encodeCapProbeResponse(resp capabilityProbeResponse) []byte {
	idLen := len(resp.ServerID)
	verLen := len(resp.BinaryVersion)
	capCount := len(resp.Capabilities)
	size := len(capProbeReplyMagic) + 2 + idLen + 2 + verLen + 2
	for _, c := range resp.Capabilities {
		size += 2 + len(c)
	}
	size += 4 + len(resp.SignedAssertion)
	out := make([]byte, 0, size)
	out = append(out, capProbeReplyMagic...)
	out = binary.BigEndian.AppendUint16(out, uint16(idLen))
	out = append(out, resp.ServerID...)
	out = binary.BigEndian.AppendUint16(out, uint16(verLen))
	out = append(out, resp.BinaryVersion...)
	out = binary.BigEndian.AppendUint16(out, uint16(capCount))
	for _, c := range resp.Capabilities {
		out = binary.BigEndian.AppendUint16(out, uint16(len(c)))
		out = append(out, c...)
	}
	out = binary.BigEndian.AppendUint32(out, uint32(len(resp.SignedAssertion)))
	out = append(out, resp.SignedAssertion...)
	return out
}

// decodeCapProbeResponse deserialises a wire payload produced by encodeCapProbeResponse.
func decodeCapProbeResponse(data []byte) (capabilityProbeResponse, error) {
	if len(data) < len(capProbeReplyMagic) || string(data[:len(capProbeReplyMagic)]) != string(capProbeReplyMagic) {
		return capabilityProbeResponse{}, errors.New("capability_probe: bad reply magic")
	}
	rest := data[len(capProbeReplyMagic):]
	readUint16 := func() (uint16, error) {
		if len(rest) < 2 {
			return 0, errors.New("capability_probe: response truncated at uint16")
		}
		v := binary.BigEndian.Uint16(rest)
		rest = rest[2:]
		return v, nil
	}
	readString := func() (string, error) {
		n, err := readUint16()
		if err != nil {
			return "", err
		}
		if len(rest) < int(n) {
			return "", fmt.Errorf("capability_probe: response truncated at string len=%d", n)
		}
		s := string(rest[:n])
		rest = rest[n:]
		return s, nil
	}

	serverID, err := readString()
	if err != nil {
		return capabilityProbeResponse{}, fmt.Errorf("capability_probe decode serverID: %w", err)
	}
	binaryVersion, err := readString()
	if err != nil {
		return capabilityProbeResponse{}, fmt.Errorf("capability_probe decode binaryVersion: %w", err)
	}
	capCount, err := readUint16()
	if err != nil {
		return capabilityProbeResponse{}, fmt.Errorf("capability_probe decode capCount: %w", err)
	}
	caps := make([]string, capCount)
	for i := range caps {
		c, err := readString()
		if err != nil {
			return capabilityProbeResponse{}, fmt.Errorf("capability_probe decode cap[%d]: %w", i, err)
		}
		caps[i] = c
	}
	if len(rest) < 4 {
		return capabilityProbeResponse{}, errors.New("capability_probe: response truncated at assertion_len")
	}
	assertLen := int(binary.BigEndian.Uint32(rest))
	rest = rest[4:]
	if len(rest) < assertLen {
		return capabilityProbeResponse{}, fmt.Errorf("capability_probe: response truncated at assertion (need %d, have %d)", assertLen, len(rest))
	}
	assertion := make([]byte, assertLen)
	copy(assertion, rest[:assertLen])
	return capabilityProbeResponse{
		ServerID:        serverID,
		Capabilities:    caps,
		BinaryVersion:   binaryVersion,
		SignedAssertion: assertion,
	}, nil
}

// CapabilityProbeHandler is the server-side handler for StreamCapabilityProbe.
// Register it with: quicTransport.Handle(transport.StreamCapabilityProbe, handler.Handle)
type CapabilityProbeHandler struct {
	serverID       string
	binaryVersion  string
	clusterID      []byte // 16 bytes, from HandshakeVerifier.ClusterID()
	kekStore       *encrypt.KEKStore
	evidenceSource CapabilityEvidenceSource
}

// NewCapabilityProbeHandler constructs a server-side handler.
// clusterID must be exactly 16 bytes.
func NewCapabilityProbeHandler(
	serverID string,
	binaryVersion string,
	clusterID []byte,
	kekStore *encrypt.KEKStore,
	evidenceSource CapabilityEvidenceSource,
) *CapabilityProbeHandler {
	if len(clusterID) != 16 {
		panic(fmt.Sprintf("NewCapabilityProbeHandler: clusterID must be 16 bytes, got %d", len(clusterID)))
	}
	return &CapabilityProbeHandler{
		serverID:       serverID,
		binaryVersion:  binaryVersion,
		clusterID:      append([]byte(nil), clusterID...),
		kekStore:       kekStore,
		evidenceSource: evidenceSource,
	}
}

// Handle processes a StreamCapabilityProbe request message and returns a response.
func (h *CapabilityProbeHandler) Handle(req *transport.Message) *transport.Message {
	probeReq, err := decodeCapProbeRequest(req.Payload)
	if err != nil {
		return transport.NewErrorResponse(req, transport.StatusError,
			fmt.Errorf("capability_probe: decode request: %w", err))
	}
	// Identity guard: refuse requests targeting a different server.
	if probeReq.ExpectedServerID != h.serverID {
		return transport.NewErrorResponse(req, transport.StatusError,
			fmt.Errorf("capability_probe: identity mismatch: expected %q, this server is %q",
				probeReq.ExpectedServerID, h.serverID))
	}

	// Collect local capabilities.
	ev := h.evidenceSource.CapabilityEvidence(h.serverID, time.Now())
	var caps []string
	for cap, ready := range ev.Capabilities {
		if ready {
			caps = append(caps, cap)
		}
	}
	sort.Strings(caps)

	// Sign the assertion binding: serverID + sorted caps + nonce.
	aad := encrypt.BuildAAD(encrypt.DomainCapabilityAssertV1, h.clusterID,
		encrypt.FieldString(h.serverID))
	canonical := canonicalEncodeCapAssertion(h.serverID, caps, probeReq.Nonce[:])
	signed, err := h.kekStore.SealWithActiveKEK(canonical, aad)
	if err != nil {
		return transport.NewErrorResponse(req, transport.StatusError,
			fmt.Errorf("capability_probe: seal assertion: %w", err))
	}

	resp := capabilityProbeResponse{
		ServerID:        h.serverID,
		Capabilities:    caps,
		BinaryVersion:   h.binaryVersion,
		SignedAssertion: signed,
	}
	return transport.NewResponse(req, encodeCapProbeResponse(resp))
}

// GetCapabilities sends a capability probe to a peer and verifies the signed assertion.
// It returns the verified response on success.
//
// Verification:
//  1. resp.ServerID == expectedServerID (identity check)
//  2. OpenWithActiveKEK(resp.SignedAssertion, aad) yields canonicalEncodeCapAssertion(expectedServerID, resp.Capabilities, nonce)
//     (signature check — also catches replayed nonces from prior calls)
func GetCapabilities(
	ctx context.Context,
	expectedServerID string,
	clusterID []byte,
	kekStore *encrypt.KEKStore,
	dialer capabilityProbeDialer,
) (capabilityProbeResponse, error) {
	if len(clusterID) != 16 {
		return capabilityProbeResponse{}, fmt.Errorf("GetCapabilities: clusterID must be 16 bytes, got %d", len(clusterID))
	}
	var nonce [32]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return capabilityProbeResponse{}, fmt.Errorf("GetCapabilities: generate nonce: %w", err)
	}
	reqPayload := encodeCapProbeRequest(capabilityProbeRequest{
		ExpectedServerID: expectedServerID,
		Nonce:            nonce,
	})

	respBytes, err := dialer(ctx, expectedServerID, reqPayload)
	if err != nil {
		return capabilityProbeResponse{}, fmt.Errorf("GetCapabilities: transport: %w", err)
	}
	resp, err := decodeCapProbeResponse(respBytes)
	if err != nil {
		return capabilityProbeResponse{}, fmt.Errorf("GetCapabilities: decode response: %w", err)
	}

	// Verification 1: ServerID field matches expected.
	if resp.ServerID != expectedServerID {
		return capabilityProbeResponse{},
			fmt.Errorf("GetCapabilities: server_id mismatch: expected %q, got %q", expectedServerID, resp.ServerID)
	}

	// Verification 2: open the assertion and compare canonical plaintext.
	aad := encrypt.BuildAAD(encrypt.DomainCapabilityAssertV1, clusterID,
		encrypt.FieldString(expectedServerID))
	expectedCanonical := canonicalEncodeCapAssertion(expectedServerID, resp.Capabilities, nonce[:])
	plain, err := kekStore.OpenWithActiveKEK(resp.SignedAssertion, aad)
	if err != nil {
		return capabilityProbeResponse{},
			fmt.Errorf("GetCapabilities: assertion signature invalid: %w", err)
	}
	if string(plain) != string(expectedCanonical) {
		return capabilityProbeResponse{},
			fmt.Errorf("GetCapabilities: assertion content mismatch (replay or tampered response)")
	}

	return resp, nil
}

// CapabilityGateDirectConfig holds the dependencies needed for the direct-RPC
// capability gate path. Stored in CapabilityGate and populated via WithDirectProbe.
type CapabilityGateDirectConfig struct {
	// dialer dials a peer and returns the raw response payload.
	// Production: func(ctx, peerAddr, payload) { resp, _ := quicTransport.Call(ctx, peerAddr, &transport.Message{Type: transport.StreamCapabilityProbe, Payload: payload}); return resp.Payload, nil }
	dialer    capabilityProbeDialer
	clusterID []byte
	kekStore  *encrypt.KEKStore
}

// directCapabilityCacheKey captures the snapshot of parameters that determine
// whether a cached gate result is still valid.
type directCapabilityCacheKey struct {
	op           compat.Operation
	configID     uint64
	activeKEKVer uint32
}

// directCapabilityCacheEntry holds a cached gate result.
type directCapabilityCacheEntry struct {
	key  directCapabilityCacheKey
	plan compat.GatePlan
	err  error
}
