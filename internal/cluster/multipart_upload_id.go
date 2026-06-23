package cluster

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/storage"
)

// multipartUploadIDGroupPrefix marks cluster multipart uploadIDs that encode
// their owning placement group: "mpg:<groupID>:<rawUploadID>".
//
// Why: multipart session ops (UploadPart/ListParts/Abort/Complete) have no
// placement record while the upload is in flight, so they historically routed
// by the stateless (bucket, key) hash over the node's boot-frozen placement
// candidate set. Dynamic-join group expansion grows that set WITHOUT recording
// an FSM placement generation, so each node can freeze a DIFFERENT candidate
// set and the same key hashes to different groups per node — a session op
// arriving at a node other than the creator misroutes and fails with
// NoSuchUpload. Encoding the owning group in the (client-opaque) uploadID at
// create time lets every node route the session directly to that group.
const multipartUploadIDGroupPrefix = "mpg:"

// encodeMultipartUploadID wraps a backend uploadID with its owning placement
// group. The wrapped form is what S3 clients see; backends always see the raw
// ID (callers strip the prefix before backend/forward calls).
func encodeMultipartUploadID(groupID, raw string) string {
	return multipartUploadIDGroupPrefix + groupID + ":" + raw
}

// parseMultipartUploadID splits a group-encoded uploadID. ok=false means the
// ID is un-prefixed (single-node wiring, base-backend paths, or uploads
// created before group encoding) — callers fall back to the legacy hash route.
func parseMultipartUploadID(id string) (groupID, raw string, ok bool) {
	rest, found := strings.CutPrefix(id, multipartUploadIDGroupPrefix)
	if !found {
		return "", "", false
	}
	groupID, raw, found = strings.Cut(rest, ":")
	if !found || groupID == "" || raw == "" {
		return "", "", false
	}
	return groupID, raw, true
}

// multipartGroupIDRouting reports whether multipart uploadIDs carry their
// owning placement group on this coordinator. Only the EC hash-placement path
// can diverge across nodes (boot-frozen candidate sets); bucket-routed
// (NumShards()==0) and group-less wirings keep raw backend IDs so single-node
// and base-backend paths are unchanged.
func (c *ClusterCoordinator) multipartGroupIDRouting() bool {
	return c.groups != nil && c.runtimeState().ecConfig.NumShards() > 0
}

// wrapMultipartUploadID group-encodes a freshly created uploadID when group
// routing applies; otherwise returns the raw ID unchanged.
func (c *ClusterCoordinator) wrapMultipartUploadID(raw, groupID string) string {
	if raw == "" || groupID == "" || !c.multipartGroupIDRouting() {
		return raw
	}
	return encodeMultipartUploadID(groupID, raw)
}

// wrapMultipartUpload returns a copy of upload whose UploadID is
// group-encoded when group routing applies. Copies so backend-owned structs
// are never mutated.
func (c *ClusterCoordinator) wrapMultipartUpload(upload *storage.MultipartUpload, groupID string) *storage.MultipartUpload {
	if upload == nil {
		return nil
	}
	wrapped := c.wrapMultipartUploadID(upload.UploadID, groupID)
	if wrapped == upload.UploadID {
		return upload
	}
	cp := *upload
	cp.UploadID = wrapped
	return &cp
}

// wrapMultipartUploads group-encodes the UploadID of every collected upload
// with the group it was listed from, so listings hand out the same IDs Create
// returned. No-op (returns the input) when group routing does not apply.
func (c *ClusterCoordinator) wrapMultipartUploads(uploads []*storage.MultipartUpload, groupID string) []*storage.MultipartUpload {
	if !c.multipartGroupIDRouting() || groupID == "" {
		return uploads
	}
	out := make([]*storage.MultipartUpload, len(uploads))
	for i, u := range uploads {
		out[i] = c.wrapMultipartUpload(u, groupID)
	}
	return out
}

// multipartVIDNamespace is the stable UUIDv5 namespace for the legacy (non-v7)
// uploadID fallback in deriveMultipartVID. It must never change: a different
// namespace would derive a different vid for the same legacy uploadID and break
// idempotent / concurrent same-vid convergence for any in-flight legacy upload.
var multipartVIDNamespace = uuid.MustParse("6f1d8b2e-9c3a-4d57-8e21-0a4b6c8d1f30")

// deriveMultipartVID maps a backend (raw) multipart uploadID to the completed
// object's VersionID, deterministically and independent of the manifest. Two
// completions of the same uploadID derive the SAME vid, so concurrent completes
// converge on one version and an idempotent retry re-derives the same id.
//
// uuid.Parse succeeds for any well-formed UUID (v4, v1, v7, …). A parseable
// UUID — including v4 — is reshaped: bytes [0:6] from the raw UUID are reused
// as the timestamp prefix (ms-ordered only for true v7; a v4's [0:6] are
// random so the resulting order is hash-arbitrary, which is acceptable because
// the create path no longer mints v4), and bytes [6:16] are filled from
// sha256(raw), re-forcing the version-7 and RFC4122 variant nibbles. Only a
// genuinely un-parseable / non-UUID string takes the deterministic UUIDv5
// fallback. Segment blobIDs stay random.
func deriveMultipartVID(rawUploadID string) (string, error) {
	sum := sha256.Sum256([]byte(rawUploadID))
	parsed, err := uuid.Parse(rawUploadID)
	if err != nil {
		// Un-parseable / non-UUID tail: deterministic v5 over the raw.
		return uuid.NewSHA1(multipartVIDNamespace, []byte(rawUploadID)).String(), nil
	}
	var b [16]byte
	copy(b[0:6], parsed[0:6])   // reuse the raw's 48-bit ms timestamp (create-time order)
	copy(b[6:16], sum[0:10])    // deterministic, manifest-independent tail
	b[6] = (b[6] & 0x0f) | 0x70 // version 7
	b[8] = (b[8] & 0x3f) | 0x80 // RFC4122 variant
	out, ferr := uuid.FromBytes(b[:])
	if ferr != nil {
		return "", fmt.Errorf("derive multipart vid: %w", ferr)
	}
	return out.String(), nil
}

// routeMultipartSession resolves the target for an op on an EXISTING multipart
// session and returns the raw backend uploadID. A group-encoded uploadID
// routes directly to its owning group, bypassing the per-node (possibly
// divergent) hash candidate set; un-prefixed IDs keep the legacy hash route.
func (c *ClusterCoordinator) routeMultipartSession(bucket, key, uploadID string) (RouteTarget, ShardGroupEntry, string, error) {
	groupID, raw, ok := parseMultipartUploadID(uploadID)
	if !ok {
		target, group, err := c.routeWriteOrBucket(bucket, key)
		return target, group, uploadID, err
	}
	target, group, err := c.runtimeState().opRouter.RouteObjectWriteGroup(groupID)
	if err != nil {
		if errors.Is(err, ErrUnknownGroup) {
			// The owning group left the topology; the session is gone. Surface
			// NoSuchUpload rather than an internal routing error.
			return RouteTarget{}, ShardGroupEntry{}, "", fmt.Errorf("multipart session group %q not in topology: %w", groupID, storage.ErrUploadNotFound)
		}
		return RouteTarget{}, ShardGroupEntry{}, "", fmt.Errorf("route multipart session group %q: %w", groupID, err)
	}
	return target, group, raw, nil
}
