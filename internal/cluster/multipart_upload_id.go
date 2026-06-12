package cluster

import (
	"errors"
	"fmt"
	"strings"

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
