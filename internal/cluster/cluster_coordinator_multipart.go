package cluster

import (
	"context"
	"sort"
	"time"

	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/storage"
)

func (c *ClusterCoordinator) CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (*storage.MultipartUpload, error) {
	return c.multipartRuntime().createMultipartUpload(ctx, bucket, key, contentType)
}

// CreateMultipartUploadWithTags routes to the resolved data group, mirroring
// CreateMultipartUpload but carrying tags. Tags materialise onto the finalised
// object via the off-raft quorum-meta blob written on Complete
// (clusterMultipartMeta.Tags → PutObjectMetaCmd.Tags). When the resolved target is
// remote the tags ride along in CreateMultipartUploadArgs.tags so the receiver
// dispatches to GroupBackend.CreateMultipartUploadWithTags.
func (c *ClusterCoordinator) CreateMultipartUploadWithTags(ctx context.Context, bucket, key, contentType string, tags []storage.Tag) (string, error) {
	return c.multipartRuntime().createMultipartUploadWithTags(ctx, bucket, key, contentType, tags)
}

func (c *ClusterCoordinator) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	return c.multipartRuntime().completeMultipartUpload(ctx, bucket, key, uploadID, parts)
}

func (c *ClusterCoordinator) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	return c.multipartRuntime().abortMultipartUpload(ctx, bucket, key, uploadID)
}

// ListMultipartUploads scans local data-group backends and forwards to owners
// for placeholder groups so bucket-wide results are complete from any node.
func (c *ClusterCoordinator) ListMultipartUploads(ctx context.Context, bucket, prefix string, maxUploads int) ([]*storage.MultipartUpload, error) {
	if c.groups == nil {
		return c.base.ListMultipartUploads(ctx, bucket, prefix, maxUploads)
	}
	var uploads []*storage.MultipartUpload
	groups := c.groups.All()
	for _, dg := range groups {
		gb := dg.Backend()
		if gb == nil {
			groupUploads, err := c.forwardListMultipartUploads(ctx, dg.ID(), bucket, prefix)
			if err != nil {
				return nil, err
			}
			// Group-encode listed IDs with the group they came from so clients
			// get the same uploadID Create returned (session ops route by it).
			uploads = append(uploads, c.wrapMultipartUploads(groupUploads, dg.ID())...)
			continue
		}
		groupUploads, err := gb.ListMultipartUploads(ctx, bucket, prefix, 0)
		if err != nil {
			return nil, err
		}
		uploads = append(uploads, c.wrapMultipartUploads(groupUploads, dg.ID())...)
	}
	if len(groups) == 0 && c.base != nil {
		return c.base.ListMultipartUploads(ctx, bucket, prefix, maxUploads)
	}
	sort.Slice(uploads, func(i, j int) bool {
		if uploads[i].CreatedAt != uploads[j].CreatedAt {
			return uploads[i].CreatedAt < uploads[j].CreatedAt
		}
		if uploads[i].Key != uploads[j].Key {
			return uploads[i].Key < uploads[j].Key
		}
		return uploads[i].UploadID < uploads[j].UploadID
	})
	if maxUploads > 0 && len(uploads) > maxUploads {
		uploads = uploads[:maxUploads]
	}
	return uploads, nil
}

func (c *ClusterCoordinator) forwardListMultipartUploads(ctx context.Context, groupID, bucket, prefix string) ([]*storage.MultipartUpload, error) {
	if c.forward == nil {
		return nil, rejectIncompleteMultipartListing(compat.OperationListMultipartUploads)
	}
	target, err := c.runtimeState().opRouter.routeGroup(groupID)
	if err != nil {
		return nil, err
	}
	if len(target.Peers) == 0 {
		return nil, rejectIncompleteMultipartListing(compat.OperationListMultipartUploads)
	}
	if err := c.requireMultipartListingPeerCapability(compat.OperationListMultipartUploads, target.Peers); err != nil {
		return nil, err
	}
	return c.forwardRuntime().listMultipartUploads(ctx, target, bucket, prefix, 0)
}

// ListParts routes by the group encoded in the uploadID (falling back to the
// legacy (bucket, key) hash for un-prefixed IDs): local group backend first;
// otherwise the peer-transport capability gate must pass before forwarding to
// the remote data-group leader.
func (c *ClusterCoordinator) ListParts(ctx context.Context, bucket, key, uploadID string, maxParts int) ([]storage.Part, error) {
	target, _, rawID, err := c.routeMultipartSession(bucket, key, uploadID)
	if err != nil {
		return nil, err
	}
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.ListParts(ctx, bucket, key, rawID, maxParts)
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	if err := c.requireMultipartListingPeerCapability(compat.OperationListParts, target.Peers); err != nil {
		return nil, err
	}
	return c.forwardRuntime().listParts(ctx, target, bucket, key, rawID, maxParts)
}

func (c *ClusterCoordinator) requireMultipartListingPeerCapability(op compat.Operation, peers []string) error {
	if c.capGate == nil {
		return nil
	}
	resolved := peers
	if book, ok := c.meta.(NodeAddressBook); ok && book != nil {
		if addrs, err := ResolveNodeAddresses(book, peers); err == nil {
			resolved = addrs
		}
	}
	_, err := c.capGate.RequirePeerTransportCapability(compat.CapabilityMultipartListingV1, op, resolved, time.Now())
	return err
}

func (c *ClusterCoordinator) multipartListingCapabilityPeers(target RouteTarget, group ShardGroupEntry) []string {
	if len(target.Peers) > 0 {
		return target.Peers
	}
	if len(group.PeerIDs) > 0 {
		return append([]string(nil), group.PeerIDs...)
	}
	if c.meta != nil {
		if entry, ok := c.meta.ShardGroup(target.GroupID); ok {
			return append([]string(nil), entry.PeerIDs...)
		}
	}
	return nil
}

func rejectIncompleteMultipartListing(op compat.Operation) error {
	return compat.Reject(compat.GatePlan{
		Capability: compat.CapabilityMultipartListingV1,
		Scope:      compat.ScopeDataGroup,
		Severity:   compat.SeverityHard,
		Operation:  op,
		Unknown:    []compat.NodeID{"data_group"},
	})
}
