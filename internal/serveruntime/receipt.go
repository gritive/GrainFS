package serveruntime

import (
	"context"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/resourcewatch"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/transport"
)

// ReceiptOptions captures the cobra-flag-derived knobs for SetupClusterReceipt.
type ReceiptOptions struct {
	Enabled        bool
	PSK            string
	Retention      time.Duration
	GossipInterval time.Duration
	WindowSize     int
}

func receiptPeerAddresses(selfNodeID, selfAddr string, seedPeers []string, nodes []cluster.MetaNodeEntry) []string {
	seen := make(map[string]struct{}, len(seedPeers)+len(nodes)+2)
	if selfNodeID != "" {
		seen[selfNodeID] = struct{}{}
	}
	if selfAddr != "" {
		seen[selfAddr] = struct{}{}
	}

	out := make([]string, 0, len(seedPeers)+len(nodes))
	add := func(addr string) {
		if addr == "" {
			return
		}
		if _, ok := seen[addr]; ok {
			return
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
	}
	for _, node := range nodes {
		if node.ID == selfNodeID || node.Address == selfAddr {
			continue
		}
		add(node.Address)
	}
	for _, peer := range seedPeers {
		add(peer)
	}
	return out
}

// HealReceiptWiring bundles the Phase 16 Slice 2 components so the caller
// can defer a single teardown. Cluster-only fields (RoutingCache,
// Broadcaster, GossipSender) are nil in no-peers mode.
type HealReceiptWiring struct {
	db           *badger.DB
	vlogEntry    *resourcewatch.RegisteredDB
	store        *receipt.Store
	keyStore     *receipt.KeyStore
	api          *receipt.API
	routingCache *receipt.RoutingCache
	broadcaster  *cluster.ReceiptBroadcaster
	gossipSender *cluster.ReceiptGossipSender
}

// Store exposes the receipt store for callers that need to record entries
// directly (e.g. the placement-monitor receipt path inside runCluster).
func (w *HealReceiptWiring) Store() *receipt.Store {
	if w == nil {
		return nil
	}
	return w.store
}

// KeyStore exposes the receipt keystore for sign callers.
func (w *HealReceiptWiring) KeyStore() *receipt.KeyStore {
	if w == nil {
		return nil
	}
	return w.keyStore
}

// Close releases resources. Safe on a nil receiver.
func (w *HealReceiptWiring) Close() {
	if w == nil {
		return
	}
	if w.vlogEntry != nil {
		resourcewatch.DeregisterDB(w.vlogEntry)
	}
	if w.store != nil {
		_ = w.store.Close()
	}
	if w.db != nil {
		_ = w.db.Close()
	}
}

// openReceiptDB opens the dedicated BadgerDB under dataDir/receipts.
// Kept separate from the meta DB so retention GC can run on receipt keys
// without touching cluster metadata.
func openReceiptDB(dataDir string) (*badger.DB, badgerrole.Decision, error) {
	return badgerrole.OpenRole(badgerrole.DefaultRegistry(), badgerrole.RoleReceipts, badgerrole.PathContext{DataDir: dataDir})
}

// receiptDBOptions exposes the small-options preset for callers that want
// a per-test receipt DB without going through OpenRole.
func receiptDBOptions(dir string) badger.Options {
	return badgerutil.SmallOptions(dir)
}

// SetupClusterReceipt wires the full Slice 2 stack in cluster mode.
// PSK comes from opts.PSK if set; callers usually pass --heal-receipt-psk
// when set, otherwise --cluster-key. Registers the StreamReceiptQuery
// handler on the router and wires the routing cache into the gossip
// receiver. Starts the gossip broadcast goroutine bound to ctx.
func SetupClusterReceipt(
	ctx context.Context,
	opts ReceiptOptions,
	dataDir, nodeID string,
	peers []string,
	quicTransport *transport.QUICTransport,
	router *transport.StreamRouter,
	gossipReceiver *cluster.GossipReceiver,
	srvOpts []server.Option,
) ([]server.Option, *HealReceiptWiring, error) {
	return setupClusterReceipt(ctx, opts, dataDir, nodeID, peers, nil, quicTransport, router, gossipReceiver, srvOpts)
}

func SetupClusterReceiptWithPeerProvider(
	ctx context.Context,
	opts ReceiptOptions,
	dataDir, nodeID string,
	peerProvider func() []string,
	quicTransport *transport.QUICTransport,
	router *transport.StreamRouter,
	gossipReceiver *cluster.GossipReceiver,
	srvOpts []server.Option,
) ([]server.Option, *HealReceiptWiring, error) {
	return setupClusterReceipt(ctx, opts, dataDir, nodeID, nil, peerProvider, quicTransport, router, gossipReceiver, srvOpts)
}

func setupClusterReceipt(
	ctx context.Context,
	opts ReceiptOptions,
	dataDir, nodeID string,
	peers []string,
	peerProvider func() []string,
	quicTransport *transport.QUICTransport,
	router *transport.StreamRouter,
	gossipReceiver *cluster.GossipReceiver,
	srvOpts []server.Option,
) ([]server.Option, *HealReceiptWiring, error) {
	if !opts.Enabled {
		return srvOpts, nil, nil
	}
	psk := opts.PSK
	// PSK is only required when there are peers — it signs the cross-node
	// StreamReceiptQuery payload. Singletons have no peer to authenticate
	// against, so we fall back to a fixed local-only sentinel. Once the
	// operator adds peers they must supply a PSK upstream.
	if psk == "" {
		if len(peers) > 0 || (peerProvider != nil && len(peerProvider()) > 0) {
			return srvOpts, nil, fmt.Errorf("heal-receipt requires a PSK: set --heal-receipt-psk or --cluster-key")
		}
		psk = "local-no-peers"
	}

	db, decision, err := openReceiptDB(dataDir)
	if err != nil {
		if feature, ok := OptionalRoleDisabled(badgerrole.DefaultRegistry(), decision); ok {
			LogOptionalRoleDisabled(badgerrole.RoleReceipts, feature, err)
			return srvOpts, nil, nil
		}
		return srvOpts, nil, fmt.Errorf("open receipt db: %w", err)
	}
	vlogEntry := resourcewatch.RegisterDB(resourcewatch.DBCategoryReceipts, db)

	ks, err := receipt.NewKeyStore(receipt.Key{ID: "cluster", Secret: []byte(psk)})
	if err != nil {
		resourcewatch.DeregisterDB(vlogEntry)
		_ = db.Close()
		return srvOpts, nil, fmt.Errorf("init receipt keystore: %w", err)
	}

	store, err := receipt.NewStore(db, receipt.StoreOptions{
		Retention:      opts.Retention,
		FlushThreshold: 100,
		FlushInterval:  50 * time.Millisecond,
	})
	if err != nil {
		resourcewatch.DeregisterDB(vlogEntry)
		_ = db.Close()
		return srvOpts, nil, fmt.Errorf("create receipt store: %w", err)
	}

	routingCache := receipt.NewRoutingCache()
	var broadcaster *cluster.ReceiptBroadcaster
	var gossipSender *cluster.ReceiptGossipSender
	if peerProvider != nil {
		broadcaster = cluster.NewReceiptBroadcasterWithPeerProvider(quicTransport, peerProvider, 3*time.Second)
		gossipSender = cluster.NewReceiptGossipSenderWithPeerProvider(
			nodeID, peerProvider, quicTransport, store, opts.GossipInterval, opts.WindowSize,
		)
	} else {
		broadcaster = cluster.NewReceiptBroadcaster(quicTransport, peers, 3*time.Second)
		gossipSender = cluster.NewReceiptGossipSender(
			nodeID, peers, quicTransport, store, opts.GossipInterval, opts.WindowSize,
		)
	}
	broadcaster.SetMetrics(receipt.BroadcastMetrics{})

	if gossipReceiver != nil {
		gossipReceiver.SetReceiptCache(routingCache)
	}
	router.Handle(transport.StreamReceiptQuery, cluster.NewReceiptQueryHandler(store))

	go gossipSender.Run(ctx)

	api := receipt.NewAPI(store, routingCache, broadcaster, opts.Retention)

	log.Info().Str("component", "receipt").Str("mode", "cluster").
		Dur("retention", opts.Retention).Dur("gossip_interval", opts.GossipInterval).Int("window", opts.WindowSize).Msg("heal-receipt API enabled")

	return append(srvOpts, server.WithReceiptAPI(api)), &HealReceiptWiring{
		db:           db,
		vlogEntry:    vlogEntry,
		store:        store,
		keyStore:     ks,
		api:          api,
		routingCache: routingCache,
		broadcaster:  broadcaster,
		gossipSender: gossipSender,
	}, nil
}
