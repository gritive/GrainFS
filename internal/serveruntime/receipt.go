package serveruntime

import (
	"context"
	"fmt"
	"sync"
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

// HealReceiptWiring bundles the Phase 16 Slice 2 components so the caller
// can defer a single teardown. Cluster-only fields (RoutingCache,
// Broadcaster, GossipSender) are nil in no-peers mode.
type HealReceiptWiring struct {
	ServerOptions []server.Option

	db           *badger.DB
	vlogEntry    *resourcewatch.RegisteredDB
	store        *receipt.Store
	keyStore     *receipt.KeyStore
	api          *receipt.API
	routingCache *receipt.RoutingCache
	broadcaster  *cluster.ReceiptBroadcaster
	gossipSender *cluster.ReceiptGossipSender
	cancel       context.CancelFunc
	gossipDone   chan struct{}
	closeOnce    sync.Once
	closeErr     error
}

type ReceiptRuntimeOptions struct {
	Options        ReceiptOptions
	DataDir        string
	NodeID         string
	Peers          []string
	QUICTransport  *transport.QUICTransport
	Router         *transport.StreamRouter
	GossipReceiver *cluster.GossipReceiver
}

type ReceiptRuntime = HealReceiptWiring

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
func (w *HealReceiptWiring) Close() error {
	if w == nil {
		return nil
	}
	w.closeOnce.Do(func() {
		if w.cancel != nil {
			w.cancel()
		}
		if w.gossipDone != nil {
			<-w.gossipDone
		}
		if w.vlogEntry != nil {
			resourcewatch.DeregisterDB(w.vlogEntry)
		}
		if w.store != nil {
			_ = w.store.Close()
		}
		if w.db != nil {
			w.closeErr = w.db.Close()
		}
	})
	return w.closeErr
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
func SetupReceiptRuntime(ctx context.Context, opts ReceiptRuntimeOptions) (*ReceiptRuntime, error) {
	runtime := &ReceiptRuntime{}
	if !opts.Options.Enabled {
		return runtime, nil
	}
	psk := opts.Options.PSK
	// PSK is only required when there are peers — it signs the cross-node
	// StreamReceiptQuery payload. Singletons have no peer to authenticate
	// against, so we fall back to a fixed local-only sentinel. Once the
	// operator adds peers they must supply a PSK upstream.
	if psk == "" {
		if len(opts.Peers) > 0 {
			return runtime, fmt.Errorf("heal-receipt requires a PSK: set --heal-receipt-psk or --cluster-key")
		}
		psk = "local-no-peers"
	}

	db, decision, err := openReceiptDB(opts.DataDir)
	if err != nil {
		if feature, ok := OptionalRoleDisabled(badgerrole.DefaultRegistry(), decision); ok {
			LogOptionalRoleDisabled(badgerrole.RoleReceipts, feature, err)
			return runtime, nil
		}
		return runtime, fmt.Errorf("open receipt db: %w", err)
	}
	vlogEntry := resourcewatch.RegisterDB(resourcewatch.DBCategoryReceipts, db)

	ks, err := receipt.NewKeyStore(receipt.Key{ID: "cluster", Secret: []byte(psk)})
	if err != nil {
		resourcewatch.DeregisterDB(vlogEntry)
		_ = db.Close()
		return runtime, fmt.Errorf("init receipt keystore: %w", err)
	}

	store, err := receipt.NewStore(db, receipt.StoreOptions{
		Retention:      opts.Options.Retention,
		FlushThreshold: 100,
		FlushInterval:  50 * time.Millisecond,
	})
	if err != nil {
		resourcewatch.DeregisterDB(vlogEntry)
		_ = db.Close()
		return runtime, fmt.Errorf("create receipt store: %w", err)
	}

	routingCache := receipt.NewRoutingCache()
	broadcaster := cluster.NewReceiptBroadcaster(opts.QUICTransport, opts.Peers, 3*time.Second)
	broadcaster.SetMetrics(receipt.BroadcastMetrics{})
	gossipSender := cluster.NewReceiptGossipSender(
		opts.NodeID, opts.Peers, opts.QUICTransport, store, opts.Options.GossipInterval, opts.Options.WindowSize,
	)

	if opts.GossipReceiver != nil {
		opts.GossipReceiver.SetReceiptCache(routingCache)
	}
	if opts.Router != nil {
		opts.Router.Handle(transport.StreamReceiptQuery, cluster.NewReceiptQueryHandler(store))
	}

	gossipCtx, cancelGossip := context.WithCancel(ctx)
	gossipDone := make(chan struct{})
	go func() {
		defer close(gossipDone)
		gossipSender.Run(gossipCtx)
	}()

	api := receipt.NewAPI(store, routingCache, broadcaster, opts.Options.Retention)

	log.Info().Str("component", "receipt").Str("mode", "cluster").
		Dur("retention", opts.Options.Retention).Dur("gossip_interval", opts.Options.GossipInterval).Int("window", opts.Options.WindowSize).Msg("heal-receipt API enabled")

	runtime.ServerOptions = []server.Option{server.WithReceiptAPI(api)}
	runtime.db = db
	runtime.vlogEntry = vlogEntry
	runtime.store = store
	runtime.keyStore = ks
	runtime.api = api
	runtime.routingCache = routingCache
	runtime.broadcaster = broadcaster
	runtime.gossipSender = gossipSender
	runtime.cancel = cancelGossip
	runtime.gossipDone = gossipDone
	return runtime, nil
}

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
	runtime, err := SetupReceiptRuntime(ctx, ReceiptRuntimeOptions{
		Options:        opts,
		DataDir:        dataDir,
		NodeID:         nodeID,
		Peers:          peers,
		QUICTransport:  quicTransport,
		Router:         router,
		GossipReceiver: gossipReceiver,
	})
	if err != nil {
		return srvOpts, runtime, err
	}
	if len(runtime.ServerOptions) == 0 && runtime.Store() == nil && runtime.KeyStore() == nil {
		return srvOpts, nil, nil
	}
	return append(srvOpts, runtime.ServerOptions...), runtime, nil
}
