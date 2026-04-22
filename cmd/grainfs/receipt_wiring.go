package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/transport"
)

// healReceiptWiring bundles the Phase 16 Slice 2 components so the caller can
// defer a single teardown. Cluster-only fields (routingCache, broadcaster,
// gossipSender) are nil in no-peers mode.
type healReceiptWiring struct {
	db           *badger.DB
	store        *receipt.Store
	keyStore     *receipt.KeyStore
	api          *receipt.API
	routingCache *receipt.RoutingCache
	broadcaster  *cluster.ReceiptBroadcaster
	gossipSender *cluster.ReceiptGossipSender
}

// Close releases resources. Safe on a nil receiver.
func (w *healReceiptWiring) Close() {
	if w == nil {
		return
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
func openReceiptDB(dataDir string) (*badger.DB, error) {
	dir := filepath.Join(dataDir, "receipts")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create receipts dir: %w", err)
	}
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	return badger.Open(opts)
}

// setupLocalReceipt wires the HealReceipt API in no-peers mode: Store + API with
// nil routes/querier so lookups are local-only. Returns the updated server
// options and a wiring handle for teardown.
func setupLocalReceipt(cmd *cobra.Command, dataDir string, opts []server.Option) ([]server.Option, *healReceiptWiring, error) {
	enabled, _ := cmd.Flags().GetBool("heal-receipt-enabled")
	if !enabled {
		return opts, nil, nil
	}
	psk, _ := cmd.Flags().GetString("heal-receipt-psk")
	if psk == "" {
		psk = "local-no-peers"
	}
	retention, _ := cmd.Flags().GetDuration("heal-receipt-retention")

	db, err := openReceiptDB(dataDir)
	if err != nil {
		return opts, nil, fmt.Errorf("open receipt db: %w", err)
	}
	ks, err := receipt.NewKeyStore(receipt.Key{ID: "local", Secret: []byte(psk)})
	if err != nil {
		_ = db.Close()
		return opts, nil, fmt.Errorf("init receipt keystore: %w", err)
	}
	store, err := receipt.NewStore(db, receipt.StoreOptions{
		Retention:      retention,
		FlushThreshold: 100,
		FlushInterval:  50 * time.Millisecond,
	})
	if err != nil {
		_ = db.Close()
		return opts, nil, fmt.Errorf("create receipt store: %w", err)
	}
	api := receipt.NewAPI(store, nil, nil, retention)

	slog.Info("heal-receipt API enabled",
		"component", "receipt", "mode", "local", "retention", retention)

	return append(opts, server.WithReceiptAPI(api)), &healReceiptWiring{
		db: db, store: store, keyStore: ks, api: api,
	}, nil
}

// setupClusterReceipt wires the full Slice 2 stack in cluster mode.
// PSK comes from --heal-receipt-psk if set, else --cluster-key.
// Registers the StreamReceiptQuery handler on the router and wires the
// routing cache into the gossip receiver. Starts the gossip broadcast
// goroutine bound to ctx.
func setupClusterReceipt(
	ctx context.Context,
	cmd *cobra.Command,
	dataDir, nodeID, clusterKey string,
	peers []string,
	quicTransport *transport.QUICTransport,
	router *transport.StreamRouter,
	gossipReceiver *cluster.GossipReceiver,
	opts []server.Option,
) ([]server.Option, *healReceiptWiring, error) {
	enabled, _ := cmd.Flags().GetBool("heal-receipt-enabled")
	if !enabled {
		return opts, nil, nil
	}
	psk, _ := cmd.Flags().GetString("heal-receipt-psk")
	if psk == "" {
		psk = clusterKey
	}
	// PSK is only required when there are peers — it signs the cross-node
	// StreamReceiptQuery payload. Singletons have no peer to authenticate
	// against, so we fall back to a fixed local-only sentinel. Once the
	// operator adds peers they must supply --cluster-key (validated below).
	if psk == "" {
		if len(peers) > 0 {
			return opts, nil, fmt.Errorf("heal-receipt requires a PSK: set --heal-receipt-psk or --cluster-key")
		}
		psk = "local-no-peers"
	}
	retention, _ := cmd.Flags().GetDuration("heal-receipt-retention")
	gossipInterval, _ := cmd.Flags().GetDuration("heal-receipt-gossip-interval")
	windowSize, _ := cmd.Flags().GetInt("heal-receipt-window")

	db, err := openReceiptDB(dataDir)
	if err != nil {
		return opts, nil, fmt.Errorf("open receipt db: %w", err)
	}

	// KeyStore is constructed here to validate the PSK at boot; the scrubber
	// (Slice 3) consumes it via receiptTrackingEmitter to sign receipts.
	ks, err := receipt.NewKeyStore(receipt.Key{ID: "cluster", Secret: []byte(psk)})
	if err != nil {
		_ = db.Close()
		return opts, nil, fmt.Errorf("init receipt keystore: %w", err)
	}

	store, err := receipt.NewStore(db, receipt.StoreOptions{
		Retention:      retention,
		FlushThreshold: 100,
		FlushInterval:  50 * time.Millisecond,
	})
	if err != nil {
		_ = db.Close()
		return opts, nil, fmt.Errorf("create receipt store: %w", err)
	}

	routingCache := receipt.NewRoutingCache()
	broadcaster := cluster.NewReceiptBroadcaster(quicTransport, peers, 3*time.Second)
	broadcaster.SetMetrics(receipt.BroadcastMetrics{})
	gossipSender := cluster.NewReceiptGossipSender(
		nodeID, peers, quicTransport, store, gossipInterval, windowSize,
	)

	// Wire gossip → cache so peer receipt IDs land here.
	if gossipReceiver != nil {
		gossipReceiver.SetReceiptCache(routingCache)
	}
	// Register the StreamReceiptQuery handler so peers can resolve our
	// local receipts via broadcast fallback.
	router.Handle(transport.StreamReceiptQuery, cluster.NewReceiptQueryHandler(store))

	go gossipSender.Run(ctx)

	api := receipt.NewAPI(store, routingCache, broadcaster, retention)

	slog.Info("heal-receipt API enabled",
		"component", "receipt", "mode", "cluster",
		"retention", retention,
		"gossip_interval", gossipInterval,
		"window", windowSize)

	return append(opts, server.WithReceiptAPI(api)), &healReceiptWiring{
		db:           db,
		store:        store,
		keyStore:     ks,
		api:          api,
		routingCache: routingCache,
		broadcaster:  broadcaster,
		gossipSender: gossipSender,
	}, nil
}
