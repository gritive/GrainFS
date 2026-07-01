package serveruntime

// Zero-CA invite-join boot orchestration (W9b, JOINER side).
//
// A brand-new node holding NO cluster secrets boots into a running voter using
// only an InviteBundle (W1) over the dedicated join transport (W4). This
// productionizes the throwaway de-risk spike (commit 48c7bdb's zero_ca_spike.go)
// into the real two-phase flow against W7's server handler:
//
//	Phase-1 (pre-boot, maybeInviteJoin):
//	  - resolve a STABLE node id (write back to opts.NodeID),
//	  - require a stable advertised raft addr,
//	  - generate the node ECDSA identity + self-signed leaf, persist UNWRAPPED at
//	    keys.d/node.key.unsealed (0600) BEFORE redeeming,
//	  - DialJoin(SeedAddr, SeedSPKI, nodeCert), then sign the canonical
//	    InviteTranscript over the LIVE session channel binding (InviteSig=ed25519
//	    invite key, NodeSig=ecdsa node key) and send JoinRequest{JoinPhase:1},
//	  - OpenFromPeer the SealedBootstrap with the bindCtx W7 used
//	    (clusterID‖inviteID‖nodeID‖leaderID), stage every secret on disk
//	    (keys/<gen>.key and cluster.id),
//	    SealNodeKey to keys.d/node.key.enc under the active KEK gen, shred
//	    node.key.unsealed,
//	  - set opts.ClusterKey in memory (transport PSK) so bootValidateConfig
//	    passes, and persist a .invite-join-pending sentinel so a crash before
//	    Phase-2 resumes the ACK.
//
//	Phase-2 (post-boot, bootInviteJoinPhase2 from bootWALAndForwardersPart1):
//	  - LoadNodeKey (assert SPKI == pending), DialJoin again, send
//	    JoinRequest{JoinPhase:2} to finalize raft membership, clear the sentinel.
//
// On restart with Phase-1 artifacts durable but not ACKed → the resume gate
// returns Resume and the post-boot path re-sends Phase-2 (idempotent on W7).
//
// The join wire is HTTP-framed; the on-disk resume record uses length-prefixed
// the join ALPN — NO JSON, NO TCP. The transcript ClusterID is the raw 16-byte
// cluster.id from the bundle (hex-decoded), matching W7's gateInvite which binds
// r.clusterID into the transcript (closes the spike's documented nil-clusterID
// gap).

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/nodeconfig"
	"github.com/gritive/GrainFS/internal/transport"
)

// inviteBundleEnv is the env var carrying the operator-handed invite token (W1
// EncodeInviteBundle output). Secrets are passed by env/file only (project rule).
const inviteBundleEnv = "GRAINFS_INVITE_BUNDLE"

// invitePendingFile is the Zero-CA invite-join resume sentinel. Present from
// Phase-1 staging until the Phase-2 ACK succeeds. Distinct from the legacy
// .join-pending sentinel (KEK-handshake join). Its presence is the "not acked"
// signal of the resume gate.
const invitePendingFile = ".invite-join-pending"

// nodeKeyUnsealedFile is the UNWRAPPED node key persisted before redeeming the
// invite, so a crash mid-Phase-1 does not lose the identity the leader already
// bound the invite to. It is shredded after SealNodeKey writes node.key.enc.
const nodeKeyUnsealedFile = "node.key.unsealed"

// inviteJoinDialTimeout bounds a single Phase-1/Phase-2 dial+exchange.
const inviteJoinDialTimeout = 30 * time.Second

// inviteJoinState is the Phase-1 outcome threaded onto bootState (via Config) so
// the post-boot Phase-2 ACK can redrive DialJoin without re-reading the bundle.
type inviteJoinState struct {
	seedAddr  string
	seedSPKI  [32]byte
	inviteID  string
	nodeID    string
	raftAddr  string
	clusterID []byte   // raw 16 bytes
	leaderID  string   // from Phase-1 reply; component of the seal bindCtx
	nodeSPKI  [32]byte // joiner's own node identity SPKI (asserted at Phase-2)
	// nodeKeyKEKGen is the KEK generation node.key.enc was sealed under in
	// Phase-1 (the highest gen the cluster actually delivered, NOT a hardcoded
	// gen-0 — a cluster that rotated+pruned gen-0 won't ship it). Phase-2 must
	// LoadNodeKey under this SAME gen.
	nodeKeyKEKGen uint32
	// peerSPKIs is the set of incumbent peer per-node SPKIs decoded from the
	// Phase-1 sealed bootstrap (PR-2a §8f M2). Used to pre-seed the joiner's
	// accept-set before Listen so the joiner can accept incumbents' per-node
	// certs from the first inbound handshake (M3/M4).
	peerSPKIs [][32]byte
	// clusterKeyDropped mirrors the same field from the sealed bootstrap.
	// Post-drop invite joiners use it to avoid reviving the shared transport PSK
	// and to keep node.key.enc sealed under the delivered KEK generation.
	clusterKeyDropped bool
}

// inviteJoinDecision is the resume gate's classification.
type inviteJoinDecision int

const (
	// inviteNormalBoot: no invite bundle, or artifacts complete + acked → boot.
	inviteNormalBoot inviteJoinDecision = iota
	// inviteFreshJoin: bundle present and Phase-1 artifacts INCOMPLETE → run (or
	// re-run, reusing any persisted identity) Phase-1.
	inviteFreshJoin
	// inviteResume: bundle present + artifacts complete but NOT acked → skip
	// Phase-1 pull, resume Phase-2 ACK only.
	inviteResume
)

// classifyInviteJoinResume is the PURE resume gate. Inputs:
//
//   - bundlePresent:         an invite bundle token was supplied (env var set).
//   - persistedInviteNodeKey: keys.d/node.key.unsealed OR keys.d/node.key.enc
//     exists (the joiner already generated + persisted its identity in Phase-1).
//     PRESERVED for the table-test signature only — the decision is now driven
//     by artifactsComplete, NOT mere key presence (see below).
//   - artifactsComplete:     cluster.id + a staged KEK gen +
//     keys.d/node.key.enc + keys.d/current.key all present (the DURABLE Phase-1
//     completion artifacts). Defined by their PRESENCE, NOT by encryption.key nor
//     by the absence of node.key.unsealed: a crash after current.key but before
//     the shred leaves a leftover unsealed key while all durable artifacts exist
//     — that state is complete (resume), and the leftover is shredded
//     idempotently on resume.
//   - acked:                 the .invite-join-pending sentinel is ABSENT (Phase-2
//     membership ACK completed).
//
// The gate is driven by artifactsComplete, NOT persistedInviteNodeKey: a Phase-1
// crash AFTER persisting node.key.unsealed + the sentinel but BEFORE staging
// secrets leaves {persisted key, sentinel present, artifacts INCOMPLETE}. Keying
// Resume on mere key presence would route that state to Phase-2, which then
// hard-fails (no keys.d/current.key) and BRICKS the node forever. Driving on
// artifactsComplete instead re-runs Phase-1 (reusing the persisted identity).
//
// Classification table:
//
//	!bundle                          → NormalBoot
//	bundle && complete && acked      → NormalBoot
//	bundle && complete && !acked     → Resume (Phase-2 only)
//	bundle && !complete (any acked)  → FreshJoin (run/re-run Phase-1)
func classifyInviteJoinResume(bundlePresent, persistedInviteNodeKey, artifactsComplete, acked bool) inviteJoinDecision {
	_ = persistedInviteNodeKey // signature preserved for the table test; unused by design
	// A complete-but-unacked sentinel resumes Phase-2 INDEPENDENT of bundle-env
	// presence: GRAINFS_INVITE_BUNDLE is one-shot, so a restart that does not
	// re-set it must still finish the membership ACK from the sentinel rather than
	// booting on staged secrets as a non-member. Phase-2 reads everything (seed
	// addr/SPKI, inviteID, nodeID, KEK gen) from the sentinel — it needs neither
	// the invite priv key nor the bundle.
	if artifactsComplete && !acked {
		return inviteResume
	}
	if !bundlePresent {
		return inviteNormalBoot
	}
	if !artifactsComplete {
		// Includes the previously-bricking case (persisted key but incomplete
		// staging): re-run Phase-1 rather than resume Phase-2.
		return inviteFreshJoin
	}
	// artifactsComplete && acked: fully joined → normal boot.
	return inviteNormalBoot
}

// inviteJoinPaths bundles the on-disk locations the gate + staging touch.
type inviteJoinPaths struct {
	clusterID       string
	keysDir         string // keys/ — staged KEK gens live here (any <N>.key)
	nodeKeyEnc      string
	nodeKeyUnsealed string
	currentKey      string // keys.d/current.key — transport PSK (resume needs it)
	pendingSentinel string
}

func inviteJoinPathsFor(dataDir string) inviteJoinPaths {
	keysDir := nodeconfig.New(dataDir).KEKDir()
	keysD := filepath.Join(dataDir, "keys.d")
	return inviteJoinPaths{
		clusterID:       filepath.Join(dataDir, nodeconfig.ClusterIDFile),
		keysDir:         keysDir,
		nodeKeyEnc:      filepath.Join(keysD, "node.key.enc"),
		nodeKeyUnsealed: filepath.Join(keysD, nodeKeyUnsealedFile),
		currentKey:      filepath.Join(keysD, "current.key"),
		pendingSentinel: filepath.Join(dataDir, invitePendingFile),
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// keysDirHasKEK reports whether keysDir holds at least one canonical <N>.key.
// Used instead of a hardcoded 0.key check: a cluster that rotated+pruned gen-0
// stages a higher gen, so the staged KEK file is NOT necessarily 0.key. A read
// error counts as "no KEK" (matches fileExists's not-present-→-false semantics).
func keysDirHasKEK(keysDir string) bool {
	empty, err := encrypt.KeysDirIsEmpty(keysDir)
	if err != nil {
		return false
	}
	return !empty
}

// gateInviteJoin reads disk to classify the resume decision for dataDir.
func gateInviteJoin(dataDir string, bundlePresent bool) inviteJoinDecision {
	p := inviteJoinPathsFor(dataDir)
	persistedKey := fileExists(p.nodeKeyUnsealed) || fileExists(p.nodeKeyEnc)
	// artifactsComplete is defined by the DURABLE completion artifacts ONLY, NOT
	// by the absence of node.key.unsealed. Phase-1 writes node.key.enc (step 8)
	// BEFORE current.key (step 9), then shreds node.key.unsealed. So:
	//   - current.key present ⇒ node.key.enc durable (the seal landed first);
	//   - a crash AFTER current.key but BEFORE the shred leaves node.key.unsealed
	//     lingering while ALL durable artifacts exist. Gating on
	//     !fileExists(nodeKeyUnsealed) would mis-route THAT state to FreshJoin and
	//     (worse, without the one-shot bundle env) to a non-member boot that never
	//     sends Phase-2 — splitting the node from the cluster. The leftover
	//     unsealed key is shredded idempotently on the resume path instead.
	artifactsComplete := fileExists(p.clusterID) &&
		keysDirHasKEK(p.keysDir) &&
		fileExists(p.nodeKeyEnc) &&
		fileExists(p.currentKey) // PSK durable ⇒ seal durable; resume can't rerun Phase-1
	acked := !fileExists(p.pendingSentinel)
	return classifyInviteJoinResume(bundlePresent, persistedKey, artifactsComplete, acked)
}

// maybeInviteJoin is the top-of-RunFromOptions hook. It returns a non-nil
// *inviteJoinState ONLY when this boot is a Zero-CA invite-join (FreshJoin or
// Resume); the caller threads it onto Config so boot enters inviteJoinMode. On
// NormalBoot it returns (nil, nil) and is a complete no-op.
//
// dataDir is the canonical PRIMARY data dir (DataDirs[0] when multi-drive, else
// opts.DataDir). ALL on-disk staging/read paths use it — NOT the raw opts.DataDir
// flag, which may be a comma-separated multi-drive list. This keeps the Phase-1
// staging dir == Phase-2 read dir (bootInviteJoinPhase2 reads cfg.DataDir) ==
// cfg.DataDir on multi-disk deployments. opts is still mutated for the in-memory
// writebacks (NodeID, ClusterKey) and read for non-disk fields (RaftAddr,
// JoinListenAddr).
func maybeInviteJoin(ctx context.Context, opts *ServeOptions, dataDir string) (*inviteJoinState, error) {
	token := os.Getenv(inviteBundleEnv)
	decision := gateInviteJoin(dataDir, token != "")
	if decision == inviteNormalBoot {
		// Stale-bundle no-op resume: a fully-joined node restarted with the
		// (now-consumed) bundle env still set hits the same cluster-key gate as
		// inviteResume — bootValidateConfig runs BEFORE ResolveClusterKey reads
		// disk, so an empty flag PSK trips ErrEmptyClusterKey. Load the PSK that
		// Phase-1 mirrored to keys.d/current.key. ReadCurrent on a truly fresh
		// dataDir returns ("", nil), so this is a no-op there.
		if opts.ClusterKey == "" {
			if ks, kerr := clusterKeystoreFromOpts(dataDir, *opts); kerr == nil {
				if psk, err := ks.ReadCurrent(); err == nil && psk != "" {
					opts.ClusterKey = psk
				}
			}
		}
		return nil, nil
	}

	if decision == inviteResume {
		// Resume Phase-2 ACK from the sentinel ALONE — independent of the bundle
		// env (one-shot; a restart need not re-set it). Everything Phase-2 needs
		// (seed addr/SPKI, inviteID, nodeID, raftAddr, KEK gen) is persisted in the
		// sentinel; the node SPKI + leaderID are reloaded from node.key.enc + the
		// sentinel at Phase-2. We do NOT decode the bundle here.
		return inviteJoinResumeFromSentinel(opts, dataDir)
	}

	bundle, err := cluster.DecodeInviteBundle(token)
	if err != nil {
		return nil, fmt.Errorf("decode invite bundle: %w", err)
	}
	clusterID, err := hex.DecodeString(bundle.ClusterIDHex)
	if err != nil {
		return nil, fmt.Errorf("decode cluster id hex: %w", err)
	}

	// Resolve a STABLE node id BEFORE Phase-1 and write it back so the later
	// normal boot (bootValidateConfig) resolves the IDENTICAL id. GenerateNodeID
	// persists <dataDir>/node-id and is idempotent.
	if opts.NodeID == "" {
		id, err := GenerateNodeID(dataDir)
		if err != nil {
			return nil, fmt.Errorf("resolve node id: %w", err)
		}
		opts.NodeID = id
	}

	// Require a stable advertised raft addr: the leader records this as the
	// joiner's membership address, so an ephemeral 127.0.0.1:0 (resolved only
	// after the local listener binds) would advertise a port the cluster can
	// never dial back.
	raftAddr := strings.TrimSpace(opts.RaftAddr)
	if raftAddr == "" || strings.HasSuffix(raftAddr, ":0") {
		return nil, fmt.Errorf("invite-join requires a stable --raft-addr (got %q); an ephemeral :0 port cannot be advertised to the cluster", opts.RaftAddr)
	}

	st := &inviteJoinState{
		seedAddr:  bundle.SeedAddr,
		seedSPKI:  bundle.SeedSPKI,
		inviteID:  bundle.InviteID,
		nodeID:    opts.NodeID,
		raftAddr:  raftAddr,
		clusterID: clusterID,
	}

	// FreshJoin: run Phase-1 (pull + stage + seal + sentinel).
	if err := inviteJoinPhase1(ctx, opts, dataDir, bundle, st); err != nil {
		return nil, err
	}
	return st, nil
}

// inviteJoinResumeFromSentinel reconstructs the Phase-2 resume state from the
// persisted sentinel (NOT the bundle env, which is one-shot). It is reached when
// the gate classifies inviteResume (artifacts complete but the ACK never
// landed), regardless of whether the bundle env is set on this restart. It also
// populates opts.ClusterKey from the transport PSK Phase-1 mirrored to
// keys.d/current.key, because bootValidateConfig's cluster-key gate runs
// BEFORE bootClusterTransport's ResolveClusterKey reads disk.
func inviteJoinResumeFromSentinel(opts *ServeOptions, dataDir string) (*inviteJoinState, error) {
	rec, ok := readInvitePendingSentinel(dataDir)
	if !ok {
		return nil, fmt.Errorf("invite-join resume: sentinel missing or unreadable")
	}
	ks, kerr := clusterKeystoreFromOpts(dataDir, *opts)
	if kerr != nil {
		return nil, fmt.Errorf("invite-join resume: build keystore: %w", kerr)
	}
	psk, err := ks.ReadCurrent()
	if err != nil || psk == "" {
		return nil, fmt.Errorf("invite-join resume: transport PSK (keys.d/current.key) missing or unreadable: %w", err)
	}
	opts.ClusterKey = psk
	if opts.NodeID == "" {
		opts.NodeID = rec.nodeID
	}
	// Idempotent cleanup: a Phase-1 crash AFTER current.key but BEFORE the shred
	// leaves node.key.unsealed lingering. artifactsComplete no longer requires its
	// absence, so resume must shred it here so the plaintext key does not linger.
	// shredFile tolerates an absent file (the common case).
	shredFile(inviteJoinPathsFor(dataDir).nodeKeyUnsealed)
	st := &inviteJoinState{
		seedAddr:          rec.seedAddr,
		seedSPKI:          rec.seedSPKI,
		inviteID:          rec.inviteID,
		nodeID:            rec.nodeID,
		raftAddr:          rec.raftAddr,
		leaderID:          rec.leaderID,
		nodeSPKI:          rec.nodeSPKI,
		nodeKeyKEKGen:     rec.nodeKeyKEKGen,
		peerSPKIs:         rec.peerSPKIs,
		clusterKeyDropped: rec.clusterKeyDropped,
		// clusterID stays nil: Phase-2 does not use it (the seal was opened in
		// Phase-1). FreshJoin needs it; resume does not.
	}
	log.Warn().Str("node_id", st.nodeID).Msg("zero-CA invite-join: resuming from sentinel (Phase-1 artifacts durable, Phase-2 ACK pending)")
	return st, nil
}

// inviteJoinPhase1 performs the secret pull + on-disk staging. It mutates st
// (leaderID, nodeSPKI) and opts (ClusterKey).
func inviteJoinPhase1(ctx context.Context, opts *ServeOptions, dataDir string, bundle cluster.InviteBundle, st *inviteJoinState) error {
	paths := inviteJoinPathsFor(dataDir)
	keysD := filepath.Join(dataDir, "keys.d")

	// 1. node ECDSA P-256 identity + self-signed leaf (Path A: cert in request).
	// REUSE an existing unsealed key if a prior Phase-1 already persisted one: a
	// re-run must present the SAME SPKI the leader may have already bound via
	// ProposeInvitePending (a fresh SPKI would mismatch the pending record and be
	// rejected). Only generate fresh when no unsealed key exists.
	var cert tls.Certificate
	var spki [32]byte
	var err error
	if priv, ok := readNodeKeyUnsealed(paths.nodeKeyUnsealed); ok {
		cert, spki, err = transport.BuildNodeIdentity(bundle.ClusterIDHex, st.nodeID, priv)
		if err != nil {
			return fmt.Errorf("rebuild node identity from persisted key: %w", err)
		}
	} else {
		cert, spki, err = transport.GenerateNodeIdentity(bundle.ClusterIDHex, st.nodeID)
		if err != nil {
			return fmt.Errorf("generate node identity: %w", err)
		}
	}
	priv, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		return fmt.Errorf("node identity key is %T, want *ecdsa.PrivateKey", cert.PrivateKey)
	}
	certDER := cert.Certificate[0]
	st.nodeSPKI = spki

	// Persist the UNWRAPPED node key BEFORE redeeming. If we crash after the
	// leader bound the invite to this SPKI but before we can seal the key under
	// the (not-yet-staged) KEK, the unsealed key lets resume reuse the identity.
	if err := writeNodeKeyUnsealed(keysD, paths.nodeKeyUnsealed, priv); err != nil {
		return err
	}

	// 2. write the resume sentinel BEFORE the dial: from here on a crash must
	// resume Phase-2 rather than restart Phase-1 with a fresh identity.
	if err := writeInvitePendingSentinel(dataDir, paths.pendingSentinel, st); err != nil {
		return err
	}

	// 3. generate the joiner nonce + advertise this node's OWN join listener
	// (W9a) so a non-leader seed can later redirect us to the leader (redirect
	// itself is a follow-up task). The nonce is joiner-generated and fixed across
	// the dial; the transcript itself is built + signed INSIDE buildPhase1 below,
	// AFTER the dial yields the live session's channel binding (Bind).
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("invite nonce: %w", err)
	}
	_, joinListenerSPKI, err := LoadOrCreateJoinListenerCert(dataDir)
	if err != nil {
		return fmt.Errorf("load join listener cert: %w", err)
	}
	joinListenAddr := resolveJoinListenAddr(opts.JoinListenAddr, st.raftAddr)

	// buildPhase1 builds + signs the canonical transcript over the LIVE join TLS
	// channel binding (bind), then returns the full Phase-1 JoinRequest. Channel
	// binding requires signing over the live session's exporter, so this runs
	// AFTER the dial. ClusterID is the raw 16-byte cluster.id (matches W7
	// gateInvite). The signed request is NEVER persisted: a resumed Phase-1
	// re-dials and re-signs over the new session's bind (see resume gate).
	buildPhase1 := func(bind []byte) (cluster.JoinRequest, error) {
		tr := encrypt.InviteTranscript{
			ClusterID: st.clusterID,
			Nonce:     nonce,
			NodeID:    st.nodeID,
			Address:   st.raftAddr,
			SPKI:      spki[:],
			Bind:      bind,
		}
		inviteSig := encrypt.SignInviteTranscript(bundle.InvitePriv, tr)
		nodeSig, err := encrypt.SignNodeTranscript(priv, tr)
		if err != nil {
			return cluster.JoinRequest{}, fmt.Errorf("sign node transcript: %w", err)
		}
		return cluster.JoinRequest{
			JoinPhase:              1,
			NodeID:                 st.nodeID,
			Address:                st.raftAddr,
			SPKI:                   spki[:],
			CertDER:                certDER,
			NodeSig:                nodeSig,
			InviteSig:              inviteSig,
			InviteID:               bundle.InviteID,
			HandshakeNonce:         nonce,
			JoinerJoinListenerAddr: joinListenAddr,
			JoinerJoinListenerSPKI: joinListenerSPKI[:],
		}, nil
	}

	// 5. dial the seed join listener (Phase-1), pinning its SPKI. The transcript
	// is signed inside buildPhase1 over the resulting channel binding.
	reply, err := inviteJoinDialWith(ctx, transport.DialJoinHTTP, bundle.SeedAddr, bundle.SeedSPKI, cert, buildPhase1)
	if err != nil {
		return fmt.Errorf("phase-1 dial: %w", err)
	}
	if !reply.Accepted {
		return fmt.Errorf("phase-1 rejected: status=%s msg=%s", reply.Status, reply.Message)
	}
	if len(reply.SealedBootstrap) == 0 {
		return fmt.Errorf("phase-1 reply missing sealed bootstrap")
	}
	st.leaderID = reply.LeaderID

	// 6. open the sealed bootstrap with the node key. bindCtx/aad MUST match W7's
	// sealBindContext exactly: clusterID‖inviteID‖nodeID‖leaderID.
	ephPub, ciphertext, err := cluster.DecodeSealedBootstrap(reply.SealedBootstrap)
	if err != nil {
		return fmt.Errorf("decode sealed bootstrap: %w", err)
	}
	bindCtx := inviteSealBindContext(st.clusterID, bundle.InviteID, st.nodeID, reply.LeaderID)
	plain, err := encrypt.OpenFromPeer(priv, encrypt.SealedToPeer{EphemeralPub: ephPub, Ciphertext: ciphertext}, bindCtx, bindCtx)
	if err != nil {
		return fmt.Errorf("open sealed bootstrap: %w", err)
	}
	// M2: switch decoder to DecodeBootstrapSecretsPayloadWithCutover so
	// peer_spkis and clusterKeyDropped are extracted and staged (PR-2a §8f).
	// Legacy payloads (old-encoder leader without cutover fields) decode cleanly:
	// FlatBuffers returns empty SPKI set + false for absent fields (M5).
	encKey, kekGens, psk, peerSPKIs, clusterKeyDropped, err := cluster.DecodeBootstrapSecretsPayloadWithCutover(plain)
	if err != nil {
		return fmt.Errorf("decode bootstrap secrets: %w", err)
	}
	if len(encKey) != 0 {
		return fmt.Errorf("decode bootstrap secrets: retired static encryption key field is not supported")
	}
	if len(peerSPKIs) == 0 {
		// Old-encoder leader or rolling-upgrade scenario (M5): peer_spkis absent.
		log.Warn().Msg("invite-join: peer_spkis empty in bootstrap; joiner accept-set will use PSK base only (rolling-upgrade compat)")
	}
	st.peerSPKIs = peerSPKIs
	st.clusterKeyDropped = clusterKeyDropped

	// 7. stage every secret on disk where the normal boot expects them.
	if err := stageInviteSecrets(dataDir, kekGens, st.clusterID); err != nil {
		return err
	}

	// 8. SealNodeKey where the next boot phase can open it. Use the cluster's
	// ACTIVE KEK generation. Post-drop joins present the per-node cert before
	// transport Listen, so bootClusterTransport loads this same KEK directly from disk
	// before wireDEKKeeper has populated state.kekStore.
	sealGen, sealKEK, err := inviteNodeKeySealKey(kekGens)
	if err != nil {
		return err
	}
	st.nodeKeyKEKGen = sealGen
	// Re-persist the sentinel with the chosen KEK gen BEFORE sealing (the step-2
	// write predates staging and carries gen 0). Ordering matters for crash
	// safety (durable artifacts are cluster.id, retained KEKs, node.key.enc, and
	// keys.d/current.key; a leftover unsealed key is shredded on resume):
	//   - crash after this write, before SealNodeKey → no node.key.enc →
	//     !artifactsComplete → FreshJoin re-runs Phase-1 (reusing the unsealed key);
	//   - crash after SealNodeKey, before PSK write → unsealed still present →
	//     !artifactsComplete → FreshJoin re-seals (atomic temp+rename overwrites);
	//   - crash after PSK write, before shred → unsealed still present →
	//     artifactsComplete → Resume shreds the leftover unsealed key and runs
	//     Phase-2;
	//   - crash after shred → artifactsComplete (PSK already durable), sentinel
	//     carries the correct gen → Resume → Phase-2 LoadNodeKey under the SAME gen.
	if err := writeInvitePendingSentinel(dataDir, paths.pendingSentinel, st); err != nil {
		return err
	}
	if err := transport.SealNodeKey(dataDir, sealKEK, cert); err != nil {
		return fmt.Errorf("seal node key: %w", err)
	}
	if err := writeNodeKeyGen(dataDir, sealGen); err != nil {
		return err
	}

	// 9. set a transport construction key in memory so bootValidateConfig's
	// cluster-key gate passes, and mirror it to keys.d/current.key for crash
	// resume. In a post-drop cluster the leader intentionally omits the revoked
	// cluster PSK; use a local random placeholder instead. bootClusterTransport
	// immediately calls FlipPresent+SetDropped for post-drop joiners, so this
	// placeholder is never accepted by peers and never reintroduces the dropped
	// cluster-key SPKI.
	if err := stageInviteJoinTransportKey(dataDir, opts, psk, clusterKeyDropped); err != nil {
		return err
	}
	shredFile(paths.nodeKeyUnsealed)

	log.Warn().
		Str("node_id", st.nodeID).
		Str("leader_id", reply.LeaderID).
		Str("seed", bundle.SeedAddr).
		Msg("zero-CA invite-join: Phase-1 complete (secrets staged, identity sealed); Phase-2 ACK pending")
	return nil
}

func stageInviteJoinTransportKey(dataDir string, opts *ServeOptions, psk []byte, clusterKeyDropped bool) error {
	transportKey := string(psk)
	if clusterKeyDropped {
		generated, err := GenerateEphemeralClusterKey()
		if err != nil {
			return fmt.Errorf("post-drop invite-join: generate local transport placeholder: %w", err)
		}
		transportKey = generated
	}
	opts.ClusterKey = transportKey
	if transportKey != "" {
		ks, kerr := clusterKeystoreFromOpts(dataDir, *opts)
		if kerr != nil {
			return fmt.Errorf("build keystore: %w", kerr)
		}
		if err := ks.WriteCurrent(transportKey); err != nil {
			return fmt.Errorf("mirror transport key to keystore: %w", err)
		}
	}
	return nil
}

func inviteNodeKeySealKey(kekGens []cluster.KEKGen) (uint32, []byte, error) {
	sealGen, sealKEK, ok := highestKEKGen(kekGens)
	if !ok {
		return 0, nil, fmt.Errorf("bootstrap secrets contain no KEK generations")
	}
	return sealGen, sealKEK, nil
}

// bootInviteJoinPhase2 finalizes membership AFTER the meta-raft transport + raft
// are up (called from bootWALAndForwardersPart1, before WaitDEKReady). It reloads the
// node key (asserting the SPKI matches the Phase-1 identity), dials the seed join
// listener with JoinPhase:2, and clears the resume sentinel on success.
func bootInviteJoinPhase2(ctx context.Context, state *bootState) error {
	st := state.inviteJoin
	if st == nil {
		return fmt.Errorf("invite-join Phase-2: nil state")
	}

	if state.kekStore == nil {
		return fmt.Errorf("invite-join Phase-2: KEK store not wired")
	}
	if st.nodeKeyKEKGen == 0 {
		if persisted, ok := readInvitePendingSentinel(state.cfg.DataDir); ok {
			st.nodeKeyKEKGen = persisted.nodeKeyKEKGen
		}
	}
	cert, spki, nodeKeyKEKGen, err := loadAndMigrateInviteNodeKey(state.cfg.DataDir, state.kekStore, st.nodeKeyKEKGen, st.nodeKeyKEKGen != 0)
	if err != nil {
		return fmt.Errorf("invite-join Phase-2: load node key: %w", err)
	}
	// On a fresh-join boot st.nodeSPKI is set from Phase-1; on resume it is zero
	// (Phase-1 ran in a prior process) — skip the equality assert in that case.
	if st.nodeSPKI != ([32]byte{}) && spki != st.nodeSPKI {
		return fmt.Errorf("invite-join Phase-2: loaded node SPKI does not match Phase-1 identity")
	}
	st.nodeSPKI = spki
	// PR-2a F5/F3 fix: store cert; T5 onPresentFlip callback needs it.
	state.perNodeCert = cert
	state.perNodeSPKI = spki
	state.perNodeKeyKEKGen = nodeKeyKEKGen

	// Resolve leaderID: prefer the Phase-1 value, fall back to the persisted
	// sentinel (resume across process restart).
	if st.leaderID == "" {
		if persisted, ok := readInvitePendingSentinel(state.cfg.DataDir); ok {
			st.leaderID = persisted.leaderID
			if st.inviteID == "" {
				st.inviteID = persisted.inviteID
			}
		}
	}

	req := cluster.JoinRequest{
		JoinPhase: 2,
		NodeID:    st.nodeID,
		Address:   st.raftAddr,
		SPKI:      spki[:],
		InviteID:  st.inviteID,
	}
	reply, err := inviteJoinDialWith(ctx, transport.DialJoinHTTP, st.seedAddr, st.seedSPKI, cert,
		func([]byte) (cluster.JoinRequest, error) { return req, nil })
	if err != nil {
		return fmt.Errorf("invite-join Phase-2 dial: %w", err)
	}
	if !reply.Accepted {
		return fmt.Errorf("invite-join Phase-2 rejected: status=%s msg=%s", reply.Status, reply.Message)
	}

	// reply.PeerSPKIs (the cluster per-node accept-set) is intentionally NOT
	// installed here: the joiner's normal cluster transport derives its accept-set
	// from the shared transport PSK (NewHTTPTransport(transportPSK) →
	// DeriveClusterIdentity), which Phase-1 already delivered. The leader dials
	// the freshly-joined node using that SAME PSK-derived cluster identity, so
	// AppendEntries catch-up (and thus the gen-0 DEK Apply the WaitDEKReady gate
	// waits on) succeeds without per-node SPKIs. Wiring reply.PeerSPKIs into a
	// registry∪PSK accept-set UNION is the deferred work tracked by the
	// SetOnPeersChanged TODO in boot_phases_raft.go.

	// Membership finalized: clear the resume barrier.
	_ = os.Remove(filepath.Join(state.cfg.DataDir, invitePendingFile))
	log.Info().
		Str("node_id", st.nodeID).
		Str("leader_id", st.leaderID).
		Msg("zero-CA invite-join: Phase-2 ACK complete — node is a cluster member")
	return nil
}

// inviteJoinDial dials a JoinListener at addr (pinning serverSPKI), then invokes
// buildReq with the live session's channel binding to obtain the JoinRequest,
// sends it framed, and reads the framed JoinReply. One field in each direction,
// length-prefixed binary (NO JSON) — mirrors W7 HandleJoinRequest. buildReq runs
// AFTER the dial so the joiner can sign over the channel binding (Bind); on a
// resumed Phase-1 each dial yields a fresh bind, so the request is re-signed and
// never persisted.
// joinDialer drives one invite round-trip over the join transport. buildReq is
// invoked with the session's RFC5705 channel binding (it must exist before the
// request body it is folded into) and returns the encoded JoinRequest body; the
// dialer returns the encoded JoinReply body. transport.DialJoinHTTP implements
// it in production; tests inject a fake. It is the single seam selecting the
// join transport without touching the consumer choreography below.
type joinDialer func(ctx context.Context, addr string, serverSPKI [32]byte, clientCert tls.Certificate, buildReq func(bind []byte) ([]byte, error)) ([]byte, error)

// inviteJoinDialWith parameterizes the invite round-trip by the join dialer so
// tests drive the SAME consumer choreography. HTTP framing carries the single
// request/reply, so this is now just: dial(buildReq) → decode reply.
func inviteJoinDialWith(ctx context.Context, dial joinDialer, addr string, serverSPKI [32]byte, clientCert tls.Certificate, buildReq func(bind []byte) (cluster.JoinRequest, error)) (cluster.JoinReply, error) {
	dialCtx, cancel := context.WithTimeout(ctx, inviteJoinDialTimeout)
	defer cancel()
	replyBlob, err := dial(dialCtx, addr, serverSPKI, clientCert, func(bind []byte) ([]byte, error) {
		req, err := buildReq(bind)
		if err != nil {
			return nil, err
		}
		return cluster.EncodeJoinRequest(req)
	})
	if err != nil {
		return cluster.JoinReply{}, err
	}
	reply, err := cluster.DecodeJoinReply(replyBlob)
	if err != nil {
		return cluster.JoinReply{}, fmt.Errorf("decode join reply: %w", err)
	}
	return *reply, nil
}

// inviteSealBindContext reproduces W7 MetaJoinReceiver.sealBindContext
// byte-for-byte: a domain tag followed by length-prefixed (4-byte big-endian)
// clusterID, inviteID, nodeID, leaderID. The joiner derives the SAME bytes to
// open the sealed bootstrap (both contextInfo and aad use this value). The
// length prefixing closes a boundary-ambiguity class; this MUST stay in sync
// with the leader's sealBindContext (the e2e seal/open round-trip enforces it).
const inviteSealDomain = "grainfs-invite-seal-v1"

func inviteSealBindContext(clusterID []byte, inviteID, nodeID, leaderID string) []byte {
	out := make([]byte, 0, len(inviteSealDomain)+16+len(clusterID)+len(inviteID)+len(nodeID)+len(leaderID))
	out = append(out, inviteSealDomain...)
	out = appendInviteSealField(out, clusterID)
	out = appendInviteSealField(out, []byte(inviteID))
	out = appendInviteSealField(out, []byte(nodeID))
	out = appendInviteSealField(out, []byte(leaderID))
	return out
}

// appendInviteSealField appends b length-prefixed (4-byte big-endian length).
// Mirrors internal/cluster.appendInviteSealField byte-for-byte.
func appendInviteSealField(out, b []byte) []byte {
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(b)))
	out = append(out, l[:]...)
	return append(out, b...)
}

// loadAndMigrateInviteNodeKey loads keys.d/node.key.enc and guarantees it is
// sealed under the active KEK generation on return. node.key.gen is the primary
// generation record; fallbackGen exists for same-process Phase-2 state and the
// invite pending sentinel on older resumes.
func loadAndMigrateInviteNodeKey(dataDir string, kekStore *encrypt.KEKStore, fallbackGen uint32, fallbackOK bool) (tls.Certificate, [32]byte, uint32, error) {
	activeGen, activeKEK, err := activeNodeKeyKEK(kekStore)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, 0, err
	}

	sealedGen, ok := readNodeKeyGen(dataDir)
	if !ok {
		if fallbackOK {
			sealedGen = fallbackGen
		} else {
			return tls.Certificate{}, [32]byte{}, 0, errors.New("node.key.gen missing for invite node key")
		}
	}
	sealKEK, err := kekStore.Get(sealedGen)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, 0, fmt.Errorf("get node key KEK gen %d: %w", sealedGen, err)
	}
	cert, spki, err := transport.LoadNodeKey(dataDir, sealKEK)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, 0, fmt.Errorf("load invite node key sealed under KEK gen %d: %w", sealedGen, err)
	}
	if sealedGen != activeGen {
		if err := sealNodeKeyAtGen(dataDir, activeGen, activeKEK, cert); err != nil {
			return tls.Certificate{}, [32]byte{}, 0, fmt.Errorf("re-seal invite node key under active KEK gen %d: %w", activeGen, err)
		}
		return cert, spki, activeGen, nil
	} else if !ok {
		if err := writeNodeKeyGen(dataDir, activeGen); err != nil {
			return tls.Certificate{}, [32]byte{}, 0, err
		}
	}
	return cert, spki, sealedGen, nil
}

func writeNodeKeyGen(dataDir string, gen uint32) error {
	keysDir := filepath.Join(dataDir, "keys.d")
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		return fmt.Errorf("write node key gen: mkdir: %w", err)
	}
	path := filepath.Join(keysDir, nodeKeyGenFile)
	tmp := path + ".tmp"
	_ = os.Remove(tmp)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("write node key gen: create tmp: %w", err)
	}
	if _, err := f.Write([]byte(strconv.FormatUint(uint64(gen), 10))); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write node key gen: write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write node key gen: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("write node key gen: close: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("write node key gen: rename: %w", err)
	}
	d, err := os.Open(keysDir)
	if err != nil {
		return fmt.Errorf("write node key gen: open dir: %w", err)
	}
	defer d.Close()
	return d.Sync()
}

func readNodeKeyGen(dataDir string) (uint32, bool) {
	path := filepath.Join(dataDir, "keys.d", nodeKeyGenFile)
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) == 0 {
		return 0, false
	}
	s := string(raw)
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, false
	}
	if strconv.FormatUint(v, 10) != s {
		return 0, false
	}
	return uint32(v), true
}

// highestKEKGen returns the highest generation present in gens and its key
// bytes — the cluster's ACTIVE KEK. KEKStore.Add advances active to the highest
// gen on load, so the receiver's store will hold this gen after staging. Returns
// ok=false when gens is empty or every entry has empty key bytes.
func highestKEKGen(gens []cluster.KEKGen) (gen uint32, key []byte, ok bool) {
	for _, g := range gens {
		if len(g.Key) == 0 {
			continue
		}
		if !ok || g.Gen > gen {
			gen, key, ok = g.Gen, g.Key, true
		}
	}
	return gen, key, ok
}

// stageInviteSecrets writes every keys/<gen>.key and cluster.id (raw 16 bytes)
// where normal boot expects them. Fresh invite joins never create encryption.key.
func stageInviteSecrets(dataDir string, kekGens []cluster.KEKGen, clusterID []byte) error {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("stage secrets: mkdir data: %w", err)
	}
	if len(clusterID) > 0 {
		if err := os.WriteFile(filepath.Join(dataDir, nodeconfig.ClusterIDFile), clusterID, 0o600); err != nil {
			return fmt.Errorf("stage secrets: cluster.id: %w", err)
		}
	}
	keysDir := nodeconfig.New(dataDir).KEKDir()
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		return fmt.Errorf("stage secrets: mkdir keys: %w", err)
	}
	for _, g := range kekGens {
		name := fmt.Sprintf("%d.key", g.Gen)
		if err := os.WriteFile(filepath.Join(keysDir, name), g.Key, 0o600); err != nil {
			return fmt.Errorf("stage secrets: keys/%s: %w", name, err)
		}
	}
	return nil
}

// --- node.key.unsealed (plain PKCS#8 PEM, 0600) ----------------------------

func writeNodeKeyUnsealed(keysD, path string, priv *ecdsa.PrivateKey) error {
	if err := os.MkdirAll(keysD, 0o700); err != nil {
		return fmt.Errorf("write unsealed node key: mkdir: %w", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return fmt.Errorf("write unsealed node key: marshal: %w", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		return fmt.Errorf("write unsealed node key: %w", err)
	}
	return nil
}

// readNodeKeyUnsealed loads the PKCS#8-PEM unsealed node key written by
// writeNodeKeyUnsealed. Returns ok=false if absent/unreadable/not-ECDSA so the
// caller falls back to generating a fresh identity.
func readNodeKeyUnsealed(path string) (*ecdsa.PrivateKey, bool) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, false
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, false
	}
	priv, ok := key.(*ecdsa.PrivateKey)
	if !ok {
		return nil, false
	}
	return priv, true
}

// shredFile best-effort overwrites + removes a small secret file.
func shredFile(path string) {
	if info, err := os.Stat(path); err == nil && info.Size() > 0 {
		if f, err := os.OpenFile(path, os.O_WRONLY, 0o600); err == nil {
			zeros := make([]byte, info.Size())
			_, _ = f.Write(zeros)
			_ = f.Sync()
			_ = f.Close()
		}
	}
	_ = os.Remove(path)
}

// --- resume sentinel (length-prefixed binary, NO JSON) ---------------------

type invitePendingRecord struct {
	inviteID          string
	leaderID          string
	seedAddr          string
	seedSPKI          [32]byte
	nodeID            string
	raftAddr          string
	nodeSPKI          [32]byte
	nodeKeyKEKGen     uint32
	peerSPKIs         [][32]byte
	clusterKeyDropped bool
}

func writeInvitePendingSentinel(dataDir, path string, st *inviteJoinState) error {
	rec := invitePendingRecord{
		inviteID:          st.inviteID,
		leaderID:          st.leaderID,
		seedAddr:          st.seedAddr,
		seedSPKI:          st.seedSPKI,
		nodeID:            st.nodeID,
		raftAddr:          st.raftAddr,
		nodeSPKI:          st.nodeSPKI,
		nodeKeyKEKGen:     st.nodeKeyKEKGen,
		peerSPKIs:         st.peerSPKIs,
		clusterKeyDropped: st.clusterKeyDropped,
	}
	var genBuf [4]byte
	binary.BigEndian.PutUint32(genBuf[:], rec.nodeKeyKEKGen)
	// Fields 0–7 (original 8 fields).
	var buf []byte
	buf = transport.JoinPutField(buf, []byte(rec.inviteID))
	buf = transport.JoinPutField(buf, []byte(rec.leaderID))
	buf = transport.JoinPutField(buf, []byte(rec.seedAddr))
	buf = transport.JoinPutField(buf, rec.seedSPKI[:])
	buf = transport.JoinPutField(buf, []byte(rec.nodeID))
	buf = transport.JoinPutField(buf, []byte(rec.raftAddr))
	buf = transport.JoinPutField(buf, rec.nodeSPKI[:])
	buf = transport.JoinPutField(buf, genBuf[:])
	// Field 8: peerSPKIs as 2-byte count + N×32 bytes blob (PR-2a §8f M4).
	spkiBlob := encodePeerSPKIsBlob(rec.peerSPKIs)
	buf = transport.JoinPutField(buf, spkiBlob)
	// Field 9: clusterKeyDropped as 1 byte.
	dropped := byte(0)
	if rec.clusterKeyDropped {
		dropped = 1
	}
	buf = transport.JoinPutField(buf, []byte{dropped})
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("write invite sentinel: mkdir: %w", err)
	}
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		return fmt.Errorf("write invite sentinel: %w", err)
	}
	return nil
}

// encodePeerSPKIsBlob packs a slice of SPKIs as: uint16 big-endian count,
// followed by count×32 bytes. Empty slice encodes as a 2-byte zero count.
func encodePeerSPKIsBlob(spkis [][32]byte) []byte {
	blob := make([]byte, 2+len(spkis)*32)
	binary.BigEndian.PutUint16(blob[:2], uint16(len(spkis)))
	for i, s := range spkis {
		copy(blob[2+i*32:], s[:])
	}
	return blob
}

// decodePeerSPKIsBlob is the inverse of encodePeerSPKIsBlob.
func decodePeerSPKIsBlob(blob []byte) ([][32]byte, bool) {
	if len(blob) < 2 {
		return nil, false
	}
	n := int(binary.BigEndian.Uint16(blob[:2]))
	if len(blob) < 2+n*32 {
		return nil, false
	}
	out := make([][32]byte, n)
	for i := range out {
		copy(out[i][:], blob[2+i*32:])
	}
	return out, true
}

func readInvitePendingSentinel(dataDir string) (invitePendingRecord, bool) {
	path := filepath.Join(dataDir, invitePendingFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return invitePendingRecord{}, false
	}
	// Try the current 10-field format first; fall back to the legacy 8-field
	// format for crash-resume of sentinels written before PR-2a §8f (M4).
	fields, err := transport.JoinReadFields(bytes.NewReader(data), 10)
	if err != nil {
		// Legacy sentinel: re-try with 8 fields on a fresh reader.
		fields, err = transport.JoinReadFields(bytes.NewReader(data), 8)
		if err != nil {
			return invitePendingRecord{}, false
		}
	}
	var rec invitePendingRecord
	rec.inviteID = string(fields[0])
	rec.leaderID = string(fields[1])
	rec.seedAddr = string(fields[2])
	copy(rec.seedSPKI[:], fields[3])
	rec.nodeID = string(fields[4])
	rec.raftAddr = string(fields[5])
	copy(rec.nodeSPKI[:], fields[6])
	if len(fields[7]) == 4 {
		rec.nodeKeyKEKGen = binary.BigEndian.Uint32(fields[7])
	}
	// Fields 8 and 9 are present only in the PR-2a §8f format.
	if len(fields) >= 9 {
		if spkis, ok := decodePeerSPKIsBlob(fields[8]); ok {
			rec.peerSPKIs = spkis
		}
	}
	if len(fields) >= 10 && len(fields[9]) == 1 {
		rec.clusterKeyDropped = fields[9][0] == 1
	}
	return rec, true
}
