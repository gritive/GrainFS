package cluster

import (
	"errors"
	"fmt"
	"time"
)

// ErrClusterConfigCAS is returned (wrapped) when a ClusterConfigPatch supplies
// a non-zero ExpectedRev that does not match the current live Rev. Admin
// endpoints (Task 10) map this to HTTP 409.
var ErrClusterConfigCAS = errors.New("cluster config CAS mismatch")

// applyClusterConfigPatch is the FSM-side apply for MetaCmdTypeClusterConfigPatch.
// Implements CAS, applies the patch to a *trial* clone, validates the merged
// result, and only commits (real apply on the live ClusterConfig) on success.
func (f *MetaFSM) applyClusterConfigPatch(data []byte) error {
	p, err := DecodeClusterConfigPatchCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: ClusterConfigPatch: %w", err)
	}

	// Reject wrapped-secret patches when no encryptor is registered
	// (--no-encryption mode). The handler (Task 10) maps this error to HTTP
	// 403 by matching the literal "encryption disabled" substring.
	if len(p.AlertWebhookSecretWrapped) > 0 && f.encryptor == nil {
		return fmt.Errorf("cluster-config alert-webhook-secret rejected: encryption disabled on this node (--no-encryption)")
	}

	current := f.clusterCfg
	if p.ExpectedRev != 0 && current.Rev() != p.ExpectedRev {
		return fmt.Errorf("cluster config CAS mismatch: expected rev %d, current %d: %w",
			p.ExpectedRev, current.Rev(), ErrClusterConfigCAS)
	}

	ts := time.Now()

	// Trial apply on a clone so we can validate before committing.
	trial := cloneClusterConfig(current)
	trial.applyPatch(p, ts)
	if err := trial.Validate(); err != nil {
		return err
	}

	// Validation passed; apply on the live object. Single-writer (FSM
	// goroutine) so no race with another apply.
	current.applyPatch(p, ts)
	return nil
}

// cloneClusterConfig produces an independent ClusterConfig sharing no mutable
// state with src — used for the validation trial in applyClusterConfigPatch.
func cloneClusterConfig(src *ClusterConfig) *ClusterConfig {
	dst := NewClusterConfig()
	srcSnap := *src.snap.Load() // value copy
	dst.snap.Store(&srcSnap)
	return dst
}
