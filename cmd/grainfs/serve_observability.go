package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// logStartupConfigSnapshot records resolved serve flags and logs changes
// across restarts. Secret-bearing flags are redacted before logging or disk IO.
func logStartupConfigSnapshot(cmd *cobra.Command, addr, dataDir, nodeID, raftAddr string, peers []string) {
	snapshot := map[string]any{
		"addr":      addr,
		"data_dir":  dataDir,
		"node_id":   nodeID,
		"raft_addr": raftAddr,
		"peers":     peers,
	}
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		switch f.Name {
		case "secret-key", "cluster-key", "alert-webhook-secret", "heal-receipt-psk":
			if f.Value.String() != "" {
				snapshot[f.Name] = "<redacted>"
			}
			return
		}
		snapshot[f.Name] = f.Value.String()
	})

	log.Debug().Interface("flags", snapshot).Msg("startup config snapshot")

	snapPath := filepath.Join(dataDir, ".last-config.json")
	if prev, err := os.ReadFile(snapPath); err == nil {
		var prevMap map[string]any
		if err := json.Unmarshal(prev, &prevMap); err == nil {
			diff := diffSnapshots(prevMap, snapshot)
			if len(diff) > 0 {
				log.Info().Interface("changed", diff).Msg("config changed since last startup")
			}
		}
	}

	if data, err := json.MarshalIndent(snapshot, "", "  "); err == nil {
		if err := os.WriteFile(snapPath, data, 0o600); err != nil {
			log.Debug().Err(err).Str("path", snapPath).Msg("could not persist startup config snapshot")
		}
	}
}

func diffSnapshots(prev, curr map[string]any) map[string]map[string]any {
	out := make(map[string]map[string]any)
	keys := make(map[string]struct{}, len(prev)+len(curr))
	for k := range prev {
		keys[k] = struct{}{}
	}
	for k := range curr {
		keys[k] = struct{}{}
	}
	sortedKeys := make([]string, 0, len(keys))
	for k := range keys {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
	for _, k := range sortedKeys {
		pv, pok := prev[k]
		cv, cok := curr[k]
		if !pok || !cok || fmt.Sprintf("%v", pv) != fmt.Sprintf("%v", cv) {
			out[k] = map[string]any{"prev": pv, "curr": cv}
		}
	}
	return out
}
