package cluster

import (
	"os"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
)

// raftV2FlagEnv is the environment variable that controls raft v2 selection.
// Value is a comma-separated list of package names.
//
// As of M5 PR 28b the phased flip is complete: leaving the env var unset
// selects v2 for both serveruntime and cluster. The cluster default-on was
// gated on wiring the per-group QUIC mux through a v2-aware dispatch
// (raft.GroupRaftQUICMux.RegisterV2 — see internal/raft/group_transport_quic.go).
// The remaining values are:
//
//   - unset (default):     serveruntime → v2. cluster → v2.
//   - "off":               v2 disabled everywhere (operator escape hatch;
//     PR 29 removes the flag entirely once v2 is validated in production).
//   - "all":               every supported package → v2 (now equivalent to
//     unset; preserved so old test harnesses keep working).
//   - "cluster", "serveruntime", or a comma-separated mix: only the named
//     packages get v2. Backward-compatible with PR 26/27 opt-in tests.
//
// Example (revert to v1 in an emergency): GRAINFS_RAFT_V2=off
const raftV2FlagEnv = "GRAINFS_RAFT_V2"

// raftV2FlagOff disables raft v2 for every package — the operator escape hatch.
const raftV2FlagOff = "off"

// validRaftV2Pkgs is the set of recognised package names.
var validRaftV2Pkgs = map[string]bool{
	"cluster":      true,
	"serveruntime": true,
	"all":          true,
}

// raftV2DefaultOnPkgs lists the packages whose default (env-unset) selection
// is v2. M5 PR 28 added serveruntime; M5 PR 28b adds cluster now that the
// per-group QUIC mux is wired through RegisterV2. PR 29 removes this list
// entirely.
var raftV2DefaultOnPkgs = []string{"serveruntime", "cluster"}

// raftV2Flag is the parsed flag set, populated once at init time.
var (
	raftV2FlagOnce sync.Once
	raftV2Enabled  map[string]bool
)

// defaultRaftV2Enabled returns the env-unset default: only the packages listed
// in raftV2DefaultOnPkgs map to true.
func defaultRaftV2Enabled() map[string]bool {
	enabled := make(map[string]bool, len(raftV2DefaultOnPkgs))
	for _, pkg := range raftV2DefaultOnPkgs {
		enabled[pkg] = true
	}
	return enabled
}

// ParseRaftV2Flag parses env (the value of GRAINFS_RAFT_V2) into a set of
// package names that have v2 enabled. Unknown values are logged as warnings
// and ignored. "all" expands to every supported package name. As of M5 PR 28,
// an empty env defaults to v2-on for serveruntime only (see
// raftV2DefaultOnPkgs); "off" is the escape hatch that disables v2 everywhere.
func ParseRaftV2Flag(env string) map[string]bool {
	if env == "" {
		return defaultRaftV2Enabled()
	}
	if strings.TrimSpace(env) == raftV2FlagOff {
		return make(map[string]bool)
	}
	enabled := make(map[string]bool)
	for _, part := range strings.Split(env, ",") {
		pkg := strings.TrimSpace(part)
		if pkg == "" {
			continue
		}
		if !validRaftV2Pkgs[pkg] {
			log.Warn().Str("pkg", pkg).Msg("GRAINFS_RAFT_V2: unknown package name ignored")
			continue
		}
		if pkg == "all" {
			for k := range validRaftV2Pkgs {
				if k != "all" {
					enabled[k] = true
				}
			}
		} else {
			enabled[pkg] = true
		}
	}
	return enabled
}

// IsV2Enabled reports whether raft v2 is opted in for the named package.
// The env var is read once at first call and cached for the process lifetime.
// Thread-safe.
func IsV2Enabled(pkg string) bool {
	raftV2FlagOnce.Do(func() {
		raftV2Enabled = ParseRaftV2Flag(os.Getenv(raftV2FlagEnv))
	})
	return raftV2Enabled[pkg]
}

// resetRaftV2FlagForTest resets the flag cache so tests can override the env
// var between test cases. Must only be called from test code.
func resetRaftV2FlagForTest() {
	raftV2FlagOnce = sync.Once{}
	raftV2Enabled = nil
}
