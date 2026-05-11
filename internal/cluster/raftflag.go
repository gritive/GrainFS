package cluster

import (
	"log/slog"
	"os"
	"strings"
	"sync"
)

// raftV2FlagEnv is the environment variable that opts packages into raft v2.
// Value is a comma-separated list of package names.
//
// Supported values: "cluster", "serveruntime", "all".
//
// Example: GRAINFS_RAFT_V2=cluster
const raftV2FlagEnv = "GRAINFS_RAFT_V2"

// validRaftV2Pkgs is the set of recognised package names.
var validRaftV2Pkgs = map[string]bool{
	"cluster":      true,
	"serveruntime": true,
	"all":          true,
}

// raftV2Flag is the parsed flag set, populated once at init time.
var (
	raftV2FlagOnce sync.Once
	raftV2Enabled  map[string]bool
)

// ParseRaftV2Flag parses env (the value of GRAINFS_RAFT_V2) into a set of
// package names that have v2 enabled. Unknown values are logged as warnings
// and ignored. "all" expands to every supported package name.
func ParseRaftV2Flag(env string) map[string]bool {
	enabled := make(map[string]bool)
	if env == "" {
		return enabled
	}
	for _, part := range strings.Split(env, ",") {
		pkg := strings.TrimSpace(part)
		if pkg == "" {
			continue
		}
		if !validRaftV2Pkgs[pkg] {
			slog.Warn("GRAINFS_RAFT_V2: unknown package name ignored", "pkg", pkg)
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
