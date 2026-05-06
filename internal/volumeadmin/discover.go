package volumeadmin

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// AutoDiscoverSocket walks the candidate sources for the admin socket and
// returns the first match as a "unix:<path>" URL. On no match returns a
// DX1 multi-line friendly error.
//
// Priority: dataFlag → $GRAINFS_DATA → ./grainfs.toml's data_dir.
func AutoDiscoverSocket(dataFlag string) (string, error) {
	type cand struct {
		path   string
		source string
	}
	var candidates []cand
	if dataFlag != "" {
		candidates = append(candidates, cand{filepath.Join(dataFlag, "admin.sock"), "from --data flag"})
	}
	if env := os.Getenv("GRAINFS_DATA"); env != "" {
		candidates = append(candidates, cand{filepath.Join(env, "admin.sock"), "from $GRAINFS_DATA"})
	}
	if dir := readGrainfsTomlDataDir(); dir != "" {
		candidates = append(candidates, cand{filepath.Join(dir, "admin.sock"), "from grainfs.toml"})
	}
	for _, c := range candidates {
		if info, err := os.Stat(c.path); err == nil && info.Mode()&os.ModeSocket != 0 {
			return "unix:" + c.path, nil
		}
	}
	tried := []string{}
	for _, c := range candidates {
		tried = append(tried, fmt.Sprintf("%s (%s)", c.path, c.source))
	}
	if len(tried) == 0 {
		tried = []string{"./grainfs.toml (not found), $GRAINFS_DATA (unset), --data (not given)"}
	}
	return "", fmt.Errorf("admin socket not found.\n  Tried: %s\n  Hint:  grainfs <command> --data /path/to/grainfs/data\n         또는 GRAINFS_DATA=/path 환경변수 설정\n         또는 ./grainfs.toml 에 data_dir = \"/path\" 추가",
		strings.Join(tried, "\n         "))
}

// readGrainfsTomlDataDir parses ./grainfs.toml looking for data_dir = "...".
// Returns empty string if file absent or key missing. Minimal parser since
// the use case is a single-line discovery hop.
func readGrainfsTomlDataDir() string {
	data, err := os.ReadFile("grainfs.toml")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data_dir") {
			continue
		}
		eq := strings.Index(line, "=")
		if eq < 0 {
			continue
		}
		val := strings.TrimSpace(line[eq+1:])
		val = strings.Trim(val, `"' `)
		if val != "" {
			return val
		}
	}
	return ""
}
