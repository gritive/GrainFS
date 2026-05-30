package encrypt

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
)

// FactorSource yields raw machine-binding factors used to derive the
// environmental encryption key (EEK). It is an interface so tests can inject a
// deterministic set of factors without depending on real hardware.
type FactorSource interface {
	// Factors returns zero or more machine-binding strings. Empty strings are
	// tolerated by callers (they are dropped during canonicalization).
	Factors() ([]string, error)
}

// CanonicalIKM produces a deterministic, order-independent input keying material
// from the given factors. Empty/whitespace factors are dropped. It returns an
// error if no non-empty factor remains, so the EEK is never derived from empty
// keying material.
//
// Framing is length-prefixed (uint32 big-endian length + bytes per factor) so
// that distinct factor sets cannot collide via naive concatenation.
func CanonicalIKM(factors []string) ([]byte, error) {
	cleaned := make([]string, 0, len(factors))
	for _, f := range factors {
		f = strings.TrimSpace(f)
		if f != "" {
			cleaned = append(cleaned, f)
		}
	}
	if len(cleaned) == 0 {
		return nil, fmt.Errorf("machine factors: no resolvable factor")
	}
	sort.Strings(cleaned)

	var buf []byte
	var lenBuf [4]byte
	for _, f := range cleaned {
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(f)))
		buf = append(buf, lenBuf[:]...)
		buf = append(buf, f...)
	}
	return buf, nil
}

// DefaultFactorSource resolves machine-binding factors from the local host:
// machine-id (where available), sorted NIC MAC addresses, and a best-effort CPU
// brand string. Any factor that cannot be resolved is simply omitted.
type DefaultFactorSource struct{}

// Factors implements FactorSource.
func (DefaultFactorSource) Factors() ([]string, error) {
	var out []string
	if id := machineID(); id != "" {
		out = append(out, "machine-id:"+id)
	}
	for _, mac := range macAddrs() {
		out = append(out, "mac:"+mac)
	}
	if cpu := cpuBrand(); cpu != "" {
		out = append(out, "cpu:"+cpu)
	}
	return out, nil
}

// machineID reads a stable per-machine identifier. Linux exposes one directly;
// macOS is read via ioreg. Returns "" if none is available.
func machineID() string {
	for _, p := range []string{"/etc/machine-id", "/var/lib/dbus/machine-id"} {
		if b, err := os.ReadFile(p); err == nil {
			if s := strings.TrimSpace(string(b)); s != "" {
				return s
			}
		}
	}
	if runtime.GOOS == "darwin" {
		if out, err := exec.Command("ioreg", "-rd1", "-c", "IOPlatformExpertDevice").Output(); err == nil {
			for _, line := range strings.Split(string(out), "\n") {
				if !strings.Contains(line, "IOPlatformUUID") {
					continue
				}
				if i := strings.Index(line, "= \""); i >= 0 {
					rest := line[i+3:]
					if j := strings.Index(rest, "\""); j >= 0 {
						return rest[:j]
					}
				}
			}
		}
	}
	return ""
}

// macAddrs returns sorted, deduped, lowercased MAC addresses of non-loopback
// interfaces that have hardware addresses.
func macAddrs() []string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	seen := map[string]struct{}{}
	var macs []string
	for _, ifi := range ifaces {
		if ifi.Flags&net.FlagLoopback != 0 {
			continue
		}
		hw := strings.ToLower(ifi.HardwareAddr.String())
		if hw == "" {
			continue
		}
		if _, ok := seen[hw]; ok {
			continue
		}
		seen[hw] = struct{}{}
		macs = append(macs, hw)
	}
	sort.Strings(macs)
	return macs
}

// cpuBrand returns a best-effort CPU brand string, or "".
func cpuBrand() string {
	switch runtime.GOOS {
	case "linux":
		f, err := os.Open("/proc/cpuinfo")
		if err != nil {
			return ""
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "model name") {
				if i := strings.Index(line, ":"); i >= 0 {
					return strings.TrimSpace(line[i+1:])
				}
			}
		}
	case "darwin":
		if out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
			return strings.TrimSpace(string(out))
		}
	}
	return ""
}
