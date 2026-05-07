package volumeadmin

import (
	"fmt"
	"strconv"
	"strings"
)

// FormatBytes returns "1.0 GiB", "500.0 MiB" etc using IEC binary units.
// raw=true returns the integer string. n<0 returns "n/a".
func FormatBytes(n int64, raw bool) string {
	if raw {
		return strconv.FormatInt(n, 10)
	}
	if n < 0 {
		return "n/a"
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	f := float64(n)
	i := 0
	for f >= 1024 && i < len(units)-1 {
		f /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%d %s", n, units[i])
	}
	return fmt.Sprintf("%.1f %s", f, units[i])
}

// ParseSize parses a size string and returns bytes. Suffixes are
// case-insensitive and follow Kubernetes convention: binary units use the
// "i" infix (Ki/Mi/Gi/Ti/Pi or KiB/MiB/GiB/TiB/PiB) and equal 1024^n; SI
// decimal units use the "B" suffix (KB/MB/GB/TB/PB) and equal 1000^n. A
// bare "B" means bytes; no suffix also means bytes. Bare K/M/G/T/P are
// rejected as ambiguous — callers must pick the binary or decimal form.
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty")
	}
	upper := strings.ToUpper(s)

	const (
		ki = int64(1) << 10
		mi = int64(1) << 20
		gi = int64(1) << 30
		ti = int64(1) << 40
		pi = int64(1) << 50

		kSI = int64(1_000)
		mSI = int64(1_000_000)
		gSI = int64(1_000_000_000)
		tSI = int64(1_000_000_000_000)
		pSI = int64(1_000_000_000_000_000)
	)

	units := []struct {
		suf       string
		m         int64
		ambiguous bool
	}{
		// binary IEC, longest first (KiB before Ki before K).
		{"PIB", pi, false}, {"TIB", ti, false}, {"GIB", gi, false}, {"MIB", mi, false}, {"KIB", ki, false},
		{"PI", pi, false}, {"TI", ti, false}, {"GI", gi, false}, {"MI", mi, false}, {"KI", ki, false},
		// SI decimal — must come before bare-letter ambiguous match
		// because "1KB" must hit KB, not K.
		{"PB", pSI, false}, {"TB", tSI, false}, {"GB", gSI, false}, {"MB", mSI, false}, {"KB", kSI, false},
		// bare letters are ambiguous (1024 vs 1000).
		{"P", 0, true}, {"T", 0, true}, {"G", 0, true}, {"M", 0, true}, {"K", 0, true},
		// "B" alone means bytes.
		{"B", 1, false},
	}

	mult := int64(1)
	for _, u := range units {
		if !strings.HasSuffix(upper, u.suf) {
			continue
		}
		if u.ambiguous {
			return 0, fmt.Errorf("unit %q is ambiguous; use %siB (1024) or %sB (1000)", u.suf, u.suf, u.suf)
		}
		s = s[:len(s)-len(u.suf)]
		mult = u.m
		break
	}

	s = strings.TrimSpace(s)
	if strings.Contains(s, ".") {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, fmt.Errorf("parse size: %w", err)
		}
		return int64(f * float64(mult)), nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse size: %w", err)
	}
	return n * mult, nil
}

// FormatVolumeHealth returns "ok" for an empty health string, otherwise the
// health verbatim. Pulled out of the runners so callers can apply it without
// duplicating the empty-string check.
func FormatVolumeHealth(health string) string {
	if health == "" {
		return "ok"
	}
	return health
}

// Capitalize returns s with its first byte upper-cased. Used by scrub status
// output ("Done. ..." vs "done. ...").
func Capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
