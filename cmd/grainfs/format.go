package main

import (
	"fmt"
	"strconv"
	"strings"
)

// formatBytes returns "1.0 GiB", "500.0 MiB" etc using IEC binary units.
// raw=true returns the integer string. n<0 returns "n/a" (used for untracked
// allocation counters).
func formatBytes(n int64, raw bool) string {
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

// parseSize accepts "1G", "1Gi", "100M", "1024", etc and returns bytes.
// Suffixes (case-insensitive): K/Ki=1024, M/Mi=1024^2, G/Gi=1024^3,
// T/Ti=1024^4, P/Pi=1024^5. No suffix means raw bytes.
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty")
	}
	mult := int64(1)
	upper := strings.ToUpper(s)
	for _, u := range []struct {
		suf string
		m   int64
	}{
		{"PIB", 1 << 50}, {"PI", 1 << 50}, {"P", 1 << 50},
		{"TIB", 1 << 40}, {"TI", 1 << 40}, {"T", 1 << 40},
		{"GIB", 1 << 30}, {"GI", 1 << 30}, {"G", 1 << 30},
		{"MIB", 1 << 20}, {"MI", 1 << 20}, {"M", 1 << 20},
		{"KIB", 1 << 10}, {"KI", 1 << 10}, {"K", 1 << 10},
		{"B", 1},
	} {
		if strings.HasSuffix(upper, u.suf) {
			s = s[:len(s)-len(u.suf)]
			mult = u.m
			break
		}
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
