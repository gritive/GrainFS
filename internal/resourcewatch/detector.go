package resourcewatch

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type Detector struct {
	cfg           DetectorConfig
	samples       []FDSnapshot
	lastLevel     FDLevel
	recoverySince time.Time
}

func NewDetector(cfg DetectorConfig) *Detector {
	if cfg.WarnRatio == 0 {
		cfg.WarnRatio = 0.80
	}
	if cfg.CriticalRatio == 0 {
		cfg.CriticalRatio = 0.90
	}
	if cfg.ETAWindow == 0 {
		cfg.ETAWindow = 30 * time.Minute
	}
	if cfg.RecoveryWindow == 0 {
		cfg.RecoveryWindow = time.Minute
	}
	if cfg.MinSamples == 0 {
		cfg.MinSamples = 2
	}
	if cfg.MaxSamples == 0 {
		cfg.MaxSamples = 20
	}
	if cfg.ClassificationCap == 0 {
		cfg.ClassificationCap = 512
	}
	return &Detector{cfg: cfg, lastLevel: FDLevelOK}
}

func (d *Detector) Observe(sample FDSnapshot) (*Decision, error) {
	if sample.Limit <= 0 || sample.Open < 0 || sample.Open > sample.Limit || sample.CollectedAt.IsZero() {
		return nil, ErrInvalidFDSample
	}

	d.samples = append(d.samples, sample)
	if len(d.samples) > d.cfg.MaxSamples {
		d.samples = append([]FDSnapshot(nil), d.samples[len(d.samples)-d.cfg.MaxSamples:]...)
	}

	ratio := float64(sample.Open) / float64(sample.Limit)
	level := d.currentLevel(ratio)
	if level == FDLevelOK && d.lastLevel != FDLevelOK {
		if d.recoverySince.IsZero() {
			d.recoverySince = sample.CollectedAt
			return nil, nil
		}
		if sample.CollectedAt.Sub(d.recoverySince) < d.cfg.RecoveryWindow {
			return nil, nil
		}
		d.lastLevel = FDLevelOK
		return d.decision(sample, FDLevelOK, "recovered", 0), nil
	}

	if level != FDLevelOK {
		d.recoverySince = time.Time{}
		if level != d.lastLevel {
			d.lastLevel = level
			return d.decision(sample, level, string(level), 0), nil
		}
		return nil, nil
	}

	eta, threshold := d.projectedETA(sample)
	if eta > 0 && eta <= d.cfg.ETAWindow && d.lastLevel == FDLevelOK {
		d.lastLevel = FDLevelWarn
		return d.decision(sample, FDLevelWarn, threshold, eta), nil
	}
	return nil, nil
}

func (d *Detector) currentLevel(ratio float64) FDLevel {
	switch {
	case ratio >= d.cfg.CriticalRatio:
		return FDLevelCritical
	case ratio >= d.cfg.WarnRatio:
		return FDLevelWarn
	default:
		return FDLevelOK
	}
}

func (d *Detector) projectedETA(sample FDSnapshot) (time.Duration, string) {
	if len(d.samples) < d.cfg.MinSamples {
		return 0, ""
	}

	first := d.samples[0]
	elapsed := sample.CollectedAt.Sub(first.CollectedAt)
	if elapsed <= 0 || sample.Open <= first.Open {
		return 0, ""
	}

	target := d.cfg.WarnRatio
	threshold := "warn"
	if float64(sample.Open)/float64(sample.Limit) >= d.cfg.WarnRatio {
		target = d.cfg.CriticalRatio
		threshold = "critical"
	}
	targetOpen := target * float64(sample.Limit)
	remaining := targetOpen - float64(sample.Open)
	if remaining <= 0 {
		return 0, ""
	}

	slope := float64(sample.Open-first.Open) / elapsed.Seconds()
	if slope <= 0 {
		return 0, ""
	}
	return time.Duration(remaining/slope) * time.Second, threshold
}

func (d *Detector) decision(sample FDSnapshot, level FDLevel, threshold string, eta time.Duration) *Decision {
	ratio := float64(sample.Open) / float64(sample.Limit)
	categories := map[FDCategory]int{}
	for category, count := range sample.Categories {
		categories[category] = count
	}

	message := fmt.Sprintf("FD usage %.1f%% (%d/%d)", ratio*100, sample.Open, sample.Limit)
	switch {
	case level == FDLevelOK:
		message = fmt.Sprintf("%s recovered below warning threshold", message)
	case eta > 0:
		message = fmt.Sprintf("%s projected to cross %s threshold in %s", message, threshold, eta.Round(time.Second))
	default:
		message = fmt.Sprintf("%s crossed %s threshold", message, threshold)
	}
	if len(categories) > 0 {
		message = fmt.Sprintf("%s; top categories: %s", message, formatCategories(categories))
	}

	return &Decision{
		Level:      level,
		Threshold:  threshold,
		Ratio:      ratio,
		ETA:        eta,
		Message:    message,
		Snapshot:   sample,
		Categories: categories,
	}
}

func formatCategories(categories map[FDCategory]int) string {
	type pair struct {
		category FDCategory
		count    int
	}
	pairs := make([]pair, 0, len(categories))
	for category, count := range categories {
		pairs = append(pairs, pair{category: category, count: count})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].count == pairs[j].count {
			return pairs[i].category < pairs[j].category
		}
		return pairs[i].count > pairs[j].count
	})
	parts := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		parts = append(parts, fmt.Sprintf("%s=%d", pair.category, pair.count))
	}
	return strings.Join(parts, ", ")
}
