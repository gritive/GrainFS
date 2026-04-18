//go:build !unix

package cluster

func sysDiskStat(_ string) (usedPct float64, availBytes uint64) {
	return 0, 0
}
