//go:build unix

package cluster

import "syscall"

func sysDiskStat(dir string) (usedPct float64, availBytes uint64) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return 0, 0
	}
	available := stat.Bavail * uint64(stat.Bsize)
	total := stat.Blocks * uint64(stat.Bsize)
	if total == 0 {
		return 0, 0
	}
	used := 100.0 - float64(available)*100.0/float64(total)
	return used, available
}
