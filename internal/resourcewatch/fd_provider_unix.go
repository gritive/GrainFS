//go:build unix

package resourcewatch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type FDProviderOptions struct {
	Dir               string
	ClassificationCap int
}

type ProcessFDProvider struct {
	dir               string
	classificationCap int
}

func NewFDProvider(opts FDProviderOptions) *ProcessFDProvider {
	dir := opts.Dir
	if dir == "" {
		dir = "/proc/self/fd"
		if _, err := os.Stat(dir); err != nil {
			dir = "/dev/fd"
		}
	}
	if opts.ClassificationCap == 0 {
		opts.ClassificationCap = 512
	}
	return &ProcessFDProvider{dir: dir, classificationCap: opts.ClassificationCap}
}

func (p *ProcessFDProvider) Snapshot(ctx context.Context) (FDSnapshot, error) {
	select {
	case <-ctx.Done():
		return FDSnapshot{}, ctx.Err()
	default:
	}

	limit, err := currentFDLimit()
	if err != nil {
		return FDSnapshot{}, err
	}
	names, readDir, err := readFDNames(p.dir, limit)
	if err != nil {
		return FDSnapshot{}, err
	}
	categories := make(map[FDCategory]int)
	for i, name := range names {
		if i >= p.classificationCap {
			break
		}
		target, err := os.Readlink(filepath.Join(readDir, name))
		if err != nil {
			categories[FDCategoryUnknown]++
			continue
		}
		categories[classifyFDTarget(target)]++
	}

	return FDSnapshot{
		Open:        len(names),
		Limit:       limit,
		Categories:  categories,
		CollectedAt: time.Now(),
	}, nil
}

func readFDNames(dir string, limit int) ([]string, string, error) {
	entries, err := os.ReadDir(dir)
	if err == nil {
		return dirEntryNames(entries), dir, nil
	}
	dotDir := filepath.Join(dir, ".")
	entries, dotErr := os.ReadDir(dotDir)
	if dotErr == nil {
		return dirEntryNames(entries), dotDir, nil
	}
	if names := scanFDNames(dir, limit); len(names) > 0 {
		return names, dir, nil
	}
	return nil, dir, err
}

func dirEntryNames(entries []os.DirEntry) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func scanFDNames(dir string, limit int) []string {
	if limit > 1<<20 {
		limit = 1 << 20
	}
	names := make([]string, 0, 64)
	for fd := 0; fd < limit; fd++ {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil {
			continue
		}
		name := fmt.Sprintf("%d", fd)
		names = append(names, name)
	}
	return names
}

func currentFDLimit() (int, error) {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		return 0, err
	}
	return int(lim.Cur), nil
}

func classifyFDTarget(target string) FDCategory {
	lower := strings.ToLower(target)
	switch {
	case strings.Contains(lower, "socket:") || strings.Contains(lower, "sock"):
		return FDCategorySocket
	case strings.Contains(lower, ".sst") || strings.Contains(lower, "manifest") || strings.Contains(lower, "badger"):
		return FDCategoryBadger
	case strings.Contains(lower, "receipt") || strings.Contains(lower, "event"):
		return FDCategoryReceiptOrEventStore
	case strings.Contains(lower, "/nfs") || strings.Contains(lower, "nfs-") || strings.Contains(lower, "nfs_") || strings.Contains(lower, "session"):
		return FDCategoryNFSSession
	case strings.Contains(lower, "/") || strings.Contains(lower, ".log") || strings.Contains(lower, ".db"):
		return FDCategoryRegularFile
	default:
		return FDCategoryUnknown
	}
}
