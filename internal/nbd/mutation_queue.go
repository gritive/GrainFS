package nbd

import (
	"context"
	"math"
	"sync"
)

type mutationQueue struct {
	blockSize uint64
	nextSeq   uint64
	entries   []mutationEntry
}

type mutationEntry struct {
	seq        uint64
	firstBlock uint64
	lastBlock  uint64
	fns        []func() error
}

func newMutationQueue(blockSize uint64) *mutationQueue {
	if blockSize == 0 {
		blockSize = 1
	}
	return &mutationQueue{blockSize: blockSize}
}

func (q *mutationQueue) AppendRange(offset uint64, length uint32, fns []func() error) {
	if q == nil || len(fns) == 0 {
		return
	}

	firstBlock, lastBlock := q.blockRange(offset, length)
	copiedFns := append([]func() error(nil), fns...)

	q.nextSeq++
	q.entries = append(q.entries, mutationEntry{
		seq:        q.nextSeq,
		firstBlock: firstBlock,
		lastBlock:  lastBlock,
		fns:        copiedFns,
	})
}

func (q *mutationQueue) Flush(ctx context.Context) error {
	if q == nil || len(q.entries) == 0 {
		return nil
	}

	entries := q.entries
	q.entries = nil

	return flushMutationEntries(ctx, entries)
}

func (q *mutationQueue) Drain() {
	if q == nil || len(q.entries) == 0 {
		return
	}

	entries := q.entries
	q.entries = nil

	for _, entry := range entries {
		for _, fn := range entry.fns {
			_ = fn()
		}
	}
}

func (q *mutationQueue) Len() int {
	if q == nil {
		return 0
	}
	return len(q.entries)
}

func (q *mutationQueue) blockRange(offset uint64, length uint32) (uint64, uint64) {
	firstBlock := offset / q.blockSize
	if length == 0 {
		return firstBlock, firstBlock
	}

	lastByte := offset + uint64(length) - 1
	if lastByte < offset {
		lastByte = math.MaxUint64
	}

	return firstBlock, lastByte / q.blockSize
}

func flushMutationEntries(ctx context.Context, entries []mutationEntry) error {
	remaining := entries
	for len(remaining) > 0 {
		wave, next := buildMutationWave(remaining)
		if err := runMutationWave(ctx, wave); err != nil {
			return err
		}
		remaining = next
	}
	return nil
}

func buildMutationWave(entries []mutationEntry) ([]mutationEntry, []mutationEntry) {
	wave := make([]mutationEntry, 0, len(entries))
	next := make([]mutationEntry, 0, len(entries))

	for _, entry := range entries {
		if overlapsAnyMutation(entry, wave) || overlapsAnyMutation(entry, next) {
			next = append(next, entry)
			continue
		}
		wave = append(wave, entry)
	}

	return wave, next
}

func overlapsAnyMutation(entry mutationEntry, entries []mutationEntry) bool {
	for _, existing := range entries {
		if rangesOverlap(entry.firstBlock, entry.lastBlock, existing.firstBlock, existing.lastBlock) {
			return true
		}
	}
	return false
}

func rangesOverlap(aFirst, aLast, bFirst, bLast uint64) bool {
	return aFirst <= bLast && bFirst <= aLast
}

func runMutationWave(ctx context.Context, wave []mutationEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(wave) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	errs := make([]error, len(wave))

	for i, entry := range wave {
		i := i
		entry := entry
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, fn := range entry.fns {
				if err := ctx.Err(); err != nil {
					errs[i] = err
					return
				}
				if err := fn(); err != nil {
					errs[i] = err
					return
				}
			}
		}()
	}

	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
