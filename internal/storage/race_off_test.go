//go:build !race

package storage

// raceDetectorEnabled is false in normal builds; the alloc-measurement tests
// (TotalAlloc-based buffer right-sizing / pooling guards) run only here — race
// instrumentation inflates TotalAlloc, making the byte thresholds meaningless
// under -race.
const raceDetectorEnabled = false
