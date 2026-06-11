//go:build !race

package transport

// raceDetectorEnabled is false in normal builds; the alloc-measurement
// memory-flat test runs only here (race instrumentation inflates TotalAlloc,
// making the streaming-vs-buffering threshold meaningless under -race).
const raceDetectorEnabled = false
