// Package ecspike is a 48h de-risk spike for Phase 18 Cluster EC.
//
// Goal: prove that 4+2 Reed-Solomon shards distributed across 6 grainfs nodes
// survive a single-node failure end-to-end.
//
// THIS IS THROWAWAY CODE. Do not import from production packages. The package
// uses klauspost/reedsolomon directly to keep internal/erasure untouched.
//
// Design: ~/.gstack/projects/gritive-grains/whitekid-master-design-20260421-024627.md
package ecspike

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/klauspost/reedsolomon"
)

// Config describes an ecspike cluster: a set of grainfs S3 endpoints that can
// independently store shard objects.
type Config struct {
	Nodes    []string // S3 endpoint URLs (http://host:port), one per node
	DataK    int      // data shard count (e.g. 4)
	ParityM  int      // parity shard count (e.g. 2)
	Bucket   string   // shared bucket name; assumed to exist on all nodes
	S3Client func(endpoint string) *s3.Client
}

// numShards returns K+M.
func (c *Config) numShards() int { return c.DataK + c.ParityM }

// Placement returns which node index (0..len(Nodes)-1) stores shard shardIdx for key.
// Uses FNV32(key) + shardIdx mod N so every key's shards land on N distinct nodes
// when shardCount == nodeCount.
func (c *Config) Placement(key string, shardIdx int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return (int(h.Sum32()) + shardIdx) % len(c.Nodes)
}

// shardKey returns the S3 object key for a given logical key + shard index.
func shardKey(key string, shardIdx int) string {
	return key + "/_ecspike_shard_" + strconv.Itoa(shardIdx)
}

// header holds the minimal metadata we need to reconstruct: original data size.
// Prefixed to each shard object so any single shard can drive reconstruction.
const headerSize = 8

func encodeHeader(origSize int64) []byte {
	h := make([]byte, headerSize)
	binary.BigEndian.PutUint64(h, uint64(origSize))
	return h
}

func decodeHeader(data []byte) (int64, []byte, error) {
	if len(data) < headerSize {
		return 0, nil, fmt.Errorf("shard too small for header: %d bytes", len(data))
	}
	size := int64(binary.BigEndian.Uint64(data[:headerSize]))
	return size, data[headerSize:], nil
}

// Put splits data into K+M Reed-Solomon shards and writes each shard to its
// placed node via S3 PutObject. All uploads happen in parallel. Any failure
// aborts the operation (caller may retry). Returns nil on full success.
func Put(ctx context.Context, cfg *Config, key string, data []byte) error {
	enc, err := reedsolomon.New(cfg.DataK, cfg.ParityM)
	if err != nil {
		return fmt.Errorf("new encoder: %w", err)
	}

	shards, err := enc.Split(data)
	if err != nil {
		return fmt.Errorf("split: %w", err)
	}
	if err := enc.Encode(shards); err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	header := encodeHeader(int64(len(data)))
	g, gctx := errgroupWithContext(ctx)

	for i := range shards {
		i := i
		payload := make([]byte, 0, headerSize+len(shards[i]))
		payload = append(payload, header...)
		payload = append(payload, shards[i]...)

		nodeIdx := cfg.Placement(key, i)
		endpoint := cfg.Nodes[nodeIdx]
		objKey := shardKey(key, i)

		g.Go(func() error {
			client := cfg.S3Client(endpoint)
			_, err := client.PutObject(gctx, &s3.PutObjectInput{
				Bucket: aws.String(cfg.Bucket),
				Key:    aws.String(objKey),
				Body:   bytes.NewReader(payload),
			})
			if err != nil {
				return fmt.Errorf("put shard %d to node %d (%s): %w", i, nodeIdx, endpoint, err)
			}
			return nil
		})
	}
	return g.Wait()
}

// Get reconstructs data by reading shards from their placed nodes. Up to
// cfg.ParityM node failures are tolerated (as long as K shards are reachable).
// Returns the original bytes.
func Get(ctx context.Context, cfg *Config, key string) ([]byte, error) {
	n := cfg.numShards()
	shards := make([][]byte, n)
	origSize := int64(-1)
	var mu sync.Mutex

	g, gctx := errgroupWithContext(ctx)
	for i := range n {
		i := i
		nodeIdx := cfg.Placement(key, i)
		endpoint := cfg.Nodes[nodeIdx]
		objKey := shardKey(key, i)

		g.Go(func() error {
			client := cfg.S3Client(endpoint)
			out, err := client.GetObject(gctx, &s3.GetObjectInput{
				Bucket: aws.String(cfg.Bucket),
				Key:    aws.String(objKey),
			})
			if err != nil {
				// Missing shard is ok as long as we get K of them; leave nil.
				return nil
			}
			defer out.Body.Close()
			payload, err := io.ReadAll(out.Body)
			if err != nil {
				return nil
			}
			size, body, err := decodeHeader(payload)
			if err != nil {
				return nil
			}
			mu.Lock()
			if origSize < 0 {
				origSize = size
			}
			shards[i] = body
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	if origSize < 0 {
		return nil, fmt.Errorf("no shards readable for key %q", key)
	}

	enc, err := reedsolomon.New(cfg.DataK, cfg.ParityM)
	if err != nil {
		return nil, fmt.Errorf("new decoder: %w", err)
	}
	if err := enc.ReconstructData(shards); err != nil {
		return nil, fmt.Errorf("reconstruct: %w", err)
	}

	var buf bytes.Buffer
	if err := enc.Join(&buf, shards, int(origSize)); err != nil {
		return nil, fmt.Errorf("join: %w", err)
	}
	return buf.Bytes(), nil
}

// errgroup lite — avoid pulling golang.org/x/sync to keep spike deps minimal.
type eg struct {
	wg     sync.WaitGroup
	once   sync.Once
	err    error
	cancel context.CancelFunc
}

func errgroupWithContext(parent context.Context) (*eg, context.Context) {
	ctx, cancel := context.WithCancel(parent)
	return &eg{cancel: cancel}, ctx
}

func (g *eg) Go(f func() error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		if err := f(); err != nil {
			g.once.Do(func() {
				g.err = err
				g.cancel()
			})
		}
	}()
}

func (g *eg) Wait() error {
	g.wg.Wait()
	g.cancel()
	return g.err
}

// unused import guard to avoid refactor during spike
var _ = time.Time{}
