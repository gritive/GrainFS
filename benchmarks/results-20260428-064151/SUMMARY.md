# GrainFS vs MinIO 벤치마크 결과

- **실행 시각:** 2026년  4월 28일 화요일 06시 53분 37초 KST
- **소요 시간:** 706s
- **노드 수:** 4 (cluster mode)
- **EC 설정:** grainfs 2+2, minio 자동 (4-drive면 2+2)
- **워크로드:** mixed PUT→GET→50% DELETE (s3_bench.js)
- **VUs:** 20, Duration: 30s
- **OS:** Darwin 25.4.0 arm64
- **Git:** 5148b69 (master)

## SINGLE mode

### PUT latency / throughput

| size | system | ops | p50 (ms) | p99 (ms) | avg (ms) | failed |
|------|--------|-----|----------|----------|----------|--------|
| 4KB | grainfs | 18579 | 6.10 | 27.45 | 7.34 | 0 |
| 4KB | minio | 18653 | 1.52 | 27.48 | 3.48 | 0 |
| 64KB | grainfs | 173 | 2.96 | 8.53 | 3.15 | 1 |
| 64KB | minio | 171 | 2.00 | 13.20 | 2.63 | 1 |
| 1024KB | grainfs | 0 | 0.00 | 0.00 | 0.00 | 1 |
| 1024KB | minio | 0 | 0.00 | 0.00 | 0.00 | 1 |

### GET latency / throughput

| size | system | ops | p50 (ms) | p99 (ms) | avg (ms) | failed |
|------|--------|-----|----------|----------|----------|--------|
| 4KB | grainfs | 18579 | 0.44 | 14.64 | 1.37 | 0 |
| 4KB | minio | 18653 | 0.77 | 19.05 | 1.90 | 0 |
| 64KB | grainfs | 173 | 1.49 | 6.90 | 1.94 | 1 |
| 64KB | minio | 171 | 1.36 | 5.49 | 1.85 | 1 |
| 1024KB | grainfs | 0 | 0.00 | 0.00 | 0.00 | 1 |
| 1024KB | minio | 0 | 0.00 | 0.00 | 0.00 | 1 |

### DELETE latency / throughput

| size | system | ops | p50 (ms) | p99 (ms) | avg (ms) | failed |
|------|--------|-----|----------|----------|----------|--------|
| 4KB | grainfs | 9312 | 5.15 | 21.92 | 6.32 | 0 |
| 4KB | minio | 9416 | 1.09 | 18.00 | 2.15 | 0 |
| 64KB | grainfs | 90 | 2.09 | 3.96 | 2.09 | 1 |
| 64KB | minio | 91 | 1.33 | 5.37 | 1.57 | 1 |
| 1024KB | grainfs | 0 | 0.00 | 0.00 | 0.00 | 1 |
| 1024KB | minio | 0 | 0.00 | 0.00 | 0.00 | 1 |

## CLUSTER mode

### PUT latency / throughput

| size | system | ops | p50 (ms) | p99 (ms) | avg (ms) | failed |
|------|--------|-----|----------|----------|----------|--------|
| 4KB | grainfs | 9787 | 11.34 | 248.70 | 47.67 | 188 |
| 4KB | minio | 9363 | 2.98 | 343.93 | 58.99 | 0 |
| 64KB | grainfs | 150 | 23.74 | 125.82 | 32.16 | 1 |
| 64KB | minio | 168 | 4.49 | 326.05 | 21.60 | 1 |
| 1024KB | grainfs | 0 | 0.00 | 0.00 | 0.00 | 1 |
| 1024KB | minio | 0 | 0.00 | 0.00 | 0.00 | 1 |

### GET latency / throughput

| size | system | ops | p50 (ms) | p99 (ms) | avg (ms) | failed |
|------|--------|-----|----------|----------|----------|--------|
| 4KB | grainfs | 9787 | 0.40 | 9.81 | 1.01 | 188 |
| 4KB | minio | 9363 | 0.91 | 6.14 | 1.27 | 0 |
| 64KB | grainfs | 150 | 2.71 | 9.16 | 3.08 | 1 |
| 64KB | minio | 168 | 2.48 | 8.54 | 3.22 | 1 |
| 1024KB | grainfs | 0 | 0.00 | 0.00 | 0.00 | 1 |
| 1024KB | minio | 0 | 0.00 | 0.00 | 0.00 | 1 |

### DELETE latency / throughput

| size | system | ops | p50 (ms) | p99 (ms) | avg (ms) | failed |
|------|--------|-----|----------|----------|----------|--------|
| 4KB | grainfs | 4946 | 4.71 | 66.80 | 6.17 | 188 |
| 4KB | minio | 4756 | 1.70 | 10.27 | 2.41 | 0 |
| 64KB | grainfs | 84 | 2.19 | 5.56 | 2.46 | 1 |
| 64KB | minio | 75 | 3.07 | 5.81 | 3.13 | 1 |
| 1024KB | grainfs | 0 | 0.00 | 0.00 | 0.00 | 1 |
| 1024KB | minio | 0 | 0.00 | 0.00 | 0.00 | 1 |

## 비교 비율 (grainfs/minio, 1.0보다 작으면 grains가 빠름)

### SINGLE

| size | put p50 | put p99 | get p50 | get p99 | put ops/s | get ops/s |
|------|---------|---------|---------|---------|-----------|-----------|
| 4KB | 4.01x | 1.00x | 0.57x | 0.77x | 1.00x | 1.00x |
| 64KB | 1.48x | 0.65x | 1.10x | 1.26x | 1.01x | 1.01x |
| 1024KB |  |  |  |  | — | — |

### CLUSTER

| size | put p50 | put p99 | get p50 | get p99 | put ops/s | get ops/s |
|------|---------|---------|---------|---------|-----------|-----------|
| 4KB | 3.81x | 0.72x | 0.44x | 1.60x | 1.05x | 1.05x |
| 64KB | 5.29x | 0.39x | 1.09x | 1.07x | 0.89x | 0.89x |
| 1024KB |  |  |  |  | — | — |

## grainfs pprof (cluster, 첫 size)

프로파일 위치: `benchmarks/results-20260428-064151/grainfs-cluster-pprof/`

분석:
```
go tool pprof -top -nodecount=15 benchmarks/results-20260428-064151/grainfs-cluster-pprof/cpu.pb.gz
go tool pprof -top -nodecount=15 benchmarks/results-20260428-064151/grainfs-cluster-pprof/heap_post.pb.gz
go tool pprof -http=:8080 benchmarks/results-20260428-064151/grainfs-cluster-pprof/cpu.pb.gz
```

