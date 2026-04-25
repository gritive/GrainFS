// GrainFS S3 API Benchmark Suite
// Run: k6 run benchmarks/s3_bench.js --env BASE_URL=http://localhost:9000
//
// Measures: PUT/GET/DELETE throughput, latency (P50/P99), concurrency handling

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
// Local randomString — avoids network fetch of jslib on first run (cold-start VU delays).
function randomString(len) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let s = '';
  for (let i = 0; i < len; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return s;
}

const BASE = __ENV.BASE_URL || 'http://localhost:9000';
// Fixed name so all VUs share the same bucket (Date.now() differs per VU).
const BUCKET = __ENV.BUCKET || 'grainfs-bench';

// Custom metrics
const putLatency = new Trend('grainfs_put_latency', true);
const getLatency = new Trend('grainfs_get_latency', true);
const deleteLatency = new Trend('grainfs_delete_latency', true);
const putOps = new Counter('grainfs_put_ops');
const getOps = new Counter('grainfs_get_ops');
const deleteOps = new Counter('grainfs_delete_ops');

export const options = {
  scenarios: {
    // Warm-up: create bucket and seed objects
    setup_bucket: {
      executor: 'shared-iterations',
      vus: 1,
      iterations: 1,
      exec: 'setupBucket',
      startTime: '0s',
    },
    // Warm-up: ramp to 5 VUs, discard results
    warm_up: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: '5s', target: 5 },
        { duration: '5s', target: 5 },
      ],
      exec: 'mixedWorkload',
      startTime: '2s',
      gracefulStop: '5s',
    },
    // Main benchmark: mixed workload
    mixed_workload: {
      executor: 'ramping-vus',
      startVUs: 5,
      stages: [
        { duration: '10s', target: 10 },
        { duration: '20s', target: 10 },
        { duration: '5s', target: 0 },
      ],
      exec: 'mixedWorkload',
      startTime: '14s',
    },
  },
  thresholds: {
    'grainfs_put_latency': ['p(99)<500'],
    'grainfs_get_latency': ['p(99)<200'],
    http_req_failed: ['rate<0.01'],
  },
};

export function setupBucket() {
  const res = http.put(`${BASE}/${BUCKET}`);
  check(res, { 'bucket created': (r) => r.status === 200 });
}

export function mixedWorkload() {
  const key = `obj-${__VU}-${__ITER}-${randomString(6)}`;
  const sizes = [1024, 4096, 16384, 65536]; // 1KB, 4KB, 16KB, 64KB
  const size = sizes[Math.floor(Math.random() * sizes.length)];
  const payload = randomString(size);

  // PUT
  const putRes = http.put(`${BASE}/${BUCKET}/${key}`, payload, {
    headers: { 'Content-Type': 'application/octet-stream' },
  });
  putLatency.add(putRes.timings.duration);
  putOps.add(1);
  check(putRes, { 'put ok': (r) => r.status === 200 });

  // GET
  const getRes = http.get(`${BASE}/${BUCKET}/${key}`);
  getLatency.add(getRes.timings.duration);
  getOps.add(1);
  check(getRes, { 'get ok': (r) => r.status === 200 });

  // DELETE (50% of the time to build up objects)
  if (Math.random() < 0.5) {
    const delRes = http.del(`${BASE}/${BUCKET}/${key}`);
    deleteLatency.add(delRes.timings.duration);
    deleteOps.add(1);
    check(delRes, { 'delete ok': (r) => r.status === 204 });
  }

  sleep(0.01);
}

export function handleSummary(data) {
  const report = {
    timestamp: new Date().toISOString(),
    summary: {
      total_requests: data.metrics.http_reqs ? data.metrics.http_reqs.values.count : 0,
      failed_requests: data.metrics.http_req_failed ? data.metrics.http_req_failed.values.passes : 0,
    },
    put: extractMetric(data, 'grainfs_put_latency', 'grainfs_put_ops'),
    get: extractMetric(data, 'grainfs_get_latency', 'grainfs_get_ops'),
    delete: extractMetric(data, 'grainfs_delete_latency', 'grainfs_delete_ops'),
  };

  return {
    stdout: formatReport(report),
    'benchmarks/report.json': JSON.stringify(report, null, 2),
  };
}

function extractMetric(data, latencyKey, opsKey) {
  const latency = data.metrics[latencyKey];
  const ops = data.metrics[opsKey];
  if (!latency) return { ops: 0 };
  return {
    ops: ops ? ops.values.count : 0,
    p50_ms: latency.values['p(50)'].toFixed(2),
    p99_ms: latency.values['p(99)'].toFixed(2),
    avg_ms: latency.values.avg.toFixed(2),
    min_ms: latency.values.min.toFixed(2),
    max_ms: latency.values.max.toFixed(2),
  };
}

function formatReport(r) {
  let out = '\n=== GrainFS Benchmark Report ===\n';
  out += `Timestamp: ${r.timestamp}\n`;
  out += `Total Requests: ${r.summary.total_requests}\n`;
  out += `Failed: ${r.summary.failed_requests}\n\n`;

  for (const op of ['put', 'get', 'delete']) {
    const m = r[op];
    if (!m || !m.ops) continue;
    out += `${op.toUpperCase()}: ${m.ops} ops | P50: ${m.p50_ms}ms | P99: ${m.p99_ms}ms | Avg: ${m.avg_ms}ms\n`;
  }

  out += '\nFull report: benchmarks/report.json\n';
  return out;
}
