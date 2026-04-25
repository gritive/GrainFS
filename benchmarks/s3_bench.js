// GrainFS S3 API Benchmark Suite
// Run (no auth):      k6 run benchmarks/s3_bench.js --env BASE_URL=http://localhost:9000
// Run (with auth):    k6 run benchmarks/s3_bench.js \
//                       --env BASE_URL=http://localhost:9000 \
//                       --env AWS_ACCESS_KEY=mykey \
//                       --env AWS_SECRET_KEY=mysecret
//
// Measures: PUT/GET/DELETE throughput, latency (P50/P99), concurrency handling

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import crypto from 'k6/crypto';

// ── AWS Signature V4 ────────────────────────────────────────────────────────

const AWS_ACCESS_KEY = __ENV.AWS_ACCESS_KEY || '';
const AWS_SECRET_KEY = __ENV.AWS_SECRET_KEY || '';
const AWS_REGION     = __ENV.AWS_REGION     || 'us-east-1';

// Decode a hex string to an ArrayBuffer (needed for binary HMAC key chaining).
function hexToBuffer(hex) {
  const buf = new Uint8Array(hex.length / 2);
  for (let i = 0; i < buf.length; i++) {
    buf[i] = parseInt(hex.substr(i * 2, 2), 16);
  }
  return buf.buffer;
}

// Derive the SigV4 signing key for a given date/region/service.
function signingKey(secretKey, dateStamp, region, service) {
  const kDate    = crypto.hmac('sha256', 'AWS4' + secretKey, dateStamp,    'hex');
  const kRegion  = crypto.hmac('sha256', hexToBuffer(kDate),  region,      'hex');
  const kService = crypto.hmac('sha256', hexToBuffer(kRegion), service,    'hex');
  const kSigning = crypto.hmac('sha256', hexToBuffer(kService), 'aws4_request', 'hex');
  return kSigning;
}

// Parse URL without the Web API URL class (not available in k6/goja runtime).
function parseURL(url) {
  // Strip scheme: http:// or https://
  let rest = url.replace(/^https?:\/\//, '');
  // Split host from path+query
  const slashIdx = rest.indexOf('/');
  let host, pathQuery;
  if (slashIdx === -1) {
    host      = rest;
    pathQuery = '/';
  } else {
    host      = rest.slice(0, slashIdx);
    pathQuery = rest.slice(slashIdx);
  }
  // Split path from query
  const qIdx = pathQuery.indexOf('?');
  const path  = qIdx === -1 ? pathQuery       : pathQuery.slice(0, qIdx);
  const query = qIdx === -1 ? ''              : pathQuery.slice(qIdx + 1);
  return { host, path: path || '/', query };
}

// Build Authorization + x-amz-date headers for an S3 request.
// Returns a new headers object (does not mutate the input).
function signRequest(method, url, body, headers) {
  if (!AWS_ACCESS_KEY || !AWS_SECRET_KEY) return headers;

  const parsed      = parseURL(url);
  const host        = parsed.host;
  const path        = parsed.path;
  const queryString = parsed.query;

  const now      = new Date();
  const amzDate  = now.toISOString().replace(/[-:]/g, '').replace(/\.\d{3}/, '');
  const dateStamp = amzDate.slice(0, 8);

  const payloadHash  = crypto.sha256(body || '', 'hex');

  // Canonical headers must be sorted alphabetically by header name.
  // Only include Content-Type when the caller is actually sending it.
  const contentType = headers['Content-Type'];
  let canonicalHeaders;
  let signedHeaders;
  if (contentType) {
    canonicalHeaders =
      'content-type:' + contentType + '\n' +
      'host:'          + host        + '\n' +
      'x-amz-content-sha256:' + payloadHash + '\n' +
      'x-amz-date:'    + amzDate     + '\n';
    signedHeaders = 'content-type;host;x-amz-content-sha256;x-amz-date';
  } else {
    canonicalHeaders =
      'host:'                  + host        + '\n' +
      'x-amz-content-sha256:' + payloadHash + '\n' +
      'x-amz-date:'           + amzDate     + '\n';
    signedHeaders = 'host;x-amz-content-sha256;x-amz-date';
  }

  const canonicalRequest = [
    method, path, queryString,
    canonicalHeaders, signedHeaders, payloadHash,
  ].join('\n');

  const credentialScope = `${dateStamp}/${AWS_REGION}/s3/aws4_request`;
  const stringToSign = [
    'AWS4-HMAC-SHA256',
    amzDate,
    credentialScope,
    crypto.sha256(canonicalRequest, 'hex'),
  ].join('\n');

  const sk        = signingKey(AWS_SECRET_KEY, dateStamp, AWS_REGION, 's3');
  const signature = crypto.hmac('sha256', hexToBuffer(sk), stringToSign, 'hex');

  const authorization =
    `AWS4-HMAC-SHA256 Credential=${AWS_ACCESS_KEY}/${credentialScope}` +
    `, SignedHeaders=${signedHeaders}` +
    `, Signature=${signature}`;

  return Object.assign({}, headers, {
    'Authorization':         authorization,
    'x-amz-date':            amzDate,
    'x-amz-content-sha256':  payloadHash,
  });
}

// ── Benchmark config ────────────────────────────────────────────────────────

// Local randomString — avoids external jslib network fetch on first run.
function randomString(len) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let s = '';
  for (let i = 0; i < len; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return s;
}

const BASE   = __ENV.BASE_URL || 'http://localhost:9000';
// Fixed name so all VUs share the same bucket (Date.now() differs per VU).
const BUCKET = __ENV.BUCKET || 'grainfs-bench';

// Custom metrics
const putLatency    = new Trend('grainfs_put_latency',    true);
const getLatency    = new Trend('grainfs_get_latency',    true);
const deleteLatency = new Trend('grainfs_delete_latency', true);
const putOps    = new Counter('grainfs_put_ops');
const getOps    = new Counter('grainfs_get_ops');
const deleteOps = new Counter('grainfs_delete_ops');

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(50)', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    // Warm-up: create bucket
    setup_bucket: {
      executor: 'shared-iterations',
      vus: 1,
      iterations: 1,
      exec: 'setupBucket',
      startTime: '0s',
    },
    // Warm-up: ramp to 5 VUs to heat server + OS page cache
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
    // Main benchmark
    mixed_workload: {
      executor: 'ramping-vus',
      startVUs: 5,
      stages: [
        { duration: '10s', target: 10 },
        { duration: '20s', target: 10 },
        { duration: '5s',  target: 0  },
      ],
      exec: 'mixedWorkload',
      startTime: '14s',
    },
  },
  thresholds: {
    'grainfs_put_latency': ['p(99)<500'],
    'grainfs_get_latency': ['p(99)<200'],
    http_req_failed:       ['rate<0.01'],
  },
};

export function setupBucket() {
  const url     = `${BASE}/${BUCKET}`;
  const headers = signRequest('PUT', url, '', { 'Content-Type': 'application/octet-stream' });
  const res     = http.put(url, null, { headers });
  check(res, { 'bucket created': (r) => r.status === 200 });
}

export function mixedWorkload() {
  const key     = `obj-${__VU}-${__ITER}-${randomString(6)}`;
  const sizes   = [1024, 4096, 16384, 65536]; // 1KB, 4KB, 16KB, 64KB
  const size    = sizes[Math.floor(Math.random() * sizes.length)];
  const payload = randomString(size);

  // PUT
  const putURL     = `${BASE}/${BUCKET}/${key}`;
  const putHeaders = signRequest('PUT', putURL, payload, { 'Content-Type': 'application/octet-stream' });
  const putRes     = http.put(putURL, payload, { headers: putHeaders });
  putLatency.add(putRes.timings.duration);
  putOps.add(1);
  check(putRes, { 'put ok': (r) => r.status === 200 });

  // GET
  const getURL     = `${BASE}/${BUCKET}/${key}`;
  const getHeaders = signRequest('GET', getURL, '', {});
  const getRes     = http.get(getURL, { headers: getHeaders });
  getLatency.add(getRes.timings.duration);
  getOps.add(1);
  check(getRes, { 'get ok': (r) => r.status === 200 });

  // DELETE (50% of the time to keep objects building up)
  if (Math.random() < 0.5) {
    const delURL     = `${BASE}/${BUCKET}/${key}`;
    const delHeaders = signRequest('DELETE', delURL, '', {});
    const delRes     = http.del(delURL, null, { headers: delHeaders });
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
      total_requests:  data.metrics.http_reqs      ? data.metrics.http_reqs.values.count          : 0,
      failed_requests: data.metrics.http_req_failed ? data.metrics.http_req_failed.values.passes   : 0,
    },
    put:    extractMetric(data, 'grainfs_put_latency',    'grainfs_put_ops'),
    get:    extractMetric(data, 'grainfs_get_latency',    'grainfs_get_ops'),
    delete: extractMetric(data, 'grainfs_delete_latency', 'grainfs_delete_ops'),
  };
  return {
    stdout:                    formatReport(report),
    'benchmarks/report.json':  JSON.stringify(report, null, 2),
  };
}

function extractMetric(data, latencyKey, opsKey) {
  const latency = data.metrics[latencyKey];
  const ops     = data.metrics[opsKey];
  if (!latency || latency.values['p(99)'] == null) return { ops: 0 };
  return {
    ops:    ops ? ops.values.count : 0,
    p50_ms: latency.values['p(50)'].toFixed(2),
    p99_ms: latency.values['p(99)'].toFixed(2),
    avg_ms: latency.values.avg.toFixed(2),
    min_ms: latency.values.min.toFixed(2),
    max_ms: latency.values.max.toFixed(2),
  };
}

function formatReport(r) {
  let out = '\n=== GrainFS Benchmark Report ===\n';
  out += `Timestamp:      ${r.timestamp}\n`;
  out += `Total Requests: ${r.summary.total_requests}\n`;
  out += `Failed:         ${r.summary.failed_requests}\n\n`;
  for (const op of ['put', 'get', 'delete']) {
    const m = r[op];
    if (!m || !m.ops) continue;
    out += `${op.toUpperCase().padEnd(6)}: ${String(m.ops).padStart(4)} ops | P50: ${m.p50_ms}ms | P99: ${m.p99_ms}ms | Avg: ${m.avg_ms}ms\n`;
  }
  out += '\nFull report: benchmarks/report.json\n';
  return out;
}
