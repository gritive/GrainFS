// GrainFS S3 API Benchmark Suite
// Run: k6 run benchmarks/s3_bench.js \
//   --env BASE_URL=http://localhost:9000 \
//   --env ACCESS_KEY=test --env SECRET_KEY=testtest
//
// Measures: PUT/GET/DELETE throughput, latency (P50/P99), concurrency handling

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { randomString } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import crypto from 'k6/crypto';

const BASE = __ENV.BASE_URL || 'http://localhost:9000';
// BUCKET must come entirely from the environment so all VUs share the same value.
// k6 evaluates module-level code per-VU, so Date.now() would differ across VUs.
const BUCKET = __ENV.BUCKET || 'bench';
const ACCESS_KEY = __ENV.ACCESS_KEY || '';
const SECRET_KEY = __ENV.SECRET_KEY || '';
const REGION = 'us-east-1';
const SERVICE = 's3';
const MAX_VUS = parseInt(__ENV.MAX_VUS || '10');
const BENCH_DURATION = __ENV.DURATION || '30s';
const OBJECT_SIZE_KB = parseInt(__ENV.OBJECT_SIZE_KB || '64');

// Custom metrics
const putLatency = new Trend('grainfs_put_latency', true);
const getLatency = new Trend('grainfs_get_latency', true);
const deleteLatency = new Trend('grainfs_delete_latency', true);
const putOps = new Counter('grainfs_put_ops');
const getOps = new Counter('grainfs_get_ops');
const deleteOps = new Counter('grainfs_delete_ops');

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    setup_bucket: {
      executor: 'shared-iterations',
      vus: 1,
      iterations: 1,
      exec: 'setupBucket',
      startTime: '0s',
    },
    mixed_workload: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: '10s', target: MAX_VUS },
        { duration: BENCH_DURATION, target: MAX_VUS },
        { duration: '5s', target: 0 },
      ],
      exec: 'mixedWorkload',
      startTime: '2s',
    },
  },
  thresholds: {
    'grainfs_put_latency':    ['p(99)<500'],
    'grainfs_get_latency':    ['p(99)<200'],
    'grainfs_delete_latency': ['p(99)<1000'],
    http_req_failed: ['rate<0.05'],
  },
};

// ---- AWS SigV4 signing ----

function hexToBytes(hex) {
  const buf = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) {
    buf[i / 2] = parseInt(hex.substring(i, i + 2), 16);
  }
  return buf.buffer;
}

function derivedSigningKey(secretKey, dateStamp) {
  const kDate   = hexToBytes(crypto.hmac('sha256', 'AWS4' + secretKey, dateStamp, 'hex'));
  const kRegion = hexToBytes(crypto.hmac('sha256', kDate,   REGION,        'hex'));
  const kService= hexToBytes(crypto.hmac('sha256', kRegion, SERVICE,       'hex'));
  return              hexToBytes(crypto.hmac('sha256', kService,'aws4_request','hex'));
}

// parseURL parses http://host/path?query into {host, path, query}
function parseURL(url) {
  // Strip scheme
  let rest = url.replace(/^https?:\/\//, '');
  const slashIdx = rest.indexOf('/');
  let host, pathAndQuery;
  if (slashIdx === -1) {
    host = rest; pathAndQuery = '/';
  } else {
    host = rest.slice(0, slashIdx);
    pathAndQuery = rest.slice(slashIdx) || '/';
  }
  const qIdx = pathAndQuery.indexOf('?');
  const path  = qIdx === -1 ? pathAndQuery : pathAndQuery.slice(0, qIdx);
  const query = qIdx === -1 ? '' : pathAndQuery.slice(qIdx + 1);
  return { host, path, query };
}

// sign returns Authorization + required headers for the given request.
// addContentType=true appends Content-Type to output headers (NOT signed) for PUT.
function sign(method, url, body, addContentType) {
  if (!ACCESS_KEY || !SECRET_KEY) {
    return addContentType ? { 'Content-Type': 'application/octet-stream' } : {};
  }

  const { host, path, query } = parseURL(url);

  const now      = new Date();
  const amzdate  = now.toISOString().replace(/[:\-]|\.\d{3}/g, '').slice(0, 15) + 'Z';
  const dateStamp= amzdate.slice(0, 8);

  const payload     = body || '';
  const payloadHash = crypto.sha256(payload, 'hex');

  // Only sign host + x-amz-* headers; content-type is excluded to avoid
  // Hertz/fasthttp canonicalization differences causing signature mismatches.
  const hdrMap = {
    'host'                 : host,
    'x-amz-content-sha256': payloadHash,
    'x-amz-date'          : amzdate,
  };

  const sortedKeys      = Object.keys(hdrMap).sort();
  const canonicalHeaders= sortedKeys.map(k => k + ':' + hdrMap[k]).join('\n') + '\n';
  const signedHeaders   = sortedKeys.join(';');

  const canonicalReq = [method, path, query || '', canonicalHeaders, signedHeaders, payloadHash].join('\n');

  const credScope    = dateStamp + '/' + REGION + '/' + SERVICE + '/aws4_request';
  const stringToSign = ['AWS4-HMAC-SHA256', amzdate, credScope, crypto.sha256(canonicalReq, 'hex')].join('\n');

  const sigKey   = derivedSigningKey(SECRET_KEY, dateStamp);
  const signature= crypto.hmac('sha256', sigKey, stringToSign, 'hex');

  const auth = `AWS4-HMAC-SHA256 Credential=${ACCESS_KEY}/${credScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;

  const outHeaders = {
    'Authorization'        : auth,
    'x-amz-content-sha256' : payloadHash,
    'x-amz-date'           : amzdate,
  };
  if (addContentType) outHeaders['Content-Type'] = 'application/octet-stream';
  return outHeaders;
}

// ---- Scenarios ----

export function setupBucket() {
  const url = `${BASE}/${BUCKET}`;
  const res = http.put(url, null, { headers: sign('PUT', url, '', false) });
  check(res, { 'bucket ready': (r) => r.status === 200 || r.status === 409 });
}

export function mixedWorkload() {
  const key     = `obj-${__VU}-${__ITER}-${randomString(6)}`;
  const size    = OBJECT_SIZE_KB > 0 ? OBJECT_SIZE_KB * 1024 : [1024, 4096, 16384, 65536][Math.floor(Math.random() * 4)];
  const payload = randomString(size);

  // PUT
  const putUrl = `${BASE}/${BUCKET}/${key}`;
  const putRes = http.put(putUrl, payload, { headers: sign('PUT', putUrl, payload, true) });
  putLatency.add(putRes.timings.duration);
  putOps.add(1);
  if (!check(putRes, { 'put ok': (r) => r.status === 200 })) {
    console.log(`PUT failed: status=${putRes.status} body=${putRes.body.substring(0, 200)}`);
  }

  // GET
  const getUrl = `${BASE}/${BUCKET}/${key}`;
  const getRes = http.get(getUrl, { headers: sign('GET', getUrl, '', false) });
  getLatency.add(getRes.timings.duration);
  getOps.add(1);
  check(getRes, { 'get ok': (r) => r.status === 200 });

  // DELETE (50% of the time)
  if (Math.random() < 0.5) {
    const delUrl = `${BASE}/${BUCKET}/${key}`;
    const delRes = http.del(delUrl, null, { headers: sign('DELETE', delUrl, '', false) });
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
      total_requests : data.metrics.http_reqs        ? data.metrics.http_reqs.values.count         : 0,
      failed_requests: data.metrics.http_req_failed  ? data.metrics.http_req_failed.values.passes  : 0,
    },
    put   : extractMetric(data, 'grainfs_put_latency',    'grainfs_put_ops'),
    get   : extractMetric(data, 'grainfs_get_latency',    'grainfs_get_ops'),
    delete: extractMetric(data, 'grainfs_delete_latency', 'grainfs_delete_ops'),
  };

  return {
    stdout: formatReport(report),
    'benchmarks/report.json': JSON.stringify(report, null, 2),
  };
}

function extractMetric(data, latencyKey, opsKey) {
  const latency = data.metrics[latencyKey];
  const ops     = data.metrics[opsKey];
  if (!latency) return { ops: 0 };
  return {
    ops   : ops ? ops.values.count : 0,
    p50_ms: (latency.values['med'] ?? latency.values['p(50)'] ?? 0).toFixed(2),
    p99_ms: (latency.values['p(99)'] ?? 0).toFixed(2),
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
