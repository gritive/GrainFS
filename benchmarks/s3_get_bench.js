// GrainFS S3 GET-only benchmark.
// Preloads fixed objects during setup, then measures repeated GET latency.

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import crypto from 'k6/crypto';

const BASE = __ENV.BASE_URL || 'http://localhost:9000';
const BUCKET = __ENV.BUCKET || 'bench';
const ACCESS_KEY = __ENV.ACCESS_KEY || '';
const SECRET_KEY = __ENV.SECRET_KEY || '';
const REGION = 'us-east-1';
const SERVICE = 's3';
const MAX_VUS = parseInt(__ENV.MAX_VUS || '1');
const BENCH_DURATION = __ENV.DURATION || '30s';
const RAMP_UP = __ENV.RAMP_UP || '1s';
const RAMP_DOWN = __ENV.RAMP_DOWN || '1s';
const GRACEFUL_RAMP_DOWN = __ENV.GRACEFUL_RAMP_DOWN || '2s';
const GRACEFUL_STOP = __ENV.GRACEFUL_STOP || '5s';
const OBJECT_SIZE_KB = parseInt(__ENV.OBJECT_SIZE_KB || '65536');
const OBJECT_COUNT = parseInt(__ENV.OBJECT_COUNT || '4');
const SLEEP_SECONDS = parseFloat(__ENV.SLEEP_SECONDS || '0');

const getLatency = new Trend('grainfs_get_latency', true);
const getOps = new Counter('grainfs_get_ops');
const getSuccess = new Rate('grainfs_get_success');

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    get_workload: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: RAMP_UP, target: MAX_VUS },
        { duration: BENCH_DURATION, target: MAX_VUS },
        { duration: RAMP_DOWN, target: 0 },
      ],
      exec: 'getWorkload',
      gracefulRampDown: GRACEFUL_RAMP_DOWN,
      gracefulStop: GRACEFUL_STOP,
    },
  },
  thresholds: {
    grainfs_get_latency: ['p(99)<5000'],
    grainfs_get_success: ['rate>0.99'],
    http_req_failed: ['rate<0.01'],
  },
};

function hexToBytes(hex) {
  const buf = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) {
    buf[i / 2] = parseInt(hex.substring(i, i + 2), 16);
  }
  return buf.buffer;
}

function derivedSigningKey(secretKey, dateStamp) {
  const kDate = hexToBytes(crypto.hmac('sha256', 'AWS4' + secretKey, dateStamp, 'hex'));
  const kRegion = hexToBytes(crypto.hmac('sha256', kDate, REGION, 'hex'));
  const kService = hexToBytes(crypto.hmac('sha256', kRegion, SERVICE, 'hex'));
  return hexToBytes(crypto.hmac('sha256', kService, 'aws4_request', 'hex'));
}

function parseURL(url) {
  let rest = url.replace(/^https?:\/\//, '');
  const slashIdx = rest.indexOf('/');
  let host, pathAndQuery;
  if (slashIdx === -1) {
    host = rest;
    pathAndQuery = '/';
  } else {
    host = rest.slice(0, slashIdx);
    pathAndQuery = rest.slice(slashIdx) || '/';
  }
  const qIdx = pathAndQuery.indexOf('?');
  const path = qIdx === -1 ? pathAndQuery : pathAndQuery.slice(0, qIdx);
  const query = qIdx === -1 ? '' : pathAndQuery.slice(qIdx + 1);
  return { host, path, query };
}

function sign(method, url, body, addContentType) {
  if (!ACCESS_KEY || !SECRET_KEY) {
    return addContentType ? { 'Content-Type': 'application/octet-stream' } : {};
  }

  const { host, path, query } = parseURL(url);
  const now = new Date();
  const amzdate = now.toISOString().replace(/[:\-]|\.\d{3}/g, '').slice(0, 15) + 'Z';
  const dateStamp = amzdate.slice(0, 8);
  const payload = body || '';
  const payloadHash = crypto.sha256(payload, 'hex');
  const hdrMap = {
    host: host,
    'x-amz-content-sha256': payloadHash,
    'x-amz-date': amzdate,
  };
  const sortedKeys = Object.keys(hdrMap).sort();
  const canonicalHeaders = sortedKeys.map((k) => k + ':' + hdrMap[k]).join('\n') + '\n';
  const signedHeaders = sortedKeys.join(';');
  const canonicalReq = [method, path, query || '', canonicalHeaders, signedHeaders, payloadHash].join('\n');
  const credScope = dateStamp + '/' + REGION + '/' + SERVICE + '/aws4_request';
  const stringToSign = ['AWS4-HMAC-SHA256', amzdate, credScope, crypto.sha256(canonicalReq, 'hex')].join('\n');
  const sigKey = derivedSigningKey(SECRET_KEY, dateStamp);
  const signature = crypto.hmac('sha256', sigKey, stringToSign, 'hex');
  const auth = `AWS4-HMAC-SHA256 Credential=${ACCESS_KEY}/${credScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;
  const outHeaders = {
    Authorization: auth,
    'x-amz-content-sha256': payloadHash,
    'x-amz-date': amzdate,
  };
  if (addContentType) outHeaders['Content-Type'] = 'application/octet-stream';
  return outHeaders;
}

function payloadOfSize(size) {
  let chunk = '0123456789abcdef';
  while (chunk.length < 1024) {
    chunk += chunk;
  }
  let out = '';
  while (out.length < size) {
    out += chunk.slice(0, Math.min(chunk.length, size - out.length));
  }
  return out;
}

export function setup() {
  const bucketUrl = `${BASE}/${BUCKET}`;
  const bucketRes = http.put(bucketUrl, null, {
    headers: sign('PUT', bucketUrl, '', false),
    responseCallback: http.expectedStatuses(200, 409),
  });
  check(bucketRes, { 'bucket ready': (r) => r.status === 200 || r.status === 409 });

  const payload = payloadOfSize(OBJECT_SIZE_KB * 1024);
  const keys = [];
  for (let i = 0; i < OBJECT_COUNT; i++) {
    const key = `get-only-${OBJECT_SIZE_KB}kb-${i}`;
    const putUrl = `${BASE}/${BUCKET}/${key}`;
    const putRes = http.put(putUrl, payload, {
      headers: sign('PUT', putUrl, payload, true),
      responseCallback: http.expectedStatuses(200),
      timeout: '120s',
    });
    check(putRes, { 'preload put ok': (r) => r.status === 200 });
    keys.push(key);
  }
  return { keys };
}

export function getWorkload(data) {
  const keys = data.keys;
  const key = keys[(__VU + __ITER) % keys.length];
  const getUrl = `${BASE}/${BUCKET}/${key}`;
  const res = http.get(getUrl, {
    headers: sign('GET', getUrl, '', false),
    responseCallback: http.expectedStatuses(200),
    timeout: '120s',
  });
  const ok = res.status === 200;
  getSuccess.add(ok);
  if (ok) {
    getLatency.add(res.timings.duration);
    getOps.add(1);
  } else if (__ITER < 3) {
    console.log(`GET failed: status=${res.status} key=${key}`);
  }
  check(res, { 'get ok': () => ok });
  if (SLEEP_SECONDS > 0) {
    sleep(SLEEP_SECONDS);
  }
}

export function handleSummary(data) {
  const latency = data.metrics.grainfs_get_latency;
  const ops = data.metrics.grainfs_get_ops;
  const success = data.metrics.grainfs_get_success;
  const report = {
    timestamp: new Date().toISOString(),
    topology: __ENV.TOPOLOGY || '',
    object_size_kb: OBJECT_SIZE_KB,
    object_count: OBJECT_COUNT,
    vus: MAX_VUS,
    get: {
      ops: ops ? ops.values.count : 0,
      success_rate: success ? success.values.rate : 0,
      p50_ms: latency ? (latency.values.med ?? latency.values['p(50)'] ?? 0).toFixed(2) : '0.00',
      p95_ms: latency ? (latency.values['p(95)'] ?? 0).toFixed(2) : '0.00',
      p99_ms: latency ? (latency.values['p(99)'] ?? 0).toFixed(2) : '0.00',
      avg_ms: latency ? latency.values.avg.toFixed(2) : '0.00',
      max_ms: latency ? latency.values.max.toFixed(2) : '0.00',
    },
    http_reqs: data.metrics.http_reqs ? data.metrics.http_reqs.values.count : 0,
    http_req_failed: data.metrics.http_req_failed ? data.metrics.http_req_failed.values.rate : 0,
  };

  let out = '\n=== GrainFS GET-only Benchmark Report ===\n';
  out += `Topology: ${report.topology}\n`;
  out += `Object: ${report.object_size_kb}KB x ${report.object_count}\n`;
  out += `GET: ${report.get.ops} ok ops | success: ${report.get.success_rate} | P50: ${report.get.p50_ms}ms | P95: ${report.get.p95_ms}ms | P99: ${report.get.p99_ms}ms | Avg: ${report.get.avg_ms}ms | Max: ${report.get.max_ms}ms\n`;
  out += `Failed rate: ${report.http_req_failed}\n`;
  out += '\nFull report: benchmarks/get_report.json\n';

  return {
    stdout: out,
    'benchmarks/get_report.json': JSON.stringify(report, null, 2),
  };
}
