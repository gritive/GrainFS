// GrainFS single-node S3 mixed profile benchmark.
// Measures PUT/GET latency and throughput with a configurable operation mix.

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import crypto from 'k6/crypto';

const BASE_INPUT = (__ENV.BASE_URL || 'http://localhost:9000').trim();
const BASE_URLS = (__ENV.BASE_URLS || BASE_INPUT)
  .split(',')
  .map((s) => s.trim())
  .filter((s) => s.length > 0);
const BASE = BASE_INPUT || BASE_URLS[0];
const SEED_BASE = (__ENV.SEED_URL || BASE).trim();
const BUCKET = __ENV.BUCKET || 'bench';
const ACCESS_KEY = __ENV.ACCESS_KEY || '';
const SECRET_KEY = __ENV.SECRET_KEY || '';
const REGION = 'us-east-1';
const SERVICE = 's3';
const MAX_VUS = parseInt(__ENV.MAX_VUS || '1', 10);
const BENCH_DURATION = __ENV.DURATION || '30s';
const RAMP_UP = __ENV.RAMP_UP || '1s';
const RAMP_DOWN = __ENV.RAMP_DOWN || '1s';
const GRACEFUL_RAMP_DOWN = __ENV.GRACEFUL_RAMP_DOWN || '2s';
const GRACEFUL_STOP = __ENV.GRACEFUL_STOP || '5s';
const OBJECT_SIZE_KB = parseInt(__ENV.OBJECT_SIZE_KB || '4096', 10);
const OBJECT_SIZE_BYTES = OBJECT_SIZE_KB * 1024;
const OBJECT_COUNT = parseInt(__ENV.OBJECT_COUNT || '16', 10);
const WRITE_PERCENT = parseInt(__ENV.WRITE_PERCENT || '50', 10);
const MIX = __ENV.MIX || `write-${WRITE_PERCENT}`;
const INGRESS = __ENV.INGRESS || (BASE_URLS.length > 1 ? 'round-robin' : 'single');
const SLEEP_SECONDS = parseFloat(__ENV.SLEEP_SECONDS || '0');
const SETUP_TIMEOUT = __ENV.SETUP_TIMEOUT || '5m';
const SUMMARY_JSON = __ENV.SUMMARY_JSON || 'benchmarks/s3_mixed_profile_report.json';

const putLatency = new Trend('grainfs_put_latency', true);
const getLatency = new Trend('grainfs_get_latency', true);
const putOps = new Counter('grainfs_put_ops');
const getOps = new Counter('grainfs_get_ops');
const putBytes = new Counter('grainfs_put_bytes');
const getBytes = new Counter('grainfs_get_bytes');
const putSuccess = new Rate('grainfs_put_success');
const getSuccess = new Rate('grainfs_get_success');

const PAYLOAD = payloadOfSize(OBJECT_SIZE_BYTES);

export const options = {
  discardResponseBodies: true,
  setupTimeout: SETUP_TIMEOUT,
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    mixed_workload: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: RAMP_UP, target: MAX_VUS },
        { duration: BENCH_DURATION, target: MAX_VUS },
        { duration: RAMP_DOWN, target: 0 },
      ],
      exec: 'mixedWorkload',
      gracefulRampDown: GRACEFUL_RAMP_DOWN,
      gracefulStop: GRACEFUL_STOP,
    },
  },
  thresholds: {
    grainfs_put_success: ['rate>0.95'],
    grainfs_get_success: ['rate>0.95'],
    http_req_failed: ['rate<0.05'],
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
  let host;
  let pathAndQuery;
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

function shouldWrite() {
	return Math.floor(Math.random() * 100) < WRITE_PERCENT;
}

function baseURL() {
  if (INGRESS === 'round-robin' && BASE_URLS.length > 1) {
    return BASE_URLS[(__VU + __ITER) % BASE_URLS.length];
  }
  return BASE;
}

export function setup() {
  const bucketUrl = `${SEED_BASE}/${BUCKET}`;
  const bucketRes = http.put(bucketUrl, null, {
    headers: sign('PUT', bucketUrl, '', false),
    responseCallback: http.expectedStatuses(200, 409),
    timeout: '120s',
  });
  check(bucketRes, { 'bucket ready': (r) => r.status === 200 || r.status === 409 });

	const keys = [];
	for (let i = 0; i < OBJECT_COUNT; i++) {
		const key = `profile-seed-${OBJECT_SIZE_KB}kb-${i}`;
		const putBase = SEED_BASE;
		const putUrl = `${putBase}/${BUCKET}/${key}`;
    const putRes = http.put(putUrl, PAYLOAD, {
      headers: sign('PUT', putUrl, PAYLOAD, true),
      responseCallback: http.expectedStatuses(200),
      responseType: 'text',
      timeout: '120s',
    });
    check(putRes, { 'preload put ok': (r) => r.status === 200 });
    keys.push(key);
  }
  return { keys };
}

export function mixedWorkload(data) {
	if (shouldWrite()) {
		const key = `profile-write-${__VU}-${__ITER}`;
		const putUrl = `${baseURL()}/${BUCKET}/${key}`;
    const res = http.put(putUrl, PAYLOAD, {
      headers: sign('PUT', putUrl, PAYLOAD, true),
      responseCallback: http.expectedStatuses(200),
      responseType: 'text',
      timeout: '120s',
    });
    const ok = res.status === 200;
    putSuccess.add(ok);
    putOps.add(1);
    if (ok) {
      putLatency.add(res.timings.duration);
      putBytes.add(OBJECT_SIZE_BYTES);
    } else if (__ITER < 3) {
      console.log(`PUT failed: status=${res.status} key=${key} body=${String(res.body).slice(0, 240)}`);
    }
    check(res, { 'put ok': () => ok });
  } else {
		const key = data.keys[(__VU + __ITER) % data.keys.length];
		const getUrl = `${baseURL()}/${BUCKET}/${key}`;
    const res = http.get(getUrl, {
      headers: sign('GET', getUrl, '', false),
      responseCallback: http.expectedStatuses(200),
      responseType: 'text',
      timeout: '120s',
    });
    const ok = res.status === 200;
    getSuccess.add(ok);
    getOps.add(1);
    if (ok) {
      getLatency.add(res.timings.duration);
      getBytes.add(OBJECT_SIZE_BYTES);
    } else if (__ITER < 3) {
      console.log(`GET failed: status=${res.status} key=${key} body=${String(res.body).slice(0, 240)}`);
    }
    check(res, { 'get ok': () => ok });
  }

  if (SLEEP_SECONDS > 0) {
    sleep(SLEEP_SECONDS);
  }
}

export function handleSummary(data) {
  const report = {
    timestamp: new Date().toISOString(),
    mix: MIX,
    write_percent: WRITE_PERCENT,
    object_size_kb: OBJECT_SIZE_KB,
    object_count: OBJECT_COUNT,
		vus: MAX_VUS,
		ingress: INGRESS,
		base_urls: BASE_URLS,
    base_url: BASE,
    seed_url: SEED_BASE,
    put: extractOp(data, 'put'),
    get: extractOp(data, 'get'),
    http_reqs: data.metrics.http_reqs ? data.metrics.http_reqs.values.count : 0,
    http_req_failed: data.metrics.http_req_failed ? data.metrics.http_req_failed.values.rate : 0,
  };

  let out = '\n=== GrainFS S3 Mixed Profile Report ===\n';
  out += `Mix: ${report.mix} (${report.write_percent}% write)\n`;
  out += `Object: ${report.object_size_kb}KB x ${report.object_count} seed objects\n`;
  out += `VUs: ${report.vus}\n`;
  out += formatOp('PUT', report.put);
  out += formatOp('GET', report.get);
  out += `Failed rate: ${report.http_req_failed}\n`;
  out += `\nFull report: ${SUMMARY_JSON}\n`;

  return {
    stdout: out,
    [SUMMARY_JSON]: JSON.stringify(report, null, 2),
  };
}

function extractOp(data, op) {
  const latency = data.metrics[`grainfs_${op}_latency`];
  const ops = data.metrics[`grainfs_${op}_ops`];
  const bytes = data.metrics[`grainfs_${op}_bytes`];
  const success = data.metrics[`grainfs_${op}_success`];
  return {
    ops: ops ? ops.values.count : 0,
    bytes: bytes ? bytes.values.count : 0,
    bytes_per_sec: bytes ? bytes.values.rate.toFixed(2) : '0.00',
    success_rate: success ? success.values.rate : 0,
    p50_ms: latency ? (latency.values.med ?? latency.values['p(50)'] ?? 0).toFixed(2) : '0.00',
    p95_ms: latency ? (latency.values['p(95)'] ?? 0).toFixed(2) : '0.00',
    p99_ms: latency ? (latency.values['p(99)'] ?? 0).toFixed(2) : '0.00',
    avg_ms: latency ? latency.values.avg.toFixed(2) : '0.00',
    max_ms: latency ? latency.values.max.toFixed(2) : '0.00',
  };
}

function formatOp(label, metric) {
  return `${label}: ${metric.ops} ok ops | ${metric.bytes_per_sec} B/s | success: ${metric.success_rate} | P50: ${metric.p50_ms}ms | P95: ${metric.p95_ms}ms | P99: ${metric.p99_ms}ms | Avg: ${metric.avg_ms}ms | Max: ${metric.max_ms}ms\n`;
}
