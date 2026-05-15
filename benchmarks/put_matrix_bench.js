import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { randomString } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import crypto from 'k6/crypto';

const BASE = __ENV.BASE_URL || 'http://localhost:9000';
const BUCKET = __ENV.BUCKET || 'bench';
const ACCESS_KEY = __ENV.ACCESS_KEY || '';
const SECRET_KEY = __ENV.SECRET_KEY || '';
const REGION = 'us-east-1';
const SERVICE = 's3';
const OBJECT_SIZE_KB = parseInt(__ENV.OBJECT_SIZE_KB || '64');
const MATRIX_CELL = __ENV.MATRIX_CELL || 'unknown';
const ITERATIONS = parseInt(__ENV.ITERATIONS || '25');
const VUS = parseInt(__ENV.VUS || '1');
const USE_UNSIGNED_PAYLOAD = (__ENV.UNSIGNED_PAYLOAD || '1') !== '0';
const PAYLOAD = 'x'.repeat(OBJECT_SIZE_KB * 1024);

const putLatency = new Trend('grainfs_matrix_put_latency', true);
const putOps = new Counter('grainfs_matrix_put_ops');

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    put_matrix: {
      executor: 'shared-iterations',
      vus: VUS,
      iterations: ITERATIONS,
      exec: 'putOnly',
    },
  },
  thresholds: {
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
  const host = slashIdx === -1 ? rest : rest.slice(0, slashIdx);
  const pathAndQuery = slashIdx === -1 ? '/' : rest.slice(slashIdx) || '/';
  const qIdx = pathAndQuery.indexOf('?');
  return {
    host,
    path: qIdx === -1 ? pathAndQuery : pathAndQuery.slice(0, qIdx),
    query: qIdx === -1 ? '' : pathAndQuery.slice(qIdx + 1),
  };
}

function sign(method, url, body, addContentType) {
  if (!ACCESS_KEY || !SECRET_KEY) {
    return addContentType ? { 'Content-Type': 'application/octet-stream' } : {};
  }
  const { host, path, query } = parseURL(url);
  const now = new Date();
  const amzdate = now.toISOString().replace(/[:\-]|\.\d{3}/g, '').slice(0, 15) + 'Z';
  const dateStamp = amzdate.slice(0, 8);
  const payloadHash = USE_UNSIGNED_PAYLOAD ? 'UNSIGNED-PAYLOAD' : crypto.sha256(body || '', 'hex');
  const hdrMap = {
    host,
    'x-amz-content-sha256': payloadHash,
    'x-amz-date': amzdate,
  };
  const sortedKeys = Object.keys(hdrMap).sort();
  const canonicalHeaders = sortedKeys.map((k) => k + ':' + hdrMap[k]).join('\n') + '\n';
  const signedHeaders = sortedKeys.join(';');
  const canonicalReq = [method, path, query || '', canonicalHeaders, signedHeaders, payloadHash].join('\n');
  const credScope = dateStamp + '/' + REGION + '/' + SERVICE + '/aws4_request';
  const stringToSign = ['AWS4-HMAC-SHA256', amzdate, credScope, crypto.sha256(canonicalReq, 'hex')].join('\n');
  const signature = crypto.hmac('sha256', derivedSigningKey(SECRET_KEY, dateStamp), stringToSign, 'hex');
  const out = {
    Authorization: `AWS4-HMAC-SHA256 Credential=${ACCESS_KEY}/${credScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`,
    'x-amz-content-sha256': payloadHash,
    'x-amz-date': amzdate,
  };
  if (addContentType) out['Content-Type'] = 'application/octet-stream';
  return out;
}

export function setup() {
  const url = `${BASE}/${BUCKET}`;
  http.put(url, null, {
    headers: sign('PUT', url, '', false),
    responseCallback: http.expectedStatuses(200, 409),
  });
}

export function putOnly() {
  const key = `${MATRIX_CELL}/obj-${__VU}-${__ITER}-${randomString(8)}`;
  const putUrl = `${BASE}/${BUCKET}/${key}`;
  const res = http.put(putUrl, PAYLOAD, { headers: sign('PUT', putUrl, PAYLOAD, true) });
  putLatency.add(res.timings.duration);
  putOps.add(1);
  check(res, { 'put ok': (r) => r.status === 200 });
}

export function handleSummary(data) {
  const latency = data.metrics.grainfs_matrix_put_latency;
  const report = {
    cell: MATRIX_CELL,
    size_kb: OBJECT_SIZE_KB,
    ops: data.metrics.grainfs_matrix_put_ops ? data.metrics.grainfs_matrix_put_ops.values.count : 0,
    p50_ms: latency ? latency.values.med : 0,
    p95_ms: latency ? latency.values['p(95)'] : 0,
    p99_ms: latency ? latency.values['p(99)'] : 0,
  };
  return {
    stdout: `${JSON.stringify(report)}\n`,
    [`benchmarks/put-matrix-${MATRIX_CELL}.json`]: JSON.stringify(report, null, 2),
  };
}
