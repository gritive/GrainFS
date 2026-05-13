// GrainFS Iceberg REST Catalog table API benchmark.
// Run through benchmarks/bench_iceberg_table.sh so the warehouse bucket exists.

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { randomString } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import crypto from 'k6/crypto';

const BASE = __ENV.BASE_URL || 'http://127.0.0.1:9000';
const ACCESS_KEY = __ENV.ACCESS_KEY || '';
const SECRET_KEY = __ENV.SECRET_KEY || '';
const REGION = 'us-east-1';
const SERVICE = 's3';
const MAX_VUS = parseInt(__ENV.MAX_VUS || '10');
const BENCH_DURATION = __ENV.DURATION || '30s';
const RAMP_UP = __ENV.RAMP_UP || '10s';
const RAMP_DOWN = __ENV.RAMP_DOWN || '5s';
const NAMESPACE_PREFIX = __ENV.NAMESPACE_PREFIX || 'bench_ns';

const configLatency = new Trend('grainfs_iceberg_config_latency', true);
const createNamespaceLatency = new Trend('grainfs_iceberg_create_namespace_latency', true);
const createTableLatency = new Trend('grainfs_iceberg_create_table_latency', true);
const loadTableLatency = new Trend('grainfs_iceberg_load_table_latency', true);
const commitTableLatency = new Trend('grainfs_iceberg_commit_table_latency', true);
const deleteTableLatency = new Trend('grainfs_iceberg_delete_table_latency', true);
const deleteNamespaceLatency = new Trend('grainfs_iceberg_delete_namespace_latency', true);

const createNamespaceOps = new Counter('grainfs_iceberg_create_namespace_ops');
const createTableOps = new Counter('grainfs_iceberg_create_table_ops');
const loadTableOps = new Counter('grainfs_iceberg_load_table_ops');
const commitTableOps = new Counter('grainfs_iceberg_commit_table_ops');
const deleteTableOps = new Counter('grainfs_iceberg_delete_table_ops');
const deleteNamespaceOps = new Counter('grainfs_iceberg_delete_namespace_ops');

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    mixed_table_api: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: RAMP_UP, target: MAX_VUS },
        { duration: BENCH_DURATION, target: MAX_VUS },
        { duration: RAMP_DOWN, target: 0 },
      ],
      exec: 'mixedTableAPI',
    },
  },
  thresholds: {
    grainfs_iceberg_create_namespace_latency: ['p(99)<500'],
    grainfs_iceberg_create_table_latency: ['p(99)<1000'],
    grainfs_iceberg_load_table_latency: ['p(99)<300'],
    grainfs_iceberg_commit_table_latency: ['p(99)<1000'],
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

function signS3(method, url, body) {
  if (!ACCESS_KEY || !SECRET_KEY) {
    return {};
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
  return {
    Authorization: `AWS4-HMAC-SHA256 Credential=${ACCESS_KEY}/${credScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`,
    'x-amz-content-sha256': payloadHash,
    'x-amz-date': amzdate,
  };
}

export function setup() {
  const bucketURL = `${BASE}/grainfs-tables`;
  const bucketRes = http.put(bucketURL, null, {
    headers: signS3('PUT', bucketURL, ''),
    responseCallback: http.expectedStatuses(200, 409),
  });
  check(bucketRes, { 'warehouse bucket ready': (r) => r.status === 200 || r.status === 409 });
}

export function mixedTableAPI() {
  const suffix = `${__VU}_${__ITER}_${randomString(6)}`;
  const namespace = `${NAMESPACE_PREFIX}_${suffix}`;
  const table = `t_${suffix}`;
  const tableLocation = `s3://grainfs-tables/warehouse/${namespace}/${table}`;

  const configRes = http.get(`${BASE}/iceberg/v1/config?warehouse=warehouse`);
  configLatency.add(configRes.timings.duration);
  check(configRes, { 'config ok': (r) => r.status === 200 });

  const createNamespaceBody = JSON.stringify({
    namespace: [namespace],
    properties: { owner: 'k6' },
  });
  const nsRes = http.post(`${BASE}/iceberg/v1/namespaces`, createNamespaceBody, jsonParams());
  createNamespaceLatency.add(nsRes.timings.duration);
  createNamespaceOps.add(1);
  if (!check(nsRes, { 'create namespace ok': (r) => r.status === 200 || r.status === 409 })) {
    console.log(`create namespace failed: status=${nsRes.status} body=${nsRes.body.substring(0, 200)}`);
    return;
  }

  const createTableBody = JSON.stringify({
    name: table,
    schema: {
      type: 'struct',
      'schema-id': 0,
      fields: [{ id: 1, name: 'a', required: false, type: 'int' }],
    },
    properties: { benchmark: 'iceberg-table-api' },
  });
  const createTableRes = http.post(`${BASE}/iceberg/v1/namespaces/${namespace}/tables`, createTableBody, jsonParams());
  createTableLatency.add(createTableRes.timings.duration);
  createTableOps.add(1);
  if (!check(createTableRes, { 'create table ok': (r) => r.status === 200 })) {
    console.log(`create table failed: status=${createTableRes.status} body=${createTableRes.body.substring(0, 200)}`);
    return;
  }

  const loadRes = http.get(`${BASE}/iceberg/v1/namespaces/${namespace}/tables/${table}`);
  loadTableLatency.add(loadRes.timings.duration);
  loadTableOps.add(1);
  if (!check(loadRes, { 'load table ok': (r) => r.status === 200 })) {
    console.log(`load table failed: status=${loadRes.status} body=${loadRes.body.substring(0, 200)}`);
    return;
  }

  const snapshotID = __VU * 1000000000 + __ITER + 1;
  const commitBody = JSON.stringify({
    requirements: [{ type: 'assert-ref-snapshot-id', ref: 'main', 'snapshot-id': null }],
    updates: [
      {
        action: 'add-snapshot',
        snapshot: {
          'snapshot-id': snapshotID,
          'sequence-number': 1,
          'timestamp-ms': Date.now(),
          'manifest-list': `${tableLocation}/metadata/snap-${snapshotID}.avro`,
          summary: { operation: 'append' },
          'schema-id': 0,
        },
      },
      {
        action: 'set-snapshot-ref',
        'ref-name': 'main',
        type: 'branch',
        'snapshot-id': snapshotID,
      },
    ],
  });
  const commitRes = http.post(`${BASE}/iceberg/v1/namespaces/${namespace}/tables/${table}`, commitBody, jsonParams());
  commitTableLatency.add(commitRes.timings.duration);
  commitTableOps.add(1);
  check(commitRes, { 'commit table ok': (r) => r.status === 200 });

  const deleteTableRes = http.del(`${BASE}/iceberg/v1/namespaces/${namespace}/tables/${table}`);
  deleteTableLatency.add(deleteTableRes.timings.duration);
  deleteTableOps.add(1);
  check(deleteTableRes, { 'delete table ok': (r) => r.status === 204 });

  const deleteNamespaceRes = http.del(`${BASE}/iceberg/v1/namespaces/${namespace}`);
  deleteNamespaceLatency.add(deleteNamespaceRes.timings.duration);
  deleteNamespaceOps.add(1);
  check(deleteNamespaceRes, { 'delete namespace ok': (r) => r.status === 204 });

  sleep(0.01);
}

export function handleSummary(data) {
  const report = {
    timestamp: new Date().toISOString(),
    summary: {
      total_requests: data.metrics.http_reqs ? data.metrics.http_reqs.values.count : 0,
      failed_requests: data.metrics.http_req_failed ? data.metrics.http_req_failed.values.passes : 0,
    },
    create_namespace: extractMetric(data, 'grainfs_iceberg_create_namespace_latency', 'grainfs_iceberg_create_namespace_ops'),
    create_table: extractMetric(data, 'grainfs_iceberg_create_table_latency', 'grainfs_iceberg_create_table_ops'),
    load_table: extractMetric(data, 'grainfs_iceberg_load_table_latency', 'grainfs_iceberg_load_table_ops'),
    commit_table: extractMetric(data, 'grainfs_iceberg_commit_table_latency', 'grainfs_iceberg_commit_table_ops'),
    delete_table: extractMetric(data, 'grainfs_iceberg_delete_table_latency', 'grainfs_iceberg_delete_table_ops'),
    delete_namespace: extractMetric(data, 'grainfs_iceberg_delete_namespace_latency', 'grainfs_iceberg_delete_namespace_ops'),
  };

  return {
    stdout: formatReport(report),
    'benchmarks/iceberg_table_report.json': JSON.stringify(report, null, 2),
  };
}

function jsonParams() {
  return { headers: { 'Content-Type': 'application/json' } };
}

function extractMetric(data, latencyKey, opsKey) {
  const latency = data.metrics[latencyKey];
  const ops = data.metrics[opsKey];
  if (!latency) return { ops: 0 };
  return {
    ops: ops ? ops.values.count : 0,
    p50_ms: (latency.values.med ?? latency.values['p(50)'] ?? 0).toFixed(2),
    p99_ms: (latency.values['p(99)'] ?? 0).toFixed(2),
    avg_ms: latency.values.avg.toFixed(2),
    min_ms: latency.values.min.toFixed(2),
    max_ms: latency.values.max.toFixed(2),
  };
}

function formatReport(r) {
  let out = '\n=== GrainFS Iceberg Table API Benchmark Report ===\n';
  out += `Timestamp: ${r.timestamp}\n`;
  out += `Total Requests: ${r.summary.total_requests}\n`;
  out += `Failed: ${r.summary.failed_requests}\n\n`;
  for (const op of ['create_namespace', 'create_table', 'load_table', 'commit_table', 'delete_table', 'delete_namespace']) {
    const m = r[op];
    if (!m || !m.ops) continue;
    out += `${op}: ${m.ops} ops | P50: ${m.p50_ms}ms | P99: ${m.p99_ms}ms | Avg: ${m.avg_ms}ms\n`;
  }
  out += '\nFull report: benchmarks/iceberg_table_report.json\n';
  return out;
}
