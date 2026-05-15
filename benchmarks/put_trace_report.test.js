const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

test('forward send stages do not hide receiver-side dominant stages', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'put-trace-report-'));
  const trace = path.join(dir, 'trace.jsonl');
  const base = {
    ts: '2026-05-15T00:00:00Z',
    bucket: 'bench',
    key: 'large-key',
    group_id: 'group-1',
    ingress: 'forwarded_non_leader',
    size_class: 'large',
    forward_mode: 'stream',
  };
  const events = [
    { ...base, stage: 'forward_send_stream', duration_micros: 220000, forward_attempts: 1, bytes: 8388745 },
    { ...base, ingress: 'receiver', stage: 'receiver_backend_put', duration_micros: 90000 },
    { ...base, ingress: 'receiver', stage: 'meta_index_propose', duration_micros: 130000, meta_propose_site: 'receiver', meta_propose_count: 1 },
    { ...base, ingress: 'receiver', stage: 'shard_write_remote', duration_micros: 45000, shard_index: 1 },
  ];
  fs.writeFileSync(trace, events.map((ev) => JSON.stringify(ev)).join('\n') + '\n');

  execFileSync('node', ['benchmarks/put_trace_report.js', trace], {
    cwd: path.resolve(__dirname, '..'),
    stdio: 'ignore',
  });

  const report = JSON.parse(fs.readFileSync(path.resolve(__dirname, 'put-trace-report.json'), 'utf8'));
  assert.equal(report[0].dominant_stage, 'meta_index_propose');
  assert.equal(report[0].dominant_inclusive_stage, 'forward_send_stream');
});
