#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const files = process.argv.slice(2);
if (files.length === 0) {
  console.error('usage: node benchmarks/put_trace_report.js <trace.jsonl>...');
  process.exit(1);
}

const events = [];
for (const file of files) {
  if (!fs.existsSync(file)) continue;
  const lines = fs.readFileSync(file, 'utf8').split('\n').filter(Boolean);
  for (const line of lines) {
    events.push(JSON.parse(line));
  }
}

function percentile(values, p) {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[idx];
}

function matrixCell(ev) {
  const ingress = ev.ingress === 'receiver' ? 'forwarded_non_leader' : ev.ingress;
  const size = ev.size_class || 'unknown';
  const mode = ev.forward_mode || 'none';
  return `${ingress}|${size}|${mode}`;
}

function matrixParts(cell) {
  const [ingress, sizeClass, forwardMode] = cell.split('|');
  return { ingress, sizeClass, forwardMode };
}

const inclusiveWrapperStages = new Set([
	'http_put_total',
	'forward_send_frame',
	'forward_send_stream',
]);

function isHTTPPutStage(stage) {
	return stage.startsWith('http_put_');
}

function topStageSummary(stageDurations, limit) {
  return [...stageDurations.entries()]
    .filter(([stage]) => !inclusiveWrapperStages.has(stage))
    .map(([stage, values]) => ({
      stage,
      p99_ms: +(percentile(values, 99) / 1000).toFixed(2),
    }))
    .sort((a, b) => b.p99_ms - a.p99_ms)
    .slice(0, limit)
    .map((item) => `${item.stage}:${item.p99_ms}`)
    .join('<br>');
}

const byRequest = new Map();
for (const ev of events) {
	const reqKey = `${ev.bucket}/${ev.key}`;
	if (!byRequest.has(reqKey)) byRequest.set(reqKey, []);
  byRequest.get(reqKey).push(ev);
}

const byCell = new Map();
for (const reqEvents of byRequest.values()) {
	const first = reqEvents.find((ev) => !isHTTPPutStage(ev.stage) && ev.ingress !== 'receiver') ||
		reqEvents.find((ev) => ev.ingress !== 'receiver') ||
		reqEvents[0];
  const cell = matrixCell(first);
  if (!byCell.has(cell)) byCell.set(cell, []);
  byCell.get(cell).push(reqEvents);
}

const rows = [];
for (const [cell, requests] of byCell.entries()) {
  const stageDurations = new Map();
  const metaCounts = [];
  const receiverMetaCounts = [];
  const coordinatorMetaCounts = [];
  const forwardAttempts = [];
  const forwardedBytes = [];
  const leaderHintUsed = [];
  const notLeaderRetries = [];
  const slowShardMicros = [];
  const groupIDs = new Set();
  for (const reqEvents of requests) {
    let metaCount = 0;
    let receiverMetaCount = 0;
    let coordinatorMetaCount = 0;
    let attempts = 0;
    let requestForwardedBytes = 0;
    let requestLeaderHintUsed = 0;
    let requestNotLeaderRetries = 0;
    let slowShard = 0;
    for (const ev of reqEvents) {
      if (ev.group_id) groupIDs.add(ev.group_id);
      if (!stageDurations.has(ev.stage)) stageDurations.set(ev.stage, []);
      stageDurations.get(ev.stage).push(ev.duration_micros || 0);
      if (ev.stage === 'meta_index_propose') {
        const count = ev.meta_propose_count || 0;
        metaCount += count;
        if (ev.meta_propose_site === 'receiver') receiverMetaCount += count;
        if (ev.meta_propose_site === 'coordinator') coordinatorMetaCount += count;
      }
      attempts = Math.max(attempts, ev.forward_attempts || 0);
      if (ev.leader_hint_used) requestLeaderHintUsed = 1;
      requestNotLeaderRetries = Math.max(requestNotLeaderRetries, ev.not_leader_retries || 0);
      if (ev.stage === 'forward_send_frame' || ev.stage === 'forward_send_stream') {
        requestForwardedBytes = Math.max(requestForwardedBytes, ev.bytes || 0);
      }
      if (ev.stage === 'shard_write_local' || ev.stage === 'shard_write_remote') {
        slowShard = Math.max(slowShard, ev.duration_micros || 0);
      }
    }
    metaCounts.push(metaCount);
    receiverMetaCounts.push(receiverMetaCount);
    coordinatorMetaCounts.push(coordinatorMetaCount);
    forwardAttempts.push(attempts);
    forwardedBytes.push(requestForwardedBytes);
    leaderHintUsed.push(requestLeaderHintUsed);
    notLeaderRetries.push(requestNotLeaderRetries);
    slowShardMicros.push(slowShard);
  }

  let dominantInclusiveStage = '';
  let dominantInclusiveP99 = -1;
  let dominantStage = '';
  let dominantP99 = -1;
  for (const [stage, values] of stageDurations.entries()) {
    const p99 = percentile(values, 99);
    if (p99 > dominantInclusiveP99) {
      dominantInclusiveP99 = p99;
      dominantInclusiveStage = stage;
    }
    if (!inclusiveWrapperStages.has(stage) && p99 > dominantP99) {
      dominantP99 = p99;
      dominantStage = stage;
    }
  }
  if (!dominantStage) {
    dominantStage = dominantInclusiveStage;
    dominantP99 = dominantInclusiveP99;
  }

  const parts = matrixParts(cell);
  rows.push({
    cell,
    ingress: parts.ingress,
    size_class: parts.sizeClass,
    forward_mode: parts.forwardMode,
    group_ids: [...groupIDs].sort(),
    requests: requests.length,
    dominant_stage: dominantStage,
    dominant_stage_p95_ms: +(percentile(stageDurations.get(dominantStage) || [], 95) / 1000).toFixed(2),
    dominant_stage_p99_ms: +(dominantP99 / 1000).toFixed(2),
    top_stage_p99_ms: topStageSummary(stageDurations, 3),
    dominant_inclusive_stage: dominantInclusiveStage,
    dominant_inclusive_stage_p95_ms: +(percentile(stageDurations.get(dominantInclusiveStage) || [], 95) / 1000).toFixed(2),
    dominant_inclusive_stage_p99_ms: +(dominantInclusiveP99 / 1000).toFixed(2),
    meta_index_propose_count_p99: percentile(metaCounts, 99),
    meta_index_propose_count_p99_receiver: percentile(receiverMetaCounts, 99),
    meta_index_propose_count_p99_coordinator: percentile(coordinatorMetaCounts, 99),
    forward_attempts_p99: percentile(forwardAttempts, 99),
    leader_hint_used_p99: percentile(leaderHintUsed, 99),
    not_leader_retries_p99: percentile(notLeaderRetries, 99),
    forwarded_bytes_p99: percentile(forwardedBytes, 99),
    slowest_shard_p99_ms: +(percentile(slowShardMicros, 99) / 1000).toFixed(2),
  });
}

rows.sort((a, b) => a.cell.localeCompare(b.cell));
console.log('| Cell | Requests | Dominant stage | Stage p95 ms | Stage p99 ms | Top stage p99 ms | Inclusive stage | Inclusive p99 ms | Meta recv/coordinator p99 | Forward attempts p99 | Leader hint used p99 | NotLeader retries p99 | Forwarded bytes p99 | Slowest shard p99 ms |');
console.log('|------|----------|----------------|--------------|--------------|------------------|-----------------|------------------|---------------------------|----------------------|----------------------|-----------------------|---------------------|----------------------|');
for (const row of rows) {
  console.log(`| ${row.cell} | ${row.requests} | ${row.dominant_stage} | ${row.dominant_stage_p95_ms} | ${row.dominant_stage_p99_ms} | ${row.top_stage_p99_ms} | ${row.dominant_inclusive_stage} | ${row.dominant_inclusive_stage_p99_ms} | ${row.meta_index_propose_count_p99_receiver}/${row.meta_index_propose_count_p99_coordinator} | ${row.forward_attempts_p99} | ${row.leader_hint_used_p99} | ${row.not_leader_retries_p99} | ${row.forwarded_bytes_p99} | ${row.slowest_shard_p99_ms} |`);
}

const outPath = path.join('benchmarks', 'put-trace-report.json');
fs.writeFileSync(outPath, JSON.stringify(rows, null, 2));
console.log(`\nWrote ${outPath}`);
