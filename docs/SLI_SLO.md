# GrainFS Service Level Indicators (SLIs) and Objectives (SLOs)

## Overview

This document defines the quantitative metrics that determine if GrainFS is "production ready" for deployment evaluation.

## Target Availability: 99.9% (3-nines)

**Calculation:** (Total Time - Downtime) / Total Time

**Measurement Window:** Rolling 30 days

**Downtime Definition:** Any 5-second period where health check fails or S3 API returns 5xx errors

**Budget:** 43.2 minutes/month downtime allowed

---

## SLI 1: API Availability

**Metric:** Ratio of successful HTTP requests to total requests

**Measurement:**
```
successful_requests / total_requests
```

**Prometheus Query:**
```promql
sum(rate(grainfs_http_requests_total{status=~"2.."}[5m])) /
sum(rate(grainfs_http_requests_total[5m]))
```

**SLO:** ≥ 99.9% (monthly)

---

## SLI 2: API Latency (P99)

**Metric:** 99th percentile request latency

**Measurement:** Histogram of request durations

**Prometheus Query:**
```promql
histogram_quantile(0.99,
  sum(rate(grainfs_http_request_duration_seconds_bucket[5m])) by (le)
)
```

**SLO:** ≤ 100ms for P99 (monthly)

**Breakdown by operation:**
- PUT Object: P99 ≤ 200ms
- GET Object: P99 ≤ 50ms
- LIST Objects: P99 ≤ 500ms

---

## SLI 3: Data Durability

**Metric:** Probability of data loss

**Measurement:** Objects lost / Total objects stored

**Prometheus Query:**
```promql
sum(increase(grainfs_objects_lost_total[30d])) /
sum(grainfs_objects_total)
```

**SLO:** ≥ 99.999999999% (11-nines, annual)

**Verification:** Jepsen tests + network partition tests

---

## SLI 4: Recovery Time Objective (RTO)

**Metric:** Time to recover from node failure

**Measurement:** Time from node failure detection to cluster rejoining

**Prometheus Query:**
```promql
avg(grainfs_node_recovery_duration_seconds)
```

**SLO:** ≤ 10 minutes for single node failure

**Verification:** `grainfs recover --auto` execution time

---

## SLI 5: Recovery Point Objective (RPO)

**Metric:** Maximum data loss during failure

**Measurement:** Time of last successful write before failure

**SLO:** ≤ 1 second (Raft log replication)

**Verification:** Raft log commit latency

---

## SLI 6: Data Consistency

**Metric:** Linearizability violations detected

**Measurement:** Count of consistency anomalies

**SLO:** 0 violations

**Verification:** Jepsen test suite (must pass before deployment)

---

## Alerting Thresholds

**SEV1 (Page Immediately):**
- Availability < 99% for 5 minutes
- P99 latency > 500ms for 5 minutes
- Data loss detected

**SEV2 (Page within 15 minutes):**
- Availability < 99.5% for 15 minutes
- P99 latency > 200ms for 15 minutes
- Node unhealthy for > 5 minutes

**SEV3 (Address within 1 hour):**
- P99 latency > 100ms for 30 minutes
- Disk usage > 80%

---

## Pre-Flight Checklist (Before "Production Ready")

All tests must PASS:

```bash
# 1. Jepsen consistency tests
make test-jepsen

# 2. Network partition tests
make test-network-fault

# 3. E2E smoke tests
make test-smoke

# 4. Verify current SLO compliance
curl http://localhost:9000/metrics | grainfs_slo_compliance == 1
```

---

## How to Calculate SLO Compliance

**Example: Monthly Availability = 99.95%**

1. Total time in month: 30 days × 24 hours × 60 minutes = 43,200 minutes
2. Budget used: 21.6 minutes (50% of 43.2min budget)
3. Remaining budget: 21.6 minutes

**Error Budget Calculation:**

```
Error Budget = (1 - SLO) × Period
Error Budget for 99.9% monthly = 0.001 × 43,200 minutes = 43.2 minutes
```

---

## References

- Google SRE Book: https://sre.google/sre-book/service-level-objectives/
- SLI/SLO Best Practices: Document driven by actual metrics, not aspirations
