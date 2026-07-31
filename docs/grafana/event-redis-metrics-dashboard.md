# Redis Event Stream Metrics Dashboard

This dashboard uses the Redis event stream metrics emitted by the event ingester
under the `armada_event_redis_` prefix.

## Dashboard variables

| Variable | Query / Values | Description |
|---|---|---|
| `queue` | `label_values(armada_event_redis_queue_memory_bytes_total, queue)` | Queue selector for drill-down panels |
| `top_count` | Custom: `5,10,20,50` | Number of top rows to show |

## Panels

### 1. Total Redis memory used by Armada event streams

- **Type:** Stat
- **Query:**

```promql
sum(armada_event_redis_queue_memory_bytes_total)
```

### 2. Total number of event streams

- **Type:** Stat
- **Query:**

```promql
sum(armada_event_redis_queue_streams_total)
```

### 3. Total number of events across all streams

- **Type:** Stat
- **Query:**

```promql
sum(armada_event_redis_queue_events_total)
```

### 4. Top queues by memory

- **Type:** Bar gauge
- **Query:**

```promql
topk($top_count, sum(armada_event_redis_queue_memory_bytes_total) by (queue))
```

### 5. Top queues by event count

- **Type:** Bar gauge
- **Query:**

```promql
topk($top_count, sum(armada_event_redis_queue_events_total) by (queue))
```

### 6. Top queues by stream count

- **Type:** Bar gauge
- **Query:**

```promql
topk($top_count, sum(armada_event_redis_queue_streams_total) by (queue))
```

### 7. Queue memory share

- **Type:** Pie chart
- **Query:**

```promql
sum(armada_event_redis_queue_memory_bytes_total) by (queue)
```

### 8. Top jobsets by memory

- **Type:** Table
- **Query:**

```promql
topk($top_count, armada_event_redis_stream_memory_bytes)
```

Columns to display: `queue`, `jobset`, `Value`

### 9. Top jobsets by event count

- **Type:** Table
- **Query:**

```promql
topk($top_count, armada_event_redis_stream_event_count)
```

Columns to display: `queue`, `jobset`, `Value`

### 10. Top jobsets by age

- **Type:** Table
- **Query:**

```promql
topk($top_count, armada_event_redis_stream_age_seconds)
```

Columns to display: `queue`, `jobset`, `Value`

### 11. Top jobsets in selected queue

- **Type:** Table
- **Query:**

```promql
topk($top_count, armada_event_redis_stream_memory_bytes{queue="$queue"})
```

Columns to display: `jobset`, `Value`

### 12. Stream memory distribution

- **Type:** Heatmap
- **Query:**

```promql
rate(armada_event_redis_stream_size_bytes_distribution_bucket[5m])
```

### 13. Stream event count distribution

- **Type:** Heatmap
- **Query:**

```promql
rate(armada_event_redis_stream_size_events_distribution_bucket[5m])
```

### 14. Stream age distribution

- **Type:** Heatmap
- **Query:**

```promql
rate(armada_event_redis_stream_age_seconds_distribution_bucket[5m])
```

### 15. Metrics collection duration

- **Type:** Time series
- **Query:**

```promql
rate(armada_event_redis_metrics_collection_duration_seconds_sum[5m])
/
rate(armada_event_redis_metrics_collection_duration_seconds_count[5m])
```

### 16. Metrics collection errors

- **Type:** Time series
- **Query:**

```promql
rate(armada_event_redis_metrics_errors_total[5m])
```

### 17. Time since last successful collection

- **Type:** Time series
- **Query:**

```promql
time() - armada_event_redis_metrics_last_collection_timestamp
```

### 18. Number of streams scanned

- **Type:** Time series
- **Query:**

```promql
armada_event_redis_metrics_streams_scanned_total
```

## Useful derived queries

### Average event size per queue

```promql
avg(
  armada_event_redis_stream_memory_bytes / armada_event_redis_stream_event_count
) by (queue)
```

### Memory share of a selected queue

```promql
sum(armada_event_redis_queue_memory_bytes_total{queue="$queue"})
/
sum(armada_event_redis_queue_memory_bytes_total)
```

### Top jobsets by memory share

```promql
armada_event_redis_stream_memory_bytes
/
sum(armada_event_redis_stream_memory_bytes)
```

## Notes

- The top jobset metrics are a **global** top-N. If a single queue dominates, the top rows may all belong to that queue.
- All per-stream metrics are cleared each collection cycle, so stale jobsets disappear automatically.
- The collector only runs on the leader ingester; metrics are empty on non-leaders.
