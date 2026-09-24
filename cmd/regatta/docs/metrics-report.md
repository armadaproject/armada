# Metrics report

After submission finishes, `regatta run` polls Prometheus's `armada_queue_size`/`armada_queue_leased_pod_count` for the run's queue every 5s until both read 0 — nothing left waiting, nothing left leased, the closest available proxy for "every submitted job has finished" without regatta polling individual job state itself. A zero/absent reading only counts once the queue has actually been observed populated at least once, so a queue that Prometheus hasn't scraped yet right after a large submission isn't mistaken for already-drained.

Once drained, it waits a further `metrics.postRunDelay` (default `90s`) for Prometheus's own scrape interval to catch up with the drain moment, then queries Prometheus once for a fixed set of metrics — end-to-end job latency, scheduler cycle times, queue depth, API throughput/latency, and executor/Pulsar latency — writing the result as JSON to a generated filename, `regatta-result-<20060102-150405>.json` (the run's start time), inside `metrics.resultsPath` (default `.`).

The report's `start`/`end` window spans from just before the real jobs were submitted to the moment the queue was actually observed drained (or the drain-poll timeout, if it never drained) — not just how long the submission API call itself took — so the range/rate queries above cover the jobs' actual queued/running lifetime.

The report also embeds the fully-resolved scenario config under its `scenario` field (absolute paths, defaults applied — what actually ran, not just what the file said), so a results file is self-describing without needing to keep the original scenario file around.

This is a single post-run snapshot, not a live/continuous feed: regatta still never polls individual job state while a run is in progress. The drain poll itself has a generous internal safety-net timeout (10 minutes) in case the signal never arrives (e.g. Prometheus isn't scraping the relevant target), so a stuck/absent signal doesn't hang the run forever.

## Configuring it

The scenario file's `metrics:` block configures this:

```yaml
metrics:
  prometheus: http://localhost:9090
  postRunDelay: 90s
```

- `metrics.prometheus` is the base URL to query (default `http://localhost:9090`, matching `mage dev:up ...,prometheus`'s own address).
- `metrics.postRunDelay` is the fixed post-drain wait described above (default `90s`).
- `metrics.resultsPath` is the directory to write the generated report file into (default `.`, resolved relative to the scenario file's own directory). Every checked-in example sets `prometheus`/`postRunDelay` explicitly and leaves `resultsPath` at its default.

The `--metrics-results-path` CLI flag overrides `metrics.resultsPath` when explicitly passed. A failure to reach Prometheus is logged but never fails the run itself, since the run already succeeded by the time metrics collection starts.

`mage dev:up regatta,prometheus` and `mage dev:up regatta-ten-cluster,prometheus` point Prometheus at a scrape config covering every executor in that topology (`cmd/regatta/config/armada/prometheus/two-cluster.yaml`/`ten-cluster.yaml`), not just one, so the executor/Pulsar tier of the report reflects all clusters.
