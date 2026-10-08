# Metrics report

After submission finishes, `regatta run` polls Prometheus's `armada_queue_size`/`armada_queue_leased_pod_count` for the run's queue every 5s until both read 0 — nothing left waiting, nothing left leased, the closest available proxy for "every submitted job has finished" without regatta polling individual job state itself. A zero/absent reading only counts once the queue has actually been observed populated at least once, so a queue that Prometheus hasn't scraped yet right after a large submission isn't mistaken for already-drained.

Once drained, it waits a further `metrics.postRunDelay` (the default is `90s`, and the checked-in examples set `90s` explicitly) for Prometheus's own scrape interval to catch up with the drain moment, then queries Prometheus once for a fixed set of metrics — end-to-end job latency, scheduler cycle times, queue depth, API throughput/latency, and executor/Pulsar latency — writing the result as JSON to a generated filename, `regatta-result-<20060102-150405>.json` (the run's start time), inside `metrics.resultsPath` (default `.`).

The report's `start`/`end` window spans from just before the real jobs were submitted to the moment the queue was actually observed drained (or the drain-poll timeout, if it never drained) — not just how long the submission API call itself took — so the range/rate queries above cover the jobs' actual queued/running lifetime.

If a target's readiness check failed and the run continued anyway (`cluster.continueOnReadinessFailure`, see `scenario-files.md`), the report lists it under `readinessFailures` as `{target, error}` entries, where `error` is the check's own message. The field is absent when every check passed or was switched off. Treat the numbers from such a run as coming from an environment the scheduler never confirmed it could place jobs on.

The report also embeds the fully-resolved scenario config under its `scenario` field (absolute paths, defaults applied — what actually ran, not just what the file said), so a results file is self-describing without needing to keep the original scenario file around.

This is a single post-run snapshot, not a live/continuous feed: regatta still never polls individual job state while a run is in progress. The drain poll itself has a generous internal safety-net timeout (10 minutes) in case the signal never arrives (e.g. Prometheus isn't scraping the relevant target), so a stuck/absent signal doesn't hang the run forever.

## Configuring it

The scenario file's `metrics:` block configures this:

```yaml
metrics:
  prometheus: http://localhost:9090
  postRunDelay: 90s
  reportInterval: 120s
  resultsPath: ../results/
```

- `metrics.prometheus` is the base URL to query (default `http://localhost:9090`, matching `mage dev:up ...,prometheus`'s own address).
- `metrics.postRunDelay` is the fixed post-drain wait described above.
- `metrics.reportInterval` (default `120s`) is how often a continuous run writes a report; see below. Bounded runs ignore it.
- `metrics.resultsPath` is the directory to write the generated report file into (default `.`, resolved relative to the scenario file's own directory). The checked-in examples (in `cmd/regatta/config/scenarios/`) set all four explicitly, with `postRunDelay: 90s`, `reportInterval: 120s` and `resultsPath: ../results/`, which puts reports in `cmd/regatta/config/results/`.

The `--metrics-results-path` CLI flag overrides `metrics.resultsPath` when explicitly passed. A failure to reach Prometheus is logged but never fails the run itself, since the run already succeeded by the time metrics collection starts.

`mage dev:up regatta,prometheus` points Prometheus at a scrape config covering every executor in that topology (`_local/prometheus/config-two-cluster.yaml`), not just one, so the executor/Pulsar tier of the report reflects all clusters.

## Continuous runs

A continuous run (`totalJobs: -1`, see `scenario-files.md`) never drains, so there is no single end to report at. Instead it writes a report every `metrics.reportInterval` and a final one when you stop it with Ctrl+C. Files are named `regatta-result-<timestamp>-00001.json`, `-00002.json`, ..., and the last `-<n>-final.json`, so they sort in the order they were written.

Each report covers only the time since the previous one (the first starts when submission does), so the periods of consecutive reports have no gap and no overlap (see `lookbackSeconds` below for the one exception to what a report's samples cover) and show how the system behaves now, not an average over the whole run. A report ends `metrics.postRunDelay` before it is written, so the metrics have reached Prometheus; the final report ends when submission stopped, after waiting that delay. Pressing Ctrl+C a second time exits at once, without writing the final report.

`windowSeconds` is the length of `start` to `end`. The rate and quantile queries look back over at least a minute, so for a window shorter than that the report also reflects samples from before `start`; `lookbackSeconds` is the lookback actually used. For a continuous run with a `metrics.reportInterval` of a minute or more every report stands on its own (`lookbackSeconds` equals `windowSeconds`); a shorter interval, and the final report of an early stop, can reach back into the previous one.

Counters and histograms (scheduled jobs, `SubmitJobs` calls and latency, errors, the scheduler and executor latencies) are measured as their growth over the window: the value at `end` minus the value at the start of the lookback. Prometheus's `rate()` and `increase()` only see growth between samples inside the window, so a one-shot load that is submitted in a few seconds, between two scrapes, would read as zero submissions. The per-job latency histograms (`queued*`, `run*`) come and go with the queues' jobs, so they still use `rate()`. `submitThroughput` is `SubmitJobs` calls per second averaged over the lookback, `submitErrors` is 0 when no call failed, and a field is left out of the report only when Prometheus has no data for it.

