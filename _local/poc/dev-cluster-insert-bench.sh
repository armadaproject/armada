#!/usr/bin/env bash
#
# Measure pod insert rate into a real dev cluster through the Kubernetes API,
# in COUNT-sized batches, and what that load does to the kube-apiserver.
#
# Read _local/poc/dev-cluster-insert-bench.md before the first run.
#
#   NAMESPACE=bench SMOKE=1 CONFIRM=yes ./_local/poc/dev-cluster-insert-bench.sh  # 500 pods, sanity
#   NAMESPACE=bench CONFIRM=yes ./_local/poc/dev-cluster-insert-bench.sh          # full matrix
#   NAMESPACE=bench CONFIRM=yes ./_local/poc/dev-cluster-insert-bench.sh <context>
#   NAMESPACE=bench EXPECT_CLUSTER_UID=<uid> CONFIRM=yes ./_local/poc/dev-cluster-insert-bench.sh <context>
#
# NAMESPACE is required and must already exist: the script never creates or
# deletes namespaces, only the pods it names.
#
# Everything is tunable through the environment; see the tunables below.

set -euo pipefail

# Relative paths (OUT included) resolve against the repository root.
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

# --- tunables ---------------------------------------------------------------
COUNT=${COUNT:-20000}            # pods per measured batch
SIZES=${SIZES:-"small medium large"}
CONCURRENCY=${CONCURRENCY:-"32 64"}
RATES=${RATES-"1600"}            # paced runs, to test a sustained target rate; empty skips
PACE_SIZES=${PACE_SIZES-"medium"}
PACED_CONCURRENCY=${PACED_CONCURRENCY:-64}
COOLDOWN=${COOLDOWN:-20}         # seconds between runs, lets etcd settle
DELETE_CONCURRENCY=${DELETE_CONCURRENCY:-64}
BENCH_TIMEOUT=${BENCH_TIMEOUT:-15m}
NODE=${NODE-bench-nonexistent}    # set empty to let pods schedule and actually run
IMAGE=${IMAGE:-registry.k8s.io/pause:3.9}
NAMESPACE=${NAMESPACE:-}          # existing namespace to use; required, never created
RUN_ID=${RUN_ID:-$(date +%Y%m%d-%H%M%S)}  # keeps pod names unique across invocations
OUT=${OUT:-/tmp/armada-insert-bench-$(date +%Y%m%d-%H%M%S)}
SMOKE=${SMOKE:-0}

if [[ "$SMOKE" == "1" ]]; then
  COUNT=500
  SIZES="small"
  CONCURRENCY="8"
  RATES=""
fi

# Pod object size is padded with one annotation; the dry run prints the encoded
# bytes/pod so you can see what you are actually writing.
annotation_bytes_for_size() {
  case "$1" in
    small)  echo 0 ;;
    medium) echo 4096 ;;
    large)  echo 32768 ;;
    *)      echo "$1" ;; # numeric byte count passes straight through
  esac
}

log() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

require_tools() {
  for tool in kubectl go; do
    command -v "$tool" >/dev/null 2>&1 || die "missing $tool"
  done
}

preflight() {
  require_tools
  [[ -n "$NAMESPACE" ]] ||
    die "set NAMESPACE=<existing namespace>; this benchmark does not create namespaces"

  # A pinned context must be what both kubectl and bulk-bench use; minify it
  # into a private temp file (it holds credentials) and point both at that.
  if [[ -n "${CONTEXT:-}" ]]; then
    PINNED_KUBECONFIG=$(mktemp)
    trap 'rm -f "$PINNED_KUBECONFIG"' EXIT
    if ! kubectl config view --minify --raw --context "$CONTEXT" >"$PINNED_KUBECONFIG"; then
      printf 'error: cannot resolve context %q; available contexts:\n' "$CONTEXT" >&2
      kubectl config get-contexts -o name 2>/dev/null | sed 's/^/  /' >&2 || true
      exit 1
    fi
    export KUBECONFIG="$PINNED_KUBECONFIG"
  fi

  kubectl version >/dev/null 2>&1 || die "kubectl cannot reach a cluster"

  local context server uid
  context=$(kubectl config current-context)
  server=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
  if [[ -n "${CONTEXT:-}" && "$context" != "$CONTEXT" ]]; then
    die "resolved context '$context' does not match CONTEXT=$CONTEXT"
  fi
  # kube-system's UID is a stable per-cluster fingerprint, unlike a context name.
  uid=$(kubectl get namespace kube-system -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
  log "context: $context"
  log "server:  $server"
  log "cluster UID: ${uid:-<kube-system UID unreadable>}"
  log "namespace: $NAMESPACE"
  if [[ -n "${EXPECT_CLUSTER_UID:-}" && "$uid" != "$EXPECT_CLUSTER_UID" ]]; then
    die "cluster UID ${uid:-<unreadable>} != EXPECT_CLUSTER_UID=$EXPECT_CLUSTER_UID; refusing to run"
  fi
  # Keep the target identity for the markdown report.
  TARGET_CONTEXT=$context
  TARGET_SERVER=$server
  TARGET_UID=$uid

  local runs=0
  for _ in $SIZES; do for _ in $CONCURRENCY; do runs=$((runs + 1)); done; done
  for _ in $PACE_SIZES; do for _ in $RATES; do runs=$((runs + 1)); done; done

  log "plan: $runs runs x $COUNT pods = $((runs * COUNT)) creates (plus as many deletes)"
  if [[ -n "$NODE" ]]; then
    log "pods bound to node '$NODE' stay Pending; scheduler and kubelet are out of the measurement"
  else
    log "pods are scheduler-placed and will actually run (pause image only); scheduler and kubelet are in the measurement"
  fi

  if [[ "${CONFIRM:-}" != "yes" ]]; then
    echo
    echo "Refusing to create $((runs * COUNT)) pods on '$context' without confirmation."
    echo "Re-run with CONFIRM=yes once you are sure this is a disposable dev cluster."
    exit 1
  fi

  if kubectl get --raw /metrics >/dev/null 2>&1; then
    METRICS=true
    log "apiserver /metrics: readable"
    local apiservers
    apiservers=$(kubectl -n kube-system get pods --no-headers 2>/dev/null | grep -c apiserver || true)
    if [[ "${apiservers:-0}" -gt 1 ]]; then
      log "warning: $apiservers apiserver pods; /metrics through the API endpoint only reports one of them"
    fi
  else
    METRICS=false
    log "warning: apiserver /metrics not readable by this identity; control-plane impact will be client-side only"
  fi

  mkdir -p "$OUT"
  log "results: $OUT"
  go build -o "$OUT/bulk-bench" ./cmd/bulk-bench || die "build failed"

  # Show the encoded size each preset actually writes; "medium" is a label,
  # bytes/pod is what the apiserver has to validate and etcd has to store.
  local size bytes
  for size in $SIZES $PACE_SIZES; do
    bytes=$(annotation_bytes_for_size "$size")
    "$OUT/bulk-bench" -dry-run -count=1 -pod-annotations-bytes="$bytes" 2>/dev/null |
      sed "s/^/  [$size] /"
  done
}

# run_one <size-label> <concurrency> <rate>
run_one() {
  local size=$1 conc=$2 rate=$3
  local bytes ns tag prefix out logfile status rate_label
  bytes=$(annotation_bytes_for_size "$size")
  rate_label="unpaced"
  [[ "$rate" != "0" ]] && rate_label="${rate}/s"

  if [[ "$rate" == "0" ]]; then
    tag="${size}-c${conc}"
  else
    tag="${size}-r${rate}-c${conc}"
  fi
  ns=$NAMESPACE
  # Unique per invocation, so reruns in the namespace never collide.
  prefix="bench-${tag}-${RUN_ID}"
  out="$OUT/${tag}.json"
  logfile="$OUT/${tag}.log"

  log "=== $tag: annotations=${bytes}B concurrency=$conc rate=$rate_label ns=$ns"

  set +e
  "$OUT/bulk-bench" -mode=api -count="$COUNT" -concurrency="$conc" -rate="$rate" \
    -namespace="$ns" -name-prefix="$prefix" -node-name="$NODE" -image="$IMAGE" \
    -pod-annotations-bytes="$bytes" -apiserver-metrics="$METRICS" \
    -timeout="$BENCH_TIMEOUT" -output="$out" 2>&1 | tee "$logfile"
  status=${PIPESTATUS[0]}
  set -e
  if [[ "$status" -ne 0 ]]; then
    log "run exit=$status (see $logfile); continuing so partial results are kept"
  fi

  log "deleting $COUNT pods in $ns with concurrency $DELETE_CONCURRENCY"
  mkdir -p "$OUT/cleanup"
  set +e
  "$OUT/bulk-bench" -mode=delete -count="$COUNT" -concurrency="$DELETE_CONCURRENCY" \
    -namespace="$ns" -name-prefix="$prefix" -timeout="$BENCH_TIMEOUT" \
    -output="$OUT/cleanup/${tag}-delete.json" 2>&1 | tee "$OUT/cleanup/${tag}-delete.log"
  set -e
  log "cooling down ${COOLDOWN}s"
  sleep "$COOLDOWN"
}

summarize() {
  local csv="$OUT/summary.csv"
  printf 'run,pods,conc,rate,annotBytes,podsPerSecond,clientP50Ms,clientP99Ms,postRate,postMeanMs,etcdCreateMeanMs,webhookMs,inflightPeak,cpuCores,code201,code429,code5xx,apfRejected\n' > "$csv"
  local file
  for file in "$OUT"/*.json; do
    [[ -e "$file" ]] || continue
    awk -v run="$(basename "$file" .json)" '
      /^[[:space:]]*"(count|concurrency|rateLimit|podAnnotationsBytes|podsPerSecond|clientP50Ms|clientP99Ms|postRatePerSecond|postMeanMs|etcdCreateMeanMs|webhookAdmitMeanMs|inflightMutatingPeak|cpuCoresMean|code201|code429|code5xx|apfRejected)":/ {
        key = $0; sub(/^[[:space:]]*"/, "", key); sub(/":.*/, "", key)
        value = $0; sub(/^[^:]*:[[:space:]]*/, "", value); sub(/,$/, "", value); gsub(/"/, "", value)
        m[key] = value
      }
      END {
        printf "%s,%s,%s,%s,%s,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.0f,%.2f,%s,%s,%s,%s\n",
          run, m["count"], m["concurrency"], m["rateLimit"], m["podAnnotationsBytes"],
          m["podsPerSecond"], m["clientP50Ms"], m["clientP99Ms"],
          m["postRatePerSecond"], m["postMeanMs"], m["etcdCreateMeanMs"], m["webhookAdmitMeanMs"],
          m["inflightMutatingPeak"], m["cpuCoresMean"],
          m["code201"], m["code429"], m["code5xx"], m["apfRejected"]
      }' "$file" >> "$csv"
  done
  log "summary written to $csv"
  column -t -s, "$csv" 2>/dev/null || cat "$csv"
}

# Reads summary.csv and states whether the target rate was reached, and whether
# it was sustainable. Cause hints are heuristics from the same row, labelled as
# such; the doc has the full diagnosis table. TARGET_RATE=0 disables the target
# check, for sweeps whose point is the achieved rate itself (sequential writes).
verdict() {
  local csv="$OUT/summary.csv" target=${TARGET_RATE:-1600}
  [[ -s "$csv" ]] || return 0
  awk -F, -v target="$target" '
    NR == 1 { next }
    {
      run=$1; conc=$3; rate=$4+0; pps=$6+0; p99=$8+0
      postmean=$10+0; etcd=$11+0; webhook=$12+0; cpu=$14+0
      c429=$16+0; c5xx=$17+0; apf=$18+0
      if (rate == 0) {
        if (best == "" || pps > bestps) {
          best=run; bestps=pps; bestconc=conc
          bestpost=postmean; bestetc=etcd; bestweb=webhook
          bestcpu=cpu; best429=c429; best5xx=c5xx; bestapf=apf; bestp99=p99
        }
      } else if (rate == target) {
        paced=1; pacedpps=pps; paced429=c429; paced5xx=c5xx; pacedp99=p99
      }
    }
    END {
      print ""
      print "=== verdict ==="
      if (best == "") { print "no unpaced runs to judge"; exit }
      printf "best unpaced: %.0f pods/s (%s, concurrency %s)\n", bestps, best, bestconc
      if (target <= 0) exit
      printf "target %.0f/s: %s at peak\n", target, (bestps >= target ? "REACHED" : "NOT reached")
      if (paced) {
        sustained = (pacedpps >= target * 0.95 && paced429 == 0 && paced5xx == 0)
        printf "paced %.0f/s: achieved %.0f/s, 429=%d 5xx=%d p99=%.0fms -> %s\n",
          target, pacedpps, paced429, paced5xx, pacedp99, (sustained ? "sustained" : "DEGRADED")
      } else {
        print "paced target: not tested (RATES is empty)"
      }
      if (bestps < target || (paced && !sustained)) {
        print "likely limit (heuristic):"
        if (bestpost == 0) {
          print "  - apiserver metrics were unavailable; rerun with /metrics access to localize it"
        } else if (best429 + bestapf + paced429 > 0) {
          print "  - server throttling (429/APF): raise the priority/fairness budget or lower the target"
        } else if (bestweb > bestpost * 0.10) {
          print "  - admission webhooks: they run on every create"
        } else if (bestetc > bestpost * 0.50) {
          print "  - apiserver->etcd latency: etcd is the bottleneck"
        } else {
          print "  - apiserver CPU/admission: compare cpuCores to the apiserver container limit"
        }
        if (bestp99 > 3 * bestpost) {
          printf "  - client p99 (%.0fms) is far above the server mean; the cap may be the client or network\n", bestp99
        }
      }
    }' "$csv"
}

# Writes the table and verdict as a self-contained markdown report, so the run
# can be attached to a PR or issue without the scratch directory.
markdown_report() {
  local csv="$OUT/summary.csv" report="$OUT/benchmark.md" pacing node ns_note
  [[ -s "$csv" ]] || return 0
  if [[ -n "$RATES" ]]; then pacing="paced ($RATES/s)"; else pacing="unpaced (max throughput)"; fi
  if [[ -n "$NODE" ]]; then
    node="$NODE (Pending: scheduler/kubelet excluded)"
  else
    node="scheduler-placed (pods actually run)"
  fi
  ns_note="$NAMESPACE (existing; pods deleted by name, namespace kept)"
  {
    printf '# Pod insert benchmark — %s\n\n' "${TARGET_CONTEXT:-unknown}"
    printf -- '- Date: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf -- '- Context: `%s`\n' "${TARGET_CONTEXT:-unknown}"
    printf -- '- Server: `%s`\n' "${TARGET_SERVER:-unknown}"
    printf -- '- Cluster UID: `%s`\n' "${TARGET_UID:-unreadable}"
    printf -- '- Pods per run: %s\n' "$COUNT"
    printf -- '- Sizes: `%s` (annotation bytes), concurrency `%s`\n' "$SIZES" "$CONCURRENCY"
    printf -- '- Pacing: %s\n' "$pacing"
    printf -- '- Node binding: %s\n' "$node"
    printf -- '- Namespace: %s\n' "$ns_note"
    printf -- '- Image: `%s`\n\n' "$IMAGE"
    printf '## Results\n\n'
    awk -F, '
      BEGIN {
        print "| run | pods | conc | rate | pod bytes | pods/s | client p50 ms | client p99 ms | apiserver POST/s | POST mean ms | etcd create ms | webhook ms | inflight peak | apiserver CPU | 201 | 429 | 5xx | APF rejected |"
        print "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
      }
      NR > 1 {
        printf "| %s | %s | %s | %s | %s | %.1f | %.1f | %.1f | %.1f | %.1f | %.1f | %.1f | %.0f | %.2f | %s | %s | %s | %s |\n",
          $1, $2, $3, ($4 == "0" ? "none" : $4), $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
      }' "$csv"
    printf '\n## Verdict\n\n```\n'
    verdict
    printf '```\n'
  } >"$report"
  log "markdown report: $report"
}

main() {
  if [[ $# -gt 1 ]]; then
    printf 'usage: %s [context]\n' "${BASH_SOURCE[0]}" >&2
    exit 2
  fi
  # Optional positional argument: the kubeconfig context to target.
  if [[ $# -eq 1 ]]; then
    export CONTEXT=$1
  fi
  preflight

  # Canary: one pod, so an admission rejection or a broken path fails fast and
  # cheaply instead of after a 20k run.
  local canary_prefix="bench-canary-${RUN_ID}"
  log "canary: creating one pod in $NAMESPACE"
  if ! "$OUT/bulk-bench" -mode=api -count=1 -concurrency=1 -namespace="$NAMESPACE" \
      -name-prefix="$canary_prefix" -node-name="$NODE" -image="$IMAGE" \
      -apiserver-metrics=false -timeout=2m 2>&1 | tee "$OUT/canary.log"; then
    die "canary create failed; check admission policies and $OUT/canary.log"
  fi
  "$OUT/bulk-bench" -mode=delete -count=1 -concurrency=1 -namespace="$NAMESPACE" \
    -name-prefix="$canary_prefix" -timeout=2m >/dev/null 2>&1 ||
    log "warning: could not delete the canary pod in $NAMESPACE"

  local size conc rate
  for size in $SIZES; do
    for conc in $CONCURRENCY; do
      run_one "$size" "$conc" 0
    done
  done
  for size in $PACE_SIZES; do
    for rate in $RATES; do
      run_one "$size" "$PACED_CONCURRENCY" "$rate"
    done
  done

  summarize
  verdict
  markdown_report
  log "raw JSON and logs are in $OUT"
  log "leftover pods (if any) are in namespace $NAMESPACE"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
