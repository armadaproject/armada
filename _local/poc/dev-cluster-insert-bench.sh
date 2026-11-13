#!/usr/bin/env bash
#
# Measure pod insert rate into a real dev cluster through the Kubernetes API,
# in COUNT-sized batches, and what that load does to the kube-apiserver.
#
# Read _local/poc/dev-cluster-insert-bench.md before the first run.
#
#   SMOKE=1 ./_local/poc/dev-cluster-insert-bench.sh          # 500 pods, sanity
#   CONFIRM=yes ./_local/poc/dev-cluster-insert-bench.sh      # full matrix
#
# Everything is tunable through the environment; see the tunables below.

set -euo pipefail

# Relative paths (OUT included) resolve against the repository root.
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

# --- tunables ---------------------------------------------------------------
COUNT=${COUNT:-20000}            # pods per measured batch
SIZES=${SIZES:-"small medium large"}
CONCURRENCY=${CONCURRENCY:-"32 64"}
RATES=${RATES:-"1600"}           # paced runs, to test a sustained target rate
PACE_SIZES=${PACE_SIZES:-"medium"}
PACED_CONCURRENCY=${PACED_CONCURRENCY:-64}
COOLDOWN=${COOLDOWN:-20}         # seconds between runs, lets etcd settle
DELETE_CONCURRENCY=${DELETE_CONCURRENCY:-64}
CLEANUP_TIMEOUT=${CLEANUP_TIMEOUT:-900}
BENCH_TIMEOUT=${BENCH_TIMEOUT:-15m}
NODE=${NODE:-bench-nonexistent}  # pods stay Pending: no kubelet/scheduler noise
IMAGE=${IMAGE:-registry.k8s.io/pause:3.9}
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
  kubectl version >/dev/null 2>&1 || die "kubectl cannot reach a cluster"

  local context server
  context=$(kubectl config current-context)
  server=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
  log "context: $context"
  log "server:  $server"

  local runs=0
  for _ in $SIZES; do for _ in $CONCURRENCY; do runs=$((runs + 1)); done; done
  for _ in $PACE_SIZES; do for _ in $RATES; do runs=$((runs + 1)); done; done

  log "plan: $runs runs x $COUNT pods = $((runs * COUNT)) creates (plus as many deletes)"
  log "pods bound to node '$NODE' stay Pending; scheduler and kubelet are out of the measurement"

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

ensure_namespace() {
  local ns=$1
  kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  # PSA restricted namespaces reject a minimal pause pod; benchmark pods are
  # unschedulable and never run, so privileged enforcement is fine here.
  kubectl label namespace "$ns" pod-security.kubernetes.io/enforce=privileged --overwrite >/dev/null 2>&1 || true
}

wait_namespace_gone() {
  local ns=$1 waited=0
  while kubectl get namespace "$ns" -o name >/dev/null 2>&1; do
    if (( waited >= CLEANUP_TIMEOUT )); then
      log "warning: namespace $ns still terminating after ${CLEANUP_TIMEOUT}s; later runs may be contaminated"
      return 0
    fi
    sleep 5
    waited=$((waited + 5))
  done
}

# run_one <size-label> <concurrency> <rate>
run_one() {
  local size=$1 conc=$2 rate=$3
  local bytes ns tag out logfile status rate_label
  bytes=$(annotation_bytes_for_size "$size")
  rate_label="unpaced"
  [[ "$rate" != "0" ]] && rate_label="${rate}/s"

  if [[ "$rate" == "0" ]]; then
    tag="${size}-c${conc}"
  else
    tag="${size}-r${rate}-c${conc}"
  fi
  ns="ins-bench-${tag}"
  # DNS label: keep it inside 63 chars for long size labels.
  ns=${ns:0:63}
  out="$OUT/${tag}.json"
  logfile="$OUT/${tag}.log"

  log "=== $tag: annotations=${bytes}B concurrency=$conc rate=$rate_label ns=$ns"
  ensure_namespace "$ns"

  set +e
  "$OUT/bulk-bench" -mode=api -count="$COUNT" -concurrency="$conc" -rate="$rate" \
    -namespace="$ns" -name-prefix="bench-${tag}" -node-name="$NODE" -image="$IMAGE" \
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
    -namespace="$ns" -name-prefix="bench-${tag}" -timeout="$BENCH_TIMEOUT" \
    -output="$OUT/cleanup/${tag}-delete.json" 2>&1 | tee "$OUT/cleanup/${tag}-delete.log"
  set -e
  # Remaining objects (events, the namespace itself) go through the controller;
  # with the pods already gone this is quick.
  kubectl delete namespace "$ns" --wait=false >/dev/null 2>&1 || true
  wait_namespace_gone "$ns"
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
# such; the doc has the full diagnosis table.
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

main() {
  preflight

  # Canary: one pod, so an admission rejection or a broken path fails fast and
  # cheaply instead of after a 20k run.
  log "canary: creating one pod in ins-bench-canary"
  ensure_namespace ins-bench-canary
  if ! "$OUT/bulk-bench" -mode=api -count=1 -concurrency=1 -namespace=ins-bench-canary \
      -name-prefix bench-canary -node-name="$NODE" -image="$IMAGE" \
      -apiserver-metrics=false -timeout=2m 2>&1 | tee "$OUT/canary.log"; then
    kubectl delete namespace ins-bench-canary --wait=false >/dev/null 2>&1 || true
    die "canary create failed; check admission policies and $OUT/canary.log"
  fi
  kubectl delete namespace ins-bench-canary --wait=false >/dev/null 2>&1 || true
  wait_namespace_gone ins-bench-canary

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
  log "raw JSON and logs are in $OUT"
  log "to stop anything left behind: kubectl get ns | grep ins-bench"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
