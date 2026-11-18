#!/usr/bin/env bash
#
# Absolute max pod-create throughput through the Kubernetes API, sweeping
# concurrency to find where the rate plateaus.
#
# This does not start or use a local cluster. bulk-bench reads the kubeconfig
# and sends HTTPS creates straight to clusters[].cluster.server; kubectl is only
# used for namespaces and cleanup. Pass the target context as the only argument:
#
#   ./_local/poc/api-throughput-bench.sh <context>
#
# The preflight prints the resolved context, server and kube-system UID and
# refuses without CONFIRM=yes. EXPECT_CLUSTER_UID=<uid> additionally pins the
# cluster fingerprint (a stable per-cluster value, unlike a context name).
# See the available names with: kubectl config get-contexts -o name
#
# Pods are NOT pinned to a fake node: they schedule and actually run, so
# scheduler/kubelet load is part of the number and the cluster only runs what
# fits (the rest stay Pending; creates still count).
#
# The workload is inert by design: registry.k8s.io/pause, 100m CPU / 64Mi,
# RestartPolicy Never, no volumes, no host namespaces, no privileges. It sleeps
# and is deleted afterwards. Never override IMAGE with a real workload.
#
# NAMESPACE=<existing> uses a namespace you already have: nothing is created or
# deleted at namespace level, only uniquely named pods. Empty (default) creates
# a fresh ins-bench-<tag> namespace per run and removes it afterwards.
#
#   CONFIRM=yes ./_local/poc/api-throughput-bench.sh <context>
#   CONFIRM=yes COUNT=2000 CONCURRENCY="32 64 128" ./_local/poc/api-throughput-bench.sh <context>
#
# Results: $OUT/summary.csv plus $OUT/benchmark.md (table + verdict); the path
# is printed at the end.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

export NODE="" # scheduler-placed: pods run
export SIZES=${SIZES:-"small large"}
export CONCURRENCY=${CONCURRENCY:-"1 8 32 64 128"} # 1 is the sequential baseline
export RATES=""       # unpaced: this measures the ceiling, not a target
export PACE_SIZES=""
export TARGET_RATE=0  # no rate target: the achieved max is the result
# Keep a local cluster alive: hundreds of running pause pods are enough to find
# the plateau. Raise deliberately, and only with CONFIRM=yes.
export COUNT=${COUNT:-500}

if [[ "${IMAGE:-registry.k8s.io/pause:3.9}" != registry.k8s.io/pause:* && "${ALLOW_NONPAUSE_IMAGE:-0}" != "1" ]]; then
  echo "refusing IMAGE=$IMAGE: these pods actually run; use the pause image" >&2
  echo "set ALLOW_NONPAUSE_IMAGE=1 only if you are certain the image is harmless" >&2
  exit 1
fi

exec ./_local/poc/dev-cluster-insert-bench.sh "$@"
