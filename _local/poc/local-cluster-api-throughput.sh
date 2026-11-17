#!/usr/bin/env bash
#
# Absolute max pod-create throughput through the Kubernetes API on a local
# cluster, sweeping concurrency to find where the rate plateaus.
#
# Unlike dev-cluster-insert-bench.sh, pods are NOT pinned to a fake node: they
# schedule and actually run, so scheduler/kubelet load is part of the number and
# the cluster only runs what fits (the rest stay Pending; creates still count).
#
# The workload is inert by design: registry.k8s.io/pause, 100m CPU / 64Mi,
# RestartPolicy Never, no volumes, no host namespaces, no privileges. It sleeps
# and exits with the namespace. Never override IMAGE with a real workload.
#
#   CONFIRM=yes ./_local/poc/local-cluster-api-throughput.sh
#   CONFIRM=yes COUNT=2000 CONCURRENCY="32 64 128" ./_local/poc/local-cluster-api-throughput.sh
#
# Everything else (sizes, cleanup, summary) comes from
# dev-cluster-insert-bench.sh; see its doc for reading summary.csv.

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

exec ./_local/poc/dev-cluster-insert-bench.sh
