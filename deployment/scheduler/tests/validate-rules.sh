#!/usr/bin/env bash
# Validates the scheduler PrometheusRule recording rules.
#
# Recording rules fail silently. A metric name that does not exist and a
# `sum by (...)` on a label that does not exist are both valid PromQL.
# Prometheus returns no series, or one collapsed series, and reports no error.
# `promtool check rules` reports SUCCESS on such rules, so a lint pass alone
# is insufficient. Only unit tests that assert the recorded output detect
# this class of bug. This script renders the chart, lints the rendered rules,
# and then runs every *_test.yaml in this directory against them.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="$(dirname "${SCRIPT_DIR}")"
RULES_FILE="scheduler-prometheusrule.rules.yaml"

for tool in helm yq promtool; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "validate-rules.sh: required tool not found: ${tool}" >&2
    exit 1
  fi
done

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

# The chart requires image tags, an ingress class, cluster issuers and
# hostnames to render at all. The values below are placeholders. Only the
# PrometheusRule template is emitted, and its .spec is a plain Prometheus
# rules file. The namespace is pinned because the test fixtures select
# namespace="default".
helm template armada-scheduler "${CHART_DIR}" \
  --namespace default \
  --show-only templates/scheduler-prometheusrule.yaml \
  --set scheduler.prometheus.enabled=true \
  --set scheduler.prometheus.scrapeInterval=30s \
  --set scheduler.image.tag=test \
  --set ingester.image.tag=test \
  --set scheduler.ingressClass=nginx \
  --set scheduler.clusterIssuer=test-issuer \
  --set 'scheduler.hostnames={scheduler.example.test}' \
  --set scheduler.applicationConfig.profiling.clusterIssuer=test-issuer \
  --set 'scheduler.applicationConfig.profiling.hostnames={scheduler-profiling.example.test}' \
  --set ingester.applicationConfig.profiling.clusterIssuer=test-issuer \
  --set 'ingester.applicationConfig.profiling.hostnames={ingester-profiling.example.test}' \
  | yq '.spec' > "${WORK_DIR}/${RULES_FILE}"

promtool check rules "${WORK_DIR}/${RULES_FILE}"

# promtool resolves rule_files relative to the test file, so the tests run
# from a copy next to the rendered rules.
for test_file in "${SCRIPT_DIR}"/*_test.yaml; do
  cp "${test_file}" "${WORK_DIR}/"
  promtool test rules "${WORK_DIR}/$(basename "${test_file}")"
done
