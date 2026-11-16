#!/usr/bin/env bash
# Stream lint diagnostics and preserve its status, even if the summary is unavailable.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../../internal/lookoutui"
output=$(mktemp)
trap 'rm -f "$output"' EXIT

lint_status=0
yarn lint 2>&1 | tee "$output" || lint_status=${PIPESTATUS[0]}

# Summaries are optional. Keep full output in the log; bound the summary to 64 KiB.
if [[ -n ${GITHUB_STEP_SUMMARY:-} ]]; then
  if ! (
    printf '## 🪧 TypeScript Lint Results\n\n'
    if (( lint_status == 0 )); then
      printf '### No lint issues found.\n\n'
    else
      printf '### Lint failed (exit status %s).\n\n' "$lint_status"
    fi
    if (( $(wc -c < "$output") > 65536 )); then
      printf 'Output truncated to the last 64 KiB; see the job log for full diagnostics.\n\n'
    fi
    # Indented code keeps arbitrary backticks literal without choosing a fence.
    tail -c 65536 "$output" | sed 's/^/    /'
    printf '\n'
  ) >> "$GITHUB_STEP_SUMMARY"; then
    printf 'Warning: could not write the TypeScript lint summary.\n' >&2
  fi
fi

exit "$lint_status"
