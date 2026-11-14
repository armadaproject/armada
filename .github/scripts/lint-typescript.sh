#!/usr/bin/env bash
# Run from internal/lookoutui. Keep lint's status after writing its diagnostics.
set -euo pipefail

lint_status=0
yarn lint > lint_results.txt 2>&1 || lint_status=$?

{
  printf '## 🪧 TypeScript Lint Results\n\n'
  if (( lint_status == 0 )); then
    printf '### No lint issues found.\n\n'
  else
    printf '### Lint failed (exit status %s).\n\n' "$lint_status"
  fi
  printf '```text\n'
  cat lint_results.txt
  printf '\n```\n'
} >> "$GITHUB_STEP_SUMMARY"

cat lint_results.txt
exit "$lint_status"
