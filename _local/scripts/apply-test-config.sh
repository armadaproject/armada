#!/usr/bin/env bash
# Config hook of the testsuite for the stack of _local/compose/full.yaml. The only argument is a directory with one
# <component>.yaml file per component. An empty directory removes all overrides.
#
# The script recreates only the components whose overrides change. A component logs an unused key only as a warning,
# so the script fails when a recreated component does not use a key of its overrides.
# After a failure, the files and the running components can differ, so the next call recreates all components.
#
# Usage: apply-test-config.sh <directory>
set -euo pipefail

source_dir=$1
local_dir=$(cd "$(dirname "$0")/.." && pwd)
target_dir=$local_dir/.test-config
components=(server scheduler scheduleringester executor eventingester lookoutingester lookout binoculars)
compose=(docker compose -f "$local_dir/compose/full.yaml")
dirty=$target_dir/.dirty

recreate_all=false
[ -f "$dirty" ] && recreate_all=true

# full.yaml publishes, probes and connects to fixed ports, so an override cannot change a port.
for file in "$source_dir"/*.yaml; do
  [ -e "$file" ] || continue
  component=$(basename "$file" .yaml)
  if [[ ! " ${components[*]} " == *" $component "* ]]; then
    echo "apply-test-config: no component $component in full.yaml, expected one of: ${components[*]}" >&2
    exit 1
  fi
  port_keys=$(jq -r 'paths | map(tostring) | select(last | test("^port$|Port$")) | join(".")' "$file")
  if [ -n "$port_keys" ]; then
    echo "apply-test-config: $(basename "$file") overrides a port ($(echo $port_keys)), and the stack uses fixed ports" >&2
    exit 1
  fi
done

# From here on the script changes files and containers, so a failure marks the state as unknown.
trap 'status=$?; if [ $status -ne 0 ]; then touch "$dirty"; fi' EXIT

changed=()
for component in "${components[@]}"; do
  new=$source_dir/$component.yaml
  current=$target_dir/$component.yaml
  if [ -f "$new" ]; then
    if cmp -s "$new" "$current" && [ "$recreate_all" = false ]; then
      continue
    fi
    cp "$new" "$current"
  else
    if [ ! -f "$current" ] && [ "$recreate_all" = false ]; then
      continue
    fi
    rm -f "$current"
  fi
  changed+=("$component")
done

if [ ${#changed[@]} -eq 0 ]; then
  rm -f "$dirty"
  exit 0
fi

since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "apply-test-config: recreating ${changed[*]}"
"${compose[@]}" up -d --no-deps --force-recreate --wait --wait-timeout 180 "${changed[@]}"

# The base configs can have unused keys too, so the check looks only at keys from the overrides. A component logs an
# unused key as a dotted path with a lower-case leaf. The path stops at the first level that the config does not know.
for component in "${changed[@]}"; do
  overrides=$target_dir/$component.yaml
  [ -f "$overrides" ] || continue
  unused=$("${compose[@]}" logs --no-log-prefix --since "$since" "$component" | sed -n 's/.*Unused keys: \[\(.*\)\].*/\1/p' | tail -1)
  [ -n "$unused" ] || continue
  paths=$(jq -r 'paths(scalars) | map(tostring) | join(".") | ascii_downcase' "$overrides")
  for key in $unused; do
    key=$(echo "$key" | tr '[:upper:]' '[:lower:]')
    while read -r path; do
      if [ "$path" = "$key" ] || [[ "$path" == "$key".* ]]; then
        echo "apply-test-config: $component does not use the override key $key, see $overrides" >&2
        exit 1
      fi
    done <<<"$paths"
  done
done

rm -f "$dirty"
