#!/usr/bin/env bash
# openpuffer MinIO scaling snapshot JSON schema_version.
# Canonical value: benchmarks/report/OP_SCALING_JSON_SCHEMA_VERSION
op_scaling_json_schema_version() {
  if [[ -z "${OP_SCALING_JSON_SCHEMA_VERSION:-}" ]]; then
    local root="${OP_SCALING_JSON_ROOT:-${ROOT:-}}"
    [[ -n "$root" ]] || {
      echo "op_scaling_json_schema_version: ROOT not set" >&2
      return 1
    }
    OP_SCALING_JSON_SCHEMA_VERSION="$(
      tr -d '\n' <"${root}/benchmarks/report/OP_SCALING_JSON_SCHEMA_VERSION"
    )"
    export OP_SCALING_JSON_SCHEMA_VERSION
  fi
  printf '%s' "$OP_SCALING_JSON_SCHEMA_VERSION"
}