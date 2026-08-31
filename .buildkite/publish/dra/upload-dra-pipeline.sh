#!/usr/bin/env bash
# Generates and uploads the DRA prep + trigger steps with STACK_VERSION resolved
# from the VERSION file. Emits snapshot steps always; staging steps only on
# version branches (not main).
set -euo pipefail

STACK_VERSION="$(cat connectors/VERSION)"
BUILDKITE_BRANCH="${BUILDKITE_BRANCH:-}"

emit_dra_pair() {
  local workflow="$1"
  cat <<EOF
  - label: ":package: DRA Prep (${workflow})"
    key: "dra-prep-${workflow}"
    command: ".buildkite/publish/dra/stage_artifacts.sh"
    env:
      DRA_WORKFLOW: "${workflow}"
      PROJECT_ROOT: "."
    agents:
      provider: "gcp"
      image: family/enterprise-search-ubuntu-2204-connectors-py
      machineType: "n1-standard-4"
    plugins:
      - elastic/dra-prep#v0.1.5:
          product_id: "connectors"
          stack_version: "${STACK_VERSION}"
          workflow: "${workflow}"

  - label: ":pipeline: Trigger DRA processing (${workflow})"
    trigger: "unified-release-dra-processing"
    depends_on: "dra-prep-${workflow}"
    build:
      env:
        DRA_PRODUCT_ID: "connectors"
        DRA_STACK_VERSION: "${STACK_VERSION}"
        DRA_WORKFLOW: "${workflow}"
EOF
}

echo "steps:"

# Snapshot: main and version branches only (e.g. 9.5, 8.19)
if [[ "${BUILDKITE_BRANCH}" =~ ^(main|[0-9]+\.[0-9x]+)$ ]]; then
  emit_dra_pair "snapshot"
fi

# Staging: version branches only (not main)
if [[ "${BUILDKITE_BRANCH}" =~ ^[0-9]+\.[0-9x]+$ ]]; then
  emit_dra_pair "staging"
fi
