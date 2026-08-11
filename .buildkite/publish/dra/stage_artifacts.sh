#!/usr/bin/env bash
# Stages DRA artifacts into artifacts/ for the dra-prep-buildkite-plugin.
set -euo pipefail

WORKFLOW="${DRA_WORKFLOW:?DRA_WORKFLOW is required}"
VERSION=$(cat "${PROJECT_ROOT}/app/connectors_service/connectors/VERSION")
PROJECT_NAME="connectors"
DOCKER_ARTIFACT_KEY="elastic-connectors"

WORKFLOW_SUFFIX=""
if [[ "${WORKFLOW}" == "snapshot" ]]; then
  WORKFLOW_SUFFIX="-SNAPSHOT"
elif [[ "${WORKFLOW}" != "staging" ]]; then
  echo "Only snapshot or staging workflows are supported" >&2
  exit 1
fi

echo "--- :compression: Downloading ${WORKFLOW} artifacts"

mkdir -p artifacts/

# Docker tarballs
buildkite-agent artifact download '.artifacts/*.tar.gz' . --step build_docker_image_amd64 || true
buildkite-agent artifact download '.artifacts/*.tar.gz' . --step build_docker_image_arm64 || true

# Python packages
buildkite-agent artifact download 'app/connectors_service/dist/*.whl' . || true
buildkite-agent artifact download 'app/connectors_service/dist/*.tar.gz' . || true
buildkite-agent artifact download 'libs/connectors_sdk/dist/*.whl' . || true
buildkite-agent artifact download 'libs/connectors_sdk/dist/*.tar.gz' . || true

echo "--- :package: Building zip artifact"
make clean zip
cp "elasticsearch_connectors-${VERSION}.zip" "artifacts/${PROJECT_NAME}-${VERSION}${WORKFLOW_SUFFIX}.zip"

echo "--- :package: Staging ${WORKFLOW} artifacts"

# Docker tarballs — rename to DRA layout and add workflow suffix
mv ".artifacts/${DOCKER_ARTIFACT_KEY}-${VERSION}-amd64.tar.gz" \
   "artifacts/${PROJECT_NAME}-${VERSION}${WORKFLOW_SUFFIX}-docker-image-linux-amd64.tar.gz" 2>/dev/null || true
mv ".artifacts/${DOCKER_ARTIFACT_KEY}-${VERSION}-arm64.tar.gz" \
   "artifacts/${PROJECT_NAME}-${VERSION}${WORKFLOW_SUFFIX}-docker-image-linux-arm64.tar.gz" 2>/dev/null || true

# Python packages — add workflow suffix
for f in app/connectors_service/dist/*.whl app/connectors_service/dist/*.tar.gz \
          libs/connectors_sdk/dist/*.whl libs/connectors_sdk/dist/*.tar.gz; do
  [[ -f "$f" ]] || continue
  filename=$(basename "$f")
  # Insert workflow suffix before the last segment (e.g. -py3-none-any.whl)
  newname="${filename/-${VERSION}-/-${VERSION}${WORKFLOW_SUFFIX}-}"
  cp "$f" "artifacts/${newname}"
done

chmod -R a+r artifacts/
chmod -R a+w artifacts/

if ! ls artifacts/* 1>/dev/null 2>&1; then
  echo "ERROR: no ${WORKFLOW} artifacts found in artifacts/" >&2
  exit 1
fi

echo "Staged artifacts:"
ls -1 artifacts/
