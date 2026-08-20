#!/usr/bin/env bash
# Stages DRA artifacts into artifacts/ for the dra-prep-buildkite-plugin.
set -euo pipefail

WORKFLOW="${DRA_WORKFLOW:?DRA_WORKFLOW is required}"
VERSION=$(cat "${PROJECT_ROOT}/app/connectors_service/connectors/VERSION")
PROJECT_NAME="connectors"
DOCKER_ARTIFACT_KEY="elastic-connectors-docker"

WORKFLOW_SUFFIX=""
if [[ "${WORKFLOW}" == "snapshot" ]]; then
  WORKFLOW_SUFFIX="-SNAPSHOT"
elif [[ "${WORKFLOW}" != "staging" ]]; then
  echo "Only snapshot or staging workflows are supported" >&2
  exit 1
fi

echo "--- :package: Building zip artifact"
# Run before downloading Python artifacts — make clean deletes dist/ in both subprojects
make clean zip

echo "--- :compression: Downloading ${WORKFLOW} artifacts"

mkdir -p artifacts/

# Docker tarballs — required; the step depends_on the docker test steps so these must exist
buildkite-agent artifact download '.artifacts/*.tar.gz' . --step build_docker_image_amd64
buildkite-agent artifact download '.artifacts/*.tar.gz' . --step build_docker_image_arm64

# Dependency CSV — built by build_python_packages step
buildkite-agent artifact download 'app/connectors_service/dist/dependencies.csv' . --step build_python_packages

cp "elasticsearch_connectors-${VERSION}.zip" "artifacts/${PROJECT_NAME}-${VERSION}${WORKFLOW_SUFFIX}.zip"

echo "--- :package: Staging ${WORKFLOW} artifacts"

# Docker tarballs — rename to DRA layout and add workflow suffix
mv ".artifacts/${DOCKER_ARTIFACT_KEY}-${VERSION}-amd64.tar.gz" \
   "artifacts/${PROJECT_NAME}-${VERSION}${WORKFLOW_SUFFIX}-docker-image-linux-amd64.tar.gz"
mv ".artifacts/${DOCKER_ARTIFACT_KEY}-${VERSION}-arm64.tar.gz" \
   "artifacts/${PROJECT_NAME}-${VERSION}${WORKFLOW_SUFFIX}-docker-image-linux-arm64.tar.gz"

# Dependency CSV — named to match the DRA convention expected by
# unified-release's release-manager DSL (dependencies-{VERSION}[-SNAPSHOT].csv).
cp "app/connectors_service/dist/dependencies.csv" \
   "artifacts/dependencies-${VERSION}${WORKFLOW_SUFFIX}.csv"

chmod -R a+r artifacts/
chmod -R a+w artifacts/

if ! ls artifacts/* 1>/dev/null 2>&1; then
  echo "ERROR: no ${WORKFLOW} artifacts found in artifacts/" >&2
  exit 1
fi

echo "Staged artifacts:"
ls -1 artifacts/
