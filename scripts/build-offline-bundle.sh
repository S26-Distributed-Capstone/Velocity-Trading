#!/usr/bin/env bash
# Build an offline bundle for the air-gapped k3s cluster.
#
# Run this on a machine WITH internet access. Produces dist/images.tar
# containing the market-maker app image plus every base image the cluster
# needs. Copy that tarball to each k3s node and import it with:
#
#   sudo k3s ctr images import images.tar
#
# Usage:
#   ./scripts/build-offline-bundle.sh [version]
# Defaults to version 1.0.0.

set -euo pipefail

VERSION="${1:-1.0.0}"
APP_IMAGE="market-maker:${VERSION}"

BASE_IMAGES=(
  "eclipse-temurin:21-jre-alpine"
  "postgres:16-alpine"
  "zookeeper:3.9"
)

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DIST_DIR="${ROOT_DIR}/dist"
mkdir -p "${DIST_DIR}"

cd "${ROOT_DIR}"

echo "==> Building jar with Maven (online)"
if [[ -x "./mvnw" ]]; then
  ./mvnw -B -DskipTests clean package
else
  mvn -B -DskipTests clean package
fi

JAR_COUNT=$(ls target/marketmaker-*.jar 2>/dev/null | wc -l | tr -d ' ')
if [[ "${JAR_COUNT}" -eq 0 ]]; then
  echo "ERROR: no target/marketmaker-*.jar produced" >&2
  exit 1
fi

echo "==> Building Docker image ${APP_IMAGE} from Dockerfile.offline"
docker build -f Dockerfile.offline -t "${APP_IMAGE}" .

echo "==> Pulling base images"
for img in "${BASE_IMAGES[@]}"; do
  docker pull "${img}"
done

OUT="${DIST_DIR}/images.tar"
echo "==> Saving images to ${OUT}"
docker save -o "${OUT}" "${APP_IMAGE}" "${BASE_IMAGES[@]}"

SIZE=$(du -h "${OUT}" | cut -f1)
echo
echo "Done. Bundle: ${OUT} (${SIZE})"
echo "Contains:"
echo "  - ${APP_IMAGE}"
for img in "${BASE_IMAGES[@]}"; do echo "  - ${img}"; done
echo
echo "On each k3s node:"
echo "  sudo k3s ctr images import images.tar"