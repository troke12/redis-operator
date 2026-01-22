#!/bin/bash
set -e

# Redis Operator Build Script
# Builds and optionally pushes the operator image with dynamic version info

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Image repository
IMAGE_REPO="${IMAGE_REPO:-troke12/redis-operator}"

# Get version info
VERSION=$(git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

echo -e "${GREEN}Building Redis Operator${NC}"
echo "  Version:    $VERSION"
echo "  Commit:     $COMMIT"
echo "  Build Date: $BUILD_DATE"
echo ""

# Parse arguments
PUSH=false
SKIP_TESTS=false
TAG_LATEST=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --push)
      PUSH=true
      shift
      ;;
    --skip-tests)
      SKIP_TESTS=true
      shift
      ;;
    --latest)
      TAG_LATEST=true
      shift
      ;;
    *)
      echo -e "${RED}Unknown option: $1${NC}"
      echo "Usage: $0 [--push] [--skip-tests] [--latest]"
      echo "  --push         Push image to registry after build"
      echo "  --skip-tests   Skip running tests before build"
      echo "  --latest       Also tag and push as 'latest'"
      exit 1
      ;;
  esac
done

# Build the image
echo -e "${YELLOW}Building Docker image...${NC}"
if [ "$SKIP_TESTS" = true ]; then
  docker build -t "${IMAGE_REPO}:${VERSION}" \
    --build-arg VERSION="${VERSION}" \
    --build-arg COMMIT="${COMMIT}" \
    --build-arg BUILD_DATE="${BUILD_DATE}" \
    .
else
  make docker-build IMG="${IMAGE_REPO}:${VERSION}"
fi

echo -e "${GREEN}Build complete: ${IMAGE_REPO}:${VERSION}${NC}"

# Tag as latest if requested
if [ "$TAG_LATEST" = true ]; then
  echo -e "${YELLOW}Tagging as latest...${NC}"
  docker tag "${IMAGE_REPO}:${VERSION}" "${IMAGE_REPO}:latest"
fi

# Push if requested
if [ "$PUSH" = true ]; then
  echo -e "${YELLOW}Pushing image...${NC}"
  docker push "${IMAGE_REPO}:${VERSION}"
  echo -e "${GREEN}Pushed: ${IMAGE_REPO}:${VERSION}${NC}"

  if [ "$TAG_LATEST" = true ]; then
    docker push "${IMAGE_REPO}:latest"
    echo -e "${GREEN}Pushed: ${IMAGE_REPO}:latest${NC}"
  fi
fi

echo ""
echo -e "${GREEN}Done!${NC}"
echo ""
echo "To run locally:"
echo "  make run"
echo ""
echo "To deploy to cluster:"
echo "  kubectl set image deployment/redis-operator manager=${IMAGE_REPO}:${VERSION}"
