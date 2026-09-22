#!/bin/bash
set -euo pipefail

IMAGE="your-registry.example.com/sandbox-worker:latest"

docker build --platform linux/amd64 -t "$IMAGE" .
docker push "$IMAGE"

echo "Image pushed: $IMAGE"
echo "Run: cf target -o <org> -s <sandbox-space> && cf restage sandbox-template"
