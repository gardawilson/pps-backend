#!/bin/bash
set -e

echo "===================================="
echo " PPS Backend Deploy - $(date)"
echo "===================================="

cd "$(dirname "$0")"

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"

echo "[1/3] Git pull..."
git pull origin "$CURRENT_BRANCH"

echo "[2/3] Build & restart container..."
docker compose up -d --build

echo "[3/3] Status container:"
docker compose ps

echo ""
echo "Deploy selesai untuk branch: $CURRENT_BRANCH"
