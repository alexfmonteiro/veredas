#!/bin/sh
set -e

REPO_URL="https://x-access-token:${GITHUB_TOKEN}@github.com/alexfmonteiro/br-economic-pulse.git"
CLONE_DIR="$HOME/repo"

git config --global user.email "marimo-bot@railway.app"
git config --global user.name "marimo-railway"

echo "Cloning repository..."
git clone --depth 1 "$REPO_URL" "$CLONE_DIR"

echo "Starting marimo..."
cd "$CLONE_DIR/notebooks"
exec marimo edit . \
  --host 0.0.0.0 \
  -p "${PORT:-8080}" \
  --token \
  --token-password="${MARIMO_TOKEN:-changeme}"
