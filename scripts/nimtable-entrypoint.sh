#!/bin/sh
set -e

CONFIG_TEMPLATE="/app/config-template.yaml"
CONFIG_FILE="/app/config.yaml"

# Replace placeholders with env vars
sed \
  -e "s|__PORT__|${PORT:-3000}|g" \
  -e "s|__R2_CATALOG_URI__|${R2_CATALOG_URI}|g" \
  -e "s|__R2_CATALOG_WAREHOUSE__|${R2_CATALOG_WAREHOUSE}|g" \
  -e "s|__R2_CATALOG_TOKEN__|${R2_CATALOG_TOKEN}|g" \
  "$CONFIG_TEMPLATE" > "$CONFIG_FILE"

echo "Generated config.yaml:"
cat "$CONFIG_FILE"

echo "Starting Nimtable..."
exec node /app/dist/index.js
