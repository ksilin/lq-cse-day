#!/usr/bin/env bash
# Check the Tableflow copy every 30 s until one ORDER_ID appears. Prints the minutes waited.
# Usage: tableflow/tableflow_watch.sh <topic> <order-id>
#   example: tableflow/tableflow_watch.sh lq_orders MARKER-1
# Needs: duckdb 1.1+, and the settings LQ_REGION, LQ_ORG_ID, LQ_ENV_ID, LQ_CLUSTER_ID, LQ_API_KEY,
# LQ_API_SECRET in tools/lq.env or as environment variables. Run it; do not source it.
set -euo pipefail
topic=${1:?usage: tableflow_watch.sh <topic> <order-id>}; id=${2:?usage: tableflow_watch.sh <topic> <order-id>}
here=$(cd "$(dirname "$0")" && pwd)
if [ -f "$here/../tools/lq.env" ]; then
  while IFS='=' read -r k v; do
    case "$k" in LQ_*) [ -z "${!k:-}" ] && export "$k=$v" ;; esac
  done < <(grep -E '^LQ_[A-Z_]+=' "$here/../tools/lq.env")
fi
command -v duckdb >/dev/null || { echo "duckdb not found. Install DuckDB 1.1 or later." >&2; exit 1; }
: "${LQ_REGION:?}" "${LQ_ORG_ID:?}" "${LQ_ENV_ID:?}" "${LQ_CLUSTER_ID:?}" "${LQ_API_KEY:?}" "${LQ_API_SECRET:?}"
ep="https://tableflow.$LQ_REGION.aws.confluent.cloud/iceberg/catalog/organizations/$LQ_ORG_ID/environments/$LQ_ENV_ID"
start=$(date +%s)
echo "Started at $(date +%H:%M:%S). Looking for $id in Tableflow table $LQ_CLUSTER_ID.$topic every 30 s."
while true; do
  # A new secret on every try, so the 30-minute token expiry does not matter.
  # Only the line TF_COUNT=<n> counts. Without it, the first error line of DuckDB is shown.
  # DUCKDB_AGENT_MODE=0: DuckDB 2.0 can change its output for AI agents; 1.x ignores the variable.
  out=$(DUCKDB_AGENT_MODE=0 duckdb -noheader -csv 2>&1 <<SQL
INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;
CREATE OR REPLACE SECRET iceberg_secret (TYPE ICEBERG, CLIENT_ID '$LQ_API_KEY', CLIENT_SECRET '$LQ_API_SECRET', ENDPOINT '$ep', OAUTH2_SCOPE 'catalog');
ATTACH 'warehouse' AS iceberg_catalog (TYPE iceberg, SECRET iceberg_secret, ENDPOINT '$ep');
SELECT 'TF_COUNT=' || count(*) FROM iceberg_catalog."$LQ_CLUSTER_ID"."$topic" WHERE order_id = '$id';
SQL
  ) || true
  n=$(grep -o 'TF_COUNT=[0-9]*' <<<"$out" | cut -d= -f2 || true)
  waited=$(( $(date +%s) - start ))
  if [ -n "$n" ] && [ "$n" -gt 0 ]; then
    printf '%s  found. Visible in Tableflow after %d min %02d s\n' "$(date +%H:%M:%S)" $((waited/60)) $((waited%60))
    exit 0
  fi
  if [ -n "$n" ]; then
    printf '%s  not yet (%s)\n' "$(date +%H:%M:%S)" "$n"
  else
    err=$(sed $'s/\x1b\[[0-9;]*m//g' <<<"$out" | grep -iE 'error|exception' | head -1 | cut -c1-200 || true)
    printf '%s  not yet (no answer: %s)\n' "$(date +%H:%M:%S)" "${err:-no TF_COUNT line}"
  fi
  sleep 30
done
