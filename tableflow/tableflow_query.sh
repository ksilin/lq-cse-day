#!/usr/bin/env bash
# Run one SQL query on the Tableflow (Iceberg) copy of your topics, and print the rows as CSV.
# Use the topic name as the table name. The script selects your cluster as the schema.
# Usage: tableflow/tableflow_query.sh "<sql>"
#   example: tableflow/tableflow_query.sh "SELECT key, order_id, price FROM lq_orders_latest WHERE key = '9001'"
# Needs: duckdb 1.1+, and the settings LQ_REGION, LQ_ORG_ID, LQ_ENV_ID, LQ_CLUSTER_ID, LQ_API_KEY,
# LQ_API_SECRET in tools/lq.env or as environment variables. Run it; do not source it.
set -euo pipefail
sql=${1:?usage: tableflow_query.sh "<sql>"}
here=$(cd "$(dirname "$0")" && pwd)
if [ -f "$here/../tools/lq.env" ]; then
  while IFS='=' read -r k v; do
    case "$k" in LQ_*) [ -z "${!k:-}" ] && export "$k=$v" ;; esac
  done < <(grep -E '^LQ_[A-Z_]+=' "$here/../tools/lq.env")
fi
command -v duckdb >/dev/null || { echo "duckdb not found. Install DuckDB 1.1 or later." >&2; exit 1; }
: "${LQ_REGION:?}" "${LQ_ORG_ID:?}" "${LQ_ENV_ID:?}" "${LQ_CLUSTER_ID:?}" "${LQ_API_KEY:?}" "${LQ_API_SECRET:?}"
ep="https://tableflow.$LQ_REGION.aws.confluent.cloud/iceberg/catalog/organizations/$LQ_ORG_ID/environments/$LQ_ENV_ID"
# DUCKDB_AGENT_MODE=0: DuckDB 2.0 can change its output for AI agents; 1.x ignores the variable.
# The setup lines print "Success", "true" and deprecation warnings. .output sends them to /dev/null.
DUCKDB_AGENT_MODE=0 duckdb -csv <<SQL
.output /dev/null
INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;
CREATE OR REPLACE SECRET iceberg_secret (TYPE ICEBERG, CLIENT_ID '$LQ_API_KEY', CLIENT_SECRET '$LQ_API_SECRET', ENDPOINT '$ep', OAUTH2_SCOPE 'catalog');
ATTACH 'warehouse' AS iceberg_catalog (TYPE iceberg, SECRET iceberg_secret, ENDPOINT '$ep');
USE iceberg_catalog."$LQ_CLUSTER_ID";
.output
$sql;
SQL
