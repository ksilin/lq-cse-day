#!/usr/bin/env bash
# Submit one Flink SQL statement and print its real status.
# The CLI exits 0 also when a statement FAILED, so this script reads the JSON status.
# Usage: tools/flink_run.sh <statement-name> <sql-file-or-sql>
#   env: LQ_REGION, LQ_ENV_ID, LQ_CLUSTER_ID, LQ_POOL_ID. Load tools/lq.env first.
set -euo pipefail
name=${1:?usage: flink_run.sh <statement-name> <sql-file-or-sql>}; src=${2:?usage: flink_run.sh <statement-name> <sql-file-or-sql>}
: "${LQ_REGION:?}" "${LQ_ENV_ID:?}" "${LQ_CLUSTER_ID:?}" "${LQ_POOL_ID:?}"
if [ -f "$src" ]; then sql=$(sed '/^--/d' "$src"); else sql=$src; fi
# A create error (for example, the name exists) is printed. Then describe shows the statement with that name.
if ! err=$(confluent flink statement create "$name" --sql "$sql" --environment "$LQ_ENV_ID" \
  --database "$LQ_CLUSTER_ID" --compute-pool "$LQ_POOL_ID" --wait -o json 2>&1 >/dev/null); then
  echo "create failed: $(grep -v -i secret <<<"$err" | tail -2)" >&2
fi
confluent flink statement describe "$name" --environment "$LQ_ENV_ID" --cloud aws --region "$LQ_REGION" -o json \
  | jq -c '{name, status, status_detail: (.status_detail // .statusDetail // "")}'
