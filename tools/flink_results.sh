#!/usr/bin/env bash
# Print the result rows of a Flink statement (the CLI has no command for this).
# Usage: tools/flink_results.sh <statement-name>
#   env: LQ_REGION, LQ_ORG_ID, LQ_ENV_ID, LQ_API_KEY, LQ_API_SECRET (the Global key). Load tools/lq.env first.
# The Global key worked on 2026-10-27. The Flink REST docs ask for a Flink API key; use one if the Global key gets 401.
# An HTTP error prints the status and the body, and exits 1.
set -euo pipefail
name=${1:?usage: flink_results.sh <statement-name>}
: "${LQ_REGION:?}" "${LQ_ORG_ID:?}" "${LQ_ENV_ID:?}" "${LQ_API_KEY:?}" "${LQ_API_SECRET:?}"
body=$(curl -sS -w '\n%{http_code}' -u "$LQ_API_KEY:$LQ_API_SECRET" \
  "https://flink.$LQ_REGION.aws.confluent.cloud/sql/v1/organizations/$LQ_ORG_ID/environments/$LQ_ENV_ID/statements/$name/results")
status=${body##*$'\n'}; body=${body%$'\n'*}
if [ "$status" != 200 ]; then echo "HTTP $status: $body" >&2; exit 1; fi
jq -r '.results.data[]?.row | map(tostring) | join(" | ")' <<<"$body"
