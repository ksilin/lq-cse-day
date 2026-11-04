#!/usr/bin/env bash
# Print one order event as Avro JSON, for `confluent kafka topic produce --value-format avro`.
# Usage: tools/marker_json.sh <order-id> [customer-id] [status] [price]
# Defaults: customer 0, status MARKER, price 0. produced_at is now, in milliseconds.
# The Avro JSON needs the union branch names, for example {"string": "..."}.
# A bad argument prints an error and no event, so a pipe into the producer writes nothing.
set -euo pipefail
export LC_ALL=C  # a decimal point, not a comma
id=${1:?usage: marker_json.sh <order-id> [customer-id] [status] [price]}; customer=${2:-0}; status=${3:-MARKER}; price=${4:-0}
fail() { echo "marker_json.sh: $1" >&2; exit 1; }
[[ $customer =~ ^[0-9]+$ ]] || fail "customer-id must be a whole number, not '$customer'"
[[ $price =~ ^[0-9]+(\.[0-9]+)?$ ]] || fail "price must be a number such as 10 or 10.25, not '$price'"
for v in "$id" "$status"; do
  [[ $v =~ ^[A-Za-z0-9._:-]+$ ]] || fail "order-id and status may have only letters, digits and . _ : -, not '$v'"
done
printf '{"order_id":{"string":"%s"},"customer_id":{"int":%d},"product_id":{"string":"marker"},"price":{"double":%s},"status":{"string":"%s"},"source":{"string":"cli"},"produced_at":{"long.timestamp-millis":%d}}\n' \
  "$id" "$customer" "$price" "$status" "$(( $(date +%s) * 1000 ))"
