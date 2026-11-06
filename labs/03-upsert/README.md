# Lab 3: an upsert table holds one current row per key

Verified: 2026-11-05 in an existing environment on Confluent Cloud (AWS eu-central-1), Confluent CLI v4.82.0, DuckDB v2.0.0, Python 3.14. Earlier runs: 2026-10-26, 2026-10-27 and 2026-11-02.

## What we test

Does a Lightning table on a compacted topic return only the newest row for a key?

1. A query for one key on the upsert table returns exactly one row.
2. After a new event for that key, the same query returns the new values.
3. The append table from Lab 1 still holds every event for that key.
4. The same update shows in Lightning in seconds to about 1 minute, and in Tableflow minutes later. Between the two, Tableflow still returns the old value.

A Flink statement keeps the newest order per customer in a compacted topic `lq_orders_latest`. You turn on RTCE and Tableflow for it. Then you write orders for customer 9001 and compare both tables. At the end, you time one update in Lightning and in Tableflow.

This lab takes about 50 minutes. About 10 minutes of it is waiting.

## How it works

`sql/10_create_orders_latest.sql` creates a compacted topic with a raw key `key` and `'changelog.mode' = 'upsert'`.
`sql/11_feed_orders_latest.sql` groups `lq_orders` by customer and keeps the `LAST_VALUE` of each column.
RTCE sees `cleanup.policy=compact` and chooses upsert mode by itself: the last write for a key wins.
The append table `lq_orders` keeps every event, so the newest row per key needs `ORDER BY ... DESC LIMIT 1` there.
Customer 9001 is yours alone: the feed writes only customers 3000 to 3249.
Tableflow also supports upsert: for a compacted topic with `'changelog.mode' = 'upsert'`, the Iceberg table keeps one row per key.
`tableflow/tableflow_query.sh` runs one SQL query on the Tableflow copy with DuckDB, the same way as `tableflow_watch.sh` in Lab 2.

## Requirements

- Lab 1 and Lab 2 are done. The feed `lq-$LQ_NAME-feed-orders` is stopped (Lab 2, step 6). Lab 3 reads the orders that are already in `lq_orders`.
- DuckDB 1.1 or later, from Lab 2. Two terminals for Part C, both in the root folder of the repository.
- The settings in `tools/lq.env`, with the Kafka key from Lab 2. No Kafka key? Notes show the Flink way for steps 7 and 9.
- If your org cannot use upsert tables, a query on `lq_orders_latest` returns `MT_UPSERT_NOT_SUPPORTED`. Then Look shows the same answer from the append table.

## Run

### Part A: the upsert table (about 10 minutes)

1. Load the settings.

```bash
set -a; . tools/lq.env; set +a
```

2. Create the table `lq_orders_latest`. The status must be `COMPLETED`.

```bash
confluent flink statement create lq-$LQ_NAME-create-orders-latest \
  --sql "$(cat sql/10_create_orders_latest.sql)" \
  --environment "$LQ_ENV_ID" --database "$LQ_CLUSTER_ID" --compute-pool "$LQ_POOL_ID" --wait
```

CLI v4.82.0 can print `RUNNING` here, because `--wait` can return before the end. On 2026-11-05 it did. This is not an error. Wait 10 seconds and check the status:

```bash
confluent flink statement describe lq-$LQ_NAME-create-orders-latest \
  --environment "$LQ_ENV_ID" --cloud aws --region "$LQ_REGION" -o json | jq -r .status
```

3. Start the statement that keeps the newest order per customer.

```bash
confluent flink statement create lq-$LQ_NAME-feed-orders-latest \
  --sql "$(cat sql/11_feed_orders_latest.sql)" \
  --environment "$LQ_ENV_ID" --database "$LQ_CLUSTER_ID" --compute-pool "$LQ_POOL_ID" --wait
```

4. Turn on RTCE for the upsert table.

```bash
confluent rtce rtce-topic create --cloud aws --region "$LQ_REGION" \
  --topic-name lq_orders_latest \
  --description "Newest order per customer (upsert, key = customer id as text)." \
  --environment "$LQ_ENV_ID" --cluster "$LQ_CLUSTER_ID" --wait --timeout 10m
```

5. Turn on Tableflow for the upsert table. Part C compares it with Lightning. The phase starts as `PENDING`. On 2026-11-02 it was `RUNNING` after about 1 minute.

```bash
confluent tableflow topic enable lq_orders_latest \
  --cluster "$LQ_CLUSTER_ID" --environment "$LQ_ENV_ID" \
  --storage-type MANAGED --table-formats ICEBERG
```

6. Query the table until it returns rows. This took about 70 seconds after `ACTIVE` on 2026-10-26, and about 50 seconds on 2026-11-05. Up to 12 minutes is normal.

```bash
python3 tools/lq_watch.py --interval 10 "SELECT KEY FROM lq_orders_latest LIMIT 1"
```

Part A done. Next: write orders for customer 9001.

### Part B: one key, two orders (about 5 minutes)

7. Write the first order for customer 9001, price 10.

```bash
tools/marker_json.sh "UPD-1" 9001 NEW 10 \
  | confluent kafka topic produce lq_orders \
      --cluster "$LQ_CLUSTER_ID" --environment "$LQ_ENV_ID" \
      --api-key "$LQ_KAFKA_KEY" --api-secret "$LQ_KAFKA_SECRET" \
      --value-format avro --schema "$LQ_SCHEMA_ID"
```

8. Wait until the upsert table has it. This takes up to about 2 minutes, because the Flink statement between the tables commits about once per minute. On 2026-11-05 it took 89 seconds.

```bash
python3 tools/lq_watch.py --interval 2 "SELECT KEY FROM lq_orders_latest WHERE KEY = '9001' AND ORDER_ID = 'UPD-1'"
```

9. Write the second order for the same customer, price 20.

```bash
tools/marker_json.sh "UPD-2" 9001 NEW 20 \
  | confluent kafka topic produce lq_orders \
      --cluster "$LQ_CLUSTER_ID" --environment "$LQ_ENV_ID" \
      --api-key "$LQ_KAFKA_KEY" --api-secret "$LQ_KAFKA_SECRET" \
      --value-format avro --schema "$LQ_SCHEMA_ID"
```

10. Wait until the upsert table shows the second order.

```bash
python3 tools/lq_watch.py --interval 2 "SELECT KEY FROM lq_orders_latest WHERE KEY = '9001' AND ORDER_ID = 'UPD-2'"
```

Part B done. Next: time one update in Lightning and in Tableflow.

### Part C: the same update in Lightning and in Tableflow (about 10 minutes)

Each watch counts from its own start. So start each watch right after the step before it.

11. Terminal 1: check that Tableflow has the old order `UPD-2`, price 20. If the answer has only the header line `key,order_id,price`, wait 1 minute and run it again. Tableflow publishes about every 5 minutes. On 2026-11-05 the second try had the row.

```bash
tableflow/tableflow_query.sh "SELECT key, order_id, price FROM lq_orders_latest WHERE key = '9001'"
```

12. Terminal 1: write a third order for customer 9001, price 30.

```bash
tools/marker_json.sh "UPD-3" 9001 NEW 30 \
  | confluent kafka topic produce lq_orders \
      --cluster "$LQ_CLUSTER_ID" --environment "$LQ_ENV_ID" \
      --api-key "$LQ_KAFKA_KEY" --api-secret "$LQ_KAFKA_SECRET" \
      --value-format avro --schema "$LQ_SCHEMA_ID"
```

13. Terminal 2, at once: watch Tableflow. Keep it running until it prints `found`. The script reads `tools/lq.env` by itself.

```bash
tableflow/tableflow_watch.sh lq_orders_latest "UPD-3"
```

14. Terminal 1: watch the append table, then the upsert table. Write down both times.

```bash
python3 tools/lq_watch.py --interval 1 "SELECT ORDER_ID FROM lq_orders WHERE ORDER_ID = 'UPD-3'"
```

```bash
python3 tools/lq_watch.py --interval 1 "SELECT KEY FROM lq_orders_latest WHERE KEY = '9001' AND ORDER_ID = 'UPD-3'"
```

15. Terminal 1, at once: ask Lightning and Tableflow for key 9001. Lightning has the new price 30. Tableflow still has the old price 20.

```bash
python3 tools/lq.py "SELECT KEY, ORDER_ID, PRICE FROM lq_orders_latest WHERE KEY = '9001'"
```

```bash
tableflow/tableflow_query.sh "SELECT key, order_id, price FROM lq_orders_latest WHERE key = '9001'"
```

Wait until terminal 2 prints `found`. Write down the Tableflow time.

Part C done. Next: compare the tables in Look.

## Look

I ran these commands on 2026-11-05, after Part C. Lightning returns each value as a string. Since 2026-11-02, it writes a `DOUBLE` with `.0`, for example `"30.0"`. On 2026-10-27 it wrote `"20"`.

The upsert table has one row for key `9001`: the third order. The column `KEY` is a reserved word, but it works without quotes here.

```bash
python3 tools/lq.py --json "SELECT KEY, ORDER_ID, PRICE FROM lq_orders_latest WHERE KEY = '9001'" | jq -c .rows
```

```
[["9001","UPD-3","30.0"]]
```

The append table has all three orders for customer 9001.

```bash
python3 tools/lq.py --json "SELECT ORDER_ID, PRICE FROM lq_orders WHERE CUSTOMER_ID = 9001 ORDER BY PRODUCED_AT DESC" | jq -c .rows
```

```
[["UPD-3","30.0"],["UPD-2","20.0"],["UPD-1","10.0"]]
```

The append table gives the same current row with `ORDER BY ... DESC LIMIT 1`. This works without the upsert flag.

```bash
python3 tools/lq.py --json "SELECT ORDER_ID, PRICE FROM lq_orders WHERE CUSTOMER_ID = 9001 ORDER BY PRODUCED_AT DESC LIMIT 1" | jq -c .rows
```

```
[["UPD-3","30.0"]]
```

Tableflow has the same result some minutes later. Its upsert table also keeps one row per key.

```bash
tableflow/tableflow_query.sh "SELECT key, order_id, price FROM lq_orders_latest WHERE key = '9001'"
```

```
key,order_id,price
9001,UPD-3,30.0
```

The Tableflow copy of the append table has all three orders.

```bash
tableflow/tableflow_query.sh "SELECT order_id, price FROM lq_orders WHERE customer_id = 9001 ORDER BY produced_at DESC"
```

```
order_id,price
UPD-3,30.0
UPD-2,20.0
UPD-1,10.0
```

## Expected

The upsert table returns one row per key, with the newest values. The append table returns every event.

Part C, on 2026-11-05. `UPD-3` was written at 17:58:27 UTC, with the feed stopped:

```
Lightning, append table     visible after 3.6 s
Lightning, upsert table     visible after 56.6 s
Tableflow, upsert table     visible after 4 min 08 s
```

On 2026-11-02 the same times were 2.6 s, 49.6 s and 4 min 08 s.

Step 15 ran at 17:59:27 UTC, when the Lightning upsert table had `UPD-3`. Lightning had the new price, and Tableflow still had the old price:

```
Lightning   [["9001","UPD-3","30.0"]]
Tableflow   9001,UPD-2,20.0
```

The upsert table is slower than the append table, because a Flink statement sits between them. It commits about once per minute. Tableflow publishes about every 5 minutes. Other times for the upsert table, measured on 2026-10-26 with customer 3000:

```
update (new order)                 visible in the upsert table after 11.2 s
CANCELLED order (delete)           key gone after 57.2 s
new order again (re-add)           visible after 64.0 s
```

## Notes

- No Kafka key: write the orders of steps 7 and 9 with Flink. On 2026-10-27 the order was in the upsert table 44 seconds after the submit. For step 7: `confluent flink statement create lq-$LQ_NAME-upd-1 --sql "$(sed -e "s/{id}/UPD-1/" -e "s/{customer}/9001/" -e "s/{price}/10/" sql/21_insert_order.sql)" --environment "$LQ_ENV_ID" --database "$LQ_CLUSTER_ID" --compute-pool "$LQ_POOL_ID"`. For step 9, use `lq-$LQ_NAME-upd-2`, `UPD-2` and price `20`.
- If the feed runs during this lab, each step can take up to 2 minutes, because the feed holds back every new order until its next commit.
- `MT_UPSERT_NOT_SUPPORTED` means that the org cannot use upsert tables. Ask your Confluent account team. Use the `ORDER BY ... DESC LIMIT 1` query from Look on the append table instead.
- Between `ACTIVE` and the first rows, the upsert table answered `HTTP 500 MT_METADATA_UPDATE`, "Lightning Table temporarily unavailable". `lq.py` treats this as "retry".
- This lab uses a raw key. In Flink, a raw key column must be named `key`. The docs say that an Avro, Protobuf or JSON Schema key also works, but that a key schema change is not supported. We did not test a key with a schema.
- Upsert or append: upsert keeps one row per key, so storage stays small and the client does not sort. Append keeps the history, but each query must sort.
- Fresh or simple: the append table plus `ORDER BY ... DESC LIMIT 1` is the freshest current row, in seconds. The upsert table needs no sort, but it waits for the Flink commit. Tableflow is for analytics in minutes, not for the current state. For a count or a sum per key, use a Flink snapshot query: see [docs/limits.md](../../docs/limits.md).
- `tableflow_query.sh` prints DuckDB errors, for example `Table with name ... does not exist` if Tableflow is off for the topic. Use lower-case column names in DuckDB.
- Before you change `sql/11_feed_orders_latest.sql`, run `EXPLAIN` on it in the Flink workspace. The upsert key and the sink primary key must both be `(key)`.

## Clean up

Do the Clean up of Lab 1 when you stop. It also deletes the Flink statements and the Kafka key of Labs 2 and 3.

## Versions

- Confluent CLI v4.82.0. Homebrew cask `confluent-cli` shows 4.82.0 as newest on 2026-11-04.
- Upsert tables worked on 2026-10-26, 2026-10-27, 2026-11-02 and 2026-11-05 (live RTCE test on a compacted topic).
- DuckDB v2.0.0 on the test machine, 2026-11-02. Tableflow upsert tables are GA since 2025-10-02.
