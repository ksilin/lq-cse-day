# Troubleshooting

Find the code or the text that you see. Each item gives the cause, then the fix. `tools/lq.py` prints the code and a hint.

Run `python3 tools/lq_doctor.py` first. It checks the login, the IDs, the region, the keys, RTCE, the feed and Tableflow, and prints a fix for each problem.

## New environment or new cluster

- `CREATE TABLE` fails with "Unable to process the request due to technical difficulties", or the CLI says `Statement with name "lq-$LQ_NAME-create-orders" already exists`. Cause: the environment or the cluster is too new. On 2026-11-02, in an existing environment, it failed about 1.5 minutes after the cluster create, and worked after about 4 minutes. Fix: wait 1 minute, `confluent flink statement delete lq-$LQ_NAME-create-orders --force --environment "$LQ_ENV_ID" --cloud aws --region "$LQ_REGION"`, then run Lab 1 step 9 again. Seen two times on 2026-10-27.
- RTCE phase `FAILED` for `lq_orders` right after step 11. Cause: the topic did not exist, because step 9 failed. Fix: the simplest fix is a new environment, or a new cluster in an existing environment (Lab 1, Clean up, then Lab 1 again). RTCE can keep old state for a topic name. The docs also say: fix the cause, then disable the Lightning Table and enable it again.
- `Statement with name "lq-$LQ_NAME-create-orders" already exists` in an existing environment (Lab 1, step 4b). Cause: a statement with this name is already in the environment. Flink statement names are unique in an environment. Fix: if it is yours from an earlier run, delete it with Lab 1, Clean up, step 1b. If another person uses the same `LQ_NAME` (often `changeme`, the default), choose another `LQ_NAME` in `tools/lq.env`.

## Access

- `HTTP 403 api_not_enabled`, "The REST API is not enabled for this organization." Cause: the Global key belongs to another org. REST is on for every org now, so this is the most likely cause. Fix: keep `LQ_MODE=auto` or set `LQ_MODE=mcp`. Then `lq.py` uses MCP, which works in every org.
- `HTTP 401 unauthenticated`, "The API key is invalid, or is not a key that authenticates a whole organization." Cause: a cluster key, a wrong secret, or a space in the secret. Fix: use a Global key of the lab org. Copy the secret again.
- Global key create fails. Cause: you already have 2 Global keys (the limit per user). Fix: delete one, or use one of them. The key must belong to the lab org.
- `FE_AUTHORIZATION_FAILED`. Cause: no DeveloperRead on the cluster or on Schema Registry. Fix: ask an OrganizationAdmin of your org for EnvironmentAdmin on the lab environment. EnvironmentAdmin covers both.
- `expired token` from the CLI. Fix: `confluent login --save`, then `confluent organization use "$LQ_ORG_ID"`.

## The table is not ready

These are normal for some minutes after RTCE is `ACTIVE`. `lq.py` exits with code 2. Retry every 30 seconds, up to 10 minutes (12 for upsert).

- `FE_CUSTOMER_NOT_FOUND`: the cluster has no Lightning table yet. Also check `LQ_ENV_ID` and `LQ_CLUSTER_ID`.
- `FE_TABLE_NOT_FOUND`, "Table does not exist". Also check the name with `python3 tools/lq.py "SHOW TABLES"`.
- `DP_TABLE_NOT_AVAILABLE`, "Table data is not yet available."
- `HTTP 500 MT_METADATA_UPDATE`, "Lightning Table temporarily unavailable" (seen on new upsert tables).
- `FE_BACKEND_UNAVAILABLE`, `FE_TABLE_NOT_AVAILABLE`, `DP_INVALID_TABLE`.
- `HTTP 404`. The docs say that the table is still being set up. `lq.py` treats it as "retry".
- `0 rows` right after the start: the Flink feed has not committed yet. Retry after one minute.

## SQL

- `FE_SQL_VALIDATION_ERROR`, "Column 'order_id' not found ... did you mean 'ORDER_ID'?" Cause: a quoted lower-case name. Fix: no quotes, or upper case in quotes.
- `QE_UNSUPPORTED_*` (aggregation, join, subquery, set operation, window function). Cause: not supported in Lightning Queries. Fix: change the query, or do the work in Flink before RTCE.
- `FE_SQL_SELECT_ONLY`, "Only SELECT queries are supported." Cause: `INSERT`, DDL, or a typo such as `SELEC`.
- `FE_SQL_MULTI_STATEMENT`. Cause: two statements. Fix: one per request.
- Exactly 200 rows. Cause: you used MCP, which returns at most 200 rows with no warning. REST returns all rows.
- `result_too_large`. Cause: a query without `LIMIT` gave a result larger than 100 MB. Fix: add a `LIMIT` or a `WHERE` clause on a key. `--max-download-mb 0` removes the limit, but a large result needs several GB of memory.
- A value looks different from older output, for example `30.0` instead of `30`, or `+00` at the end of a time. Cause: the value format changed on 2026-11-02 and by 2026-11-05. Fix: compare numbers and times, not text.
- `note: the server runs this query asynchronously`. Cause: a large result or a slow query. This is normal. `lq.py` waits for the job and downloads the result.
- `FE_STATEMENT_FAILED`. Cause: an asynchronous query failed, for example because its result is too large or it ran too long. Fix: add a `LIMIT` or a narrower `WHERE` clause.

## Upsert

- `MT_UPSERT_NOT_SUPPORTED`, "Upsert (compacted) topics are not supported in RTCE." Cause: the org cannot use upsert tables. Fix: use the append table with `ORDER BY PRODUCED_AT DESC LIMIT 1`.
- `If a key is provided with a 'raw' format it must use 'key' as the column name`. Fix: name the raw key column `key`.
- `MT_INVALID_MATERIALIZATION_MODE`. Cause: `cleanup.policy=delete` with `changelog.mode=upsert`. Fix: use `compact`.

## Flink

- The CLI exits with 0, but the statement failed. Check: `confluent flink statement describe <name> --environment <env> --cloud aws --region eu-central-1 -o json | jq -r .status`.
- `Statement with name ... already exists`. Fix: use a new statement name, or delete the old statement.
- `Only a single statement is supported at a time.` Fix: one statement per file.
- `Unsupported format: json`. Fix: use `avro-registry` or `json-registry`.
- Statement `PENDING` for more than 60 seconds. Cause: the compute pool is full. Fix: stop old statements.
- `Schema Registry subject '<topic>-value' doesn't match the existing one`. Cause: an old subject after a topic delete. Fix: use a new topic name.

## Freshness and Tableflow

- A marker takes about 60 seconds. Cause: a Flink statement writes to the same topic, and RTCE reads only committed data. Fix: stop the feed (Lab 2, step 6).
- `lq_watch.py` prints `n`. Cause: a network error or no answer within 60 s. The watch goes on. If it prints only `n`, check the network and `LQ_REGION`.
- `RATE_LIMIT_EXCEEDED` or HTTP 429. Cause: too many queries from the org. `lq_watch.py` goes on by itself. Fix: use `--interval 5`.
- `lq_watch.py` stops with `FE_TABLE_NOT_FOUND` after 2 minutes. Cause: a wrong table name. Fix: check the name with `SHOW TABLES`.
- `tableflow_watch.sh` prints `not yet (no answer: ... Unauthorized_401 ...)`. Cause: a wrong Global key or secret. Fix: check `LQ_API_KEY` and `LQ_API_SECRET`.
- The CLI producer says `Error: schema not found`. Cause: a wrong `LQ_SCHEMA_ID`. Fix: look it up again (Lab 2, step 3).
- The CLI producer says `SASL authentication error: Authentication failed. If you are using a Global API key for authentication, please check whether this cluster type is supported`. Cause: you used the Global key. A Global key cannot produce to a Basic or Standard cluster. It works for Lightning queries. Fix: use the Kafka key from Lab 2, step 2.
- The CLI producer says `Select an API key for resource lkc-…`. Cause: no `--api-key` and `--api-secret`. Fix: load `tools/lq.env` again, and check `LQ_KAFKA_KEY`.
- The CLI producer says `cannot decode textual union: expected: '{'`. Cause: the Avro JSON has no union branch names. Fix: use `tools/marker_json.sh`.
- `tableflow_watch.sh` prints `not yet (no answer)` all the time. Cause: DuckDB cannot attach: wrong key, or Tableflow not `RUNNING`. Fix: check the phase (Lab 2, step 5) and the key.
- DuckDB HTTP 401 after 15 minutes. Cause: the catalog token expired (`"expires_in": 900`). Fix: run the SQL file again. `tableflow_watch.sh` does this on every try.
- `HTTP 500` with an empty message, or a DNS error for `sql.<region>`. Cause: the region has no Lightning Queries. Fix: use eu-central-1.

## Shell

- zsh: `no such file or directory` after `source`. Fix: run scripts with `bash` or `./`; only `tools/lq.env` is loaded with `set -a; . tools/lq.env; set +a`.
- Paste of a long command breaks in iTerm2. Fix: every command in the labs is one line; paste one block at a time.
