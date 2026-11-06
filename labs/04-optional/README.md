# Lab 4 (optional): limits, delete, a Java client, and MCP

Verified: 2026-11-06 in an existing environment on Confluent Cloud (AWS eu-central-1), Confluent CLI v4.82.0, Python 3.14.7, Java 21, Claude Code 2.1.314.

## What we test

What does Lightning Queries refuse, and how do other clients use it?

1. Unsupported SQL fails with a clear error code. REST returns all rows, and runs a large query asynchronously. MCP returns at most 200 rows.
2. A tombstone removes a key from the upsert table, but not from the append table.
3. A Java client and an MCP client get the same data as `lq.py`.

Do the four exercises in any order. Each takes 5 to 15 minutes.

## How it works

O1 sends SQL that Lightning Queries does not support, and reads the error codes.
O2 writes a `CANCELLED` order for customer 9001. The `HAVING` clause in `sql/11_feed_orders_latest.sql` then removes the key, and Flink writes a tombstone.
O3 runs `clients/java/LightningQuery.java`, a single-file client that uses only the JDK.
O4 calls the RTCE MCP endpoint. Claude Code can use the same endpoint as a tool server.

## Requirements

- Lab 3 is done. The settings in `tools/lq.env` are loaded: `set -a; . tools/lq.env; set +a`.
- O3: Java 17 or later: `java -version`. The Java client uses REST only, so O3 needs an org with REST access.
- O4: Claude Code, if you want to ask questions in natural language: `claude --version`.

## Run

### O1: limits (about 5 minutes)

1. Run each query in Look, section O1. Compare the error code with the list below.

- Aggregates, `GROUP BY` and `DISTINCT`: `QE_UNSUPPORTED_AGGREGATION`.
- `JOIN`: `QE_UNSUPPORTED_JOIN`. Subqueries: `QE_UNSUPPORTED_SUBQUERY`.
- `UNION`, `INTERSECT`, `EXCEPT`: `QE_UNSUPPORTED_SET_OP`. Window functions: `QE_UNSUPPORTED_WINDOW_FUNCTION`.
- `INSERT` and typos such as `SELEC`: `FE_SQL_SELECT_ONLY`. Two statements: `FE_SQL_MULTI_STATEMENT`.
- MCP stops a query after 20 seconds. REST runs a slow query asynchronously instead.

Optional: run a count that Lightning rejects as a Flink snapshot query. It took 14 seconds on 2026-11-06. Flink column names are lower case.

```bash
confluent flink query --sql "SELECT status, COUNT(*) AS n FROM lq_orders GROUP BY status;" \
  --compute-pool "$LQ_POOL_ID" --database "$LQ_CLUSTER_ID" --environment "$LQ_ENV_ID"
```

[docs/limits.md](../../docs/limits.md) compares Lightning, Flink snapshot queries and Tableflow.

### O2: delete a key (about 10 minutes)

2. Write a `CANCELLED` order for customer 9001.

```bash
tools/marker_json.sh "CANCEL-1" 9001 CANCELLED | confluent kafka topic produce lq_orders --cluster "$LQ_CLUSTER_ID" --environment "$LQ_ENV_ID" --api-key "$LQ_KAFKA_KEY" --api-secret "$LQ_KAFKA_SECRET" --value-format avro --schema "$LQ_SCHEMA_ID"
```

No Kafka key? Use Flink: `confluent flink statement create lq-$LQ_NAME-cancel-1 --sql "$(sed -e "s/{n}/1/" -e "s/{customer}/9001/" sql/20_cancel_order.sql)" --environment "$LQ_ENV_ID" --database "$LQ_CLUSTER_ID" --compute-pool "$LQ_POOL_ID"`.

3. Wait until key `9001` is gone from the upsert table. This takes up to about 1 minute.

Flink keeps the state of a key for 4 hours after its last order. Your last order for customer 9001 was Lab 3 step 12. After 4 hours, the cancel does not remove the key. Then do these steps first: write a new order for 9001 (Lab 3 step 12, with a new order ID). Wait until the upsert table shows it. Then do step 2 again.

```bash
python3 tools/lq_watch.py --until-empty --interval 2 "SELECT KEY FROM lq_orders_latest WHERE KEY = '9001'"
```

### O3: Java client (about 5 minutes)

4. Run the client. It reads the same settings from the environment.

```bash
java clients/java/LightningQuery.java "SELECT ORDER_ID, PRICE FROM lq_orders WHERE CUSTOMER_ID = 9001 ORDER BY PRODUCED_AT DESC"
```

The client always sends `max_result_rows` 200. So it never gets an asynchronous answer, and a query without `LIMIT` cannot freeze it. But it returns at most 200 rows, and it does not warn you.

Challenge: change the client so that it prints only the `ORDER_ID` values.

### O4: MCP (about 15 minutes)

5. Build your MCP URL.

```bash
echo "https://mcp.$LQ_REGION.aws.confluent.cloud/mcp/v1/context-engine/organizations/$LQ_ORG_ID/environments/$LQ_ENV_ID/kafka-clusters/$LQ_CLUSTER_ID"
```

6. Add it to Claude Code with sign-in. Replace `<url>` with the output of step 5.

```bash
claude mcp add --transport http --client-id claude-cli confluent-rtce "<url>"
```

7. Start `claude`, type `/mcp`, select `confluent-rtce`, select Authenticate, and sign in to your lab org in the browser.

If the sign-in fails, use your Global key instead. Claude Code then stores the key in its settings file, so remove the server after the lab (Clean up).

```bash
claude mcp remove confluent-rtce; claude mcp add --transport http confluent-rtce "<url>" --header "Authorization: Basic $(printf '%s' "$LQ_API_KEY:$LQ_API_SECRET" | base64)"
```

8. Ask these questions, one at a time:

- "What topics do I have access to in the Real-Time Context Engine?"
- "Describe the schema of the topic lq_orders."
- "Show the 5 most expensive orders in lq_orders."
- "What is the latest order of customer 9001 in lq_orders, and how many orders does it have?"

The last question needs a count. The `queryData` tool description tells the model: "explain the limitation to the user instead of attempting them". On 2026-11-06, Claude Code gave the latest order, and said that it cannot get the count. Older runs counted the rows by hand.

MCP returns at most 200 rows, and the model does not know when the result was cut. Above 200 orders, its count is wrong. Tell customers this.

## Look

I ran these commands on 2026-11-06 in an org with REST access. In an org without REST access, the error lines start with `MCP` instead of `HTTP 400`, and the message text can differ a little. The error codes are the same.

### O1

```bash
python3 tools/lq.py "SELECT CUSTOMER_ID, COUNT(*) FROM lq_orders GROUP BY CUSTOMER_ID"
```

```
HTTP 400 | QE_UNSUPPORTED_AGGREGATION | GROUP BY and aggregation functions are not supported.
  hint: Not supported in Lightning Queries: COUNT, SUM, GROUP BY, DISTINCT.
```

```bash
python3 tools/lq.py "SELECT * FROM lq_orders a JOIN lq_orders_latest b ON a.ORDER_ID = b.ORDER_ID"
```

```
HTTP 400 | QE_UNSUPPORTED_JOIN | JOIN queries are not supported.
  hint: Not supported in Lightning Queries: JOIN. Join in Flink before RTCE, or in the client.
```

```bash
python3 tools/lq.py "SELECT * FROM lq_orders WHERE CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM lq_orders_latest)"
```

```
HTTP 400 | QE_UNSUPPORTED_SUBQUERY | Subquery expressions are not supported.
  hint: Not supported in Lightning Queries: subqueries. Use two queries.
```

```bash
python3 tools/lq.py "SELEC * FROM lq_orders"
```

```
HTTP 400 | FE_SQL_SELECT_ONLY | Only SELECT queries are supported.
  hint: Lightning Queries runs only SELECT, SHOW TABLES and DESCRIBE. Check the spelling of SELECT.
```

REST has no row limit. A query with a large result, or a slow query, runs asynchronously. The API then answers `202` with a job URL. `lq.py` polls the job and downloads the result files. MCP returns at most 200 rows, also when you ask for more, and gives no warning.

```bash
python3 tools/lq.py --json "SELECT ORDER_ID FROM lq_orders LIMIT 500" | jq '.rows | length'
```

```
500
```

```bash
LQ_MODE=mcp python3 tools/lq.py --json --max-rows 500 "SELECT ORDER_ID FROM lq_orders LIMIT 500" | jq '.rows | length'
```

```
200
```

A large result runs asynchronously. Keep the `LIMIT`: a query with no `LIMIT` returns every row. On 2026-11-06 that was one file of 632 MB with 6,031,066 rows. `lq.py` stops a download above 100 MB with `result_too_large`.

<!-- doc-check: skip -->
```bash
python3 tools/lq.py "SELECT ORDER_ID FROM lq_orders LIMIT 300000" | tail -1
```

On 2026-11-06 this printed `note: the server runs this query asynchronously (statement_…)`, then `300000 rows in 2.79 s`. `lq.py` used about 155 MB of memory.

### O2

The upsert table has no row for customer 9001 now.

```bash
python3 tools/lq.py --json "SELECT KEY, ORDER_ID FROM lq_orders_latest WHERE KEY = '9001'" | jq -c .rows
```

```
[]
```

The append table still has the cancel event. It also keeps all older orders of customer 9001 (`UPD-1` to `UPD-3`).

```bash
python3 tools/lq.py --json "SELECT ORDER_ID, STATUS FROM lq_orders WHERE CUSTOMER_ID = 9001 AND STATUS = 'CANCELLED'" | jq -c .rows
```

```
[["CANCEL-1","CANCELLED"]]
```

### O3

The first line has the HTTP status and the time. `cut` removes the time, because it changes on each run.

```bash
java clients/java/LightningQuery.java "SELECT ORDER_ID FROM lq_orders WHERE CUSTOMER_ID = 9001 ORDER BY PRODUCED_AT DESC" | head -1 | cut -d' ' -f1-2
```

```
HTTP 200
```

### O4

The MCP endpoint also accepts the Global key as HTTP Basic. This lists its three tools and their required arguments.

```bash
curl -sS -u "$LQ_API_KEY:$LQ_API_SECRET" -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" "https://mcp.$LQ_REGION.aws.confluent.cloud/mcp/v1/context-engine/organizations/$LQ_ORG_ID/environments/$LQ_ENV_ID/kafka-clusters/$LQ_CLUSTER_ID" -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | sed -n 's/^data: //p' | jq -c '.result.tools[] | [.name, .inputSchema.required]'
```

```
["getMetadata",["topic_name"]]
["listTopics",null]
["queryData",["topic_name","query","max_result_rows"]]
```

## Expected

- O1: each unsupported query fails with `HTTP 400` and the codes above. On 2026-11-06 all 7 codes of step 1 came back, over REST and over MCP.
- O2: step 3 prints `Gone after` a number of seconds. On 2026-11-06 this took 48.9 s. On 2026-10-26 it took 57.2 s.
- O3: `HTTP 200` and the rows as raw JSON. An unsupported query prints `HTTP 400` and the error, and the client exits with 1.
- O4: Claude Code calls `listTopics`, `getMetadata` and `queryData`, and answers from your data. For the count, it explains that `COUNT` is not supported.

## Notes

- The upsert key comes back when a new order for customer 9001 arrives. On 2026-11-06 a re-add showed after 32.0 s. On 2026-10-26 it took 64 s.
- A tombstone in an append table deletes nothing. Only upsert tables remove keys.
- MCP returns rows as `{"row": [...]}` objects. REST returns lists. Code that reads both must handle both shapes.
- MCP sign-in errors: `unable to retrieve client by id`, `You are not a member of Confluent Cloud organization`, or a browser that cannot reach `localhost` (VPN). Use the login of your lab org, and turn off the VPN for the sign-in.
- O4 with Claude Code changes your Claude Code configuration. Remove it after the lab with `claude mcp remove confluent-rtce`.

## Clean up

1. Remove the MCP server from Claude Code, if you added it in O4.

```bash
claude mcp remove confluent-rtce
```

2. Do the Clean up of Lab 1 when you stop. It also deletes the Flink statements and the Kafka key of Labs 2 to 4.

## Versions

- Java: OpenJDK 21.0.12 on the test machine. The client needs Java 17 or later (text blocks, `String.formatted`).
- Claude Code 2.1.314 on the test machine, 2026-11-06 (`claude --version`).
- Confluent CLI v4.82.0 on 2026-11-06 (`confluent version`).
