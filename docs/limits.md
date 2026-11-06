# Limits and quirks of Lightning Tables

Read this before you run your own queries, and before you show Lightning Tables to a customer.

Each item gives the fact, how to avoid the problem, and what to tell a customer. A fact comes from the public docs (checked on 2026-11-06), or from our own runs, with the date. The docs:

```text
https://docs.confluent.io/cloud/current/lightning-tables/limitations.html
https://docs.confluent.io/cloud/current/lightning-tables/query/sql-reference.html
https://docs.confluent.io/cloud/current/lightning-tables/query/api.html
```

## The five that cause most problems

1. **A query without `LIMIT` returns every row.**
   - Fact: REST has no row limit. A large or slow query runs asynchronously, and the result comes as files. On 2026-11-06, `SELECT * FROM lq_orders` on our lab topic gave one file of 632 MB with 6,031,066 rows. The feed in Lab 1 adds rows all the time, so the result grows during the day.
   - Avoid: always add `LIMIT`, or a `WHERE` clause on a key. `tools/lq.py` stops a download above 100 MB with `result_too_large`. Change the limit with `--max-download-mb`, or set `LQ_MAX_DOWNLOAD_MB` in `tools/lq.env`. `0` means no limit, and a result of that size needs several GB of memory.
   - Tell customers: a client must set `LIMIT` or `max_result_rows`, and must handle the asynchronous answer (`202`, poll the job, download the files). The docs say that a result has a maximum size, but they give no number.
2. **MCP returns at most 200 rows, with no warning.**
   - Fact: `queryData` returns 200 rows also when you ask for more. The answer does not say that rows are missing.
   - Avoid: use REST for more rows. Exactly 200 rows means that the result was cut.
   - Tell customers: an agent that counts or sums over MCP gets a wrong number above 200 rows, and does not know it.
3. **`ACTIVE` does not mean ready.**
   - Fact: the first rows came 30 seconds to 10 minutes after `ACTIVE` (up to 12 minutes for upsert). Before that, the API answers with codes such as `FE_TABLE_NOT_FOUND`, `DP_TABLE_NOT_AVAILABLE` or `MT_METADATA_UPDATE`. The docs say that `404` also means "still being set up".
   - Avoid: retry every 30 seconds. `tools/lq.py` exits with code 2 for these codes, and `tools/lq_watch.py` retries by itself.
   - Tell customers: build retry logic for the first minutes after a table is turned on.
4. **Values come back as strings, and the format changed.**
   - Fact: every value in the JSON answer is a string. The schema gives the type. `DOUBLE` changed from `"20"` to `"20.0"` on 2026-11-02. `TIMESTAMP` changed from `2026-10-26 14:53:34.880000` to `2026-11-05 17:17:35.865+00` by 2026-11-05.
   - Avoid: compare values as numbers or times, not as text.
   - Tell customers: parse with the schema types. Do not parse the text format.
5. **No aggregates, joins or subqueries.**
   - Fact: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `GROUP BY`, `DISTINCT`, joins, subqueries, `UNION`/`INTERSECT`/`EXCEPT`, window functions, DDL and DML give an error, for example `QE_UNSUPPORTED_AGGREGATION`.
   - Avoid: prepare the data in Flink before RTCE, or use a Flink snapshot query (next section).
   - Tell customers: Lightning Tables answer "give me the rows for this key, now". They do not answer "how many, in total".

## When Lightning Tables are not the answer: Flink snapshot queries

A Flink snapshot query runs one bounded batch query, returns the result, and stops. It supports aggregates, `GROUP BY` and joins. With Tableflow on for the topic, it reads the Tableflow (Iceberg) data plus the newest Kafka data. So the result is fresh.

```bash
confluent flink query --sql "SELECT status, COUNT(*) AS n FROM lq_orders GROUP BY status;" \
  --compute-pool "$LQ_POOL_ID" --database "$LQ_CLUSTER_ID" --environment "$LQ_ENV_ID"
```

```text
+--------+---------+
| status |    n    |
+--------+---------+
| NEW    | 6031065 |
| MARKER | 4       |
+--------+---------+
```

I ran it on 2026-11-06 with CLI v4.82.0. It took 14 seconds over about 6 million rows. Lightning Tables reject the same query with `QE_UNSUPPORTED_AGGREGATION`. A lookup of one key took 12 seconds with Flink, and 0.5 seconds with Lightning Tables.

- In SQL, start with `SET 'sql.snapshot.mode' = 'now';`. In a Flink workspace, set **Mode** to **Snapshot**.
- Column names in Flink are case-sensitive and lower case (`status`). Lightning Tables show them in upper case (`STATUS`). The same SQL text does not work in both.
- Confluent bills it in CFU, and it needs a compute pool. The docs recommend a pool that is separate from your streaming statements.
- It is not a check for Tableflow freshness. It adds the newest Kafka data, so a new event shows at once.

The three ways to read a topic, in one line each:

- Lightning Tables: one key or a few rows, in under a second, seconds after the write.
- Flink snapshot query: aggregates and joins, fresh, in seconds, billed in CFU.
- Tableflow: Iceberg tables for analytics engines, about 5 minutes behind.

## More limits

- **Rate limit.** REST and MCP share one rate limit. Too many queries give HTTP 429. All people in one org share it during the labs. Use `lq_watch.py --interval 5` if you see 429.
- **Quoted names.** Without quotes, names are not case-sensitive. With quotes, they must be upper case: `"ORDER_ID"` works, `"order_id"` gives `FE_SQL_VALIDATION_ERROR`.
- **Committed data only.** RTCE reads only committed data. A Flink statement that writes to the same topic holds back every new event until its next commit, about 60 seconds. Tell customers: the freshness depends on the producer.
- **Schema changes.** The docs: after an unsupported schema change, the table stops taking new records, and queries return an error. The status stays `ACTIVE`. Disable and enable does not fix it. Renamed columns, new required columns and key schema changes are not supported. Tell customers: test a schema change on a copy first.
- **Data types.** The docs: a topic with `INTERVAL`, `MULTISET`, `RAW`, a `TIMESTAMP WITH TIME ZONE` field, or a `MAP` with nullable non-character keys cannot become a Lightning Table. The Flink type `TIMESTAMP_LTZ` works: Lab 1 `DESCRIBE` shows it as `TIMESTAMP(6) WITH TIME ZONE`.
- **Retention.** The docs: the table keeps its data after the topic retention ends. An upsert table deletes a key only on a tombstone. Tell customers: plan the deletion of personal data.
- **Async job visibility.** The docs: every principal that can read the topic can poll every async job on that topic, and get its result URL. Tell customers: the result of a query is visible to all readers of the topic.
- **Upsert key.** The docs: the key can be raw bytes, Avro, Protobuf or JSON Schema. The labs tested only a raw key. With a raw key, the Flink column must be named `key`. The topic needs `cleanup.policy=compact`.
- **Same topic name again.** Do not delete a topic and create it again with the same name. In our runs RTCE kept old state. For a table in `FAILED`, the docs say: fix the cause, then disable and enable the table.
- **Keys.** Only a Global API key works for queries. A Cloud key gives `resource_not_found` over MCP, and a cluster key gives HTTP 401. A user can have at most 2 Global keys. A Global key cannot produce to a Basic or Standard cluster: the labs use a Kafka key for that.
- **Clouds and networks.** AWS only. The Console query pane does not work over a private network. The docs: with private networking, MCP access can cross environments.
- **Tableflow.** It publishes about every 5 minutes. This is best effort, and a busy topic can publish sooner. No setting makes it faster. The DuckDB catalog token expires after 15 minutes.
- **Cost.** The cluster, the Flink pool, RTCE and Tableflow cost money while they run. Clean up in Lab 1 deletes them.
