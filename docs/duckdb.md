# DuckDB and Tableflow

This page is for the person who runs the track (the track lead). It tells what DuckDB does in the labs, and how Tableflow gives the data to DuckDB. Then it shows how to use DuckDB by hand. It ends with the quirks, the alternatives, and what to say in the room.

All commands ran on 2026-11-06 with DuckDB v2.0.0, curl 8.7.1 and jq 1.8.2 on macOS, in region `eu-central-1`. The outputs show invented IDs: `a1b2c3d4-0000-0000-0000-000000000000` for the org, `env-abc123` for the environment, `lkc-abc123` for the cluster. Other values are real.

## 1. What DuckDB does in the labs

- DuckDB reads the Tableflow copy of a topic. Tableflow writes this copy as Iceberg tables.
- DuckDB reads only that copy. It does not read Kafka, and it does not read the Lightning table.
- Thus, a row that DuckDB finds is a row that Tableflow published. Lab 2 and Lab 3 use this to measure when an event arrives in Tableflow, and they compare that time with Lightning.
- Three files use DuckDB:
  - `tableflow/tableflow_watch.sh <topic> <order-id>` looks for one `ORDER_ID` every 30 s. When it finds the row, it prints the minutes and seconds that it waited. It makes a new DuckDB secret on each try, so the token expiry has no effect.
  - `tableflow/tableflow_query.sh "<sql>"` runs one SQL statement on the Tableflow copy and prints CSV. The topic name is the table name. The script selects your cluster as the schema. The lab UI calls this script with fixed queries only.
  - `tableflow/duckdb_tableflow.sql` is a template for a manual session. You replace `{key}`, `{secret}`, `{org}`, `{env}`, `{lkc}` and `{topic}`.
- All three need DuckDB 1.1 or later, and `LQ_REGION`, `LQ_ORG_ID`, `LQ_ENV_ID`, `LQ_CLUSTER_ID`, `LQ_API_KEY` and `LQ_API_SECRET` (the Global API key) from `tools/lq.env`.

Check the DuckDB version:

```bash
duckdb --version
```

```text
v2.0.0 (Cyanoptera) d54cbbe3df
```

## 2. How Tableflow gives the data

- Tableflow writes each enabled topic as an Iceberg table in Confluent managed storage (S3).
- Tableflow gives an Iceberg REST catalog at this endpoint:
  `https://tableflow.<region>.aws.confluent.cloud/iceberg/catalog/organizations/<org>/environments/<env>`
- The namespace is the cluster ID (`lkc-...`). The table name is the topic name.
- A client gets an OAuth token with the client-credentials grant. The client ID and the client secret are the Global API key and its secret. The scope is `catalog`.
- The catalog signs the S3 requests for the client (`s3.remote-signing-enabled`). Thus the client needs no AWS credentials.

The calls below are the raw catalog calls. They show what DuckDB does before it reads a row. Set the endpoint first:

```bash
set -a; . tools/lq.env; set +a
export EP="https://tableflow.$LQ_REGION.aws.confluent.cloud/iceberg/catalog/organizations/$LQ_ORG_ID/environments/$LQ_ENV_ID"
```

Get a token. The command prints only the token length, not the token:

```bash
TOKEN=$(curl -s -X POST "$EP/v1/oauth/tokens" \
  -d grant_type=client_credentials -d scope=catalog \
  -d client_id="$LQ_API_KEY" --data-urlencode client_secret="$LQ_API_SECRET" \
  | jq -r .access_token)
echo "token length: ${#TOKEN}"
```

```text
token length: 1196
```

The other fields of the token answer (the command removes the token):

```bash
curl -s -X POST "$EP/v1/oauth/tokens" \
  -d grant_type=client_credentials -d scope=catalog \
  -d client_id="$LQ_API_KEY" --data-urlencode client_secret="$LQ_API_SECRET" \
  | jq 'del(.access_token)'
```

```json
{
  "expires_in": 900,
  "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
  "token_type": "bearer"
}
```

The catalog configuration. It lists the endpoints that the catalog supports. All are reads:

```bash
curl -s -H "Authorization: Bearer $TOKEN" "$EP/v1/config" | jq .
```

```json
{
  "defaults": {},
  "endpoints": [
    "GET v1/config",
    "POST /v1/oauth/tokens",
    "GET /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials"
  ],
  "overrides": {}
}
```

The namespaces. There is one namespace for each cluster with Tableflow:

```bash
curl -s -H "Authorization: Bearer $TOKEN" "$EP/v1/namespaces" | jq -c .
```

```json
{"namespaces":[["lkc-abc123"]]}
```

The tables in the cluster. There is one table for each topic with Tableflow:

```bash
curl -s -H "Authorization: Bearer $TOKEN" "$EP/v1/namespaces/$LQ_CLUSTER_ID/tables" | jq -r '.identifiers[].name'
```

```text
p00_orders
lq_orders
lq_orders_latest
p02_orders
p01_orders
p03_orders
```

One table. The `jq` filter shows only some fields: the metadata file name, the format version and the snapshot count. It also shows the last update, the field names, and the keys of `config`. Do not print the values of `config`: the `token` value is a credential.

```bash
curl -s -H "Authorization: Bearer $TOKEN" "$EP/v1/namespaces/$LQ_CLUSTER_ID/tables/lq_orders_latest" \
  | jq '{metadata_file: (."metadata-location"|split("/")|last),
         format_version: .metadata."format-version",
         snapshots: (.metadata.snapshots|length),
         last_updated: (.metadata."last-updated-ms"/1000|todate),
         fields: [.metadata.schemas[-1].fields[].name],
         config_keys: (.config|keys)}'
```

```json
{
  "metadata_file": "00009-4a3284b8-ae2f-4867-9baa-f2c83beaef5f.metadata.json",
  "format_version": 2,
  "snapshots": 13,
  "last_updated": "2026-11-06T04:09:38Z",
  "fields": [
    "key",
    "customer_id",
    "order_id",
    "price",
    "status",
    "produced_at",
    "$$topic",
    "$$partition",
    "$$headers",
    "$$leader-epoch",
    "$$offset",
    "$$timestamp",
    "$$timestamp-type",
    "$$raw-key",
    "$$raw-value"
  ],
  "config_keys": [
    "s3.remote-signing-enabled",
    "token"
  ]
}
```

- The catalog gives metadata only. It gives no rows.
- To get rows, a reader must do more steps after the catalog call:
  1. Read the metadata file. It gives the current snapshot.
  2. Read the manifest list of that snapshot (Avro).
  3. Read the manifests (Avro). They give the data files.
  4. Read the data files (Parquet). They hold the rows.
- These steps are the work of a query engine. DuckDB does them. `curl` does not.

## 3. Use DuckDB by hand

Use this session to show the Tableflow copy outside the lab scripts.

Set the endpoint as an environment variable:

```bash
set -a; . tools/lq.env; set +a
export LQ_TF_EP="https://tableflow.$LQ_REGION.aws.confluent.cloud/iceberg/catalog/organizations/$LQ_ORG_ID/environments/$LQ_ENV_ID"
DUCKDB_AGENT_MODE=0 duckdb
```

- `getenv()` reads the key, the secret and the endpoint from the environment. Thus the secret is not in a SQL file and not in the shell history.
- In the DuckDB CLI, `getenv()` works. In other DuckDB clients it can be disabled.
- `DUCKDB_AGENT_MODE=0` keeps the normal output. DuckDB 2.0 changes its output when it thinks that an AI agent runs it. DuckDB 1.x ignores the variable.

Type these statements in the session:

```sql
INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;
CREATE OR REPLACE SECRET tf (TYPE ICEBERG, CLIENT_ID getenv('LQ_API_KEY'), CLIENT_SECRET getenv('LQ_API_SECRET'), ENDPOINT getenv('LQ_TF_EP'), OAUTH2_SCOPE 'catalog');
ATTACH 'warehouse' AS tf (TYPE iceberg, SECRET tf, ENDPOINT getenv('LQ_TF_EP'));
SHOW ALL TABLES;
```

```text
WARNING:
The ICEBERG secret option 'endpoint' is deprecated; use 'uri' instead

┌─────────┐
│ Success │
│ boolean │
├─────────┤
│ true    │
└─────────┘
WARNING:
The Iceberg attach option 'endpoint' is deprecated; use 'uri' instead

┌──────────┬────────────┬──────────────────┬──────────────┬──────────────┬───────────┐
│ database │   schema   │       name       │ column_names │ column_types │ temporary │
│ varchar  │  varchar   │     varchar      │  varchar[]   │  varchar[]   │  boolean  │
├──────────┼────────────┼──────────────────┼──────────────┼──────────────┼───────────┤
│ tf       │ lkc-abc123 │ lq_orders        │ [__]         │ [UNKNOWN]    │ false     │
│ tf       │ lkc-abc123 │ lq_orders_latest │ [__]         │ [UNKNOWN]    │ false     │
│ tf       │ lkc-abc123 │ p00_orders       │ [__]         │ [UNKNOWN]    │ false     │
│ tf       │ lkc-abc123 │ p01_orders       │ [__]         │ [UNKNOWN]    │ false     │
│ tf       │ lkc-abc123 │ p02_orders       │ [__]         │ [UNKNOWN]    │ false     │
│ tf       │ lkc-abc123 │ p03_orders       │ [__]         │ [UNKNOWN]    │ false     │
└──────────┴────────────┴──────────────────┴──────────────┴──────────────┴───────────┘
```

- The `ATTACH` makes the catalog a DuckDB database with the name `tf`. The schema is the cluster ID. The tables are the topics.
- `SHOW ALL TABLES` shows no columns (`[__]`, `UNKNOWN`). DuckDB loads the columns of a table only when a query uses that table.

Select the cluster as the schema, then look up key `9001` in the upsert table. Use your cluster ID in quotes, because the ID has a hyphen:

```sql
USE tf."lkc-abc123";
SELECT key, order_id, status, price, produced_at FROM lq_orders_latest WHERE key = '9001';
```

```text
┌─────────┬──────────────┬─────────┬────────┬──────────────────────────┐
│   key   │   order_id   │ status  │ price  │       produced_at        │
│ varchar │   varchar    │ varchar │ double │ timestamp with time zone │
├─────────┼──────────────┼─────────┼────────┼──────────────────────────┤
│ 9001    │ UI-L3-034411 │ NEW     │   10.0 │ 2026-11-06 04:44:11+01   │
└─────────┴──────────────┴─────────┴────────┴──────────────────────────┘
```

Count the rows in the append table:

```sql
SELECT count(*) AS rows_in_tableflow FROM lq_orders;
```

```text
┌───────────────────┐
│ rows_in_tableflow │
│       int64       │
├───────────────────┤
│           6031069 │
└───────────────────┘
```

Show the Kafka partition, offset and timestamp of the newest rows. These are Iceberg metadata columns. Put their names in double quotes:

```sql
SELECT "$$partition" AS p, "$$offset" AS off, "$$timestamp" AS ts, order_id, status
FROM lq_orders ORDER BY "$$timestamp" DESC LIMIT 3;
```

```text
┌───────┬─────────┬────────────────────────────┬──────────────┬─────────┐
│   p   │   off   │             ts             │   order_id   │ status  │
│ int32 │  int64  │  timestamp with time zone  │   varchar    │ varchar │
├───────┼─────────┼────────────────────────────┼──────────────┼─────────┤
│     1 │ 1013444 │ 2026-11-06 05:05:53.935+01 │ UI-L2-040549 │ MARKER  │
│     5 │ 1012579 │ 2026-11-06 04:44:16.383+01 │ UI-L3-034411 │ NEW     │
│     0 │ 1008905 │ 2026-11-06 04:38:49.693+01 │ UI-L2-033845 │ MARKER  │
└───────┴─────────┴────────────────────────────┴──────────────┴─────────┘
```

The full session (setup and the three queries) took 1.9 s on a laptop.

## 4. Quirks

- Token expiry. The token answer says `"expires_in": 900` (15 minutes). DuckDB does not get a new token by itself. Older notes say about 30 minutes; the test below shows 15 minutes. On 2026-11-06, one DuckDB session queried `lq_orders_latest`, waited 16 minutes, and queried it again:

  ```text
  before,252
  HTTP Error: GetTableInformation endpoint returned response code Unauthorized_401 with message "Invalid or missing auth token"
  ```

  Fix: run the setup statements again (`CREATE OR REPLACE SECRET` and `ATTACH`). `tableflow_watch.sh` and `tableflow_query.sh` make a new secret on each call, so they do not see this problem.
- Deprecation warning. DuckDB 2.0 says that the option `endpoint` is deprecated and that you must use `uri`. The scripts keep `endpoint` because DuckDB 1.x needs it. The warning is not an error. `tableflow_query.sh` sends it to `/dev/null`.
- Time zone. DuckDB shows timestamps in the local time zone of the laptop. Lightning shows UTC. It is the same moment. On 2026-11-06, the same row:
  - DuckDB: `2026-11-06 04:44:11+01`
  - Lightning (`python3 tools/lq.py "SELECT KEY, ORDER_ID, PRODUCED_AT FROM lq_orders_latest WHERE KEY = '9001'"`): `2026-11-06 03:44:11+00`
- Extra columns. The Iceberg table has metadata columns that Lightning does not have: `$$topic`, `$$partition`, `$$headers`, `$$leader-epoch`, `$$offset`, `$$timestamp`, `$$timestamp-type`, `$$raw-key`, `$$raw-value`. `SELECT *` in DuckDB thus gives more columns than `SELECT *` in Lightning.
- Column names. DuckDB shows lower-case names (`order_id`). Lightning shows upper-case names (`ORDER_ID`). The lab UI uses `SELECT ORDER_ID ...` for Lightning and `SELECT order_id ...` for DuckDB.
- Local files. DuckDB can read and write local files (`COPY ... TO`, `read_csv`, and more). Never let a tool send free SQL from a user to DuckDB. The lab UI sends only fixed queries to DuckDB. The free SQL box of Lab 1 goes to Lightning, not to DuckDB.
- Delay. Tableflow publishes about every 5 minutes. This is best effort, not a guarantee. Thus DuckDB can miss the newest rows that Lightning already returns. Lightning cannot count (`COUNT` gives `QE_UNSUPPORTED_AGGREGATION`), so compare one `ORDER_ID`, not a count.
- Schemas. The Tableflow documentation says that the key and the value must have a schema. Our topics worked without that: `lq_orders` has no key (value only), and `lq_orders_latest` has a raw key. Both are tables in the catalog, and DuckDB reads both. We did not see enforcement.

## 5. Why DuckDB, and the alternatives

We use DuckDB because it is one binary, it needs no server, and it reads the Tableflow catalog with OAuth and remote signing. The lab tools need no Python packages.

- Flink snapshot query.
  - Needs: a Flink compute pool and a statement in snapshot mode.
  - It is not a Tableflow check. It joins the Tableflow data with the newest Kafka data. Thus it shows a new event at once, and it cannot time Tableflow.
  - It is a strong query alternative for aggregates and joins. On 2026-11-06, `SELECT status, COUNT(*) ... GROUP BY status` over 6 million rows took 14 s. A one-key lookup took 12 s. The same lookup in Lightning took 0.5 s.
  - Right for a customer who already uses Flink and needs aggregates or joins over history, and who accepts seconds of latency.
- PyIceberg.
  - Needs: the pip packages `pyiceberg` and `pyarrow`. The lab tools use only the Python standard library.
  - Right for a customer who reads Iceberg from Python code, for example in a data pipeline or a notebook.
- Spark.
  - Needs: a JVM and the Iceberg Spark runtime. It is heavy for a laptop.
  - Right for a customer who already runs Spark jobs on large data.
- Trino.
  - Needs: a server to run, and a catalog configuration.
  - Right for a customer who wants a shared SQL engine for many users over many Iceberg tables.
- Databricks or Snowflake.
  - Needs: an account, a catalog integration with Tableflow, and money for compute.
  - Right for a customer who already uses one of them. Then the Tableflow tables appear next to their other data.
- Only `curl`.
  - Needs: nothing more than `curl` and `jq`.
  - It gets metadata (tables, snapshots, schemas), not rows. Right for a quick check that a table exists and when it changed last.

## 6. For the track lead

What to say in the room:

1. Tableflow writes each topic as an Iceberg table, and DuckDB reads that table through the Tableflow REST catalog with your Global API key.
2. DuckDB reads only the Tableflow copy, so the time until DuckDB finds your marker is the Tableflow delay.
3. Lightning reads the Kafka data directly, so it finds the same marker in seconds, not minutes.

The 3 most likely DuckDB failures on the day:

- `duckdb not found. Install DuckDB 1.1 or later.`, or an old version (`duckdb --version` gives less than 1.1). Fix: `brew install duckdb` on macOS, or download the CLI from duckdb.org. Then run `duckdb --version` again.
- HTTP 401 (`Invalid or missing auth token`) in a manual DuckDB session after 15 minutes. Cause: the catalog token expired. Fix: run `CREATE OR REPLACE SECRET` and `ATTACH` again, or start a new session. The scripts do this on each call.
- The table does not exist, or `tableflow_watch.sh` prints `not yet (no answer: ...)` all the time. Cause: Tableflow is not `RUNNING` yet for the topic, or the key is wrong. Fix: check the Tableflow phase (Lab 2, step 5). Wait until it is `RUNNING`, then wait about 5 more minutes for the first snapshot. Use `SHOW ALL TABLES` (section 3) or the `curl` table list (section 2) to see which tables exist.
