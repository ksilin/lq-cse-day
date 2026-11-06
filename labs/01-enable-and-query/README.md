# Lab 1: turn on RTCE for a topic and run a Lightning query

Verified: 2026-11-04 on Confluent Cloud (AWS eu-central-1), Confluent CLI v4.82.0, Python 3.14. First run in a new environment: 2026-10-27.

## What we test

Can you turn on the Real-Time Context Engine (RTCE) for your own topic and query it?

1. A Flink statement fills your topic `lq_orders` with order events.
2. RTCE for the topic is `ACTIVE`.
3. A Lightning query with `tools/lq.py` returns rows from the topic.

You build a small environment in your own org, or you use an existing environment. You create the topic and the feed with Flink SQL. Then you turn on RTCE and query the topic.

This lab takes about 40 minutes. About 10 minutes of it is waiting.

## How it works

Flink SQL creates the topic `lq_orders` with an Avro value schema (`sql/01_create_orders.sql`).
A continuous Flink statement copies the built-in sample data `examples.marketplace.orders` into it (`sql/02_feed_orders.sql`).
RTCE reads the topic and keeps it as a Lightning table in its own storage.
You can query a Lightning table in two ways: over REST (Lightning Queries) or over MCP (the `queryData` tool).
REST is Early Access, and it works only in an org with REST access. MCP works in every org.
`tools/lq.py` tries REST first. If the org has no REST access, it uses MCP. Both return the same rows.

## Requirements

- A Confluent Cloud org where you can create an environment (OrganizationAdmin). Your own org is fine.
- Or, if you cannot create an environment: an existing environment where you are EnvironmentAdmin. Other people can run the lab in it too.
- An AWS region with RTCE: `confluent rtce region list`. The examples use `eu-central-1`.
- Confluent CLI v4.81 or later: `confluent version`. And `jq`.
- Python 3.10 or later: `python3 --version`. The tools use only the Python standard library, so you install no packages.
- This repository, cloned. Run all commands from its root folder.
- Cost: a Standard cluster, a Flink pool, RTCE and Tableflow run for about 3 hours. The section Clean up deletes all of them.

## Run

### Part A: login and settings (about 5 minutes)

1. Log in. If you have more than one org, select the org for the lab with `confluent organization use <org-id>`.

```bash
confluent login --save
```

2. Create your settings file, and add the ID of your org to it.

```bash
cp tools/lq.env.example tools/lq.env
```

Then open `tools/lq.env` in an editor. Change `LQ_NAME=changeme` to your initials, for example `LQ_NAME=ksi`. Every resource name of the labs contains it, for example `lq-ksi-feed-orders`. Use 2 to 12 lowercase letters or digits, first a letter. Two people with the same name in one environment break each other's Flink statements.

```bash
echo "LQ_ORG_ID=$(confluent organization describe -o json | jq -r .id)" >> tools/lq.env
```

3. Optional: change `LQ_REGION` in `tools/lq.env` to another region from `confluent rtce region list`. Then load the settings.

```bash
set -a; . tools/lq.env; set +a
```

Part A done. Next: build your environment.

### Part B: your environment (about 5 minutes)

Each command adds one ID to `tools/lq.env`. Load the settings again after each command.

4. Set the environment. Use step 4a for a new environment, or step 4b for an existing environment.

4a. Create a new environment `lq-$LQ_NAME`. This needs OrganizationAdmin, and the org must be below its limit of environments.

```bash
echo "LQ_ENV_ID=$(confluent environment create "lq-$LQ_NAME" --governance-package essentials -o json | jq -r .id)" >> tools/lq.env; set -a; . tools/lq.env; set +a
```

4b. Use an existing environment if you cannot create one. You must be EnvironmentAdmin on it. Other people can run the lab in the same environment, because every name contains your `LQ_NAME`. Use a name that nobody else in the environment uses. Find the ID of your environment:

```bash
confluent environment list
```

Then add the ID to `tools/lq.env`. Replace `env-xxxxxx` with your ID.

```bash
echo "LQ_ENV_ID=env-xxxxxx" >> tools/lq.env; set -a; . tools/lq.env; set +a
```

Steps 5 and 6 create your own cluster and Flink pool in this environment. Clean up, step 1b, deletes only your resources, and keeps the environment.

5. Create a Standard cluster in your region. It is `UP` after about 20 seconds.

```bash
echo "LQ_CLUSTER_ID=$(confluent kafka cluster create "lq-$LQ_NAME-cluster" --cloud aws --region "$LQ_REGION" --type standard --environment "$LQ_ENV_ID" -o json | jq -r .id)" >> tools/lq.env; set -a; . tools/lq.env; set +a
```

6. Create a Flink compute pool in the same region.

```bash
echo "LQ_POOL_ID=$(confluent flink compute-pool create "lq-$LQ_NAME-pool" --cloud aws --region "$LQ_REGION" --max-cfu 10 --environment "$LQ_ENV_ID" -o json | jq -r .id)" >> tools/lq.env; set -a; . tools/lq.env; set +a
```

7. Create a Global API key, and add it to `tools/lq.env`. The secret goes into the file, not onto the screen. A Cloud API key does not work. This is also true for a Cloud key from Terraform.

```bash
confluent api-key create --resource global \
  --description "Lightning Queries lab $LQ_NAME" \
  -o json \
  | jq -r '"LQ_API_KEY=\(.api_key)\nLQ_API_SECRET=\(.api_secret)"' \
  >> tools/lq.env
set -a; . tools/lq.env; set +a
```

If this fails, you already have 2 Global keys. That is the limit. Use one of them: add `LQ_API_KEY` and `LQ_API_SECRET` to `tools/lq.env` by hand. The key must belong to the same org.

8. Check the setup with the doctor. It prints one line per check: the result, the name of the check, and the details. Below a `WARN` or `FAIL` line, it prints a `fix:` line.

<!-- doc-check: skip -->
```bash
python3 tools/lq_doctor.py
```

This is the output right after step 7. I ran it on 2026-10-27.

```
OK    CLI login                        org My Org (a1b2c3d4-e5f6-4789-abcd-0123456789ab)
OK    name                             LQ_NAME=ksi, statements lq-ksi-...
OK    region                           eu-central-1 has RTCE
OK    environment                      env-abc123
OK    cluster                          lkc-abc123 STANDARD UP in eu-central-1
OK    Flink pool                       lfcp-abc123 PROVISIONED
WARN  query over REST                  FE_CUSTOMER_NOT_FOUND: not ready yet
      fix: Normal before RTCE is on (Lab 1, step 11). After that, wait 1 minute, then check again.
WARN  query over MCP                   FE_CUSTOMER_NOT_FOUND: not ready yet
      fix: Normal before RTCE is on (Lab 1, step 11). After that, wait 1 minute, then check again.
SKIP  RTCE lq_orders                   RTCE is off for this topic
SKIP  RTCE lq_orders_latest            RTCE is off for this topic
SKIP  feed lq-ksi-feed-orders          no statement with this name
SKIP  feed lq-ksi-feed-orders-latest   no statement with this name
SKIP  Tableflow lq_orders              Tableflow is off for this topic
SKIP  Kafka key                        LQ_KAFKA_KEY is not set
SKIP  schema ID                        LQ_SCHEMA_ID is not set
```

Read the first word of each line:

- `OK` and `SKIP` are correct now. `SKIP` means that a later step sets this up.
- `WARN` on the two `query` lines is normal now, because RTCE is off until step 11.
- `WARN` on the line `name` means that `LQ_NAME` is still `changeme`. Change it in `tools/lq.env` now, and load the settings again. Step 9 creates the first statement with this name.
- `FAIL` must go away before step 9. Do what the `fix:` line says, then run the doctor again.

Part B done. Next: create the topic and the feed.

### Part C: topic, feed and RTCE (about 2 minutes, then wait)

9. Wait first: step 5 must be at least 4 minutes ago. A new cluster needs some minutes before Flink can create a table in it. This is true for a new and for an existing environment. If you start too early, step 9 fails with "technical difficulties".

   Then create the table `lq_orders`. Flink creates the topic and the value schema. The status must be `COMPLETED`.

```bash
confluent flink statement create lq-$LQ_NAME-create-orders --sql "$(cat sql/01_create_orders.sql)" --environment "$LQ_ENV_ID" --database "$LQ_CLUSTER_ID" --compute-pool "$LQ_POOL_ID" --wait
```

If the status is `FAILED` with "technical difficulties", the cluster or the environment is too new. The CLI can also print `Statement with name "lq-$LQ_NAME-create-orders" already exists` for the same failure. Wait 1 minute, delete the statement, and run step 9 again:

```bash
confluent flink statement delete lq-$LQ_NAME-create-orders --force --environment "$LQ_ENV_ID" --cloud aws --region "$LQ_REGION"
```

CLI v4.82.0 can print `RUNNING` here, because `--wait` can return before the end. This is not an error. Wait 10 seconds and check the status. It must be `COMPLETED`:

```bash
confluent flink statement describe lq-$LQ_NAME-create-orders \
  --environment "$LQ_ENV_ID" --cloud aws --region "$LQ_REGION" -o json | jq -r .status
```

Do not go on before step 9 is `COMPLETED`. Without the table, step 10 fails with `Cannot find table`, and step 11 sets RTCE to `FAILED`.

10. Start the feed. It runs until you stop it. The status must be `RUNNING`.

```bash
confluent flink statement create lq-$LQ_NAME-feed-orders --sql "$(cat sql/02_feed_orders.sql)" --environment "$LQ_ENV_ID" --database "$LQ_CLUSTER_ID" --compute-pool "$LQ_POOL_ID" --wait
```

11. Turn on RTCE for the topic. It waits until the phase is `ACTIVE`, about 40 seconds.

```bash
confluent rtce rtce-topic create --cloud aws --region "$LQ_REGION" --topic-name lq_orders --description "Orders (append). One row per order event." --environment "$LQ_ENV_ID" --cluster "$LQ_CLUSTER_ID" --wait --timeout 10m
```

12. Turn on Tableflow for the same topic. Lab 2 needs it, and it takes about 2 to 8 minutes to start.

```bash
confluent tableflow topic enable lq_orders --cluster "$LQ_CLUSTER_ID" --environment "$LQ_ENV_ID" --storage-type MANAGED --table-formats ICEBERG
```

13. Run your first Lightning query. Always keep the `LIMIT`: Notes explain why.

```bash
python3 tools/lq.py "SELECT * FROM lq_orders LIMIT 5"
```

If you get `FE_CUSTOMER_NOT_FOUND`, `FE_TABLE_NOT_FOUND`, `DP_TABLE_NOT_AVAILABLE` or `FE_BACKEND_UNAVAILABLE`, the table is not ready yet. Retry every 30 seconds. On 2026-10-27 the first rows came about 30 seconds after `ACTIVE`. Up to 10 minutes is normal.

If you get `0 rows`, the feed has not committed yet. Flink writes in transactions and commits about once per minute. RTCE reads only committed data. Retry after one minute.

After step 10, the doctor shows `WARN feed lq-$LQ_NAME-feed-orders RUNNING`. This is normal. The feed must run in Lab 1. Lab 2 stops it.

If `lq.py` prints `note: this org has no Lightning Queries REST access`, it used MCP. Set `LQ_MODE=mcp` in `tools/lq.env` to skip the REST try next time.

Part C done. Next: look at the results.

## Look

I ran these commands on 2026-10-27 in an org with REST access. In an org without REST access, `lq.py` prints the same rows, plus one `note:` line.

The feed statement runs. This is the state right after step 10: Lab 2 stops the feed later, so doc-check skips this block. The CLI notice about the Flink endpoint goes to stderr, so `2>/dev/null` hides it.

<!-- doc-check: skip -->
```bash
confluent flink statement describe lq-$LQ_NAME-feed-orders --environment "$LQ_ENV_ID" --cloud aws --region "$LQ_REGION" -o json 2>/dev/null | jq -r .status
```

```
RUNNING
```

Your table is `ACTIVE` in RTCE. `SHOW TABLES` lists every Lightning table in the cluster, so `grep` keeps only this one.

```bash
python3 tools/lq.py --json "SHOW TABLES" | jq -r '.rows[] | .[0] + " " + .[2]' | grep "^lq_orders "
```

```
lq_orders ACTIVE
```

RTCE knows the columns. The names are in upper case.

```bash
python3 tools/lq.py --json "DESCRIBE lq_orders" | jq -r '.rows[] | .[0] + " " + .[1]'
```

```
ORDER_ID STRING
CUSTOMER_ID INT
PRODUCT_ID STRING
PRICE DOUBLE
STATUS STRING
SOURCE STRING
PRODUCED_AT TIMESTAMP(6) WITH TIME ZONE
```

A filter with two conditions works. The second condition skips your own orders from Lab 2 and Lab 3. Every value comes back as a string.

```bash
python3 tools/lq.py --json "SELECT STATUS, SOURCE FROM lq_orders WHERE PRICE > 0 AND SOURCE = 'feed' LIMIT 3" | jq -c .rows
```

```
[["NEW","feed"],["NEW","feed"],["NEW","feed"]]
```

The same query over MCP, the path that works in every org. `LQ_MODE=mcp` sets the path for one command.

```bash
LQ_MODE=mcp python3 tools/lq.py --json "SELECT STATUS, SOURCE FROM lq_orders WHERE PRICE > 0 AND SOURCE = 'feed' LIMIT 3" | jq -c .rows
```

```
[["NEW","feed"],["NEW","feed"],["NEW","feed"]]
```

Aggregates do not work. `grep` keeps only the error code, because the message text differs a little between REST and MCP.

```bash
python3 tools/lq.py "SELECT COUNT(*) FROM lq_orders" 2>&1 | grep -o 'QE_[A-Z_]*' | head -1
```

```
QE_UNSUPPORTED_AGGREGATION
```

## Expected

Step 13 prints rows and a time. I ran it with `LIMIT 3` on 2026-11-05:

```
ORDER_ID                              CUSTOMER_ID  PRODUCT_ID  PRICE  STATUS  SOURCE  PRODUCED_AT
------------------------------------  -----------  ----------  -----  ------  ------  --------------------------
fb4df270-c084-4e11-ac37-3fe3119b9614  3068         1072        85.65  NEW     feed    2026-11-05 17:17:35.865+00
5352e0e2-b43a-4c38-84f3-5c7a6e3a39b9  3089         1399        89.08  NEW     feed    2026-11-05 17:17:35.865+00
aa58fd22-573f-4019-88c9-db5d749275d0  3040         1429        54.86  NEW     feed    2026-11-05 17:17:35.865+00
3 rows in 0.51 s
```

You are done with Lab 1 when `lq.py` prints rows from your table.

## Notes

- The raw REST request, for an org with REST access: `curl -sS -X POST "https://sql.$LQ_REGION.aws.confluent.cloud/query/v1" -u "$LQ_API_KEY:$LQ_API_SECRET" -H "Content-Type: application/json" -d "{\"catalog_name\":\"$LQ_ENV_ID\",\"database_name\":\"$LQ_CLUSTER_ID\",\"query\":\"SELECT STATUS FROM lq_orders LIMIT 1\"}" | jq -c .result.data`. The section Tools of the main README shows the REST and the MCP request.
- `403 api_not_enabled` over REST means that the org has no REST access. It can also mean that the Global key belongs to another org. A Global key belongs to the org that was selected when you created it.
- Column names are not case-sensitive without quotes. With quotes they must be upper case: `"ORDER_ID"` works, `"order_id"` fails with `FE_SQL_VALIDATION_ERROR`.
- **Always add `LIMIT`, or a `WHERE` clause on a key.** Without it, REST returns every row. On 2026-11-06, `SELECT * FROM lq_orders` gave one file of 632 MB with 6,031,066 rows. The feed adds rows all the time. `tools/lq.py` stops a download above 100 MB with `result_too_large`. More limits: [docs/limits.md](../../docs/limits.md).
- `tools/lq.py` shows the first 100 rows in the table, and `--json` prints all. MCP returns at most 200 rows.
- `PRODUCED_AT` is in UTC. It is a `TIMESTAMP_LTZ` in Flink. `DESCRIBE` shows it as `TIMESTAMP(6) WITH TIME ZONE`, and it works.
- The CLI exits with 0 also when a Flink statement fails. Check the status with the first Look command, or with `python3 tools/lq_doctor.py`.
- `confluent lightning query` prints `Table does not exist: 'lq_orders'` for a table that is not ready yet. `lq.py` shows the same case as `FE_TABLE_NOT_FOUND`. Retry in 30 seconds.
- Never delete a topic and create it again under the same name. RTCE can keep old state for it. Use a new name.

## Clean up

Keep everything if you continue with Lab 2 and Lab 3. Do this when you stop. It stops all costs of the lab.

1. Delete what you created. Use step 1a if you created the environment (step 4a). Use step 1b if you used an existing environment (step 4b).

1a. Delete the environment. This deletes the cluster, the Flink pool, the topic, RTCE and Tableflow.

```bash
confluent environment delete "$LQ_ENV_ID" --force
```

1b. Keep the existing environment, and delete only your resources in it. First delete your Flink statements, also those of Labs 2 to 4. The command deletes only names with your `LQ_NAME`. It prints an error for a statement that does not exist. You can ignore that error.

```bash
for s in create-orders feed-orders marker-2 create-orders-latest feed-orders-latest upd-1 upd-2 cancel-1; do confluent flink statement delete "lq-$LQ_NAME-$s" --force --environment "$LQ_ENV_ID" --cloud aws --region "$LQ_REGION"; done
```

Then delete the Flink pool and the cluster. The cluster delete also deletes the topic.

```bash
confluent flink compute-pool delete "$LQ_POOL_ID" --environment "$LQ_ENV_ID" --force
```

```bash
confluent kafka cluster delete "$LQ_CLUSTER_ID" --environment "$LQ_ENV_ID" --force
```

The schemas of `lq_orders` and `lq_orders_latest` stay in the Schema Registry of the environment. They cost nothing.

2. Delete the Kafka key of Lab 2, step 2. Skip this step if you did not do Lab 2.

```bash
confluent api-key delete "$LQ_KAFKA_KEY" --force
```

   Then delete the Global key of step 7. Skip this if it is a key that you had before the lab.

```bash
confluent api-key delete "$LQ_API_KEY" --force
```

3. Delete the settings file. It holds the secret.

```bash
rm tools/lq.env
```

## Versions

- Confluent CLI v4.82.0. Homebrew cask `confluent-cli` shows 4.82.0 as newest on 2026-11-04. The labs need v4.81 or later. `confluent lightning query` needs v4.82.
- REST API path `query/v1`, as in the docs. The old path `query/v1alpha1` also still works (2026-11-04).
- Python 3.14.7 on the test machine. `tools/lq.py` needs Python 3.10 or later.
