# Lightning Queries track — EMEA CSE Day, 2026-11-06

In this track, you turn on the Real-Time Context Engine (RTCE) for a Kafka topic. Then you query the topic with Lightning Queries over REST, or over MCP. Later labs compare the freshness with Tableflow, and query a table with one current row per key.

This repository has Lab 1 now. Labs 2 to 4 follow before 2026-11-06.

## What you will do

- **Lab 1, enable and query (about 40 minutes).** Create a cluster and a Flink pool. Fill a topic with order events. Turn on RTCE. Run a Lightning query. Proof: `tools/lq.py` returns rows. Start here: [labs/01-enable-and-query/README.md](labs/01-enable-and-query/README.md).
- **Lab 2, freshness (coming).** Write a marker event. See it in Lightning Queries within seconds, while Tableflow does not have it yet.
- **Lab 3, upsert (coming).** Query a table that holds one current row per key, and time one update in Lightning Queries and in Tableflow.
- **Lab 4, optional exercises (coming).**

## Prerequisites

Install and check these before the CSE Day:

- A Confluent Cloud org where you can create an environment (OrganizationAdmin). Your own org is fine.
- Or, if you cannot create an environment: an existing environment where you are EnvironmentAdmin. Other people can run the lab in it too.
- An AWS region with RTCE. Lightning Queries runs only on AWS. List the regions with `confluent rtce region list`.
- macOS, Linux, or WSL on Windows. The labs use `bash`, `sed` and `awk`.
- Confluent CLI v4.81 or later, and `jq`.
- Python 3.10 or later. The tools use only the standard library, so you install no Python packages.
- DuckDB 1.1 or later (Lab 2).

## Your environment

- Lab 1 builds it: an environment `lq-$LQ_NAME`, a Standard cluster `lq-$LQ_NAME-cluster`, and a Flink pool `lq-$LQ_NAME-pool`. `LQ_NAME` is your short name, for example your initials.
- If you cannot create an environment, Lab 1 builds the cluster and the pool in an existing environment.
- REST access (Lightning Queries) is Early Access, and only some orgs have it. MCP works in every org. `tools/lq.py` uses REST if it can, and MCP if it cannot. You get the same rows.
- It costs money while it runs. Lab 1, Clean up, deletes all of it.

## When something fails

1. Run `python3 tools/lq_doctor.py`. It checks your setup and prints a fix for each problem. It only reads.
2. Read [docs/troubleshooting.md](docs/troubleshooting.md).

## Tools

The labs use these scripts. Each script starts with a comment that gives its usage. The Python tools need Python 3.10 or later and use only the standard library. They read `tools/lq.env`. An environment variable with the same name overrides the file.

Run queries:

- `tools/lq.py "<sql>"` runs one Lightning query and prints the rows as a table. `--json` prints the columns and rows as JSON. `-f <file>` reads the SQL from a file. Exit code 0 means rows (or no rows). Exit code 2 means "retry later", for example when the table is not ready yet. Exit code 1 means any other error.
- `tools/lq_watch.py "<sql>"` repeats a query once per second until it returns rows, and prints the seconds it waited. `--until-empty` waits until the query returns no rows. Lab 2 and Lab 3 measure the freshness with it.

Check the setup:

- `tools/lq_doctor.py` checks the settings, the resources, the Flink statements, RTCE, Tableflow and one query. It prints OK, WARN, FAIL or SKIP for each check, plus the fix. It only reads.
- `tools/lq_selftest.py` runs 4 live steps: `SHOW TABLES`, one `SELECT`, the expected error for `COUNT(*)`, and one marker event from produce until it is visible.
- `tools/test_lq.py` and `tools/test_doctor.py` test the tools with no network. Run `python3 -m unittest discover -s tools`.

Flink and Kafka:

- `tools/flink_run.sh <name> <sql-file-or-sql>` submits one Flink statement and prints its real status. The CLI also exits with 0 when a statement failed, so the script reads the status from the JSON.
- `tools/flink_results.sh <name>` prints the result rows of a Flink statement over the Flink REST API. The CLI has no command for this.
- `tools/marker_json.sh <order-id> [customer-id] [status] [price]` prints one order event as Avro JSON for `confluent kafka topic produce --value-format avro`.

### How `lq.py` works

1. It sends one HTTP POST to `https://sql.<region>.aws.confluent.cloud/query/v1alpha1`. The Global API key is the basic auth.
2. The body names the environment as `catalog_name` and the cluster as `database_name`. The topic name is the table name.
3. In `LQ_MODE=auto`, the answer `403 api_not_enabled` means that the org has no REST access. Then `lq.py` sends the same query to the MCP tool `queryData`, and uses MCP for the rest of the run.
4. Both paths give the same columns and rows. `lq.py` adds the time of the request and a hint for each known error code.

The same requests with `curl`. Load the settings first with `set -a; . tools/lq.env; set +a`. REST works only in an org with REST access:

```bash
curl -sS -X POST "https://sql.$LQ_REGION.aws.confluent.cloud/query/v1alpha1" \
  -u "$LQ_API_KEY:$LQ_API_SECRET" -H "Content-Type: application/json" \
  -d "{\"catalog_name\":\"$LQ_ENV_ID\",\"database_name\":\"$LQ_CLUSTER_ID\",\"query\":\"SELECT ORDER_ID, STATUS FROM lq_orders LIMIT 1\"}" \
  | jq -c '[.result.schema.columns[].name]'
```

```text
["ORDER_ID","STATUS"]
```

MCP works in every org. It needs `LQ_ORG_ID`. The answer is a JSON-RPC message. Its text field holds the result as a JSON string:

```bash
curl -sS -X POST "https://mcp.$LQ_REGION.aws.confluent.cloud/mcp/v1/context-engine/organizations/$LQ_ORG_ID/environments/$LQ_ENV_ID/kafka-clusters/$LQ_CLUSTER_ID" \
  -u "$LQ_API_KEY:$LQ_API_SECRET" -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"queryData","arguments":{"topic_name":"lq_orders","query":"SELECT ORDER_ID, STATUS FROM lq_orders LIMIT 1","max_result_rows":1}}}' \
  | sed -n 's/^data: //p' | jq -c '.result.content[0].text | fromjson | [.status, [.rows.schema.columns[].name]]'
```

```text
["success",["ORDER_ID","STATUS"]]
```

I ran both commands on 2026-11-04. Add `.result.data` (REST) or `.rows.data` (MCP) to the `jq` filter to see the rows.


## License

Apache License 2.0. See [LICENSE](LICENSE).
