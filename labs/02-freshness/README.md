# Lab 2: a new event is in Lightning Queries in seconds and in Tableflow in minutes

Verified: 2026-11-05 on Confluent Cloud (AWS eu-central-1), Confluent CLI v4.82.0, DuckDB v2.0.0, Python 3.14. First run in a new environment: 2026-10-27.

## What we test

How fast does a new event show in Lightning Queries, and how fast in Tableflow?

1. A marker event that you write shows in a Lightning query within seconds.
2. At that moment, Tableflow does not have the marker yet.
3. Tableflow shows the marker some minutes later.

You write one marker event into `lq_orders`. Two watch scripts query Lightning and Tableflow until the marker shows. Each prints the time it waited.

This lab takes about 40 minutes. About 5 minutes of it is waiting for Tableflow.

## How it works

`confluent kafka topic produce` writes one event to your cluster, without a transaction. It is a normal Kafka producer.
RTCE reads the topic and updates the Lightning table within seconds.
Tableflow writes the topic to an Iceberg table in Confluent storage, about every 5 minutes.
`tools/lq_watch.py` repeats a Lightning query every second until the marker shows.
`tableflow/tableflow_watch.sh` repeats a DuckDB query on the Iceberg table every 30 seconds until the marker shows.

## Requirements

- Lab 1 is done. RTCE and Tableflow are on for `lq_orders`.
- DuckDB 1.1 or later: `duckdb --version`.
- The settings in `tools/lq.env` from Lab 1.

## Run

### Part A: settings for the marker producer (about 5 minutes)

1. Load the settings from Lab 1.

```bash
set -a; . tools/lq.env; set +a
```

2. Create a Kafka API key for your cluster, and add it to `tools/lq.env`. Your Global key does not work here, because Global keys do not support Standard clusters.

```bash
confluent api-key create --resource "$LQ_CLUSTER_ID" \
  --environment "$LQ_ENV_ID" \
  --description "Lightning Queries lab $LQ_NAME marker producer" \
  -o json \
  | jq -r '"LQ_KAFKA_KEY=\(.api_key)\nLQ_KAFKA_SECRET=\(.api_secret)"' \
  >> tools/lq.env
```

If the create fails, you have reached the quota of 10 cluster keys per user. Delete an old key, or use the Flink marker in Notes.
A new key can need a minute before it works. If step 7 gives `Authentication failed`, wait one minute and try again.

3. Find the ID of the value schema of your topic, and add it to `tools/lq.env`.

```bash
id=$(confluent schema-registry schema describe --subject lq_orders-value \
  --version latest --environment "$LQ_ENV_ID" | awk '/Schema ID/ {print $3}')
echo "LQ_SCHEMA_ID=$id" >> tools/lq.env
```

4. Load the settings again, and check them. The lines `Kafka key` and `schema ID` must be `OK`.

```bash
set -a; . tools/lq.env; set +a
```

```bash
python3 tools/lq_doctor.py
```

Part A done. Next: prepare the test.

### Part B: prepare (about 2 minutes)

5. Check that Tableflow is `RUNNING`. If it shows `PENDING`, wait and check again. It took 1.5 to 8 minutes in the test runs.

```bash
confluent tableflow topic describe lq_orders \
  --cluster "$LQ_CLUSTER_ID" --environment "$LQ_ENV_ID" -o json | jq -r .phase
```

6. Stop the feed. While a Flink statement writes to the topic, every new event waits for the next Flink commit, up to about 60 seconds. Notes explain why.

```bash
confluent flink statement stop lq-$LQ_NAME-feed-orders \
  --environment "$LQ_ENV_ID" --cloud aws --region "$LQ_REGION"
```

Part B done. Next: the test. Have two terminals ready, and load the settings in both.

### Part C: the test (about 5 minutes)

7. Terminal 1: write the marker with the CLI producer, then watch Lightning. Paste the whole block at once. `tools/marker_json.sh` only prints the event as Avro JSON.

```bash
tools/marker_json.sh "MARKER-1" \
  | confluent kafka topic produce lq_orders \
      --cluster "$LQ_CLUSTER_ID" --environment "$LQ_ENV_ID" \
      --api-key "$LQ_KAFKA_KEY" --api-secret "$LQ_KAFKA_SECRET" \
      --value-format avro --schema "$LQ_SCHEMA_ID" \
  && python3 tools/lq_watch.py "SELECT ORDER_ID FROM lq_orders WHERE ORDER_ID = 'MARKER-1'"
```

8. Terminal 2, at once: watch Tableflow. Keep it running until it prints `found`.

```bash
tableflow/tableflow_watch.sh lq_orders "MARKER-1"
```

9. Write down both times. You need them for the summary at the end of the track.

Part C done. The feed stays stopped. Lab 3 does not need it, and Lab 3 is faster without it.

## Look

I ran these commands on 2026-11-05, after step 8 printed `found`.

The marker is in Lightning. `SOURCE` is `cli` because the CLI producer wrote it.

```bash
python3 tools/lq.py --json "SELECT ORDER_ID, STATUS, SOURCE FROM lq_orders WHERE ORDER_ID = 'MARKER-1'" | jq -c .rows
```

```
[["MARKER-1","MARKER","cli"]]
```

The marker is in Tableflow. `grep` keeps only the result word, because the time changes on each run.

```bash
tableflow/tableflow_watch.sh lq_orders "MARKER-1" | grep -o found
```

```
found
```

## Expected

Terminal 1 prints a time in seconds: 1.5 to 8 seconds. Terminal 2 prints a time in minutes: 1 to 4.5 minutes. The CLI producer itself needs about 5 seconds to connect and write. I ran seven markers. The first five used a Kafka REST producer (2026-10-26). The last two used the CLI producer (2026-10-27 and 2026-11-05):

```
Lightning 2.0 s   Tableflow about 1.5 min
Lightning 2.5 s   Tableflow 4 min 09 s
Lightning 7.2 s   Tableflow 4 min 09 s
Lightning 5.1 s   Tableflow 3 min 39 s
Lightning 5.4 s   Tableflow 1 min 02 s
Lightning 1.6 s   Tableflow 1 min 02 s   (CLI producer)
Lightning 1.5 s   Tableflow 4 min 23 s   (CLI producer)
```

Output of step 7 on 2026-11-05:

```
Starting Kafka Producer. Use Ctrl-C or Ctrl-D to exit.
Started at 18:43:02. '.' = no rows yet, 'r' = table not ready, 'n' = network error, '!' = done
.!
Visible after 1.5 s (at 18:43:04)
```

Output of step 8 in the same run:

```
18:47:13  found. Visible in Tableflow after 4 min 23 s
```

The Tableflow docs say: "Tableflow publishes new data to a table approximately every 5 minutes." No setting makes this faster.

## Notes

- Why stop the feed: RTCE reads only committed data. A reader cannot read past the oldest open transaction in a partition. With the feed running, a marker waited 48 seconds; with the feed stopped, 2 to 3 seconds.
- For customers: Lightning freshness depends on the producer. A Flink exactly-once producer adds up to one checkpoint interval, about 60 seconds.
- No Kafka key? Use Flink instead. It takes about 10 seconds, because the statement must start: `confluent flink statement create lq-$LQ_NAME-marker-2 --sql "$(sed -e "s/{n}/2/g" sql/03_insert_marker.sql)" --environment "$LQ_ENV_ID" --database "$LQ_CLUSTER_ID" --compute-pool "$LQ_POOL_ID"`.
- Do not use a Flink `SELECT` to check Tableflow. A Flink snapshot query reads the Iceberg table plus the newest Kafka data, so it shows the marker at once.
- To see the marker rows in DuckDB yourself, use `tableflow/duckdb_tableflow.sql`. Replace the placeholders in it. The DuckDB secret expires after about 30 minutes with HTTP 401; run the file again.

## Clean up

Keep everything for Lab 3. The feed stays stopped. To start it again: `confluent flink statement resume lq-$LQ_NAME-feed-orders --environment "$LQ_ENV_ID" --cloud aws --region "$LQ_REGION"`.

## Versions

- DuckDB v2.0.0. Homebrew shows 2.0.0 as newest on 2026-10-26. DuckDB 2.0 prints a warning that the option `endpoint` is deprecated. The scripts keep it, so that DuckDB 1.x also works.
- Confluent CLI v4.82.0. Homebrew cask `confluent-cli` shows 4.82.0 as newest on 2026-11-04.
- DuckDB v2.0.0 is still the newest Homebrew version on 2026-11-06.
