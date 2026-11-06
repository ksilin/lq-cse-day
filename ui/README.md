# Lab UI: Lightning Tables and Tableflow side by side

Verified: 2026-11-06 on macOS 27, Python 3.14.7, Confluent CLI v4.82.0, DuckDB v2.0.0, AWS eu-central-1.

Status: built on 2026-11-06. Checks 1 to 5 of the build contract pass. Check 6, your review of the page, is open.

## What we test

Do a new event and an update show in the Lightning Tables in seconds, and in Tableflow minutes later?
The UI answers this on one page, with real writes and real queries.
The build contract, the state machine and the security rules are at the end of this file.

### What we build

A small local web page for Labs 1 to 3. It shows when a new event or an update arrives in a Lightning Table and when it arrives in Tableflow.

1. Lab 1 tab: run a Lightning query and see the rows and the time.
2. Lab 2 tab: write a marker and see two stopwatches, Lightning and Tableflow, until both find it.
3. Lab 3 tab: write an update for one key and see three tables: the append table, the upsert table and Tableflow.
4. A status bar on top with the result of the doctor checks.

The design is `mockups/mockup-dashboard.html`, with the large view of `mockups/mockup-race.html` in the Lab 2 tab.

The UI is an extra. The lab READMEs stay complete without it.

## How it works

- `ui/server.py` is a Python standard-library server (`http.server`). It needs no packages.
- It reads `tools/lq.env`, as all other tools do.
- Lightning queries go through `tools/lq.py` (`run_query`), as a Python import.
- Tableflow queries go through `tableflow/tableflow_query.sh` (DuckDB).
- A write goes through `tools/marker_json.sh` and `confluent kafka topic produce`, as in Lab 2 step 7.
- The page polls the server once per second. The server runs the watchers in threads.

Start it with:

```bash
python3 ui/server.py
```

## Requirements

- Labs 1 and 2 are done. RTCE and Tableflow are on for `lq_orders`. Lab 3 needs the same for `lq_orders_latest`.
- `tools/lq.env` has the Global key, the Kafka key and `LQ_SCHEMA_ID` (Lab 2, steps 2 and 3).
- Python 3.10 or later. No packages.
- DuckDB 1.1 or later, and a CLI login (`confluent login --save`).
- Stop the feed for exact times (Lab 2, Part B). With a running feed, each event waits up to 60 s.

## Run

1. Load the settings and start the server. It prints its URL. Keep the terminal open.

```bash
set -a; . tools/lq.env; set +a
python3 ui/server.py
```

2. Open `http://127.0.0.1:8765/` in a browser. The status bar shows the doctor checks after about 20 seconds.

3. Lab 2 tab: push "Write marker". Wait until both lanes show VISIBLE. This takes about 5 minutes.

4. Lab 3 tab: push "Write update". Wait until all three tiles show CURRENT. This takes about 5 minutes.

5. Push "Copy result" to get the result line for your notes.

Step 5 of 5 done. Next: check the result in Look.

To drive a run without a browser, read the token from the page and send it in the header `X-Lab-Token`:

```bash
TOKEN=$(curl -s http://127.0.0.1:8765/ | grep -o 'name="lab-token" content="[^"]*"' | cut -d'"' -f4)
curl -s -H "X-Lab-Token: $TOKEN" -H 'Content-Type: application/json' -d '{"lab":2}' http://127.0.0.1:8765/api/run
curl -s -H "X-Lab-Token: $TOKEN" http://127.0.0.1:8765/api/state | jq -c '.run | {id, state, result}'
```

## Look

I ran these commands on 2026-11-06, with the server from Run step 1 and the token from the block above.

The history has one Lab 2 run and one Lab 3 run. Both are DONE. The history stayed after a restart of the server.

<!-- doc-check: skip -->
```bash
curl -s -H "X-Lab-Token: $TOKEN" http://127.0.0.1:8765/api/state | jq -r '.history[] | "\(.id)  \(.state)  \(.result)"'
```

```
UI-L3-034411  DONE  Lightning append 4.2 s   Lightning upsert 35.4 s   Tableflow 4 min 01 s
UI-L2-033845  DONE  Lightning 5.3 s   Tableflow 4 min 22 s
```

A request without the token gets 403. A request with a wrong `Host` header gets 403, also with the token.

<!-- doc-check: skip -->
```bash
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8765/api/state
curl -s -w " %{http_code}\n" -H "X-Lab-Token: $TOKEN" -H "Host: evil.example:8765" http://127.0.0.1:8765/api/state
```

```
403
{"error": "Wrong Host header. Open http://127.0.0.1:<port>/."} 403
```

The Lab 1 query shows the error code and the hint of `tools/lq.py`. A statement other than `SELECT`, `SHOW` or `DESCRIBE` does not leave the server.

<!-- doc-check: skip -->
```bash
curl -s -H "X-Lab-Token: $TOKEN" -H 'Content-Type: application/json' -d '{"sql":"SELECT NOPE FROM lq_orders"}' http://127.0.0.1:8765/api/query | jq -c '{error_code, hint}'
curl -s -w " %{http_code}\n" -H "X-Lab-Token: $TOKEN" -H 'Content-Type: application/json' -d '{"sql":"DROP TABLE lq_orders"}' http://127.0.0.1:8765/api/query
```

```
{"error_code":"FE_SQL_VALIDATION_ERROR","hint":"Check the SQL. Column names are upper case. Quote reserved words such as \"KEY\"."}
{"error": "Send one SELECT, SHOW or DESCRIBE statement."} 400
```

The offline tests pass. They need no network and no server.

```bash
python3 -m unittest discover -s ui 2>&1 | tail -1
```

```
OK
```

The file `ui/runs.jsonl` does not contain the Kafka secret or the Global secret. The command prints the number of lines with a secret. `printf` is a shell builtin, so no process list shows the secret.

```bash
set -a; . tools/lq.env; set +a; grep -cFf <(printf '%s\n' "$LQ_KAFKA_SECRET" "$LQ_API_SECRET") ui/runs.jsonl || true
```

```
0
```

## Expected

Lab 2 run `UI-L2-033845`, written at 03:38:45 UTC, with the feed stopped:

```
Lightning 5.3 s   Tableflow 4 min 22 s
```

Lab 3 run `UI-L3-034411`, written at 03:44:11 UTC, price 10:

```
Lightning append 4.2 s   Lightning upsert 35.4 s   Tableflow 4 min 01 s
```

These times are in the ranges of the lab READMEs. Lab 2 has 1.5 s to 7.2 s for Lightning, and 1 to 5 minutes for Tableflow. Lab 3 has 56.6 s for the upsert table on 2026-11-05.

## Notes

- A Lab 3 run changes key 9001 in `lq_orders_latest`. After a UI run, the Look commands of Lab 3 show the UI order, not `UPD-3`. Run Lab 3 in the terminal first.
- The Lab 3 price starts at 10 and goes up by 10 for each run in `ui/runs.jsonl`. Remove that file to start at 10 again.
- The stopwatches count from the end of the write, as `lq_watch.py` does after the producer. The producer needs about 5 seconds.
- One DuckDB call takes about 30 seconds. The Tableflow lane starts one call every 30 seconds, so a new call starts when the last one ends.
- The server makes a new token at each start. An old browser tab gets 403: load the page again.

## Clean up

Stop the server with Ctrl-C. To remove the history, delete the file of the runs:

```bash
rm -f ui/runs.jsonl
```

The marker and update events stay in `lq_orders`. They do not stop the labs.

## Versions

I read these versions on 2026-11-06, from the installed tools.

- Python 3.14.7 (`python3 --version`). The server uses only the standard library.
- Confluent CLI v4.82.0 (`confluent version`).
- DuckDB v2.0.0 (`duckdb --version`).
- Browser check: Chromium through Playwright, no console errors on the Lab 2 and Lab 3 tabs.

## Build contract

### Goal and input

- Result: `ui/server.py`, `ui/runs.py` (state machine), `ui/static/index.html`, `ui/test_ui.py`, and this README with Run, Look and Expected.

### Permitted actions

- Read `tools/lq.env`.
- Run Lightning queries and Tableflow queries.
- Write order events to `lq_orders` with the Kafka key from Lab 2.
- Run the live tests in an existing lab environment.

### Forbidden actions

- Create, delete or rotate an API key.
- Create or delete a resource: environment, cluster, pool, topic, statement, RTCE or Tableflow.
- Commit to the public repository, or push anything.
- Print a secret, or write it to a log, a file or a command line in an error message.

### Verification

1. Offline tests in `ui/test_ui.py` with a fake query runner and a fake producer. They cover every state transition below, the timeouts, the 429 back-off and the reconcile step.
2. The existing 70 tests still pass.
3. One live Lab 2 run and one live Lab 3 run in the UI. The times must be in the ranges of the lab READMEs.
4. A check for internal names and secrets finds nothing in `ui/`.
5. `doc-check.sh ui/README.md` is clean.
6. You open the page and accept it. My statement "it works" is not acceptance.

### Stop and escalation

- Success: checks 1 to 5 pass, and you accept the page.
- If the same check fails after 2 fix attempts, I stop and ask you.
- Time limit: 3 hours of work.
- I ask you before an action that costs money or needs a new credential.

### Evidence

Each claim in Run, Look and Expected has the command and its real output.

## Lab run: the state machine

Each run in Lab 2 or Lab 3 goes through these states. No other transition is permitted.

```text
IDLE -> WRITING -> WATCHING -> DONE
                 \          \-> TIMEOUT
                  \-> UNKNOWN -> (reconcile) -> WATCHING or FAILED
WRITING -> FAILED
```

- WRITING: the producer runs. The limit is 30 s.
- WRITING to FAILED: the producer exits with an error before it writes, for example `Authentication failed`.
- WRITING to UNKNOWN: the producer timed out. The event can be written or not.
- UNKNOWN: the server asks Lightning for the marker ID for 60 s. Found: WATCHING. Not found: FAILED, with a "write again" button. The server never writes again by itself.
- WATCHING: one lane per table. Lab 2 has 2 lanes. Lab 3 has 3 lanes. The run ends when every lane ends.
- DONE: every lane found the event.
- TIMEOUT: one or more lanes reached their limit. The page shows which lanes found the event and which did not.

Only one run can be active. The write buttons are off while a run is active.

## Watcher contract

Each lane is one bounded loop.

- Lightning lanes: one query per second. Limit: 3 minutes.
- Tableflow lane: one query every 30 seconds. Limit: 10 minutes.
- Exit "found": the query returns the row of this run.
- Exit "error": a non-retryable error code, for example `FE_SQL_VALIDATION_ERROR`. The page shows the code and the hint of `lq.py`.
- A retryable error, for example `FE_TABLE_NOT_FOUND`, counts as "not yet".
- HTTP 429: the lane doubles its interval, up to 10 s, and the page shows "rate limit, slower polling". All participants in one org share one rate limit.

## The write

- Each run has a new ID: `UI-L2-<hhmmss>` for Lab 2, `UI-L3-<hhmmss>` for Lab 3. So two runs never mix.
- Lab 3 writes for customer 9001. The price goes up by 10 for each run.
- The server passes the producer arguments as a list, never through a shell.

## State and history

- `ui/runs.jsonl` (git-ignored) gets one line per state change, with the time.
- The page shows the history of all runs after a restart.
- A run that was active at a restart shows `INTERRUPTED`. The server does not continue it.
- "Copy result" gives one line in the format of the lab READMEs, for example `Lightning 1.5 s   Tableflow 4 min 23 s`.

## Security

- The server listens on `127.0.0.1` only.
- Each request must have a token in a header. The server makes a new token at each start and puts it in the page. Another web site in the same browser cannot send a write.
- The server rejects a request with a `Host` header other than `127.0.0.1:<port>` or `localhost:<port>`.
- Free SQL goes only to Lightning (Lab 1 tab). Lightning accepts only `SELECT` and `SHOW`.
- Tableflow gets only fixed queries with a checked marker ID. DuckDB can read and write local files, so the page never sends free SQL to DuckDB.

## Out of scope

- Resume of an interrupted run.
- More than one user per server. Each participant runs their own server.
- A login. The server is local only.
- The Lab 4 exercises.
