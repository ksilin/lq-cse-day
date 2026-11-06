"""State machine and watchers of the lab UI. No network code here.

The server injects the Lightning query runner, the Tableflow runner, the producer
and the clock. The tests inject fakes. See ui/README.md, "Lab run: the state machine".
"""
from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
import urllib.error
from dataclasses import dataclass, field
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tools"))
import lq  # noqa: E402

# States of one run.
IDLE, WRITING, WATCHING, DONE, TIMEOUT = "IDLE", "WRITING", "WATCHING", "DONE", "TIMEOUT"
UNKNOWN, FAILED, INTERRUPTED = "UNKNOWN", "FAILED", "INTERRUPTED"
ACTIVE = {WRITING, WATCHING, UNKNOWN}
FINAL = {DONE, TIMEOUT, FAILED, INTERRUPTED}
# The only permitted transitions. INTERRUPTED is set only at a server start.
TRANSITIONS = {
    (IDLE, WRITING), (WRITING, WATCHING), (WRITING, FAILED), (WRITING, UNKNOWN),
    (UNKNOWN, WATCHING), (UNKNOWN, FAILED), (WATCHING, DONE), (WATCHING, TIMEOUT),
}

PRODUCE_LIMIT = 30.0       # s, the producer limit (WRITING)
RECONCILE_LIMIT = 60.0     # s, the Lightning search for the marker ID (UNKNOWN)
RECONCILE_INTERVAL = 1.0
LQ_INTERVAL, LQ_LIMIT = 1.0, 180.0      # Lightning lanes: 1 s, 3 minutes
TF_INTERVAL, TF_LIMIT = 30.0, 600.0     # Tableflow lane: 30 s, 10 minutes
MAX_INTERVAL = 10.0        # s, the slowest interval after HTTP 429
LAB3_CUSTOMER = 9001
LAB3_STEP = 10             # the Lab 3 price goes up by 10 for each run

RUN_ID = re.compile(r"^UI-L[23]-[0-2][0-9][0-5][0-9][0-5][0-9]$")
TF_TABLES = ("lq_orders", "lq_orders_latest")


class TransitionError(Exception):
    pass


class Busy(Exception):
    """A run is active. Only one run can be active."""


@dataclass
class ProduceResult:
    """The result of one producer call. stderr has only filtered lines."""
    returncode: int
    stderr: list[str] = field(default_factory=list)
    timed_out: bool = False


class ForbiddenQuery(Exception):
    """A query that must not go to DuckDB. Not a ValueError, so no watcher swallows it."""


class TableflowError(Exception):
    """The Tableflow query failed. The text is filtered: it cannot contain a secret."""


# ---------------------------------------------------------------- secret filter

SECRET_MARKS = ("--api-secret", "client_secret", "authorization", "x-amz-", "secret '")


def secret_values(settings: dict) -> list[str]:
    """The values of all settings with SECRET in the name. Short values are ignored."""
    return [v for k, v in settings.items() if "SECRET" in k.upper() and v and len(v) >= 6]


def redact_lines(text: str, secrets: list[str]) -> list[str]:
    """Return the lines of text without any line that contains a secret or a credential mark.
    A URL loses its query string: a presigned URL holds a credential there."""
    out = []
    for line in (text or "").splitlines():
        low = line.lower()
        if any(s and s in line for s in secrets) or any(m in low for m in SECRET_MARKS):
            continue
        line = re.sub(r"(https?://[^\s?]+)\?\S*", r"\1?...", line)
        line = re.sub(r"\x1b\[[0-9;]*m", "", line).strip()
        if line:
            out.append(line[:300])
    return out


def scrub(obj, secrets: list[str]):
    """Last guard before a value goes to a file or to the page: replace each secret with [removed]."""
    text = json.dumps(obj)
    if not any(s and s in text for s in secrets):
        return obj
    for s in secrets:
        if s:
            text = text.replace(json.dumps(s)[1:-1], "[removed]")
    return json.loads(text)


# ---------------------------------------------------------------- fixed queries

def check_run_id(run_id: str) -> str:
    if not isinstance(run_id, str) or not RUN_ID.match(run_id):
        raise ValueError("not a run ID of this UI")
    return run_id


def lightning_sql(table: str, run_id: str) -> str:
    """The fixed Lightning query of one lane."""
    check_run_id(run_id)
    if table == "lq_orders":
        return f"SELECT ORDER_ID, PRICE FROM lq_orders WHERE ORDER_ID = '{run_id}'"
    if table == "lq_orders_latest":
        return f"SELECT KEY, ORDER_ID, PRICE FROM lq_orders_latest WHERE KEY = '{LAB3_CUSTOMER}'"
    raise ValueError("unknown table")


def tableflow_sql(table: str, run_id: str) -> str:
    """The fixed Tableflow (DuckDB) query of one lane. DuckDB can read and write local files,
    so only these two texts can go to DuckDB, and only with a checked run ID."""
    check_run_id(run_id)
    if table == "lq_orders":
        return f"SELECT order_id, price FROM lq_orders WHERE order_id = '{run_id}'"
    if table == "lq_orders_latest":
        return f"SELECT key, order_id, price FROM lq_orders_latest WHERE key = '{LAB3_CUSTOMER}'"
    raise ValueError("unknown table")


def is_fixed_tableflow_sql(sql: str) -> bool:
    m = re.search(r"'(UI-L[23]-\d{6})'", sql)
    candidates = [tableflow_sql(t, m.group(1)) for t in TF_TABLES] if m else []
    return sql in candidates or sql == tableflow_sql("lq_orders_latest", "UI-L3-000000")


def lab1_sql_ok(sql: str) -> bool:
    """Free SQL goes only to Lightning, and only one SELECT, SHOW or DESCRIBE statement."""
    s = sql.strip().rstrip(";").strip()
    return bool(re.match(r"(?is)^(SELECT|SHOW|DESCRIBE)\b", s)) and ";" not in s


# ---------------------------------------------------------------- lanes

def fmt_time(s: float | None) -> str:
    """Seconds in the format of the lab READMEs: 1.5 s, 4 min 23 s."""
    if s is None:
        return "-"
    if s < 60:
        return f"{s:.1f} s"
    t = int(round(s))
    return f"{t // 60} min {t % 60:02d} s"


class Lane:
    """One bounded watch loop on one table."""

    def __init__(self, key: str, label: str, kind: str, table: str, run_id: str,
                 interval: float, limit: float):
        self.key, self.label, self.kind, self.table = key, label, kind, table
        self.run_id, self.interval, self.limit = run_id, interval, limit
        self.sql = (tableflow_sql if kind == "tf" else lightning_sql)(table, run_id)
        self.status = "waiting"     # waiting, polling, found, error, timeout
        self.found_after: float | None = None
        self.polls = 0
        self.log = ""
        self.rate_limited = False
        self.error_code = ""
        self.error_hint = ""
        self.note = ""              # the last "not yet" reason, filtered
        self.value = None           # the price this table shows now (Lab 3)
        self.value_order = None     # the ORDER_ID of that price
        self.next_at: float | None = None
        self.ended_at: float | None = None

    def _mark(self, c: str) -> None:
        self.log = (self.log + c)[-200:]

    def _rows_found(self, columns: list[str], rows: list[list]) -> bool:
        cols = [c.upper() for c in columns]
        oi = cols.index("ORDER_ID") if "ORDER_ID" in cols else None
        pi = cols.index("PRICE") if "PRICE" in cols else None
        found = False
        for r in rows:
            order = r[oi] if oi is not None and oi < len(r) else None
            if pi is not None and pi < len(r):
                self.value, self.value_order = r[pi], order
            if order == self.run_id:
                found = True
                if pi is not None and pi < len(r):
                    self.value, self.value_order = r[pi], order
        return found

    def poll(self, query, tableflow) -> str:
        """Run one query. Return found, not_yet, rate_limited or error."""
        self.polls += 1
        try:
            if self.kind == "tf":
                if not is_fixed_tableflow_sql(self.sql):
                    raise ForbiddenQuery("Tableflow gets only fixed queries")
                columns, rows = tableflow(self.sql)
            else:
                _, columns, rows = query(self.sql)
        except lq.QueryError as e:
            if e.status == 429 or e.code in ("FE_RATE_LIMITED", "RATE_LIMIT_EXCEEDED"):
                self.interval = min(self.interval * 2, MAX_INTERVAL)
                self.rate_limited = True
                self._mark("r")
                self.note = "rate limit, slower polling"
                return "rate_limited"
            if e.retryable:
                self._mark("r")
                self.note = f"{e.code}: not ready yet"
                return "not_yet"
            self._mark("x")
            self.error_code = e.code or f"HTTP {e.status}"
            self.error_hint = lq.HINTS.get(e.code) or lq.STATUS_HINTS.get(e.status, "")
            return "error"
        except TableflowError as e:
            self._mark("n")
            self.note = str(e)[:200]
            return "not_yet"
        except (urllib.error.URLError, TimeoutError, ValueError, OSError):
            self._mark("n")
            self.note = "network: no answer, retry"
            return "not_yet"
        if self._rows_found(columns, rows):
            self._mark("!")
            return "found"
        self._mark(".")
        self.note = ""
        return "not_yet"

    def run(self, t0: float, query, tableflow, clock, sleep, stop: threading.Event | None = None) -> None:
        """Poll until found, error or the limit. t0 is the end of the write."""
        self.status = "polling"
        while True:
            self.next_at = None  # a query runs now
            started = clock()
            result = self.poll(query, tableflow)
            now = clock()
            if result == "found":
                self.status, self.found_after, self.ended_at = "found", now - t0, now
                return
            if result == "error":
                self.status, self.ended_at = "error", now
                return
            if now - t0 >= self.limit or (stop is not None and stop.is_set()):
                self.status, self.ended_at = "timeout", now
                return
            # One query per interval, from start to start. A slow DuckDB call does not add its time.
            self.next_at = started + self.interval
            sleep(max(0.0, self.next_at - now))

    def to_dict(self, now: float, t0: float | None) -> dict:
        if self.found_after is not None:
            elapsed = self.found_after
        elif self.ended_at is not None and t0 is not None:
            elapsed = self.ended_at - t0
        elif t0 is not None and self.status == "polling":
            elapsed = now - t0
        else:
            elapsed = None
        return {"key": self.key, "label": self.label, "kind": self.kind, "table": self.table,
                "status": self.status, "found_after": self.found_after, "elapsed": elapsed,
                "polls": self.polls, "log": self.log, "rate_limited": self.rate_limited,
                "interval": self.interval, "limit": self.limit, "error_code": self.error_code,
                "error_hint": self.error_hint, "note": self.note, "value": self.value,
                "value_order": self.value_order,
                "next_in": (max(0.0, self.next_at - now) if self.next_at and self.status == "polling" else None)}


def make_lanes(lab: int, run_id: str) -> list[Lane]:
    if lab == 2:
        return [Lane("lq", "Lightning", "lq", "lq_orders", run_id, LQ_INTERVAL, LQ_LIMIT),
                Lane("tf", "Tableflow", "tf", "lq_orders", run_id, TF_INTERVAL, TF_LIMIT)]
    if lab == 3:
        return [Lane("app", "Lightning append", "lq", "lq_orders", run_id, LQ_INTERVAL, LQ_LIMIT),
                Lane("ups", "Lightning upsert", "lq", "lq_orders_latest", run_id, LQ_INTERVAL, LQ_LIMIT),
                Lane("tf", "Tableflow", "tf", "lq_orders_latest", run_id, TF_INTERVAL, TF_LIMIT)]
    raise ValueError("lab must be 2 or 3")


def result_line(lab: int, lanes: list[dict]) -> str:
    """One line in the format of the lab READMEs, for "Copy result"."""
    parts = []
    for ln in lanes:
        if ln["status"] == "found":
            t = fmt_time(ln["found_after"])
        elif ln["status"] == "error":
            t = f"error {ln['error_code']}"
        else:
            t = f"not found after {fmt_time(ln['limit'])}" if ln["status"] == "timeout" else "-"
        parts.append(f"{ln['label']} {t}")
    return "   ".join(parts)


# ---------------------------------------------------------------- run and store

class Run:
    def __init__(self, run_id: str, lab: int, price: float, customer: int, status: str):
        self.id, self.lab, self.price, self.customer, self.status = run_id, lab, price, customer, status
        self.state = IDLE
        self.lanes = make_lanes(lab, run_id)
        self.t_write: float | None = None   # monotonic, start of the write
        self.t0: float | None = None        # monotonic, end of the write: the stopwatches count from here
        self.written_utc = ""
        self.message = ""
        self.can_write_again = False
        self.reconcile_polls = 0

    def to_dict(self, now: float) -> dict:
        lanes = [ln.to_dict(now, self.t0) for ln in self.lanes]
        d = {"id": self.id, "lab": self.lab, "state": self.state, "price": self.price,
             "customer": self.customer, "written_utc": self.written_utc, "message": self.message,
             "can_write_again": self.can_write_again, "lanes": lanes,
             "reconcile_polls": self.reconcile_polls}
        if self.state in (DONE, TIMEOUT):
            d["result"] = result_line(self.lab, lanes)
        return d


class Store:
    """ui/runs.jsonl: one line per state change, with the time."""

    def __init__(self, path: str, secrets: list[str]):
        self.path, self.secrets = path, secrets
        self.lock = threading.Lock()

    def append(self, record: dict) -> None:
        record = scrub(record, self.secrets)
        with self.lock, open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, separators=(",", ":")) + "\n")

    def load(self) -> list[dict]:
        if not os.path.exists(self.path):
            return []
        out = []
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                try:
                    out.append(json.loads(line))
                except ValueError:
                    continue
        return out


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


class Manager:
    """Owns the one active run and the history.

    query(sql) -> (elapsed, columns, rows); raises lq.QueryError.
    tableflow(sql) -> (columns, rows); raises TableflowError.
    producer(run_id, customer, status, price) -> ProduceResult.
    timer() -> (clock, sleep) for one loop. Production: (time.monotonic, time.sleep).
    """

    def __init__(self, store: Store, query, tableflow, producer, timer=None,
                 parallel: bool = True, now_hms=None, wall=utc_now):
        self.store, self.query, self.tableflow, self.producer = store, query, tableflow, producer
        self.timer = timer or (lambda: (time.monotonic, time.sleep))
        self.clock = self.timer()[0]
        self.parallel = parallel
        self.now_hms = now_hms or (lambda: datetime.now(timezone.utc).strftime("%H%M%S"))
        self.wall = wall
        self.lock = threading.Lock()
        self.active: Run | None = None
        self.last: Run | None = None
        self.history: dict[str, dict] = {}
        self.stop = threading.Event()
        self._load()

    # history ---------------------------------------------------------------

    def _load(self) -> None:
        for rec in self.store.load():
            self.history[rec.get("id", "?")] = rec
        for rid, rec in list(self.history.items()):
            if rec.get("state") in ACTIVE:
                new = dict(rec, state=INTERRUPTED, at=self.wall(),
                           message="The server stopped during this run. It does not continue it.")
                self.store.append(new)
                self.history[rid] = new

    def history_list(self, lab: int | None = None, limit: int = 50) -> list[dict]:
        items = [r for r in self.history.values() if lab is None or r.get("lab") == lab]
        return list(reversed(items))[:limit]

    def next_lab3_price(self) -> int:
        prices = [r.get("price") or 0 for r in self.history.values() if r.get("lab") == 3]
        if self.active and self.active.lab == 3:
            prices.append(self.active.price)
        return int(max(prices, default=0)) + LAB3_STEP

    # transitions -----------------------------------------------------------

    def _set(self, run: Run, state: str, message: str = "") -> None:
        if (run.state, state) not in TRANSITIONS:
            raise TransitionError(f"{run.state} -> {state} is not permitted")
        run.state = state
        if message:
            run.message = message
        rec = {"at": self.wall(), "id": run.id, "lab": run.lab, "state": state, "price": run.price,
               "customer": run.customer, "written_utc": run.written_utc, "message": run.message,
               "can_write_again": run.can_write_again}
        if state in (DONE, TIMEOUT):
            lanes = [ln.to_dict(self.clock(), run.t0) for ln in run.lanes]
            rec["lanes"] = [{k: ln[k] for k in ("key", "label", "status", "found_after", "limit",
                                                 "error_code", "polls", "value")} for ln in lanes]
            rec["result"] = result_line(run.lab, lanes)
        self.store.append(rec)
        self.history[run.id] = scrub(rec, self.store.secrets)

    # start -----------------------------------------------------------------

    def start(self, lab: int) -> Run:
        if lab not in (2, 3):
            raise ValueError("lab must be 2 or 3")
        with self.lock:
            if self.active is not None:
                raise Busy("A run is active. Wait until it ends.")
            run_id = f"UI-L{lab}-{self.now_hms()}"
            if run_id in self.history:
                raise Busy("A run with this ID exists. Wait one second and write again.")
            if lab == 2:
                run = Run(run_id, 2, 0, 0, "MARKER")
            else:
                run = Run(run_id, 3, self.next_lab3_price(), LAB3_CUSTOMER, "NEW")
            self.active = run
            self._set(run, WRITING)
        if self.parallel:
            threading.Thread(target=self._drive, args=(run,), daemon=True, name=run_id).start()
        else:
            self._drive(run)
        return run

    # drive -----------------------------------------------------------------

    def _drive(self, run: Run) -> None:
        try:
            self._write(run)
            if run.state == UNKNOWN:
                self._reconcile(run)
            if run.state == WATCHING:
                self._watch(run)
        except Exception as e:  # never leave a run active; never echo the text, it can hold a command line
            if run.state in ACTIVE:
                target = TIMEOUT if run.state == WATCHING else FAILED
                self._set(run, target, f"Internal error ({type(e).__name__}). See the server terminal.")
            print(f"run {run.id}: internal error {type(e).__name__}", file=sys.stderr)
        finally:
            with self.lock:
                self.last, self.active = run, None

    def _write(self, run: Run) -> None:
        clock = self.clock
        run.t_write = clock()
        run.written_utc = self.wall()
        res = self.producer(run.id, run.customer, run.status, run.price)
        if res.timed_out:
            run.t0 = run.t_write  # the write time is not known: count from the start of the write
            self._set(run, UNKNOWN, f"The producer did not end within {PRODUCE_LIMIT:.0f} s. "
                                    "The event can be written or not. Searching Lightning for the ID.")
            return
        if res.returncode != 0:
            text = "; ".join(res.stderr[:3]) or "no error text"
            run.can_write_again = True
            self._set(run, FAILED, f"The producer stopped with exit code {res.returncode}: {text}")
            return
        run.t0 = clock()
        self._set(run, WATCHING, f"Wrote {run.id} at {run.written_utc}.")

    def _reconcile(self, run: Run) -> None:
        """UNKNOWN: ask Lightning for the run ID for 60 s. Never write again."""
        clock, sleep = self.timer()
        start = clock()
        sql = lightning_sql("lq_orders", run.id)
        while True:
            run.reconcile_polls += 1
            try:
                _, _, rows = self.query(sql)
                if any(r and r[0] == run.id for r in rows):
                    self._set(run, WATCHING, f"Found {run.id} in Lightning. The write worked. "
                                             "The stopwatches count from the start of the write.")
                    return
            except lq.QueryError as e:
                if not e.retryable:
                    break
            except (urllib.error.URLError, TimeoutError, ValueError, OSError):
                pass
            if clock() - start >= RECONCILE_LIMIT:
                break
            sleep(RECONCILE_INTERVAL)
        run.can_write_again = True
        self._set(run, FAILED, f"{run.id} is not in Lightning after {RECONCILE_LIMIT:.0f} s. "
                               "The write did not work. Push \"Write again\".")

    def _watch(self, run: Run) -> None:
        def one(lane: Lane) -> None:
            clock, sleep = self.timer()
            lane.run(run.t0, self.query, self.tableflow, clock, sleep, self.stop)

        if self.parallel:
            threads = [threading.Thread(target=one, args=(ln,), daemon=True) for ln in run.lanes]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        else:
            for ln in run.lanes:
                one(ln)
        if all(ln.status == "found" for ln in run.lanes):
            self._set(run, DONE, "Every table shows the event.")
        else:
            missing = ", ".join(ln.label for ln in run.lanes if ln.status != "found")
            self._set(run, TIMEOUT, f"Not found: {missing}.")

    # view ------------------------------------------------------------------

    def snapshot(self) -> dict:
        now = self.clock()
        run = self.active or self.last
        return scrub({"active": self.active is not None,
                      "run": run.to_dict(now) if run else None,
                      "history": self.history_list(),
                      "next_lab3_price": self.next_lab3_price()}, self.store.secrets)
