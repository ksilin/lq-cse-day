"""Offline tests of the lab UI. Run: python3 -m unittest discover -s ui

No network. Fake query runner, fake Tableflow runner, fake producer, fake clock.
"""
import http.client
import json
import os
import sys
import tempfile
import threading
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "tools"))

import lq  # noqa: E402
import runs  # noqa: E402
import server  # noqa: E402

SECRET = "s3cr3t-VALUE-0123456789abcdef"
SETTINGS = {"LQ_REGION": "eu-central-1", "LQ_ENV_ID": "env-abc123", "LQ_CLUSTER_ID": "lkc-abc123",
            "LQ_API_KEY": "KEY", "LQ_API_SECRET": SECRET, "LQ_KAFKA_KEY": "KKEY",
            "LQ_KAFKA_SECRET": SECRET + "-kafka", "LQ_SCHEMA_ID": "100001", "LQ_NAME": "test"}


class FakeClock:
    """A clock and a sleep that only move when a loop sleeps. fork() gives each loop its own clock."""

    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def sleep(self, s):
        self.now += s

    def fork(self):
        return FakeClock(self.now)


class Script:
    """Answers per table. Each entry: 'none', 'found', 'old', a QueryError, or an OSError.
    The last entry repeats."""

    def __init__(self, **answers):
        self.answers = {k: list(v) for k, v in answers.items()}
        self.calls = {k: 0 for k in answers}
        self.sqls = []

    def _next(self, table):
        a = self.answers[table]
        i = self.calls[table]
        self.calls[table] += 1
        return a[min(i, len(a) - 1)]

    def _rows(self, kind, sql, upper):
        self.sqls.append((kind, sql))
        table = kind + ":" + ("lq_orders_latest" if "lq_orders_latest" in sql else "lq_orders")
        a = self._next(table)
        if isinstance(a, Exception):
            raise a
        rid = sql.split("'")[1] if "ORDER_ID = '" in sql or "order_id = '" in sql else None
        cols = ["ORDER_ID", "PRICE"] if upper else ["order_id", "price"]
        if "lq_orders_latest" in sql:
            cols = ["KEY", "ORDER_ID", "PRICE"] if upper else ["key", "order_id", "price"]
        if a == "none":
            return cols, []
        current = self.current_id()
        if a == "old":
            row = ["UI-L3-000001", "10.0"]
        else:
            row = [rid or current, "20.0"]
        if "lq_orders_latest" in sql:
            row = ["9001"] + row
        return cols, [row]

    current_id = staticmethod(lambda: "")

    def query(self, sql):
        cols, rows = self._rows("lq", sql, True)
        return 0.1, cols, rows

    def tableflow(self, sql):
        return self._rows("tf", sql, False)


class FakeProducer:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def __call__(self, run_id, customer, status, price):
        self.calls.append((run_id, customer, status, price))
        return self.result


def manager(tmp, script, producer, clock=None, hms=None):
    clock = clock or FakeClock()
    hms = iter(hms or [f"1200{i:02d}" for i in range(60)])
    script.current_id = lambda: (getattr(producer, "calls", None) or [("",)])[-1][0]
    m = runs.Manager(runs.Store(os.path.join(tmp, "runs.jsonl"), runs.secret_values(SETTINGS)),
                     script.query, script.tableflow, producer,
                     timer=lambda: ((c := clock.fork()), c.sleep),
                     parallel=False, now_hms=lambda: next(hms), wall=lambda: "2026-11-06 12:00:00 UTC")
    return m


def states(tmp, run_id):
    with open(os.path.join(tmp, "runs.jsonl")) as f:
        return [json.loads(x)["state"] for x in f if json.loads(x)["id"] == run_id]


OK = runs.ProduceResult(0)


class TestTransitions(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def test_lab2_done(self):
        s = Script(**{"lq:lq_orders": ["none", "none", "found"], "tf:lq_orders": ["none", "none", "found"]})
        m = manager(self.tmp, s, FakeProducer(OK))
        run = m.start(2)
        self.assertEqual(run.id, "UI-L2-120000")
        self.assertEqual(run.state, runs.DONE)
        self.assertEqual(states(self.tmp, run.id), ["WRITING", "WATCHING", "DONE"])
        lanes = {ln.key: ln for ln in run.lanes}
        self.assertEqual(lanes["lq"].found_after, 2.0)
        self.assertEqual(lanes["tf"].found_after, 60.0)
        self.assertEqual(lanes["lq"].log, "..!")
        self.assertEqual(run.to_dict(0)["result"], "Lightning 2.0 s   Tableflow 1 min 00 s")
        self.assertIsNone(m.active)

    def test_lab3_done_three_lanes_price_and_customer(self):
        s = Script(**{"lq:lq_orders": ["found"], "lq:lq_orders_latest": ["old", "old", "found"],
                      "tf:lq_orders_latest": ["old", "found"]})
        p = FakeProducer(OK)
        m = manager(self.tmp, s, p)
        run = m.start(3)
        self.assertEqual(run.state, runs.DONE)
        self.assertEqual([ln.key for ln in run.lanes], ["app", "ups", "tf"])
        self.assertEqual(p.calls, [("UI-L3-120000", 9001, "NEW", 10)])
        ups = run.lanes[1]
        self.assertEqual(ups.value_order, "UI-L3-120000")
        self.assertEqual(run.lanes[2].found_after, 30.0)

    def test_lab3_price_goes_up_by_10_per_run(self):
        s = Script(**{"lq:lq_orders": ["found"], "lq:lq_orders_latest": ["found"], "tf:lq_orders_latest": ["found"]})
        p = FakeProducer(OK)
        m = manager(self.tmp, s, p)
        m.start(3)
        m.start(3)
        m2 = manager(self.tmp, s, p, hms=["130000"])  # a restart reads the history
        m2.start(3)
        self.assertEqual([c[3] for c in p.calls], [10, 20, 30])

    def test_writing_to_failed(self):
        s = Script(**{"lq:lq_orders": ["found"], "tf:lq_orders": ["found"]})
        m = manager(self.tmp, s, FakeProducer(runs.ProduceResult(1, ["Error: Authentication failed"])))
        run = m.start(2)
        self.assertEqual(run.state, runs.FAILED)
        self.assertIn("Authentication failed", run.message)
        self.assertIn("exit code 1", run.message)
        self.assertTrue(run.can_write_again)
        self.assertEqual(states(self.tmp, run.id), ["WRITING", "FAILED"])
        self.assertEqual(s.sqls, [])  # no watcher ran

    def test_unknown_reconcile_found(self):
        s = Script(**{"lq:lq_orders": ["none", "none", "found"], "tf:lq_orders": ["found"]})
        p = FakeProducer(runs.ProduceResult(-1, [], timed_out=True))
        m = manager(self.tmp, s, p)
        run = m.start(2)
        self.assertEqual(states(self.tmp, run.id), ["WRITING", "UNKNOWN", "WATCHING", "DONE"])
        self.assertEqual(run.reconcile_polls, 3)
        self.assertEqual(len(p.calls), 1)  # never writes again by itself

    def test_unknown_reconcile_not_found_fails_after_60s(self):
        s = Script(**{"lq:lq_orders": ["none"], "tf:lq_orders": ["none"]})
        p = FakeProducer(runs.ProduceResult(-1, [], timed_out=True))
        m = manager(self.tmp, s, p)
        run = m.start(2)
        self.assertEqual(states(self.tmp, run.id), ["WRITING", "UNKNOWN", "FAILED"])
        self.assertEqual(run.reconcile_polls, 61)
        self.assertTrue(run.can_write_again)
        self.assertIn("Write again", run.message)
        self.assertEqual(len(p.calls), 1)

    def test_unknown_reconcile_retryable_errors_count_as_not_yet(self):
        e = lq.QueryError(400, "FE_TABLE_NOT_AVAILABLE", "x")
        s = Script(**{"lq:lq_orders": [e, OSError("net"), "found"], "tf:lq_orders": ["found"]})
        m = manager(self.tmp, s, FakeProducer(runs.ProduceResult(-1, [], timed_out=True)))
        self.assertEqual(m.start(2).state, runs.DONE)

    def test_timeouts_lightning_3_min_tableflow_10_min(self):
        s = Script(**{"lq:lq_orders": ["none"], "tf:lq_orders": ["none"]})
        m = manager(self.tmp, s, FakeProducer(OK))
        run = m.start(2)
        self.assertEqual(run.state, runs.TIMEOUT)
        lq_lane, tf_lane = run.lanes
        self.assertEqual(lq_lane.status, "timeout")
        self.assertEqual(lq_lane.polls, 181)  # t = 0 .. 180 s
        self.assertEqual(tf_lane.status, "timeout")
        self.assertEqual(tf_lane.polls, 21)   # t = 0, 30 .. 600 s
        self.assertIn("Not found: Lightning, Tableflow", run.message)
        self.assertEqual(run.to_dict(0)["result"],
                         "Lightning not found after 3 min 00 s   Tableflow not found after 10 min 00 s")

    def test_timeout_shows_which_lanes_found(self):
        s = Script(**{"lq:lq_orders": ["found"], "tf:lq_orders": ["none"]})
        run = manager(self.tmp, s, FakeProducer(OK)).start(2)
        self.assertEqual(run.state, runs.TIMEOUT)
        self.assertEqual([ln.status for ln in run.lanes], ["found", "timeout"])

    def test_non_retryable_error_ends_the_lane_with_code_and_hint(self):
        e = lq.QueryError(400, "FE_SQL_VALIDATION_ERROR", "bad column")
        s = Script(**{"lq:lq_orders": ["none", e], "tf:lq_orders": ["found"]})
        run = manager(self.tmp, s, FakeProducer(OK)).start(2)
        lane = run.lanes[0]
        self.assertEqual(lane.status, "error")
        self.assertEqual(lane.error_code, "FE_SQL_VALIDATION_ERROR")
        self.assertEqual(lane.error_hint, lq.HINTS["FE_SQL_VALIDATION_ERROR"])
        self.assertEqual(lane.polls, 2)
        self.assertEqual(run.state, runs.TIMEOUT)

    def test_retryable_error_counts_as_not_yet(self):
        e = lq.QueryError(404, "FE_TABLE_NOT_FOUND", "x")
        s = Script(**{"lq:lq_orders": [e, e, "found"], "tf:lq_orders": [runs.TableflowError("DuckDB exit 1"), "found"]})
        run = manager(self.tmp, s, FakeProducer(OK)).start(2)
        self.assertEqual(run.state, runs.DONE)
        self.assertEqual(run.lanes[0].log, "rr!")
        self.assertEqual(run.lanes[1].log, "n!")

    def test_network_error_counts_as_not_yet(self):
        s = Script(**{"lq:lq_orders": [lq.NetworkError("https://x/y?sig=1", "down"), "found"],
                      "tf:lq_orders": ["found"]})
        run = manager(self.tmp, s, FakeProducer(OK)).start(2)
        self.assertEqual(run.lanes[0].log, "n!")

    def test_429_doubles_interval_up_to_10s(self):
        e = lq.QueryError(429, "", "slow down")
        s = Script(**{"lq:lq_orders": [e, e, e, e, e, "found"], "tf:lq_orders": ["found"]})
        run = manager(self.tmp, s, FakeProducer(OK)).start(2)
        lane = run.lanes[0]
        self.assertTrue(lane.rate_limited)
        self.assertEqual(lane.interval, 10.0)
        # waits: 2 + 4 + 8 + 10 + 10 = 34 s
        self.assertEqual(lane.found_after, 34.0)
        self.assertEqual(lane.to_dict(0, 0)["rate_limited"], True)

    def test_interval_counts_from_the_start_of_each_query(self):
        # A DuckDB call of 10 s: the next call starts 30 s after the start of the last one, not 40 s.
        clock = FakeClock(0.0)
        starts = []

        def slow_tf(sql):
            starts.append(clock.now)
            clock.now += 10
            return ["order_id", "price"], ([["UI-L2-120000", "0"]] if len(starts) == 3 else [])
        lane = runs.Lane("tf", "Tableflow", "tf", "lq_orders", "UI-L2-120000", 30, 600)
        lane.run(0.0, None, slow_tf, clock, clock.sleep)
        self.assertEqual(starts, [0.0, 30.0, 60.0])
        self.assertEqual(lane.found_after, 70.0)

    def test_rate_limit_code_without_429_also_slows_down(self):
        e = lq.QueryError(400, "RATE_LIMIT_EXCEEDED", "x")
        s = Script(**{"lq:lq_orders": [e, "found"], "tf:lq_orders": ["found"]})
        run = manager(self.tmp, s, FakeProducer(OK)).start(2)
        self.assertEqual(run.lanes[0].interval, 2.0)
        self.assertEqual(run.lanes[0].note, "rate limit, slower polling")

    def test_only_one_active_run(self):
        m = manager(self.tmp, Script(), FakeProducer(OK))
        m.active = runs.Run("UI-L2-000000", 2, 0, 0, "MARKER")
        with self.assertRaises(runs.Busy):
            m.start(3)

    def test_same_id_twice_is_refused(self):
        s = Script(**{"lq:lq_orders": ["found"], "tf:lq_orders": ["found"]})
        m = manager(self.tmp, s, FakeProducer(OK), hms=["120000", "120000"])
        m.start(2)
        with self.assertRaises(runs.Busy):
            m.start(2)

    def test_forbidden_transitions(self):
        m = manager(self.tmp, Script(), FakeProducer(OK))
        allowed = runs.TRANSITIONS
        all_states = [runs.IDLE, runs.WRITING, runs.WATCHING, runs.DONE, runs.TIMEOUT, runs.UNKNOWN, runs.FAILED]
        for a in all_states:
            for b in all_states:
                run = runs.Run("UI-L2-000000", 2, 0, 0, "MARKER")
                run.state = a
                if (a, b) in allowed:
                    m._set(run, b)
                    self.assertEqual(run.state, b)
                else:
                    with self.assertRaises(runs.TransitionError, msg=f"{a}->{b}"):
                        m._set(run, b)
        self.assertEqual(len(allowed), 8)

    def test_restart_marks_active_run_interrupted_and_keeps_history(self):
        s = Script(**{"lq:lq_orders": ["found"], "tf:lq_orders": ["found"]})
        m = manager(self.tmp, s, FakeProducer(OK))
        done = m.start(2)
        stuck = runs.Run("UI-L2-120500", 2, 0, 0, "MARKER")
        m._set(stuck, runs.WRITING)
        m2 = manager(self.tmp, s, FakeProducer(OK))
        self.assertEqual(m2.history[stuck.id]["state"], runs.INTERRUPTED)
        self.assertEqual(m2.history[done.id]["state"], runs.DONE)
        self.assertEqual(states(self.tmp, stuck.id), ["WRITING", "INTERRUPTED"])
        self.assertIsNone(m2.active)
        self.assertEqual([h["id"] for h in m2.history_list(2)], [stuck.id, done.id])

    def test_internal_error_never_leaves_a_run_active(self):
        def boom(*a):
            raise RuntimeError(f"cmd --api-secret {SECRET}")
        m = manager(self.tmp, Script(), boom)
        run = m.start(2)
        self.assertEqual(run.state, runs.FAILED)
        self.assertIsNone(m.active)
        self.assertNotIn(SECRET, run.message)


class TestTableflowFixedQueries(unittest.TestCase):
    def test_fixed_queries_only(self):
        self.assertTrue(runs.is_fixed_tableflow_sql(runs.tableflow_sql("lq_orders", "UI-L2-120000")))
        self.assertTrue(runs.is_fixed_tableflow_sql(runs.tableflow_sql("lq_orders_latest", "UI-L3-120000")))
        for bad in ["SELECT * FROM read_csv('/etc/passwd')",
                    "COPY lq_orders TO '/tmp/x.csv'",
                    "SELECT order_id, price FROM lq_orders WHERE order_id = 'UI-L2-120000' OR 1=1",
                    "SELECT order_id, price FROM lq_orders WHERE order_id = 'X'; ATTACH 'x'"]:
            self.assertFalse(runs.is_fixed_tableflow_sql(bad), bad)

    def test_run_id_is_checked(self):
        for bad in ["UI-L2-12000", "UI-L4-120000", "x' OR '1'='1", "UI-L2-120000'; --", "UI-L2-999999"]:
            with self.assertRaises(ValueError, msg=bad):
                runs.tableflow_sql("lq_orders", bad)
        with self.assertRaises(ValueError):
            runs.tableflow_sql("other_table", "UI-L2-120000")

    def test_lane_refuses_a_changed_query(self):
        lane = runs.Lane("tf", "Tableflow", "tf", "lq_orders", "UI-L2-120000", 30, 600)
        lane.sql = "SELECT * FROM read_text('/etc/hosts')"
        with self.assertRaises(runs.ForbiddenQuery):
            lane.poll(None, lambda sql: ([], []))

    def test_server_tableflow_runner_refuses_free_sql(self):
        tf = server.make_tableflow(SETTINGS, [SECRET])
        with self.assertRaises(runs.ForbiddenQuery):
            tf("SELECT 1")

    def test_lab1_sql_check(self):
        for ok in ["SELECT * FROM lq_orders", "show tables", "DESCRIBE lq_orders;", "  select 1  "]:
            self.assertTrue(runs.lab1_sql_ok(ok), ok)
        for bad in ["DROP TABLE x", "SELECT 1; SELECT 2", "INSERT INTO x VALUES (1)", ""]:
            self.assertFalse(runs.lab1_sql_ok(bad), bad)


class TestSecrets(unittest.TestCase):
    def test_redact_lines_removes_secret_and_command_lines(self):
        text = (f"Error: Authentication failed\nrunning confluent --api-secret {SECRET}\n"
                f"value {SECRET}\nCREATE SECRET x (CLIENT_SECRET 'abc')\n"
                "GET https://bucket.s3.amazonaws.com/f.json?X-Amz-Signature=abc&x=1\n"
                "see https://host/path?token=1 for details\nAuthorization: Basic abc")
        out = runs.redact_lines(text, [SECRET])
        self.assertEqual(out, ["Error: Authentication failed", "see https://host/path?... for details"])

    def test_producer_failure_message_has_no_secret(self):
        tmp = tempfile.mkdtemp()
        lines = runs.redact_lines(f"Error: bad\nconfluent ... --api-secret {SECRET}", [SECRET])
        m = manager(tmp, Script(), FakeProducer(runs.ProduceResult(1, lines)))
        m.start(2)
        with open(os.path.join(tmp, "runs.jsonl")) as f:
            text = f.read()
        self.assertNotIn(SECRET, text)
        self.assertIn("Error: bad", text)

    def test_store_scrubs_a_secret_as_last_guard(self):
        tmp = tempfile.mkdtemp()
        st = runs.Store(os.path.join(tmp, "runs.jsonl"), [SECRET])
        st.append({"id": "UI-L2-120000", "message": f"x {SECRET} y"})
        with open(os.path.join(tmp, "runs.jsonl")) as f:
            text = f.read()
        self.assertNotIn(SECRET, text)
        self.assertIn("[removed]", text)

    def test_real_producer_error_lines_are_filtered(self):
        # The real producer with a fake `confluent` that prints its arguments, with the secret, and fails.
        tmp = tempfile.mkdtemp()
        fake = os.path.join(tmp, "confluent")
        with open(fake, "w") as f:
            f.write("#!/bin/sh\necho \"args: $*\" >&2\nexit 3\n")
        os.chmod(fake, 0o755)
        old = os.environ["PATH"]
        os.environ["PATH"] = tmp + os.pathsep + old
        try:
            prod = server.make_producer(SETTINGS, runs.secret_values(SETTINGS))
            res = prod("UI-L2-120000", 0, "MARKER", 0)
        finally:
            os.environ["PATH"] = old
        self.assertEqual(res.returncode, 3)
        self.assertEqual(res.stderr, [])  # the line with the arguments holds the secret: removed
        self.assertFalse(res.timed_out)


class TestServer(unittest.TestCase):
    """The HTTP layer, with a fake manager runner. Token and Host checks."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        s = Script(**{"lq:lq_orders": ["found"], "tf:lq_orders": ["found"]})
        cls.m = manager(cls.tmp, s, FakeProducer(OK))

        def query(sql, max_rows=None):
            if "boom" in sql:
                raise lq.QueryError(400, "FE_SQL_VALIDATION_ERROR", "Object 'boom' not found")
            return 0.25, ["NAME"], [["lq_orders"]]

        doctor = server.Doctor(SETTINGS, [SECRET], run_checks=lambda s: [])
        cls.app = server.App(SETTINGS, cls.m, query, doctor, port=0, token="tok-123")
        cls.httpd = server.ThreadingHTTPServer(("127.0.0.1", 0), server.make_handler(cls.app))
        cls.app.port = cls.httpd.server_address[1]
        cls.port = cls.app.port
        threading.Thread(target=cls.httpd.serve_forever, daemon=True).start()
        cls._stderr = sys.stderr
        sys.stderr = open(os.devnull, "w")

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        sys.stderr.close()
        sys.stderr = cls._stderr

    def req(self, method, path, body=None, token="tok-123", host=None):
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        headers = {"Host": host or f"127.0.0.1:{self.port}"}
        if token is not None:
            headers["X-Lab-Token"] = token
        data = json.dumps(body).encode() if body is not None else None
        if data is not None:
            headers["Content-Type"] = "application/json"
        c.request(method, path, body=data, headers=headers)
        r = c.getresponse()
        raw = r.read()
        c.close()
        return r.status, raw

    def test_page_has_the_token(self):
        status, raw = self.req("GET", "/", token=None)
        self.assertEqual(status, 200)
        self.assertIn(b'content="tok-123"', raw)

    def test_localhost_host_is_accepted(self):
        status, _ = self.req("GET", "/api/state", host=f"localhost:{self.port}")
        self.assertEqual(status, 200)

    def test_wrong_host_is_rejected(self):
        for host in ["evil.example", f"evil.example:{self.port}", "127.0.0.1:1", f"127.0.0.2:{self.port}"]:
            status, _ = self.req("GET", "/", host=host)
            self.assertEqual(status, 403, host)
            status, _ = self.req("GET", "/api/state", host=host)
            self.assertEqual(status, 403, host)

    def test_api_without_token_is_rejected(self):
        for method, path, body in [("GET", "/api/state", None), ("POST", "/api/run", {"lab": 2}),
                                   ("POST", "/api/query", {"sql": "SHOW TABLES"}), ("POST", "/api/doctor", {})]:
            for token in (None, "", "wrong"):
                status, _ = self.req(method, path, body, token=token)
                self.assertEqual(status, 403, (path, token))

    def test_state_has_no_secret(self):
        status, raw = self.req("GET", "/api/state")
        self.assertEqual(status, 200)
        self.assertNotIn(SECRET.encode(), raw)
        self.assertIn(b'"history"', raw)

    def test_lab1_query_and_error(self):
        status, raw = self.req("POST", "/api/query", {"sql": "SHOW TABLES"})
        self.assertEqual(json.loads(raw)["rows"], [["lq_orders"]])
        status, raw = self.req("POST", "/api/query", {"sql": "SELECT boom FROM lq_orders"})
        d = json.loads(raw)
        self.assertEqual(d["error_code"], "FE_SQL_VALIDATION_ERROR")
        self.assertEqual(d["hint"], lq.HINTS["FE_SQL_VALIDATION_ERROR"])
        status, _ = self.req("POST", "/api/query", {"sql": "DROP TABLE lq_orders"})
        self.assertEqual(status, 400)

    def test_run_endpoint(self):
        status, raw = self.req("POST", "/api/run", {"lab": 7})
        self.assertEqual(status, 400)
        status, raw = self.req("POST", "/api/run", {"lab": 2})
        self.assertEqual(status, 202)
        self.assertTrue(json.loads(raw)["id"].startswith("UI-L2-"))

    def test_run_endpoint_busy(self):
        self.m.active = runs.Run("UI-L2-000000", 2, 0, 0, "MARKER")
        try:
            status, _ = self.req("POST", "/api/run", {"lab": 3})
            self.assertEqual(status, 409)
        finally:
            self.m.active = None

    def test_no_free_tableflow_endpoint(self):
        status, _ = self.req("POST", "/api/tableflow", {"sql": "SELECT 1"})
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
