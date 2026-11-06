"""Tests for lq.py and lq_watch.py. Run: python3 -m unittest tools/test_lq.py -v"""
import base64
import io
import itertools
import json
import os
import sys
import tempfile
import unittest
import urllib.error
from contextlib import redirect_stdout
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import lq  # noqa: E402
import lq_watch  # noqa: E402

SETTINGS = {
    "LQ_REGION": "eu-central-1",
    "LQ_ORG_ID": "org-uuid",
    "LQ_ENV_ID": "env-abc",
    "LQ_CLUSTER_ID": "lkc-abc",
    "LQ_API_KEY": "k",
    "LQ_API_SECRET": "s",
}


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def ok_opener(body):
    def opener(request, timeout=None):
        return FakeResponse(json.dumps(body).encode())
    return opener


def error_opener(status, body):
    raw = body if isinstance(body, str) else json.dumps(body)

    def opener(request, timeout=None):
        raise urllib.error.HTTPError(
            request.full_url, status, "error", {}, io.BytesIO(raw.encode()))
    return opener


def api_error(status, code, detail):
    return {"errors": [{"id": "x", "status": str(status), "code": code,
                        "title": "t", "detail": detail}]}


class TestLq(unittest.TestCase):
    def test_auth_header_strips_whitespace(self):
        expected = "Basic " + base64.b64encode(b"k:s").decode()
        self.assertEqual(lq.auth_header(" k\n", "s \n"), expected)

    def test_build_request_url_and_body(self):
        req = lq.build_request(SETTINGS, "SELECT 1")
        self.assertEqual(
            req.full_url, "https://sql.eu-central-1.aws.confluent.cloud/query/v1")
        self.assertEqual(req.get_method(), "POST")
        body = json.loads(req.data)
        self.assertEqual(body["catalog_name"], "env-abc")
        self.assertEqual(body["database_name"], "lkc-abc")
        self.assertEqual(body["query"], "SELECT 1")
        # The API has no row limit. Without --max-rows, lq.py asks for all rows.
        self.assertNotIn("max_result_rows", body["options"])

    def test_max_rows_above_200_goes_to_the_api(self):
        body = json.loads(lq.build_request(SETTINGS, "SELECT 1", max_rows=500).data)
        self.assertEqual(body["options"]["max_result_rows"], 500)

    def test_parse_rows_list_shape(self):
        body = {"result": {"schema": {"columns": [{"name": "A"}, {"name": "B"}]},
                           "data": [["1", "x"]]}}
        self.assertEqual(lq.parse_rows(body), (["A", "B"], [["1", "x"]]))

    def test_parse_rows_row_object_shape(self):
        body = {"result": {"schema": {"columns": [{"name": "A"}, {"name": "B"}]},
                           "data": [{"row": ["1", "x"]}]}}
        self.assertEqual(lq.parse_rows(body), (["A", "B"], [["1", "x"]]))

    def test_parse_rows_empty(self):
        body = {"result": {"schema": {"columns": [{"name": "A"}]}, "data": []}}
        self.assertEqual(lq.parse_rows(body), (["A"], []))

    def test_error_detail_is_printed(self):
        line = lq.format_error(400, api_error(
            400, "FE_SQL_VALIDATION_ERROR", "Column 'NOPE' not found in any table"))
        self.assertIn("400", line)
        self.assertIn("FE_SQL_VALIDATION_ERROR", line)
        self.assertIn("Column 'NOPE' not found in any table", line)

    def test_api_not_enabled_hint(self):
        line = lq.format_error(403, {"code": "api_not_enabled",
                                     "message": "The REST API is not enabled for this organization."})
        self.assertIn("LQ_MODE=mcp", line)

    def test_retryable_error_prints_retry_hint(self):
        body = api_error(503, "FE_BACKEND_UNAVAILABLE", "Query engine is temporarily unavailable.")
        self.assertIn("retry", lq.format_error(503, body).lower())
        out = io.StringIO()
        with redirect_stdout(out), mock.patch("sys.stderr", out):
            code = lq.main(["SELECT 1"], opener=error_opener(503, body), settings=SETTINGS)
        self.assertEqual(code, 2)

    def test_unauthenticated_hint(self):
        # Real answer from the endpoint on 2026-10-26 for a wrong key.
        body = api_error(401, "unauthenticated",
                         "The API key is invalid, or is not a key that authenticates a whole organization.")
        self.assertIn("Global API key", lq.format_error(401, body))

    def test_unsupported_sql_codes_have_hints(self):
        # Codes seen live on 2026-10-26.
        for code in ["QE_UNSUPPORTED_AGGREGATION", "QE_UNSUPPORTED_JOIN", "QE_UNSUPPORTED_SUBQUERY",
                     "QE_UNSUPPORTED_SET_OP", "QE_UNSUPPORTED_WINDOW_FUNCTION"]:
            self.assertIn("hint:", lq.format_error(400, api_error(400, code, "x")), code)

    def test_table_not_found_hint_mentions_show_tables(self):
        line = lq.format_error(400, api_error(400, "FE_TABLE_NOT_FOUND",
                                              "Table does not exist: 'no_such_table'"))
        self.assertIn("SHOW TABLES", line)

    def test_table_not_found_is_retryable(self):
        # Right after RTCE is turned on, the table can be missing for a short time.
        self.assertTrue(lq.QueryError(400, "FE_TABLE_NOT_FOUND", "x").retryable)

    def test_new_table_not_found_as_validation_error_is_retryable(self):
        # Seen live on 2026-10-27 right after ACTIVE of a new upsert table.
        e = lq.QueryError(400, "FE_SQL_VALIDATION_ERROR",
                          "SQL validation error: At line 1, column 17: Object 'LQ_ORDERS_LATEST' not found")
        self.assertTrue(e.retryable)
        self.assertTrue(e.table_missing)
        other = lq.QueryError(400, "FE_SQL_VALIDATION_ERROR", "Column 'NOPE' not found in any table")
        self.assertFalse(other.retryable)

    def test_mt_metadata_update_is_retryable(self):
        # Seen live on a new upsert table: HTTP 500, "temporarily unavailable".
        self.assertTrue(lq.QueryError(500, "MT_METADATA_UPDATE", "x").retryable)
        self.assertIn("retry", lq.format_error(500, api_error(500, "MT_METADATA_UPDATE", "x")).lower())

    def test_rate_limit_codes_are_retryable(self):
        # Docs: "the Real-Time Context Engine returns a RATE_LIMIT_EXCEEDED error".
        self.assertTrue(lq.QueryError(429, "RATE_LIMIT_EXCEEDED", "x").retryable)
        self.assertIn("rate limit", lq.format_error(429, api_error(429, "RATE_LIMIT_EXCEEDED", "x")).lower())

    def test_http_429_is_retryable_with_any_code(self):
        self.assertTrue(lq.QueryError(429, "SOMETHING_NEW", "x").retryable)
        self.assertFalse(lq.QueryError(400, "SOMETHING_NEW", "x").retryable)

    def test_main_reports_non_json_answer(self):
        class Resp:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self): return b"<html>proxy error</html>"
        err = io.StringIO()
        with redirect_stdout(io.StringIO()), mock.patch("sys.stderr", err):
            rc = lq.main(["SELECT 1"], opener=lambda r, timeout: Resp(), settings=SETTINGS)
        self.assertEqual(rc, 1)
        self.assertIn("not JSON", err.getvalue())

    def test_non_json_error_body(self):
        line = lq.format_error(500, "Oops")
        self.assertIn("500", line)

    def test_missing_settings_named(self):
        with tempfile.NamedTemporaryFile("w", suffix=".env", delete=False) as f:
            path = f.name
        try:
            with mock.patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(SystemExit) as ctx:
                    lq.load_settings(path)
            self.assertIn("LQ_API_KEY", str(ctx.exception))
            self.assertIn("LQ_ENV_ID", str(ctx.exception))
        finally:
            os.unlink(path)

    def test_run_query_returns_rows(self):
        body = {"result": {"schema": {"columns": [{"name": "A"}]}, "data": [["1"]]}}
        elapsed, cols, rows = lq.run_query(SETTINGS, "SELECT 1", opener=ok_opener(body))
        self.assertEqual((cols, rows), (["A"], [["1"]]))
        self.assertGreaterEqual(elapsed, 0.0)

    def test_run_query_raises_query_error(self):
        opener = error_opener(400, api_error(400, "FE_CUSTOMER_NOT_FOUND", "not found"))
        with self.assertRaises(lq.QueryError) as ctx:
            lq.run_query(SETTINGS, "SELECT 1", opener=opener)
        self.assertEqual(ctx.exception.status, 400)
        self.assertEqual(ctx.exception.code, "FE_CUSTOMER_NOT_FOUND")


class TestWatch(unittest.TestCase):
    def test_watch_returns_elapsed_after_rows_appear(self):
        answers = [lq.QueryError(503, "FE_BACKEND_UNAVAILABLE", "x"),
                   lq.QueryError(400, "FE_CUSTOMER_NOT_FOUND", "x"),
                   (0.1, ["A"], []),
                   (0.1, ["A"], [["1"]])]
        calls = []

        def run(settings, sql):
            calls.append(sql)
            a = answers[len(calls) - 1]
            if isinstance(a, Exception):
                raise a
            return a

        clock = iter([0.0, 1.0, 2.0, 3.0, 4.0]).__next__
        with redirect_stdout(io.StringIO()):
            waited = lq_watch.watch(SETTINGS, "SELECT 1", run=run, clock=clock,
                                    sleep=lambda s: None)
        self.assertEqual(waited, 4.0)
        self.assertEqual(len(calls), 4)

    def test_watch_until_empty(self):
        answers = [(0.1, ["A"], [["1"]]), (0.1, ["A"], [["1"]]), (0.1, ["A"], [])]
        calls = []

        def run(settings, sql):
            calls.append(sql)
            return answers[len(calls) - 1]

        clock = itertools.count().__next__
        with redirect_stdout(io.StringIO()):
            lq_watch.watch(SETTINGS, "SELECT 1", until_empty=True, run=run,
                           clock=lambda: float(clock()), sleep=lambda s: None)
        self.assertEqual(len(calls), 3)

    def test_watch_times_out(self):
        clock = itertools.count().__next__
        with redirect_stdout(io.StringIO()):
            with self.assertRaises(lq_watch.WatchTimeout):
                lq_watch.watch(SETTINGS, "SELECT 1", timeout=3,
                               run=lambda s, q: (0.1, ["A"], []),
                               clock=lambda: float(clock()), sleep=lambda s: None)

    def test_watch_counts_network_errors_as_not_yet(self):
        import urllib.error
        answers = [urllib.error.URLError("no route"), TimeoutError("timed out"),
                   ValueError("not JSON"), (0.1, ["A"], [["1"]])]
        calls = []

        def run(settings, sql):
            calls.append(sql)
            a = answers[len(calls) - 1]
            if isinstance(a, Exception):
                raise a
            return a

        clock = itertools.count().__next__
        out = io.StringIO()
        with redirect_stdout(out):
            lq_watch.watch(SETTINGS, "SELECT 1", run=run, clock=lambda: float(clock()),
                           sleep=lambda s: None)
        self.assertEqual(len(calls), 4)
        self.assertIn("nnn!", out.getvalue())

    def test_watch_stops_when_table_stays_not_found(self):
        def run(settings, sql):
            raise lq.QueryError(400, "FE_TABLE_NOT_FOUND", "Table does not exist")

        clock = itertools.count(step=10).__next__
        with redirect_stdout(io.StringIO()):
            with self.assertRaises(lq.QueryError) as ctx:
                lq_watch.watch(SETTINGS, "SELECT 1", timeout=600, run=run,
                               clock=lambda: float(clock()), sleep=lambda s: None)
        self.assertEqual(ctx.exception.code, "FE_TABLE_NOT_FOUND")

    def test_watch_timeout_is_its_own_error(self):
        # A socket timeout must not look like the watch timeout.
        self.assertFalse(issubclass(lq_watch.WatchTimeout, TimeoutError))

    def test_watch_non_retryable_error_stops(self):
        def run(settings, sql):
            raise lq.QueryError(400, "FE_SQL_VALIDATION_ERROR", "bad column")

        with redirect_stdout(io.StringIO()):
            with self.assertRaises(lq.QueryError):
                lq_watch.watch(SETTINGS, "SELECT 1", run=run,
                               clock=lambda: 0.0, sleep=lambda s: None)


def sse(payload):
    """An MCP answer as server-sent events, as the endpoint sends it."""
    return "event: message\ndata: " + json.dumps(payload) + "\n\n"


def mcp_text(inner, is_error=False):
    result = {"content": [{"type": "text", "text": json.dumps(inner)}]}
    if is_error:
        result["isError"] = True
    return sse({"jsonrpc": "2.0", "id": 1, "result": result})


def raw_opener(text, seen=None):
    def opener(request, timeout=None):
        if seen is not None:
            seen.append(request)
        return FakeResponse(text.encode())
    return opener


class TestMcp(unittest.TestCase):
    def test_mcp_endpoint(self):
        self.assertEqual(
            lq.mcp_endpoint(SETTINGS),
            "https://mcp.eu-central-1.aws.confluent.cloud/mcp/v1/context-engine/organizations/"
            "org-uuid/environments/env-abc/kafka-clusters/lkc-abc")

    def test_mcp_request_is_a_query_data_call(self):
        seen = []
        ok = mcp_text({"status": "success", "rows": {"schema": {"columns": [{"name": "A"}]},
                                                     "data": [{"row": ["1"]}]}})
        lq.run_query(dict(SETTINGS, LQ_MODE="mcp"), "SELECT A FROM lq_orders", 5,
                     opener=raw_opener(ok, seen))
        body = json.loads(seen[0].data)
        self.assertEqual(body["method"], "tools/call")
        self.assertEqual(body["params"]["name"], "queryData")
        args = body["params"]["arguments"]
        self.assertEqual(args, {"topic_name": "lq_orders", "query": "SELECT A FROM lq_orders",
                                "max_result_rows": 5})
        self.assertIn("text/event-stream", seen[0].get_header("Accept"))

    def test_mcp_rows_are_the_same_as_rest_rows(self):
        ok = mcp_text({"status": "success", "rows": {"schema": {"columns": [{"name": "A"}, {"name": "B"}]},
                                                     "data": [{"row": ["1", "x"]}]}})
        _, cols, rows = lq.run_query(dict(SETTINGS, LQ_MODE="mcp"), "SHOW TABLES",
                                     opener=raw_opener(ok))
        self.assertEqual((cols, rows), (["A", "B"], [["1", "x"]]))

    def test_mcp_error_becomes_query_error_with_our_retry_rule(self):
        # MCP says retryable=true for COUNT(*). The retry rule of lq.py decides, not MCP.
        err = mcp_text({"status": "error", "error": {"code": "QE_UNSUPPORTED_AGGREGATION",
                                                      "message": "not supported", "retryable": True}},
                       is_error=True)
        with self.assertRaises(lq.QueryError) as ctx:
            lq.run_query(dict(SETTINGS, LQ_MODE="mcp"), "SELECT COUNT(*) FROM t", opener=raw_opener(err))
        self.assertEqual(ctx.exception.code, "QE_UNSUPPORTED_AGGREGATION")
        self.assertEqual(ctx.exception.via, "mcp")
        self.assertFalse(ctx.exception.retryable)

    def test_mcp_error_line_says_mcp_not_http(self):
        line = lq.format_error(0, {"error": {"code": "FE_TABLE_NOT_FOUND", "message": "Table does not exist"}},
                               via="mcp")
        self.assertTrue(line.startswith("MCP | FE_TABLE_NOT_FOUND"))

    def test_auto_mode_switches_to_mcp_on_api_not_enabled(self):
        calls = []
        ok = mcp_text({"status": "success", "rows": {"schema": {"columns": [{"name": "A"}]},
                                                     "data": [{"row": ["1"]}]}})

        def opener(request, timeout=None):
            calls.append(request.full_url)
            if "sql." in request.full_url:
                raise urllib.error.HTTPError(request.full_url, 403, "x", {}, io.BytesIO(json.dumps(
                    {"code": "api_not_enabled", "message": "The REST API is not enabled for this organization."}
                ).encode()))
            return FakeResponse(ok.encode())

        lq.reset_mode()
        err = io.StringIO()
        with mock.patch("sys.stderr", err):
            _, _, rows = lq.run_query(dict(SETTINGS, LQ_MODE="auto"), "SELECT A FROM t", opener=opener)
            lq.run_query(dict(SETTINGS, LQ_MODE="auto"), "SELECT A FROM t", opener=opener)
        self.assertEqual(rows, [["1"]])
        self.assertEqual([("sql." in u) for u in calls], [True, False, False])  # REST once, then MCP
        self.assertEqual(err.getvalue().count("LQ_MODE=mcp"), 1)

    def test_rest_mode_does_not_switch(self):
        lq.reset_mode()
        opener = error_opener(403, {"code": "api_not_enabled", "message": "x"})
        with self.assertRaises(lq.QueryError):
            lq.run_query(dict(SETTINGS, LQ_MODE="rest"), "SELECT 1", opener=opener)

    def test_mcp_needs_the_org_id(self):
        s = {k: v for k, v in SETTINGS.items() if k != "LQ_ORG_ID"}
        with self.assertRaises(SystemExit) as ctx:
            lq.run_query(dict(s, LQ_MODE="mcp"), "SELECT 1", opener=raw_opener(""))
        self.assertIn("LQ_ORG_ID", str(ctx.exception))

    def test_region_has_no_hidden_org_default(self):
        self.assertNotIn("LQ_ORG_ID", lq.DEFAULTS)



class Reply(io.BytesIO):
    """A fake HTTP response with a status and headers."""
    def __init__(self, status, body, headers=None):
        super().__init__(body if isinstance(body, bytes) else json.dumps(body).encode())
        self.status = status
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def script_opener(replies, seen):
    """Return the replies in order, and record each request."""
    it = iter(replies)

    def opener(request, timeout=None):
        seen.append(request)
        return next(it)
    return opener


STATUS_PATH = "/query/v1/jobs/env-abc/lkc-abc/lq_orders/statement_1/status"
def accepted():
    return Reply(202, {"kind": "Statement", "result": {"statement_id": "statement_1", "topic_name": "lq_orders"}},
                 {"Location": STATUS_PATH})


def job(state, urls=(), errors=None):
    body = {"kind": "JobStatus", "result": {"state": state, "result_urls": list(urls)}}
    if errors:
        body["errors"] = errors
    return Reply(200, body)


def result_file(rows):
    return Reply(200, {"result": {"schema": {"columns": [{"name": "A"}]}, "data": rows}})


@mock.patch("lq.time.sleep", lambda s: None)
class TestAsync(unittest.TestCase):
    def test_202_polls_the_job_and_downloads_all_result_files(self):
        seen = []
        replies = [accepted(), job("RUNNING"), job("SUCCEEDED", ["https://s3.example/r1", "https://s3.example/r2"]),
                   result_file([["1"], ["2"]]), result_file([["3"]])]
        with mock.patch("sys.stderr", io.StringIO()):
            _, cols, rows = lq.run_query(dict(SETTINGS, LQ_MODE="rest"), "SELECT A FROM lq_orders",
                                         opener=script_opener(replies, seen))
        self.assertEqual((cols, rows), (["A"], [["1"], ["2"], ["3"]]))
        self.assertEqual(seen[1].full_url, "https://sql.eu-central-1.aws.confluent.cloud" + STATUS_PATH)
        self.assertEqual(seen[1].get_method(), "GET")
        self.assertTrue(seen[1].get_header("Authorization", "").startswith("Basic "))
        # Cloud storage rejects a presigned URL with an Authorization header.
        self.assertEqual(seen[3].full_url, "https://s3.example/r1")
        self.assertIsNone(seen[3].get_header("Authorization"))
        self.assertIsNone(seen[4].get_header("Authorization"))

    def test_202_says_that_the_query_runs_asynchronously(self):
        replies = [accepted(), job("SUCCEEDED", ["https://s3.example/r1"]), result_file([["1"]])]
        err = io.StringIO()
        with mock.patch("sys.stderr", err):
            lq.run_query(dict(SETTINGS, LQ_MODE="rest"), "SELECT A FROM lq_orders", opener=script_opener(replies, []))
        self.assertIn("asynchronous", err.getvalue())
        self.assertIn("statement_1", err.getvalue())

    def test_failed_job_is_a_query_error_with_the_detail(self):
        errors = [{"code": "FE_STATEMENT_FAILED", "title": "Statement Failed",
                   "detail": "The statement exceeded the maximum execution time."}]
        replies = [accepted(), job("FAILED", errors=errors)]
        with mock.patch("sys.stderr", io.StringIO()), self.assertRaises(lq.QueryError) as ctx:
            lq.run_query(dict(SETTINGS, LQ_MODE="rest"), "SELECT A FROM lq_orders", opener=script_opener(replies, []))
        self.assertEqual(ctx.exception.code, "FE_STATEMENT_FAILED")
        self.assertIn("maximum execution time", ctx.exception.message)
        self.assertFalse(ctx.exception.retryable)

    def test_job_that_runs_too_long_is_a_retryable_timeout(self):
        replies = [accepted()] + [job("RUNNING") for _ in range(1000)]
        clock = itertools.count(0, 10)
        with mock.patch("sys.stderr", io.StringIO()), mock.patch("lq.time.monotonic", lambda: next(clock)), \
                self.assertRaises(lq.QueryError) as ctx:
            lq.run_query(dict(SETTINGS, LQ_MODE="rest"), "SELECT A FROM lq_orders", opener=script_opener(replies, []))
        self.assertEqual(ctx.exception.code, "query_timeout")
        self.assertTrue(ctx.exception.retryable)



def big_file(n_bytes, content_length=True):
    """A result file of about n_bytes. content_length=False: the server sends no size header."""
    rows = [["x" * 1000] for _ in range(max(1, n_bytes // 1010))]
    reply = result_file(rows)
    if content_length:
        reply.headers = {"Content-Length": str(len(reply.getvalue()))}
    return reply


@mock.patch("lq.time.sleep", lambda s: None)
class TestDownloadLimit(unittest.TestCase):
    """A query without LIMIT returned one 632 MB file on 2026-11-06. lq.py must stop before the memory is full."""
    def run_async(self, files, **settings):
        urls = [f"https://s3.example/r{i}" for i in range(len(files))]
        replies = [accepted(), job("SUCCEEDED", urls)] + files
        with mock.patch("sys.stderr", io.StringIO()):
            return lq.run_query(dict(SETTINGS, LQ_MODE="rest", **settings), "SELECT A FROM lq_orders",
                                opener=script_opener(replies, []))

    def test_file_above_the_limit_stops_with_result_too_large(self):
        with self.assertRaises(lq.QueryError) as ctx:
            self.run_async([big_file(2 << 20)], LQ_MAX_DOWNLOAD_MB="1")
        self.assertEqual(ctx.exception.code, "result_too_large")
        self.assertFalse(ctx.exception.retryable)
        self.assertIn("1 MB", ctx.exception.message)

    def test_limit_works_also_without_a_content_length_header(self):
        with self.assertRaises(lq.QueryError) as ctx:
            self.run_async([big_file(2 << 20, content_length=False)], LQ_MAX_DOWNLOAD_MB="1")
        self.assertEqual(ctx.exception.code, "result_too_large")

    def test_limit_counts_all_files_together(self):
        with self.assertRaises(lq.QueryError):
            self.run_async([big_file(700_000), big_file(700_000)], LQ_MAX_DOWNLOAD_MB="1")

    def test_zero_means_no_limit(self):
        _, _, rows = self.run_async([big_file(2 << 20)], LQ_MAX_DOWNLOAD_MB="0")
        self.assertGreater(len(rows), 1000)

    def test_default_limit_is_100_mb(self):
        self.assertEqual(lq.MAX_DOWNLOAD_MB, 100)
        _, _, rows = self.run_async([big_file(1 << 20)])
        self.assertGreater(len(rows), 500)

    def test_main_prints_the_hint_and_exits_1(self):
        replies = [accepted(), job("SUCCEEDED", ["https://s3.example/r1"]), big_file(2 << 20)]
        err = io.StringIO()
        with mock.patch("sys.stderr", err), redirect_stdout(io.StringIO()):
            code = lq.main(["--max-download-mb", "1", "SELECT A FROM t"], opener=script_opener(replies, []),
                           settings=dict(SETTINGS, LQ_MODE="rest"))
        self.assertEqual(code, 1)
        self.assertIn("result_too_large", err.getvalue())
        self.assertIn("LIMIT", err.getvalue())

    def test_negative_flag_value_is_a_usage_error(self):
        with mock.patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit) as ctx:
            lq.main(["--max-download-mb", "-1", "SELECT A FROM t"], settings=dict(SETTINGS, LQ_MODE="rest"))
        self.assertEqual(ctx.exception.code, 1)


class Test404(unittest.TestCase):
    """The API docs: 404 means that the table is not available yet, for example while it is set up. Retry."""
    def test_404_is_retryable_and_counts_as_table_missing(self):
        e = lq.QueryError(404, "", "not found")
        self.assertTrue(e.retryable)
        self.assertTrue(e.table_missing)

    def test_404_hint_says_retry(self):
        self.assertIn("Retry", lq.STATUS_HINTS[404])

class TestRowLimits(unittest.TestCase):
    def test_mcp_asks_for_200_rows_without_max_rows(self):
        self.assertEqual(json.loads(lq.build_mcp_request(SETTINGS, "SELECT A FROM t").data)
                         ["params"]["arguments"]["max_result_rows"], 200)

    def test_mcp_caps_max_rows_at_200(self):
        # The server returns at most 200 rows over MCP, also for 500, and with no warning.
        self.assertEqual(json.loads(lq.build_mcp_request(SETTINGS, "SELECT A FROM t", 500).data)
                         ["params"]["arguments"]["max_result_rows"], 200)

    def test_table_output_shows_100_rows_and_says_how_many_more(self):
        body = {"result": {"schema": {"columns": [{"name": "A"}]}, "data": [[str(i)] for i in range(150)]}}
        out = io.StringIO()
        with redirect_stdout(out):
            code = lq.main(["SELECT A FROM t"], opener=ok_opener(body), settings=dict(SETTINGS, LQ_MODE="rest"))
        self.assertEqual(code, 0)
        lines = [line.strip() for line in out.getvalue().splitlines()]
        text = out.getvalue()
        self.assertIn("99", lines)
        self.assertNotIn("100", lines)
        self.assertIn("50 more rows", text)
        self.assertIn("150 rows in", text)

    def test_json_output_has_all_rows(self):
        body = {"result": {"schema": {"columns": [{"name": "A"}]}, "data": [[str(i)] for i in range(150)]}}
        out = io.StringIO()
        with redirect_stdout(out):
            lq.main(["--json", "SELECT A FROM t"], opener=ok_opener(body), settings=dict(SETTINGS, LQ_MODE="rest"))
        self.assertEqual(len(json.loads(out.getvalue())["rows"]), 150)


class TestMainErrors(unittest.TestCase):
    def run_main(self, argv, opener, settings):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), mock.patch("sys.stderr", err):
            try:
                code = lq.main(argv, opener=opener, settings=settings)
            except SystemExit as e:
                code = e.code
        return code, out.getvalue() + err.getvalue()

    def test_max_rows_0_is_a_usage_error_not_a_json_error(self):
        code, text = self.run_main(["--max-rows", "0", "SELECT 1"], ok_opener({}), dict(SETTINGS, LQ_MODE="rest"))
        self.assertEqual(code, 1)  # not 2: 2 means "retry later"
        self.assertIn("--max-rows", text)
        self.assertNotIn("not JSON", text)

    def test_network_error_names_the_mcp_endpoint_in_mcp_mode(self):
        def down(request, timeout=None):
            raise urllib.error.URLError("no route")
        code, text = self.run_main(["SELECT A FROM lq_orders"], down, dict(SETTINGS, LQ_MODE="mcp"))
        self.assertEqual(code, 1)
        self.assertIn("https://mcp.eu-central-1.aws.confluent.cloud", text)
        self.assertNotIn("https://sql.", text)

    def test_network_error_names_the_rest_endpoint_in_rest_mode(self):
        def down(request, timeout=None):
            raise urllib.error.URLError("no route")
        code, text = self.run_main(["SELECT 1"], down, dict(SETTINGS, LQ_MODE="rest"))
        self.assertIn("https://sql.eu-central-1.aws.confluent.cloud", text)

    def test_timeout_names_the_url_that_did_not_answer(self):
        def slow(request, timeout=None):
            raise TimeoutError()
        code, text = self.run_main(["SELECT A FROM lq_orders"], slow, dict(SETTINGS, LQ_MODE="mcp"))
        self.assertEqual(code, 2)
        self.assertIn("https://mcp.", text)

    def test_network_error_during_the_download_names_the_download_host(self):
        seen = []
        def opener(request, timeout=None):
            seen.append(request)
            if len(seen) == 1:
                return accepted()
            if len(seen) == 2:
                return job("SUCCEEDED", ["https://s3.example/r1?X-Amz-Signature=secret"])
            raise urllib.error.URLError("no route")
        with mock.patch("lq.time.sleep", lambda s: None):
            code, text = self.run_main(["SELECT A FROM lq_orders"], opener, dict(SETTINGS, LQ_MODE="rest"))
        self.assertEqual(code, 1)
        self.assertIn("https://s3.example", text)
        # The query string of a presigned URL is a credential. Never print it.
        self.assertNotIn("X-Amz-Signature", text)

if __name__ == "__main__":
    unittest.main()
