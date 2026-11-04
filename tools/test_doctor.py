"""Tests for lq_doctor.py and lq_selftest.py. Run: python3 -m unittest discover -s tools"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import lq  # noqa: E402
import lq_doctor as d  # noqa: E402
import lq_selftest as st  # noqa: E402

S = {"LQ_REGION": "eu-central-1", "LQ_ORG_ID": "org-1", "LQ_ENV_ID": "env-1", "LQ_CLUSTER_ID": "lkc-1",
     "LQ_POOL_ID": "lfcp-1", "LQ_API_KEY": "k", "LQ_API_SECRET": "s", "LQ_MODE": "auto",
     "LQ_NAME": "ksi"}

GOOD = {
    ("organization", "describe"): {"id": "org-1", "name": "Org"},
    ("rtce", "region", "list"): [{"region": "eu-central-1"}],
    ("environment", "describe"): {"id": "env-1"},
    ("kafka", "cluster", "describe"): {"status": "UP", "type": "STANDARD", "region": "eu-central-1",
                                       "rest_endpoint": "https://pkc-1:443"},
    ("flink", "compute-pool", "describe"): {"status": "PROVISIONED"},
    ("rtce", "rtce-topic", "list"): [{"topic_name": "lq_orders", "phase": "ACTIVE"}],
    ("flink", "statement", "list"): [{"name": "lq-ksi-feed-orders", "status": "RUNNING"},
                                        {"name": "lq-ab-feed-orders-latest", "status": "RUNNING"}],
    ("tableflow", "topic", "describe"): {"phase": "RUNNING"},
}


def fake_cli(answers, fail=()):
    def cli(args):
        for k, v in answers.items():
            if tuple(args[:len(k)]) == k:
                return (1, None, "Error: Resource Not Found") if k in fail else (0, v, "")
        return 1, None, "Error: unknown"
    return cli


def ok_run(settings, sql):
    return 0.1, ["name"], [["lq_orders"]]


def by_name(checks):
    return {c.name: c for c in checks}


class TestDoctor(unittest.TestCase):
    def test_good_setup_has_no_fail_and_warns_about_the_running_feed(self):
        checks = by_name(d.run_checks(S, cli=fake_cli(GOOD), run=ok_run))
        self.assertFalse([c for c in checks.values() if c.status == "FAIL"])
        self.assertEqual(checks["feed lq-ksi-feed-orders"].status, "WARN")
        self.assertIn("60 s", checks["feed lq-ksi-feed-orders"].fix)
        self.assertEqual(checks["name"].status, "OK")
        self.assertEqual(checks["RTCE lq_orders_latest"].status, "SKIP")

    def test_feeds_of_other_people_are_ignored(self):
        checks = by_name(d.run_checks(S, cli=fake_cli(GOOD), run=ok_run))
        self.assertEqual(checks["feed lq-ksi-feed-orders-latest"].status, "SKIP")
        self.assertNotIn("feed lq-ab-feed-orders-latest", checks)

    def test_missing_or_bad_name_is_a_fail(self):
        for bad in ("", "Ksi", "k", "k-s", "abcdefghijklm"):
            s = dict(S, LQ_NAME=bad)
            checks = by_name(d.run_checks(s, cli=fake_cli(GOOD), run=ok_run))
            self.assertEqual(checks["name"].status, "FAIL", bad)
            self.assertFalse([n for n in checks if n.startswith("feed ")], bad)

    def test_default_name_is_a_warning_and_feeds_are_still_checked(self):
        checks = by_name(d.run_checks(dict(S, LQ_NAME="changeme"), cli=fake_cli(GOOD), run=ok_run))
        self.assertEqual(checks["name"].status, "WARN")
        self.assertIn("tools/lq.env", checks["name"].fix)
        self.assertIn("feed lq-changeme-feed-orders", checks)

    def test_failed_feed_is_a_fail_and_stopped_feed_is_ok(self):
        answers = {**GOOD, ("flink", "statement", "list"): [
            {"name": "lq-ksi-feed-orders", "status": "FAILED"},
            {"name": "lq-ksi-feed-orders-latest", "status": "STOPPED"}]}
        checks = by_name(d.run_checks(S, cli=fake_cli(answers), run=ok_run))
        self.assertEqual(checks["feed lq-ksi-feed-orders"].status, "FAIL")
        self.assertIn("statement describe", checks["feed lq-ksi-feed-orders"].fix)
        self.assertEqual(checks["feed lq-ksi-feed-orders-latest"].status, "OK")

    def test_tableflow_is_checked_for_both_topics(self):
        checks = by_name(d.run_checks(S, cli=fake_cli(GOOD), run=ok_run))
        self.assertEqual(checks["Tableflow lq_orders"].status, "OK")
        self.assertEqual(checks["Tableflow lq_orders_latest"].status, "OK")

    def test_statement_names_use_the_name(self):
        self.assertEqual(d.feeds("ksi"), ("lq-ksi-feed-orders", "lq-ksi-feed-orders-latest"))

    def test_not_logged_in_stops_with_the_login_fix(self):
        checks = d.run_checks(S, cli=fake_cli(GOOD, fail=[("organization", "describe")]), run=ok_run)
        self.assertEqual(len(checks), 1)
        self.assertIn("confluent login", checks[0].fix)

    def test_wrong_org_is_a_fail(self):
        answers = {**GOOD, ("organization", "describe"): {"id": "org-2"}}
        c = by_name(d.run_checks(S, cli=fake_cli(answers), run=ok_run))["CLI login"]
        self.assertEqual(c.status, "FAIL")
        self.assertIn("confluent organization use org-1", c.fix)

    def test_region_without_rtce_is_a_fail(self):
        c = by_name(d.run_checks(dict(S, LQ_REGION="eu-north-1"), cli=fake_cli(GOOD), run=ok_run))["region"]
        self.assertEqual(c.status, "FAIL")

    def test_rest_not_enabled_is_only_a_warning(self):
        def run(settings, sql):
            if settings["LQ_MODE"] == "rest":
                raise lq.QueryError(403, "api_not_enabled", "x")
            return ok_run(settings, sql)
        checks = by_name(d.run_checks(S, cli=fake_cli(GOOD), run=run))
        self.assertEqual(checks["query over REST"].status, "WARN")
        self.assertEqual(checks["query over MCP"].status, "OK")

    def test_kafka_key_401_is_a_fail(self):
        s = dict(S, LQ_KAFKA_KEY="kk", LQ_KAFKA_SECRET="ks")
        c = by_name(d.run_checks(s, cli=fake_cli(GOOD), run=ok_run, rest_status=lambda *a: 401))["Kafka key"]
        self.assertEqual(c.status, "FAIL")
        self.assertIn("not the Global key", c.fix)

    def test_schema_id_mismatch_is_a_fail(self):
        s = dict(S, LQ_SCHEMA_ID="100003")
        c = by_name(d.run_checks(s, cli=fake_cli(GOOD), run=ok_run,
                                 schema_id=lambda env: "100001"))["schema ID"]
        self.assertEqual(c.status, "FAIL")
        self.assertIn("LQ_SCHEMA_ID=100001", c.fix)

    def test_schema_id_unknown_is_a_warning(self):
        c = by_name(d.run_checks(dict(S, LQ_SCHEMA_ID="1"), cli=fake_cli(GOOD), run=ok_run,
                                 schema_id=lambda env: ""))["schema ID"]
        self.assertEqual(c.status, "WARN")

    def test_report_prints_fix_only_for_problems(self):
        text = d.report([d.Check("a", "OK", "fine", "never shown"), d.Check("b", "FAIL", "bad", "do this")])
        self.assertNotIn("never shown", text)
        self.assertIn("fix: do this", text)


class TestSelftest(unittest.TestCase):
    def test_run_steps_reports_pass_fail_and_skip(self):
        def skip():
            raise st.Skip("no key")

        def bad():
            raise lq.QueryError(400, "FE_SQL_VALIDATION_ERROR", "bad", via="mcp")
        results = st.run_steps([("a", lambda: "ok"), ("b", skip), ("c", bad)])
        self.assertEqual([r[1] for r in results], ["PASS", "SKIP", "FAIL"])
        self.assertIn("mcp", results[2][2])

    def test_skip_is_not_a_failure(self):
        self.assertEqual(st.exit_code([("a", "PASS", ""), ("b", "SKIP", "")]), 0)
        self.assertEqual(st.exit_code([("a", "FAIL", "")]), 1)


if __name__ == "__main__":
    unittest.main()
