#!/usr/bin/env python3
"""End-to-end test of your lab setup. Prints PASS, FAIL or SKIP per step.

Usage:
  set -a; . tools/lq.env; set +a
  python3 tools/lq_selftest.py

It needs Lab 1. The marker step also needs the Lab 2 settings (LQ_KAFKA_KEY,
LQ_KAFKA_SECRET, LQ_SCHEMA_ID); without them, the step says SKIP.
Stop the feed first (Lab 2, step 6), or the marker can take up to 60 s.
Exit code 0 = no FAIL. Python 3.10+, standard library only.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)
import lq  # noqa: E402
import lq_watch  # noqa: E402

TABLE = "lq_orders"


class Skip(Exception):
    """The step cannot run with the current settings."""


def run_steps(steps) -> list[tuple[str, str, str]]:
    results = []
    for name, fn in steps:
        try:
            detail = fn() or ""
            results.append((name, "PASS", str(detail)))
        except Skip as e:
            results.append((name, "SKIP", str(e)))
        except lq.QueryError as e:
            results.append((name, "FAIL", f"{e.via} {e.status or ''} {e.code}: {e.message}".replace("  ", " ")))
        except Exception as e:  # report every failure, then go on
            results.append((name, "FAIL", f"{type(e).__name__}: {e}"))
    return results


def exit_code(results) -> int:
    return 0 if all(status != "FAIL" for _, status, _ in results) else 1


def main() -> int:
    s = lq.load_settings()
    marker = f"SELFTEST-{int(time.time())}"

    def show_tables():
        _, _, rows = lq.run_query(s, "SHOW TABLES")
        names = [r[0] for r in rows]
        if TABLE not in names:
            raise RuntimeError(f"{TABLE} not in SHOW TABLES: {names}")
        return f"{len(rows)} tables"

    def select_one():
        elapsed, _, rows = lq.run_query(s, f"SELECT ORDER_ID FROM {TABLE} LIMIT 1")
        if not rows:
            raise RuntimeError("no rows")
        return f"1 row in {elapsed:.2f} s"

    def limit_error():
        try:
            lq.run_query(s, f"SELECT COUNT(*) FROM {TABLE}")
        except lq.QueryError as e:
            if e.code == "QE_UNSUPPORTED_AGGREGATION":
                return e.code
            raise
        raise RuntimeError("COUNT(*) worked: aggregates are supported now, update the labs")

    def marker_visible():
        missing = [n for n in ("LQ_KAFKA_KEY", "LQ_KAFKA_SECRET", "LQ_SCHEMA_ID") if not os.environ.get(n)]
        if missing:
            raise Skip(f"no {', '.join(missing)} (Lab 2, Part A)")
        # The same CLI producer as in Lab 2, step 7.
        event = subprocess.run([os.path.join(TOOLS, "marker_json.sh"), marker],
                               capture_output=True, text=True, check=True).stdout
        p = subprocess.run(["confluent", "kafka", "topic", "produce", TABLE,
                            "--cluster", s["LQ_CLUSTER_ID"], "--environment", s["LQ_ENV_ID"],
                            "--api-key", os.environ["LQ_KAFKA_KEY"], "--api-secret", os.environ["LQ_KAFKA_SECRET"],
                            "--value-format", "avro", "--schema", os.environ["LQ_SCHEMA_ID"]],
                           input=event, capture_output=True, text=True, timeout=60)
        if p.returncode != 0 or "Error" in p.stderr:
            raise RuntimeError(f"CLI producer failed: {p.stderr.strip().splitlines()[-1:]}")
        waited = lq_watch.watch(s, f"SELECT ORDER_ID FROM {TABLE} WHERE ORDER_ID = '{marker}'",
                                interval=1.0, timeout=180.0)
        return f"{marker} visible after {waited:.1f} s"

    results = run_steps([("SHOW TABLES", show_tables), ("SELECT", select_one),
                         ("limit error code", limit_error), ("marker freshness", marker_visible)])
    for name, status, detail in results:
        print(f"{status}  {name:<18} {detail}")
    return exit_code(results)


if __name__ == "__main__":
    sys.exit(main())
