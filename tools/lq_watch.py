#!/usr/bin/env python3
"""Repeat a Lightning query until it returns rows, and print the seconds waited.

Usage:
  python3 tools/lq_watch.py "SELECT ORDER_ID FROM lq_orders WHERE ORDER_ID = 'MARKER-1'"
  python3 tools/lq_watch.py --until-empty "SELECT KEY FROM lq_orders_latest WHERE KEY = '9001'"

Retryable errors (table not ready yet) and network errors count as "not yet".
FE_TABLE_NOT_FOUND for more than 2 minutes stops the watch: check the table name.
"""
from __future__ import annotations

import argparse
import sys
import time
import urllib.error

import lq


NOT_FOUND_LIMIT = 120.0  # seconds of FE_TABLE_NOT_FOUND before the watch stops


class WatchTimeout(Exception):
    """The watch timeout. Not a subclass of TimeoutError, so a socket timeout is not mixed up with it."""


def watch(settings: dict, sql: str, interval: float = 1.0, timeout: float = 600.0,
          until_empty: bool = False, run=None, clock=time.monotonic, sleep=time.sleep) -> float:
    run = run or (lambda s, q: lq.run_query(s, q))
    start = clock()
    not_found_since = None
    while True:
        try:
            _, _, rows = run(settings, sql)
            done = (not rows) if until_empty else bool(rows)
            mark = "." if not done else "!"
            not_found_since = None
        except lq.QueryError as e:
            if not e.retryable:
                raise
            if e.table_missing:
                not_found_since = clock() if not_found_since is None else not_found_since
                if clock() - not_found_since >= NOT_FOUND_LIMIT:
                    print()
                    raise
            done, mark = False, "r"
        except (urllib.error.URLError, TimeoutError, ValueError):
            done, mark = False, "n"
        now = clock()
        print(mark, end="", flush=True)
        if done:
            print()
            return now - start
        if now - start >= timeout:
            print()
            raise WatchTimeout(f"not done after {timeout:.0f} s")
        sleep(interval)


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="Repeat a Lightning query until it returns rows.")
    p.add_argument("sql")
    p.add_argument("--until-empty", action="store_true", help="wait until the query returns no rows")
    p.add_argument("--interval", type=float, default=1.0, help="seconds between tries (default 1)")
    p.add_argument("--timeout", type=float, default=600.0, help="give up after this many seconds")
    p.add_argument("--env-file")
    args = p.parse_args(argv)
    settings = lq.load_settings(args.env_file)
    print(f"Started at {time.strftime('%H:%M:%S')}. '.' = no rows yet, 'r' = table not ready, 'n' = network error, '!' = done")
    try:
        waited = watch(settings, args.sql.strip().rstrip(";"), args.interval, args.timeout,
                       args.until_empty)
    except lq.QueryError as e:
        print(lq.format_error(e.status, {"code": e.code, "message": e.message}, e.via), file=sys.stderr)
        return 1
    except WatchTimeout as e:
        print(f"Watch timeout: {e}", file=sys.stderr)
        return 2
    print(f"{'Gone' if args.until_empty else 'Visible'} after {waited:.1f} s "
          f"(at {time.strftime('%H:%M:%S')})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
