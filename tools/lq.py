#!/usr/bin/env python3
"""Run one Lightning query over the RTCE REST API, or over MCP.

Usage:
  python3 tools/lq.py "SELECT * FROM lq_orders LIMIT 5"
  python3 tools/lq.py -f query.sql
  python3 tools/lq.py --json "SHOW TABLES"
  python3 tools/lq.py --max-rows 500 "SELECT ORDER_ID FROM lq_orders"

Settings come from tools/lq.env (copy tools/lq.env.example). Environment
variables with the same names override the file.

LQ_MODE selects the access path:
  auto (default)  REST first. If the org has no REST access (403 api_not_enabled), use MCP.
  rest            REST only.
  mcp             MCP only (queryData tool, on in every org). Needs LQ_ORG_ID.
Both paths return the same columns and rows.

Rows: REST has no row limit. Without --max-rows, lq.py asks for all rows. A large
or slow query runs asynchronously: the API answers 202, and lq.py polls the job
and downloads the result files. MCP returns at most 200 rows, with no warning.
The table output shows the first 100 rows. --json prints all rows.

Exit codes: 0 = rows (or none) returned, 2 = retryable error (table not
ready yet, rate limit), 1 = any other error.
Python 3.10+, standard library only.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

DEFAULT_ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lq.env")
DEFAULTS = {"LQ_MODE": "auto"}
OPTIONAL = ["LQ_ORG_ID", "LQ_MAX_DOWNLOAD_MB"]
REQUIRED = ["LQ_REGION", "LQ_ENV_ID", "LQ_CLUSTER_ID", "LQ_API_KEY", "LQ_API_SECRET"]
MCP_MAX_ROWS = 200  # MCP queryData returns at most 200 rows, also when you ask for more
SHOW_ROWS = 100     # the table output shows this many rows; --json prints all
API_PATH = "query/v1"
JOB_TIMEOUT = 300.0  # seconds to wait for an asynchronous query
JOB_INTERVAL = 1.0   # seconds between two job status requests
MAX_DOWNLOAD_MB = 100  # stop a result download above this size; 0 = no limit (LQ_MAX_DOWNLOAD_MB, --max-download-mb)
CHUNK = 1 << 20

RETRY = "Table not ready yet: retry in 30 s. Up to 10 minutes after ACTIVE is normal (12 for upsert)."
HINTS = {
    "api_not_enabled": "REST is not on for this org, or the Global key is from another org. Check the org of the key. Or set LQ_MODE=mcp in tools/lq.env: MCP works in every org.",
    "FE_CUSTOMER_NOT_FOUND": "No Lightning table on this cluster yet. Check LQ_ENV_ID and LQ_CLUSTER_ID. " + RETRY,
    "FE_BACKEND_UNAVAILABLE": RETRY,
    "FE_TABLE_NOT_AVAILABLE": RETRY,
    "DP_TABLE_NOT_AVAILABLE": RETRY,
    "DP_INVALID_TABLE": RETRY,
    "MT_UPSERT_NOT_SUPPORTED": "This org cannot use upsert tables. Use the append table with ORDER BY ... DESC LIMIT 1 (Lab 3, Look).",
    "FE_UNAUTHENTICATED": "Use a Global API key. Check the key and the secret.",
    "FE_AUTHORIZATION_FAILED": "You need DeveloperRead on the cluster and on Schema Registry (EnvironmentAdmin covers both).",
    "FE_SQL_VALIDATION_ERROR": "Check the SQL. Column names are upper case. A quoted name must be upper case too, for example \"ORDER_ID\".",
    "FE_RATE_LIMITED": "Rate limit. Wait a few seconds and retry.",
    "RATE_LIMIT_EXCEEDED": "Rate limit. Wait a few seconds and retry. Query less often, for example lq_watch.py --interval 5.",
    "query_timeout": "The query took too long. MCP stops after 20 s. lq.py waits 300 s for an asynchronous query. Add a LIMIT or a narrower WHERE clause.",
    "result_too_large": "Add a LIMIT or a narrower WHERE clause. To download all of it, use --max-download-mb 0. A large result needs several GB of memory.",
    "FE_TABLE_NOT_FOUND": "Check the table name with SHOW TABLES. If you turned on RTCE a moment ago, retry in 30 s.",
    "FE_SQL_SELECT_ONLY": "Lightning Queries runs only SELECT, SHOW TABLES and DESCRIBE. Check the spelling of SELECT.",
    "FE_SQL_MULTI_STATEMENT": "Send one statement per request.",
    "FE_STATEMENT_FAILED": "The asynchronous query failed. Add a LIMIT or a narrower WHERE clause: a result has a maximum size.",
    "QE_UNSUPPORTED_AGGREGATION": "Not supported in Lightning Queries: COUNT, SUM, GROUP BY, DISTINCT.",
    "QE_UNSUPPORTED_JOIN": "Not supported in Lightning Queries: JOIN. Join in Flink before RTCE, or in the client.",
    "QE_UNSUPPORTED_SUBQUERY": "Not supported in Lightning Queries: subqueries. Use two queries.",
    "QE_UNSUPPORTED_SET_OP": "Not supported in Lightning Queries: UNION, INTERSECT, EXCEPT. Use two queries.",
    "QE_UNSUPPORTED_WINDOW_FUNCTION": "Not supported in Lightning Queries: window functions such as ROW_NUMBER().",
    "MT_METADATA_UPDATE": "The Lightning table is still starting (seen on new upsert tables). " + RETRY,
    "unauthenticated": "Use a Global API key (not a cluster key) of this org. Check the key and the secret.",
    "resource_not_found": "Use a Global API key of this org. A Cloud API key does not work. Then check LQ_ENV_ID and LQ_CLUSTER_ID.",
}
RETRYABLE = {"FE_CUSTOMER_NOT_FOUND", "FE_BACKEND_UNAVAILABLE", "FE_TABLE_NOT_AVAILABLE",
             "DP_TABLE_NOT_AVAILABLE", "DP_INVALID_TABLE", "FE_RATE_LIMITED", "RATE_LIMIT_EXCEEDED", "query_timeout",
             "FE_TABLE_NOT_FOUND", "MT_METADATA_UPDATE"}
STATUS_HINTS = {401: "Use a Global API key (not a cluster key). Check the key and the secret.",
                404: "The Lightning Table is not available yet, for example because it is still being set up. Retry in 30 s. If it stays, check that the URL path ends in /query/v1.",
                429: HINTS["FE_RATE_LIMITED"],
                500: "Server error. Retry. If it repeats, check that the region supports Lightning Queries."}


class NetworkError(urllib.error.URLError):
    """No answer from a host. url is the address without the query string: a presigned URL holds a credential there."""
    def __init__(self, url: str, reason, timed_out: bool = False):
        super().__init__(reason)
        self.url = url.split("?", 1)[0]
        self.timed_out = timed_out


def _open(request, opener):
    """Open a request. A network error or a timeout becomes a NetworkError with the URL."""
    try:
        return opener(request, timeout=60)
    except urllib.error.HTTPError:
        raise
    except urllib.error.URLError as e:
        raise NetworkError(request.full_url, e.reason) from None
    except TimeoutError:
        raise NetworkError(request.full_url, "timeout", timed_out=True) from None


class QueryError(Exception):
    def __init__(self, status: int, code: str, message: str, via: str = "rest"):
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message
        self.via = via

    @property
    def table_missing(self) -> bool:
        """The table is not there (yet). A new table can answer like this for some seconds after ACTIVE."""
        if self.code == "FE_TABLE_NOT_FOUND" or self.status == 404:
            return True
        return (self.code == "FE_SQL_VALIDATION_ERROR" and "Object '" in self.message
                and "' not found" in self.message)

    @property
    def retryable(self) -> bool:
        return self.code in RETRYABLE or self.status == 429 or self.table_missing


def load_settings(path: str | None = None) -> dict[str, str]:
    settings = dict(DEFAULTS)
    path = path or DEFAULT_ENV_FILE
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    name, value = line.split("=", 1)
                    settings[name.strip()] = value.strip().strip('"').strip("'")
    for name in list(DEFAULTS) + OPTIONAL + REQUIRED:
        if os.environ.get(name):
            settings[name] = os.environ[name]
    missing = [n for n in REQUIRED if not settings.get(n)]
    if missing:
        raise SystemExit(f"Missing settings: {', '.join(missing)}. "
                         f"Put them in {path} (copy tools/lq.env.example) or set them as environment variables.")
    return settings


def auth_header(key: str, secret: str) -> str:
    token = base64.b64encode(f"{key.strip()}:{secret.strip()}".encode()).decode()
    return "Basic " + token


def endpoint(settings: dict) -> str:
    return f"https://sql.{settings['LQ_REGION']}.aws.confluent.cloud/{API_PATH}"


def mcp_endpoint(settings: dict) -> str:
    if not settings.get("LQ_ORG_ID"):
        raise SystemExit("MCP needs LQ_ORG_ID. Get it with: confluent organization describe -o json | jq -r .id")
    return (f"https://mcp.{settings['LQ_REGION']}.aws.confluent.cloud/mcp/v1/context-engine/"
            f"organizations/{settings['LQ_ORG_ID']}/environments/{settings['LQ_ENV_ID']}/"
            f"kafka-clusters/{settings['LQ_CLUSTER_ID']}")


def build_request(settings: dict, sql: str, max_rows: int | None = None) -> urllib.request.Request:
    """A query request. Without max_rows, the API returns all rows."""
    options = {"result_format": "JSON"}
    if max_rows is not None:
        if max_rows < 1:
            raise ValueError("max_rows must be 1 or more")
        options["max_result_rows"] = max_rows
    body = {"catalog_name": settings["LQ_ENV_ID"], "database_name": settings["LQ_CLUSTER_ID"],
            "query": sql, "options": options}
    return urllib.request.Request(
        endpoint(settings), data=json.dumps(body).encode(), method="POST",
        headers={"Authorization": auth_header(settings["LQ_API_KEY"], settings["LQ_API_SECRET"]),
                 "Content-Type": "application/json", "Accept": "application/json"})


def parse_rows(body: dict) -> tuple[list[str], list[list[str]]]:
    result = body.get("result", body)
    columns = [c.get("name", "") for c in result.get("schema", {}).get("columns", [])]
    rows = []
    for item in result.get("data") or []:
        rows.append(list(item["row"]) if isinstance(item, dict) else list(item))
    return columns, rows


def _error_fields(body) -> tuple[str, str]:
    """Return (code, detail) from any error body shape seen so far."""
    if not isinstance(body, dict):
        return "", str(body).strip()[:300]
    if body.get("errors"):
        e = body["errors"][0]
        return str(e.get("code", "")), str(e.get("detail") or e.get("title") or "")
    if isinstance(body.get("error"), dict):
        e = body["error"]
        return str(e.get("code", "")), str(e.get("message") or e.get("detail") or "")
    return str(body.get("code", "") or body.get("error_code", "")), str(
        body.get("message") or body.get("detail") or "")


def format_error(status: int, body, via: str = "rest") -> str:
    code, detail = _error_fields(body)
    hint = HINTS.get(code) or STATUS_HINTS.get(status, "")
    parts = [{"mcp": "MCP", "job": "async job"}.get(via, "HTTP") if not status else f"HTTP {status}"]
    if code:
        parts.append(code)
    if detail:
        parts.append(detail)
    line = " | ".join(parts)
    return f"{line}\n  hint: {hint}" if hint else line


_mode = {"switched": False}  # auto mode: remember the switch to MCP for this process


def reset_mode() -> None:
    _mode["switched"] = False


def run_query(settings: dict, sql: str, max_rows: int | None = None,
              opener=urllib.request.urlopen) -> tuple[float, list[str], list[list[str]]]:
    mode = (settings.get("LQ_MODE") or "auto").lower()
    if mode == "mcp" or (mode == "auto" and _mode["switched"]):
        return run_query_mcp(settings, sql, max_rows, opener)
    try:
        return run_query_rest(settings, sql, max_rows, opener)
    except QueryError as e:
        if mode != "auto" or e.code != "api_not_enabled":
            raise
        _mode["switched"] = True
        print("note: this org has no Lightning Queries REST access (403 api_not_enabled). Using MCP. "
              "Set LQ_MODE=mcp in tools/lq.env to skip the REST try.", file=sys.stderr)
        return run_query_mcp(settings, sql, max_rows, opener)


def build_mcp_request(settings: dict, sql: str, max_rows: int | None = None) -> urllib.request.Request:
    if max_rows is not None and max_rows < 1:
        raise ValueError("max_rows must be 1 or more")
    max_rows = min(max_rows or MCP_MAX_ROWS, MCP_MAX_ROWS)
    # queryData needs a topic_name. The server runs the SQL as it is, so the first table name is enough.
    m = re.search(r"\b(?:FROM|DESCRIBE)\s+[`\"]?([A-Za-z0-9_.-]+)", sql, re.IGNORECASE)
    body = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "queryData", "arguments": {
                "topic_name": m.group(1) if m else "-", "query": sql, "max_result_rows": max_rows}}}
    return urllib.request.Request(
        mcp_endpoint(settings), data=json.dumps(body).encode(), method="POST",
        headers={"Authorization": auth_header(settings["LQ_API_KEY"], settings["LQ_API_SECRET"]),
                 "Content-Type": "application/json", "Accept": "application/json, text/event-stream"})


def parse_mcp(raw: str) -> tuple[list[str], list[list[str]]]:
    """Read the JSON-RPC answer (plain JSON or server-sent events) of a queryData call."""
    data = [line[5:].strip() for line in raw.splitlines() if line.startswith("data:")]
    rpc = json.loads(data[-1] if data else raw)
    if "error" in rpc:
        e = rpc["error"]
        raise QueryError(0, str(e.get("code", "")), str(e.get("message", "")), via="mcp")
    inner = json.loads(rpc["result"]["content"][0]["text"])
    if inner.get("status") != "success":
        e = inner.get("error") or {}
        raise QueryError(0, str(e.get("code", "")), str(e.get("message", "")), via="mcp")
    return parse_rows({"result": inner.get("rows") or {}})


def run_query_mcp(settings: dict, sql: str, max_rows: int | None = None,
                  opener=urllib.request.urlopen) -> tuple[float, list[str], list[list[str]]]:
    request = build_mcp_request(settings, sql, max_rows)
    start = time.monotonic()
    try:
        with _open(request, opener) as response:
            raw = response.read().decode(errors="replace")
    except urllib.error.HTTPError as e:
        text = e.read().decode(errors="replace")
        try:
            body = json.loads(text)
        except ValueError:
            body = text
        code, detail = _error_fields(body)
        raise QueryError(e.code, code, detail or format_error(e.code, body), via="mcp") from None
    elapsed = time.monotonic() - start
    columns, rows = parse_mcp(raw)
    return elapsed, columns, rows


def _send(request, opener) -> tuple[int, dict, bytes]:
    """Send one request. Return (status, headers, body). An HTTP error becomes a QueryError."""
    try:
        with _open(request, opener) as response:
            return getattr(response, "status", 200), dict(getattr(response, "headers", {}) or {}), response.read()
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        try:
            body = json.loads(raw)
        except ValueError:
            body = raw
        code, detail = _error_fields(body)
        raise QueryError(e.code, code, detail or format_error(e.code, body)) from None


def _header(headers: dict, name: str) -> str:
    for k, v in headers.items():
        if k.lower() == name.lower():
            return v
    return ""


def wait_for_job(settings: dict, location: str, opener=urllib.request.urlopen) -> list[str]:
    """Poll the job status until SUCCEEDED or FAILED. Return the result URLs."""
    url = f"https://sql.{settings['LQ_REGION']}.aws.confluent.cloud{location}"
    auth = auth_header(settings["LQ_API_KEY"], settings["LQ_API_SECRET"])
    start = time.monotonic()
    while True:
        request = urllib.request.Request(url, method="GET",
                                         headers={"Authorization": auth, "Accept": "application/json"})
        _, _, raw = _send(request, opener)
        body = json.loads(raw)
        state = body.get("result", {}).get("state")
        if state == "SUCCEEDED":
            return body["result"].get("result_urls") or []
        if state == "FAILED":
            code, detail = _error_fields(body)
            raise QueryError(0, code or "FE_STATEMENT_FAILED", detail, via="job")
        if time.monotonic() - start >= JOB_TIMEOUT:
            raise QueryError(0, "query_timeout", f"the asynchronous query was still {state} after {JOB_TIMEOUT:.0f} s",
                             via="job")
        time.sleep(JOB_INTERVAL)


def _too_large(limit: int) -> QueryError:
    return QueryError(0, "result_too_large",
                      f"the result is larger than {limit // (1 << 20)} MB, so lq.py stopped the download", via="job")


def _read_limited(request, opener, remaining: int | None) -> bytes:
    """Read one result file. Stop as soon as it is larger than remaining bytes (None = no limit)."""
    try:
        with _open(request, opener) as response:
            size = _header(dict(getattr(response, "headers", {}) or {}), "Content-Length")
            if remaining is not None and size.isdigit() and int(size) > remaining:
                raise _too_large(remaining)
            parts, total = [], 0
            while chunk := response.read(CHUNK):
                total += len(chunk)
                if remaining is not None and total > remaining:
                    raise _too_large(remaining)
                parts.append(chunk)
            return b"".join(parts)
    except urllib.error.HTTPError as e:
        raise QueryError(e.code, "", format_error(e.code, e.read().decode(errors="replace")), via="job") from None


def download_results(urls: list[str], opener=urllib.request.urlopen,
                     max_mb: int = MAX_DOWNLOAD_MB) -> tuple[list[str], list[list[str]]]:
    """Download the result files of a job. A presigned URL must have no Authorization header.
    Stop above max_mb in total (0 = no limit), before the result fills the memory."""
    columns, rows = [], []
    budget = max_mb * (1 << 20) if max_mb else None
    for url in urls:
        raw = _read_limited(urllib.request.Request(url, method="GET"), opener, budget)
        if budget is not None:
            budget -= len(raw)
        cols, part = parse_rows(json.loads(raw))
        columns = columns or cols
        rows += part
    return columns, rows


def run_query_rest(settings: dict, sql: str, max_rows: int | None = None,
                   opener=urllib.request.urlopen) -> tuple[float, list[str], list[list[str]]]:
    request = build_request(settings, sql, max_rows)
    start = time.monotonic()
    status, headers, raw = _send(request, opener)
    if status == 202:
        body = json.loads(raw)
        statement = body.get("result", {}).get("statement_id", "?")
        print(f"note: the server runs this query asynchronously ({statement}). Waiting for the result.",
              file=sys.stderr)
        urls = wait_for_job(settings, _header(headers, "Location"), opener)
        columns, rows = download_results(urls, opener, int(settings.get("LQ_MAX_DOWNLOAD_MB") or MAX_DOWNLOAD_MB))
    else:
        columns, rows = parse_rows(json.loads(raw))
    elapsed = time.monotonic() - start
    return elapsed, columns, rows


def format_table(columns: list[str], rows: list[list[str]], width: int = 40) -> str:
    def cell(v) -> str:
        s = "NULL" if v is None else str(v)
        return s if len(s) <= width else s[: width - 1] + "…"

    cells = [[cell(v) for v in r] for r in rows]
    widths = [len(c) for c in columns]
    for r in cells:
        for i, v in enumerate(r):
            if i < len(widths):
                widths[i] = max(widths[i], len(v))
            else:
                widths.append(len(v))
    lines = ["  ".join(c.ljust(widths[i]) for i, c in enumerate(columns)),
             "  ".join("-" * w for w in widths)]
    lines += ["  ".join(v.ljust(widths[i]) for i, v in enumerate(r)) for r in cells]
    return "\n".join(lines)


def _positive(text: str) -> int:
    try:
        n = int(text)
    except ValueError:
        n = 0
    if n < 1:
        raise argparse.ArgumentTypeError(f"must be a whole number of 1 or more, not {text!r}. Leave it out for all rows")
    return n


def _zero_or_more(text: str) -> int:
    try:
        n = int(text)
    except ValueError:
        n = -1
    if n < 0:
        raise argparse.ArgumentTypeError(f"must be a whole number of 0 or more, not '{text}'. 0 means no limit.")
    return n


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        # argparse exits with 2, but 2 means "retry later" here. A usage error is not retryable.
        self.print_usage(sys.stderr)
        self.exit(1, f"{self.prog}: error: {message}\n")


def main(argv: list[str], opener=urllib.request.urlopen, settings: dict | None = None) -> int:
    p = _Parser(prog="lq.py", description="Run one Lightning query over REST or MCP.")
    p.add_argument("sql", nargs="?", help="the SQL statement")
    p.add_argument("-f", "--file", help="read the SQL from a file")
    p.add_argument("--max-rows", type=_positive, help="at most this many rows (default: all rows; MCP: at most 200)")
    p.add_argument("--json", action="store_true", help="print columns and rows as JSON")
    p.add_argument("--max-download-mb", type=_zero_or_more, metavar="MB",
                   help=f"stop a large asynchronous result above this size (default {MAX_DOWNLOAD_MB}; 0 = no limit)")
    p.add_argument("--env-file", help="settings file (default tools/lq.env)")
    args = p.parse_args(argv)
    if args.file:
        with open(args.file, encoding="utf-8") as f:
            sql = f.read().strip().rstrip(";")
    elif args.sql:
        sql = args.sql.strip().rstrip(";")
    else:
        p.error("give the SQL as an argument or with -f")
    settings = settings or load_settings(args.env_file)
    if args.max_download_mb is not None:
        settings = dict(settings, LQ_MAX_DOWNLOAD_MB=str(args.max_download_mb))
    try:
        elapsed, columns, rows = run_query(settings, sql, args.max_rows, opener=opener)
    except QueryError as e:
        print(format_error(e.status, {"code": e.code, "message": e.message}, e.via), file=sys.stderr)
        return 2 if e.retryable else 1
    except NetworkError as e:
        if e.timed_out:
            print(f"No answer from {e.url} within 60 s. Retry.", file=sys.stderr)
            return 2
        print(f"Cannot reach {e.url}: {e.reason}. Check LQ_REGION and the network.", file=sys.stderr)
        return 1
    except json.JSONDecodeError as e:
        print(f"The answer is not JSON ({e}). Check LQ_REGION and the network.", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps({"columns": columns, "rows": rows, "elapsed_s": round(elapsed, 3)}, indent=2))
    else:
        if columns or rows:
            print(format_table(columns, rows[:SHOW_ROWS]))
        if len(rows) > SHOW_ROWS:
            print(f"... {len(rows) - SHOW_ROWS} more rows. Use --json to print all rows, or add a LIMIT.")
        print(f"{len(rows)} rows in {elapsed:.2f} s")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
