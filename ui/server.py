#!/usr/bin/env python3
"""The lab UI server. Python standard library only.

Usage:
  python3 ui/server.py [--port 8765]

It listens on 127.0.0.1 only. It reads tools/lq.env, as all other tools do.
Lightning queries go through tools/lq.py. Tableflow queries go through
tableflow/tableflow_query.sh. A write goes through tools/marker_json.sh and
`confluent kafka topic produce`. See ui/README.md.
"""
from __future__ import annotations

import argparse
import csv
import hmac
import io
import json
import os
import secrets
import subprocess
import sys
import threading
import time
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "tools"))
sys.path.insert(0, HERE)
import lq  # noqa: E402
import runs  # noqa: E402

MARKER_JSON = os.path.join(ROOT, "tools", "marker_json.sh")
TABLEFLOW_QUERY = os.path.join(ROOT, "tableflow", "tableflow_query.sh")
INDEX = os.path.join(HERE, "static", "index.html")
RUNS_FILE = os.path.join(HERE, "runs.jsonl")
TOKEN_HEADER = "X-Lab-Token"
MAX_BODY = 64 * 1024
LAB1_MAX_ROWS = 500
TABLEFLOW_LIMIT = 120.0  # s for one DuckDB call


# ---------------------------------------------------------------- real runners

def make_query(settings: dict):
    def query(sql: str, max_rows: int | None = None):
        return lq.run_query(settings, sql, max_rows)
    return query


def lq_env(settings: dict) -> dict:
    """The environment of a child process: this process, plus the LQ_ settings."""
    env = dict(os.environ)
    env.update({k: v for k, v in settings.items() if k.startswith("LQ_")})
    env["DUCKDB_AGENT_MODE"] = "0"
    return env


def make_tableflow(settings: dict, secrets_: list[str]):
    def tableflow(sql: str):
        if not runs.is_fixed_tableflow_sql(sql):
            raise runs.ForbiddenQuery("Tableflow gets only fixed queries")
        try:
            r = subprocess.run([TABLEFLOW_QUERY, sql], capture_output=True, text=True,
                               timeout=TABLEFLOW_LIMIT, env=lq_env(settings), cwd=ROOT)
        except subprocess.TimeoutExpired:
            raise runs.TableflowError(f"DuckDB did not answer within {TABLEFLOW_LIMIT:.0f} s") from None
        except OSError:
            raise runs.TableflowError("cannot start tableflow/tableflow_query.sh") from None
        if r.returncode != 0:
            lines = runs.redact_lines(r.stderr or r.stdout, secrets_)
            err = [ln for ln in lines if "error" in ln.lower() or "exception" in ln.lower()] or lines
            raise runs.TableflowError(f"DuckDB exit {r.returncode}: {err[0] if err else 'no error text'}")
        out = [ln for ln in r.stdout.splitlines() if ln.strip()]
        table = list(csv.reader(io.StringIO("\n".join(out))))
        if not table:
            return [], []
        return table[0], table[1:]
    return tableflow


def make_producer(settings: dict, secrets_: list[str]):
    """Write one order event to lq_orders, as in Lab 2 step 7. Argument lists only, never a shell."""
    def producer(run_id: str, customer: int, status: str, price: float) -> runs.ProduceResult:
        need = [n for n in ("LQ_KAFKA_KEY", "LQ_KAFKA_SECRET", "LQ_SCHEMA_ID") if not settings.get(n)]
        if need:
            return runs.ProduceResult(1, [f"missing settings: {', '.join(need)} (Lab 2, steps 2 and 3)"])
        runs.check_run_id(run_id)
        price_text = f"{price:g}"
        try:
            ev = subprocess.run([MARKER_JSON, run_id, str(int(customer)), status, price_text],
                                capture_output=True, text=True, timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            return runs.ProduceResult(1, ["cannot run tools/marker_json.sh"])
        if ev.returncode != 0:
            return runs.ProduceResult(ev.returncode, runs.redact_lines(ev.stderr, secrets_))
        cmd = ["confluent", "kafka", "topic", "produce", "lq_orders",
               "--cluster", settings["LQ_CLUSTER_ID"], "--environment", settings["LQ_ENV_ID"],
               "--api-key", settings["LQ_KAFKA_KEY"], "--api-secret", settings["LQ_KAFKA_SECRET"],
               "--value-format", "avro", "--schema", settings["LQ_SCHEMA_ID"]]
        try:
            p = subprocess.run(cmd, input=ev.stdout, capture_output=True, text=True,
                               timeout=runs.PRODUCE_LIMIT)
        except subprocess.TimeoutExpired:
            # Do not format the exception: its text holds the command line with the secret.
            return runs.ProduceResult(-1, [], timed_out=True)
        except OSError:
            return runs.ProduceResult(127, ["cannot start the Confluent CLI (confluent)"])
        return runs.ProduceResult(p.returncode, runs.redact_lines(p.stderr, secrets_))
    return producer


# ---------------------------------------------------------------- doctor

class Doctor:
    """Runs tools/lq_doctor.py checks in a thread, for the status bar."""

    def __init__(self, settings: dict, secrets_: list[str], run_checks=None):
        self.settings, self.secrets = settings, secrets_
        self.run_checks = run_checks
        self.lock = threading.Lock()
        self.state = {"running": False, "at": "", "checks": []}

    def start(self) -> bool:
        with self.lock:
            if self.state["running"]:
                return False
            self.state["running"] = True
        threading.Thread(target=self._run, daemon=True).start()
        return True

    def _run(self) -> None:
        checks = []
        try:
            run_checks = self.run_checks
            if run_checks is None:
                import lq_doctor
                run_checks = lq_doctor.run_checks
            for c in run_checks(self.settings):
                checks.append({"name": c.name, "status": c.status, "detail": c.detail, "fix": c.fix})
        except Exception as e:  # the text can hold a command line: show only the type
            checks = [{"name": "doctor", "status": "FAIL", "detail": f"the doctor stopped ({type(e).__name__})",
                       "fix": "Run python3 tools/lq_doctor.py in a terminal."}]
        with self.lock:
            self.state = runs.scrub({"running": False, "at": runs.utc_now(), "checks": checks}, self.secrets)

    def snapshot(self) -> dict:
        with self.lock:
            return dict(self.state)


# ---------------------------------------------------------------- HTTP

class App:
    def __init__(self, settings: dict, manager: runs.Manager, query, doctor: Doctor, port: int,
                 token: str | None = None):
        self.settings, self.manager, self.query, self.doctor = settings, manager, query, doctor
        self.port = port
        self.token = token or secrets.token_urlsafe(32)
        self.secrets = runs.secret_values(settings)

    def allowed_hosts(self) -> set[str]:
        return {f"127.0.0.1:{self.port}", f"localhost:{self.port}"}

    def index_html(self) -> bytes:
        with open(INDEX, encoding="utf-8") as f:
            return f.read().replace("__LAB_TOKEN__", self.token).encode()

    def lab1(self, body: dict) -> tuple[int, dict]:
        sql = str(body.get("sql") or "").strip().rstrip(";").strip()
        if not runs.lab1_sql_ok(sql):
            return 400, {"error": "Send one SELECT, SHOW or DESCRIBE statement."}
        try:
            elapsed, columns, rows = self.query(sql, LAB1_MAX_ROWS)
        except lq.QueryError as e:
            return 200, {"error_code": e.code or f"HTTP {e.status}", "error": e.message[:500],
                         "hint": lq.HINTS.get(e.code) or lq.STATUS_HINTS.get(e.status, ""),
                         "retryable": e.retryable}
        except (urllib.error.URLError, TimeoutError, OSError):
            return 200, {"error_code": "network", "error": "No answer from Lightning. Check the network and LQ_REGION.",
                         "hint": "", "retryable": True}
        except ValueError:
            return 200, {"error_code": "bad_answer", "error": "The answer is not JSON.", "hint": "", "retryable": True}
        return 200, {"columns": columns, "rows": rows, "elapsed": round(elapsed, 3),
                     "max_rows": LAB1_MAX_ROWS, "mode": self.settings.get("LQ_MODE", "auto")}

    def state(self) -> dict:
        d = self.manager.snapshot()
        d["doctor"] = self.doctor.snapshot()
        d["name"] = self.settings.get("LQ_NAME", "")
        return d


def make_handler(app: App):
    class Handler(BaseHTTPRequestHandler):
        server_version = "lq-lab-ui"
        sys_version = ""

        def log_message(self, fmt, *args):  # only method, path without query, status
            pass

        def _log(self, status: int) -> None:
            path = self.path.split("?", 1)[0]
            sys.stderr.write(f"{time.strftime('%H:%M:%S')} {self.command} {path} {status}\n")

        def _send(self, status: int, body: bytes, ctype: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("X-Frame-Options", "DENY")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("Content-Security-Policy",
                             "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
                             "connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; "
                             "frame-ancestors 'none'")
            self.end_headers()
            self.wfile.write(body)
            if self.path.startswith("/api/") and self.command == "GET" and status == 200:
                return  # the page polls once per second: do not log each poll
            self._log(status)

        def _json(self, status: int, obj) -> None:
            self._send(status, json.dumps(runs.scrub(obj, app.secrets)).encode(), "application/json")

        def _host_ok(self) -> bool:
            return self.headers.get("Host", "") in app.allowed_hosts()

        def _token_ok(self) -> bool:
            return hmac.compare_digest(self.headers.get(TOKEN_HEADER, ""), app.token)

        def _guard(self, api: bool) -> bool:
            if not self._host_ok():
                self._json(403, {"error": "Wrong Host header. Open http://127.0.0.1:<port>/."})
                return False
            if api and not self._token_ok():
                self._json(403, {"error": "Missing or wrong token. Load the page again."})
                return False
            return True

        def do_GET(self):
            path = self.path.split("?", 1)[0]
            api = path.startswith("/api/")
            if not self._guard(api):
                return
            try:
                if path in ("/", "/index.html"):
                    self._send(200, app.index_html(), "text/html; charset=utf-8")
                elif path == "/api/state":
                    self._json(200, app.state())
                else:
                    self._json(404, {"error": "not found"})
            except Exception as e:  # never echo the text
                sys.stderr.write(f"internal error {type(e).__name__} on {path}\n")
                self._json(500, {"error": f"internal error ({type(e).__name__})"})

        def do_POST(self):
            path = self.path.split("?", 1)[0]
            if not self._guard(True):
                return
            n = int(self.headers.get("Content-Length") or 0)
            if n > MAX_BODY:
                self._json(413, {"error": "body too large"})
                return
            try:
                body = json.loads(self.rfile.read(n) or b"{}") if n else {}
                if not isinstance(body, dict):
                    raise ValueError
            except ValueError:
                self._json(400, {"error": "The body must be a JSON object."})
                return
            try:
                if path == "/api/query":
                    self._json(*app.lab1(body))
                elif path == "/api/run":
                    lab = body.get("lab")
                    if lab not in (2, 3):
                        self._json(400, {"error": "lab must be 2 or 3"})
                        return
                    try:
                        run = app.manager.start(lab)
                    except runs.Busy as e:
                        self._json(409, {"error": str(e)})
                        return
                    self._json(202, {"id": run.id, "lab": run.lab, "price": run.price})
                elif path == "/api/doctor":
                    started = app.doctor.start()
                    self._json(202, {"started": started})
                else:
                    self._json(404, {"error": "not found"})
            except Exception as e:  # never echo the text: it can hold a command line
                sys.stderr.write(f"internal error {type(e).__name__} on {path}\n")
                self._json(500, {"error": f"internal error ({type(e).__name__})"})

        def do_PUT(self):
            self._json(405, {"error": "method not allowed"})

        do_DELETE = do_PATCH = do_PUT

    return Handler


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="The lab UI server. Listens on 127.0.0.1 only.")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--env-file", help="settings file (default tools/lq.env)")
    p.add_argument("--no-doctor", action="store_true", help="do not run the doctor at the start")
    args = p.parse_args(argv)
    settings = lq.load_settings(args.env_file)
    # lq.load_settings takes only some names from the environment. Take the others too, as lq_doctor.py does.
    for var in ("LQ_NAME", "LQ_POOL_ID", "LQ_KAFKA_KEY", "LQ_KAFKA_SECRET", "LQ_SCHEMA_ID"):
        if os.environ.get(var):
            settings[var] = os.environ[var]
    secret_list = runs.secret_values(settings)
    query = make_query(settings)
    manager = runs.Manager(runs.Store(RUNS_FILE, secret_list), lambda sql: query(sql),
                           make_tableflow(settings, secret_list), make_producer(settings, secret_list))
    doctor = Doctor(settings, secret_list)
    app = App(settings, manager, query, doctor, args.port)
    httpd = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(app))
    httpd.daemon_threads = True
    if not args.no_doctor:
        doctor.start()
    print(f"Lab UI on http://127.0.0.1:{args.port}/  (Ctrl-C stops it)", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        manager.stop.set()
        httpd.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
