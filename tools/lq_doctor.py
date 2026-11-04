#!/usr/bin/env python3
"""Check your lab setup step by step, and print the fix for each problem.

Usage:
  set -a; . tools/lq.env; set +a
  python3 tools/lq_doctor.py

It only reads. It never creates, changes or deletes anything.
Each line is OK, WARN (works, but read the note), FAIL (fix this first) or SKIP.
Exit code 0 = no FAIL. Needs a CLI login. Python 3.10+, standard library only.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lq  # noqa: E402

TABLES = ("lq_orders", "lq_orders_latest")
# LQ_NAME: 2 to 12 lowercase letters or digits, first a letter. It goes into every resource name.
NAME_RE = re.compile(r"^[a-z][a-z0-9]{1,11}$")
DEFAULT_NAME = "changeme"  # the value in tools/lq.env.example


def feeds(name: str) -> tuple[str, str]:
    """The names of the two feed statements of this person."""
    return f"lq-{name}-feed-orders", f"lq-{name}-feed-orders-latest"


@dataclass
class Check:
    name: str
    status: str  # OK, WARN, FAIL, SKIP
    detail: str
    fix: str = ""


def cli(args: list[str]) -> tuple[int, object, str]:
    """Run the Confluent CLI with -o json. Return (exit code, parsed JSON or None, error text)."""
    r = subprocess.run(["confluent", *args, "-o", "json"], capture_output=True, text=True)
    try:
        data = json.loads(r.stdout) if r.stdout.strip() else None
    except ValueError:
        data = None
    err = (r.stderr or r.stdout or "").strip().splitlines()
    return r.returncode, data, (err[0] if err else "")


def schema_id_of(env: str) -> str:
    """The value schema ID of lq_orders. `schema describe` has no -o json, so read the text."""
    r = subprocess.run(["confluent", "schema-registry", "schema", "describe", "--subject", "lq_orders-value",
                        "--version", "latest", "--environment", env], capture_output=True, text=True)
    for line in r.stdout.splitlines():
        if line.startswith("Schema ID:"):
            return line.split(":", 1)[1].strip()
    return ""


def kafka_rest_status(url: str, key: str, secret: str, opener=urllib.request.urlopen) -> int:
    req = urllib.request.Request(url, headers={"Authorization": lq.auth_header(key, secret)})
    try:
        with opener(req, timeout=30) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


def check_query(s: dict, mode: str, run=lq.run_query) -> Check:
    name = f"query over {mode.upper()}"
    try:
        _, _, rows = run(dict(s, LQ_MODE=mode), "SHOW TABLES")
    except lq.QueryError as e:
        if mode == "rest" and e.code == "api_not_enabled":
            return Check(name, "WARN", "this org has no REST access (Early Access)",
                         "Nothing to do with LQ_MODE=auto or mcp: lq.py then uses MCP.")
        if e.status == 401:
            return Check(name, "FAIL", f"401 {e.code}", "Check LQ_API_KEY and LQ_API_SECRET. Use a Global key of this org.")
        if e.retryable:
            return Check(name, "WARN", f"{e.code}: not ready yet", "Normal before RTCE is on (Lab 1, step 11). After that, wait 1 minute, then check again.")
        return Check(name, "FAIL", f"{e.code}: {e.message}"[:120], lq.HINTS.get(e.code, ""))
    except (urllib.error.URLError, TimeoutError, ValueError) as e:
        return Check(name, "FAIL", f"network: {e}"[:120], "Check the network and LQ_REGION.")
    return Check(name, "OK", f"SHOW TABLES returned {len(rows)} tables")


def run_checks(s: dict, cli=cli, run=lq.run_query, rest_status=kafka_rest_status,
               schema_id=schema_id_of) -> list[Check]:
    out: list[Check] = []
    env, lkc, pool, region = s.get("LQ_ENV_ID"), s.get("LQ_CLUSTER_ID"), s.get("LQ_POOL_ID"), s["LQ_REGION"]

    rc, org, err = cli(["organization", "describe"])
    if rc != 0:
        return out + [Check("CLI login", "FAIL", err, "Run: confluent login --save")]
    org_id = (org or {}).get("id", "")
    if not s.get("LQ_ORG_ID"):
        out.append(Check("CLI login", "FAIL", f"logged in to {org_id}, but LQ_ORG_ID is not set",
                         "Lab 1, step 2: add LQ_ORG_ID to tools/lq.env."))
    elif org_id != s["LQ_ORG_ID"]:
        out.append(Check("CLI login", "FAIL", f"CLI org {org_id} is not LQ_ORG_ID {s['LQ_ORG_ID']}",
                         f"Run: confluent organization use {s['LQ_ORG_ID']}"))
    else:
        out.append(Check("CLI login", "OK", f"org {(org or {}).get('name', '')} ({org_id})"))

    name = s.get("LQ_NAME", "")
    name_ok = bool(NAME_RE.match(name))
    if not name_ok:
        out.append(Check("name", "FAIL", f"LQ_NAME is {name!r}",
                         "In tools/lq.env, set LQ_NAME to 2 to 12 lowercase letters or digits, first a letter."))
    elif name == DEFAULT_NAME:
        out.append(Check("name", "WARN", f"LQ_NAME is still {DEFAULT_NAME}",
                         "In tools/lq.env, change changeme to your initials (Lab 1, step 2). "
                         "Two people with the same name in one environment break each other's statements."))
    else:
        out.append(Check("name", "OK", f"LQ_NAME={name}, statements lq-{name}-..."))

    rc, regions, err = cli(["rtce", "region", "list"])
    names = [r.get("region") for r in regions or []]
    if rc == 0 and region not in names:
        out.append(Check("region", "FAIL", f"{region} has no RTCE", f"Use one of: {', '.join(names)}"))
    elif rc == 0:
        out.append(Check("region", "OK", f"{region} has RTCE"))

    rc, _, err = cli(["environment", "describe", env or "-"])
    out.append(Check("environment", "OK", env) if rc == 0 else
               Check("environment", "FAIL", f"{env}: {err}", "Check LQ_ENV_ID (Lab 1, step 4)."))
    if rc != 0:
        return out

    rc, c, err = cli(["kafka", "cluster", "describe", lkc or "-", "--environment", env])
    if rc != 0:
        return out + [Check("cluster", "FAIL", f"{lkc}: {err}", "Check LQ_CLUSTER_ID (Lab 1, step 5).")]
    c = c or {}
    if c.get("region") != region:
        out.append(Check("cluster", "FAIL", f"{lkc} is in {c.get('region')}, LQ_REGION is {region}",
                         "Set LQ_REGION to the region of the cluster."))
    elif c.get("status") != "UP":
        out.append(Check("cluster", "WARN", f"{lkc} status {c.get('status')}", "Wait until the status is UP."))
    else:
        out.append(Check("cluster", "OK", f"{lkc} {c.get('type')} UP in {region}"))

    rc, p, err = cli(["flink", "compute-pool", "describe", pool or "-", "--environment", env])
    out.append(Check("Flink pool", "OK", f"{pool} {(p or {}).get('status')}") if rc == 0 else
               Check("Flink pool", "FAIL", f"{pool}: {err}", "Check LQ_POOL_ID (Lab 1, step 6)."))

    out.append(check_query(s, "rest", run))
    out.append(check_query(s, "mcp", run))

    rc, topics, err = cli(["rtce", "rtce-topic", "list", "--cloud", "aws", "--region", region,
                           "--environment", env, "--cluster", lkc])
    phases = {t.get("topic_name"): t.get("phase") for t in topics or []}
    for t in TABLES:
        ph = phases.get(t)
        if ph == "ACTIVE":
            out.append(Check(f"RTCE {t}", "OK", "ACTIVE"))
        elif ph:
            out.append(Check(f"RTCE {t}", "WARN", ph, "Wait. Up to 10 minutes is normal."))
        else:
            out.append(Check(f"RTCE {t}", "SKIP", "RTCE is off for this topic",
                             "Lab 1 step 11 (lq_orders) or Lab 3 step 4 (lq_orders_latest)."))

    rc, stmts, err = cli(["flink", "statement", "list", "--environment", env, "--cloud", "aws", "--region", region])
    status = {st.get("name"): st.get("status") for st in stmts or []}
    for f in (feeds(name) if name_ok else ()):
        st = status.get(f)
        if st == "RUNNING":
            out.append(Check(f"feed {f}", "WARN", "RUNNING", "A running feed delays every new event up to 60 s. Lab 2 step 6 stops it."))
        elif st in ("FAILED", "FAILING", "DEGRADED"):
            out.append(Check(f"feed {f}", "FAIL", st,
                             f"Read the reason: confluent flink statement describe {f} --environment {env} "
                             f"--cloud aws --region {region}"))
        elif st:
            out.append(Check(f"feed {f}", "OK", st))
        else:
            out.append(Check(f"feed {f}", "SKIP", "no statement with this name"))

    for t, step in zip(TABLES, ("Lab 1 step 12", "Lab 3 step 5")):
        rc, tf, err = cli(["tableflow", "topic", "describe", t, "--cluster", lkc, "--environment", env])
        ph = (tf or {}).get("phase") if rc == 0 else None
        out.append(Check(f"Tableflow {t}", "OK" if ph == "RUNNING" else ("WARN" if ph else "SKIP"),
                         ph or "Tableflow is off for this topic",
                         "" if ph == "RUNNING" else f"{step}, then wait about 2 to 8 minutes."))

    if s.get("LQ_KAFKA_KEY") and s.get("LQ_KAFKA_SECRET"):
        code = rest_status(f"{c.get('rest_endpoint', '').rstrip('/')}/kafka/v3/clusters/{lkc}/topics",
                           s["LQ_KAFKA_KEY"], s["LQ_KAFKA_SECRET"])
        out.append(Check("Kafka key", "OK", "works on the cluster") if code == 200 else
                   Check("Kafka key", "FAIL", f"HTTP {code}",
                         "Use a Kafka key of this cluster, not the Global key. A new key can need 1 minute."))
    else:
        out.append(Check("Kafka key", "SKIP", "LQ_KAFKA_KEY is not set", "Lab 2, step 2."))

    if s.get("LQ_SCHEMA_ID"):
        sid = schema_id(env)
        if not sid:
            out.append(Check("schema ID", "WARN", "cannot read the schema of lq_orders-value",
                             "Lab 1 step 9 creates it. Check: confluent schema-registry subject list"))
        elif sid != s["LQ_SCHEMA_ID"]:
            out.append(Check("schema ID", "FAIL", f"LQ_SCHEMA_ID {s['LQ_SCHEMA_ID']}, topic has {sid}",
                             f"Set LQ_SCHEMA_ID={sid}."))
        else:
            out.append(Check("schema ID", "OK", sid))
    else:
        out.append(Check("schema ID", "SKIP", "LQ_SCHEMA_ID is not set", "Lab 2, step 3."))
    return out


def report(checks: list[Check]) -> str:
    lines = []
    for c in checks:
        lines.append(f"{c.status:<4}  {c.name:<32} {c.detail}")
        if c.fix and c.status in ("FAIL", "WARN"):
            lines.append(f"      fix: {c.fix}")
    return "\n".join(lines)


def main() -> int:
    s = lq.load_settings()
    for var in ("LQ_NAME", "LQ_POOL_ID", "LQ_KAFKA_KEY", "LQ_KAFKA_SECRET", "LQ_SCHEMA_ID"):
        if os.environ.get(var):
            s[var] = os.environ[var]
    checks = run_checks(s)
    print(report(checks))
    return 1 if any(c.status == "FAIL" for c in checks) else 0


if __name__ == "__main__":
    sys.exit(main())
