import os, json, time, requests
from collections import Counter
from datetime import datetime, timezone
from jinja2 import Template

BASE = "https://vx961.us1.dbt.com/api/v2"
TOKEN = os.environ["DBT_CLOUD_TOKEN"]
ACCOUNT = os.environ["DBT_CLOUD_ACCOUNT_ID"]

try:
    JOB_MAP = json.loads(os.environ.get("DBT_JOB_MAP", "{}") or "{}")
except json.JSONDecodeError:
    JOB_MAP = {}

if JOB_MAP:
    JOB_IDS = list(JOB_MAP.keys())
else:
    JOB_IDS = [j.strip() for j in os.environ["DBT_CLOUD_JOB_IDS"].split(",")]

S = requests.Session()
S.headers.update({"Authorization": f"Token {TOKEN}"})

def latest_run(job_id):
    r = S.get(f"{BASE}/accounts/{ACCOUNT}/runs/",
              params={"job_definition_id": job_id, "order_by": "-finished_at", "limit": 1},
              timeout=30)
    r.raise_for_status()
    data = r.json().get("data", [])
    return data[0] if data else None

def get_artifact(run_id, name):
    r = S.get(f"{BASE}/accounts/{ACCOUNT}/runs/{run_id}/artifacts/{name}", timeout=30)
    return r.json() if r.status_code == 200 else None

def parse_status(run):
    status = run.get("status")                  # 10 success, 20 error
    in_progress = run.get("is_complete") is False

    run_results = get_artifact(run["id"], "run_results.json") or {"results": []}
    failed_tests = sum(
        1 for x in run_results["results"]
        if x.get("resource_type") == "test" and x.get("status") == "fail"
    )

    sources = get_artifact(run["id"], "sources.json") or {}
    freshness = "unknown"
    freshness_detail = ""
    if "sources" in sources:
        # legacy schema: pass if every source status is pass
        freshness = "ok" if all(s.get("status") == "pass" for s in sources["sources"]) else "fail"
        failing_sources = [s for s in sources["sources"] if s.get("status") != "pass"]
        if failing_sources:
            fs = failing_sources[0]
            name = fs.get("name") or fs.get("source_name") or fs.get("unique_id") or "source"
            freshness_detail = f"{name} {fs.get('status')}"
    elif "results" in sources:
        results = [r for r in sources["results"] if r.get("status")]
        if results:
            severity = {"error": 3, "warn": 2, "pass": 1}
            worst = max(results, key=lambda r: severity.get((r.get("status") or "").lower(), 0))
            worst_status = (worst.get("status") or "").lower()
            if worst_status == "error":
                freshness = "fail"
            elif worst_status == "warn":
                freshness = "amber"
            elif worst_status == "pass":
                freshness = "ok"
            name = worst.get("source_name") or worst.get("name") or worst.get("unique_id") or "source"
            time_ago = worst.get("max_loaded_at_time_ago_in_words")
            freshness_detail = name
            label = "fresh" if worst_status == "pass" else worst_status
            if label:
                freshness_detail += f" {label}"
            if time_ago:
                freshness_detail += f" ({time_ago})"

    color, reason = "grey", "no data"
    if in_progress:
        color, reason = "amber", "run in progress"
    elif status == 10:
        color, reason = "green", "last run success"
        if failed_tests > 0 or freshness == "fail":
            color = "amber"
            reason = f"success with issues: tests={failed_tests}, freshness={freshness}"
    elif status == 20:
        color, reason = "red", "last run failed"
    else:
        color, reason = "amber", f"status {status}"

    return color, reason, failed_tests, freshness, freshness_detail

rows = []
for jid in JOB_IDS:
    run = latest_run(jid)
    if not run:
        rows.append({
            "job_id": jid,
            "run_id": None,
            "job_name": JOB_MAP.get(jid, jid),
            "color": "grey",
            "reason": "no runs",
            "failed_tests": "-",
            "freshness": "unknown",
            "freshness_detail": "",
            "freshness_display": "unknown",
            "started_at": None,
            "finished_at": None,
            "in_progress": False,
            "href": f"https://cloud.getdbt.com/#/accounts/{ACCOUNT}/jobs/{jid}"
        })
        continue
    color, reason, failed_tests, freshness, freshness_detail = parse_status(run)
    job_data = run.get("job") or {}
    freshness_display = freshness
    if freshness_detail:
        freshness_display = f"{freshness}: {freshness_detail}"
    rows.append({
        "job_id": jid,
        "run_id": run["id"],
        "job_name": JOB_MAP.get(jid) or job_data.get("name") or jid,
        "color": color,
        "reason": reason,
        "failed_tests": failed_tests,
        "freshness": freshness,
        "freshness_detail": freshness_detail,
        "freshness_display": freshness_display,
        "started_at": run.get("started_at"),
        "finished_at": run.get("finished_at"),
        "in_progress": run.get("is_complete") is False,
        "href": f"https://cloud.getdbt.com/#/accounts/{ACCOUNT}/jobs/{jid}/runs/{run['id']}"
    })

priority = {"red": 3, "amber": 2, "green": 1, "grey": 0}
overall = max(rows, key=lambda r: priority.get(r["color"], 0))["color"] if rows else "grey"
color_counts = Counter(r["color"] for r in rows)
total_jobs = len(rows)
summary_parts = [f"{total_jobs} job{'s' if total_jobs != 1 else ''}"]
for color in ["green", "amber", "red", "grey"]:
    count = color_counts.get(color, 0)
    if count:
        summary_parts.append(f"{count} {color}")
summary_text = " · ".join(summary_parts)

os.makedirs(".statuspage/out", exist_ok=True)
with open(".statuspage/out/status.json", "w") as f:
    json.dump({
        "overall": overall,
        "generated_at": int(time.time()),
        "total_jobs": total_jobs,
        "counts": dict(color_counts),
        "jobs": rows
    }, f, indent=2)

html = Template("""
<!doctype html><meta charset="utf-8"><title>dbt Status</title>
<style>
body{font-family:system-ui;margin:24px}
.pill{padding:4px 10px;border-radius:999px;color:#fff;font-weight:600}
.green{background:#2ea043}.amber{background:#f2a900}.red{background:#d73a49}.grey{background:#6a737d}
table{border-collapse:collapse;width:100%;margin-top:16px}
th,td{padding:8px 10px;border-bottom:1px solid #e1e4e8;text-align:left}
a{color:inherit}
</style>
<h1>dbt Status <span class="pill {{overall}}">{{overall|capitalize}}</span></h1>
<p>Updated {{updated}} UTC</p>
<p>{{summary}}</p>
<table>
<thead><tr><th>Job</th><th>Status</th><th>Reason</th><th>Tests</th><th>Freshness</th><th>Started</th><th>Finished</th></tr></thead>
<tbody>
{% for j in jobs %}
<tr>
  <td><a href="{{j.href}}" target="_blank">{{j.job_name}}</a></td>
  <td><span class="pill {{j.color}}">{{j.color|capitalize}}</span></td>
  <td>{{j.reason}}</td>
  <td>{{j.failed_tests}}</td>
  <td>{{j.freshness_display}}</td>
  <td>{{j.started_at or "-"}}</td>
  <td>{{j.finished_at or "-"}}</td>
</tr>
{% endfor %}
</tbody></table>
""").render(
    overall=overall,
    jobs=rows,
    summary=summary_text,
    updated=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
)
with open(".statuspage/out/index.html", "w") as f:
    f.write(html)
