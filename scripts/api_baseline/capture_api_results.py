"""Capture RTDIP API results and timings before/after a deployment, then compare them.

Usage:
    python scripts/api_baseline/capture_api_results.py capture --label before   (run 3-4 times)
    (deploy the new API)
    python scripts/api_baseline/capture_api_results.py capture --label after    (run 3-4 times)
    python scripts/api_baseline/capture_api_results.py compare --before <dir> <dir> ... --after <dir> <dir> ...

Credentials are read from scripts/api_baseline/.env (git-ignored):
    RTDIP_TENANT_ID, RTDIP_CLIENT_ID, RTDIP_CLIENT_SECRET, RTDIP_SCOPE (e.g. "<app-id-uri>/.default")
"""

import argparse
import csv
import hashlib
import json
import re
import math
import os
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

# ---------------------------------------------------------------- Configuration
API_BASE_URL = os.getenv(
    "RTDIP_API_BASE_URL", "https://ssip-api.shell.com/api/v1"
).rstrip("/")

BUSINESS_UNIT = "downstream"
REGION = "americas"
DATA_SECURITY_LEVEL = "restricted"
DATA_TYPE = "float"
# One request per group for each endpoint/step case, containing all of that group's tags.
REQUEST_GROUPS = {
    "upgrader": {
        "asset": "scotfordupgrader",
        "tags": ["CA:SCM:REF-EFF-REL:CLAB-TVOL:TOTAL VOLUME RELEASED (COC FORM)"],
    },
    "manufacturing": {
        "asset": "scotfordmanufacturing",
        "tags": ["CA:SCM:FI3542", "CA:SCM:FI30006", "CA:SCM:FI1123", "CA:SCM:FI1051", "CA:SCM:FC3114", "CA:SCM:FC3108"],
    },
}
# START_DATE = "2025-05-01T06:00:00.000000000Z"
# END_DATE = "2025-10-31T06:00:00.000000000Z"
START_DATE = "2026-12-01T06:00:00.000000000Z"
END_DATE = "2026-10-31T06:00:00.000000000Z"
# Raw returns every event; dense tags (~1/min) over months produce millions of rows.
RAW_START_DATE = "2026-10-24T06:00:00.000000000Z"
RAW_END_DATE = "2026-10-31T06:00:00.000000000Z"
TIME_INTERVAL_RATE = 1
TIME_INTERVAL_UNIT = "day"
INCLUDE_BAD_DATA = "true"

# None means the step parameter is not sent at all.
STEP_CASES = {
    "omitted": None,
    "false": "false",
    "true": "true",
    "metadata": "metadata",
}

ENDPOINTS = {
    "raw": "/events/raw",
    "interpolate": "/events/interpolate",
    "twa": "/events/timeweightedaverage",
}

# Small raw query sent first to start the cluster; excluded from comparisons.
WARMUP_START_DATE = "2025-10-01T06:00:00.000000000Z"
WARMUP_END_DATE = "2025-10-01T07:00:00.000000000Z"

# Repeats within one run; compare also pools repeats across multiple runs.
REPEATS = 3
REQUEST_TIMEOUT_SECONDS = 600
# Relative tolerance absorbs float summation-order noise between identical queries.
VALUE_REL_TOLERANCE = 1e-9
VALUE_ABS_TOLERANCE = 1e-9
RESULTS_DIR = Path(__file__).resolve().parent / "results"
# ------------------------------------------------------------------------------


class TokenProvider:
    def __init__(self):
        missing = [
            name
            for name in (
                "RTDIP_TENANT_ID",
                "RTDIP_CLIENT_ID",
                "RTDIP_CLIENT_SECRET",
                "RTDIP_SCOPE",
            )
            if not os.environ.get(name)
        ]
        if missing:
            sys.exit(
                f"Missing values in {Path(__file__).resolve().parent / '.env'}: "
                f"{', '.join(missing)}"
            )
        self._token = None
        self._expires_at = 0.0

    def get(self) -> str:
        # Refresh five minutes before expiry so long runs do not fail midway.
        if self._token is None or time.time() > self._expires_at - 300:
            self._refresh()
        return self._token

    def _refresh(self):
        token_url = (
            f"https://login.microsoftonline.com/"
            f"{os.environ['RTDIP_TENANT_ID']}/oauth2/v2.0/token"
        )
        response = requests.post(
            token_url,
            data={
                "client_id": os.environ["RTDIP_CLIENT_ID"],
                "client_secret": os.environ["RTDIP_CLIENT_SECRET"],
                "scope": os.environ["RTDIP_SCOPE"],
                "grant_type": "client_credentials",
            },
            timeout=30,
        )
        if not response.ok:
            try:
                error = response.json()
            except ValueError:
                error = {}
            raise RuntimeError(
                f"Token request failed ({response.status_code}): "
                f"{error.get('error', 'unknown_error')} - "
                f"{error.get('error_description', 'No description returned')}"
            )
        token_response = response.json()
        self._token = token_response["access_token"]
        self._expires_at = time.time() + int(token_response["expires_in"])
        print(f"Token obtained; expires in {token_response['expires_in']} seconds.")


def tag_id(tag: str) -> str:
    # Hash keeps folder names unique after stripping characters Windows disallows (e.g. ":").
    slug = re.sub(r"[^A-Za-z0-9]+", "_", tag).strip("_")
    return f"{slug}_{hashlib.sha1(tag.encode()).hexdigest()[:6]}"


def build_params(endpoint: str, step, asset: str, tag_names, start_date=None, end_date=None):
    if start_date is None:
        start_date, end_date = (
            (RAW_START_DATE, RAW_END_DATE) if endpoint == "raw" else (START_DATE, END_DATE)
        )
    params = {
        "business_unit": BUSINESS_UNIT,
        "region": REGION,
        "asset": asset,
        "data_security_level": DATA_SECURITY_LEVEL,
        "data_type": DATA_TYPE,
        "tag_name": tag_names,
        "start_date": start_date,
        "end_date": end_date,
        "include_bad_data": INCLUDE_BAD_DATA,
    }
    if endpoint in ("interpolate", "twa"):
        params["time_interval_rate"] = TIME_INTERVAL_RATE
        params["time_interval_unit"] = TIME_INTERVAL_UNIT
    if step is not None:
        params["step"] = step
    return params


def timed_get(session, tokens, endpoint, params):
    url = API_BASE_URL + ENDPOINTS[endpoint]
    headers = {"Authorization": f"Bearer {tokens.get()}", "Accept": "application/json"}
    started = time.perf_counter()
    try:
        response = session.get(
            url, headers=headers, params=params, timeout=REQUEST_TIMEOUT_SECONDS
        )
        content = response.content
    except requests.RequestException as e:
        return {
            "http_status": None,
            "total_seconds": round(time.perf_counter() - started, 3),
            "time_to_headers_seconds": None,
            "response_bytes": 0,
            "row_count": None,
            "error": str(e)[:500],
        }, {"error": str(e)}
    total = time.perf_counter() - started

    try:
        body = response.json()
    except ValueError:
        body = {"non_json_body": response.text}
    data = body.get("data") if isinstance(body, dict) else None

    return {
        "http_status": response.status_code,
        "total_seconds": round(total, 3),
        "time_to_headers_seconds": round(response.elapsed.total_seconds(), 3),
        "response_bytes": len(content),
        "row_count": len(data) if isinstance(data, list) else None,
        "error": "" if response.ok else response.text[:500],
    }, body


def capture(label: str):
    tokens = TokenProvider()
    run_dir = RESULTS_DIR / f"{label}_{datetime.now():%Y%m%d_%H%M%S}"
    run_dir.mkdir(parents=True)
    # Results may contain restricted data; keep them out of Git.
    (RESULTS_DIR / ".gitignore").write_text("*\n")

    config = {
        "label": label,
        "captured_at": datetime.now().astimezone().isoformat(),
        "api_base_url": API_BASE_URL,
        "repeats": REPEATS,
        "step_cases": STEP_CASES,
        "request_groups": REQUEST_GROUPS,
        "tag_folders": {tag_id(t): t for g in REQUEST_GROUPS.values() for t in g["tags"]},
        "params_by_group_endpoint": {
            f"{group}/{endpoint}": build_params(endpoint, "<per case>", g["asset"], g["tags"])
            for group, g in REQUEST_GROUPS.items()
            for endpoint in ENDPOINTS
        },
    }
    (run_dir / "config.json").write_text(json.dumps(config, indent=2))

    session = requests.Session()
    metrics = []

    first_group = next(iter(REQUEST_GROUPS.values()))
    print("Warm-up raw query (starts the cluster)...")
    warmup, _ = timed_get(
        session,
        tokens,
        "raw",
        build_params("raw", None, first_group["asset"], first_group["tags"], WARMUP_START_DATE, WARMUP_END_DATE),
    )
    metrics.append(
        {"case": "warmup_raw", "group": "", "asset": first_group["asset"], "endpoint": "raw", "step": "omitted", "repeat": 0, **warmup}
    )
    print(f"  status={warmup['http_status']} time={warmup['total_seconds']}s")

    total = REPEATS * len(REQUEST_GROUPS) * len(ENDPOINTS) * len(STEP_CASES)
    done = 0
    # Repeats are the outer loop so each case is spread across the run, reducing ordering bias.
    for repeat in range(1, REPEATS + 1):
        for group, g in REQUEST_GROUPS.items():
            (run_dir / group).mkdir(exist_ok=True)
            for endpoint in ENDPOINTS:
                for step_name, step_value in STEP_CASES.items():
                    name = f"{endpoint}_step-{step_name}"
                    case = f"{group}/{name}"
                    result, body = timed_get(
                        session, tokens, endpoint, build_params(endpoint, step_value, g["asset"], g["tags"])
                    )
                    metrics.append(
                        {"case": case, "group": group, "asset": g["asset"], "endpoint": endpoint, "step": step_name, "repeat": repeat, **result}
                    )
                    (run_dir / f"{case}_r{repeat}.json").write_text(json.dumps(body, indent=2))
                    save_by_tag(run_dir, g["tags"], f"{name}_r{repeat}", body)
                    done += 1
                    print(
                        f"[{done}/{total}] {case}: status={result['http_status']} "
                        f"time={result['total_seconds']}s rows={result['row_count']}"
                    )
                    # Save progress so a failure late in the run does not lose earlier results.
                    write_csv(run_dir / "metrics.csv", metrics)

    write_csv(run_dir / "summary.csv", summarize(metrics))
    print(f"\nSaved results to {run_dir}")


def save_by_tag(run_dir: Path, tags, file_name: str, body):
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, list):
        return
    for tag in dict.fromkeys(tags + [row.get("TagName") for row in data]):
        tag_dir = run_dir / "by_tag" / tag_id(str(tag))
        tag_dir.mkdir(parents=True, exist_ok=True)
        tag_rows = [row for row in data if row.get("TagName") == tag]
        (tag_dir / f"{file_name}.json").write_text(
            json.dumps({"TagName": tag, "row_count": len(tag_rows), "data": tag_rows}, indent=2)
        )


def summarize(metrics):
    summary = []
    for case in dict.fromkeys(m["case"] for m in metrics if m["case"] != "warmup_raw"):
        runs = [m for m in metrics if m["case"] == case]
        ok_times = [m["total_seconds"] for m in runs if m["http_status"] == 200]
        summary.append(
            {
                "case": case,
                "group": runs[0]["group"],
                "endpoint": runs[0]["endpoint"],
                "step": runs[0]["step"],
                "runs": len(runs),
                "successful_runs": len(ok_times),
                "statuses": ";".join(str(m["http_status"]) for m in runs),
                "first_run_seconds": runs[0]["total_seconds"],
                "median_seconds": round(statistics.median(ok_times), 3) if ok_times else None,
                "min_seconds": min(ok_times) if ok_times else None,
                "max_seconds": max(ok_times) if ok_times else None,
                "row_count": runs[0]["row_count"],
                "response_bytes": runs[0]["response_bytes"],
            }
        )
    return summary


def write_csv(path: Path, rows):
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def read_metrics(path: Path):
    with path.open(newline="", encoding="utf-8") as f:
        return [row for row in csv.DictReader(f) if row["case"] != "warmup_raw"]


def keyed_rows(body):
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, list):
        return None
    rows, seen = {}, {}
    for row in data:
        base_key = (row.get("TagName"), row.get("EventTime"))
        # Duplicate timestamps are possible in raw data; keep each occurrence.
        seen[base_key] = seen.get(base_key, 0) + 1
        rows[base_key + (seen[base_key],)] = row.get("Value")
    return rows


def compare_values(before, after):
    if isinstance(before, (int, float)) and isinstance(after, (int, float)):
        if math.isclose(before, after, rel_tol=VALUE_REL_TOLERANCE, abs_tol=VALUE_ABS_TOLERANCE):
            return True, 0.0
        return False, abs(after - before)
    return before == after, None


def rows_equal(a, b):
    return a.keys() == b.keys() and all(compare_values(a[k], b[k])[0] for k in a)


def load_side(dirs):
    """Collect every request's status, timing and parsed rows per case across runs."""
    cases = {}
    for run_dir in dirs:
        for m in read_metrics(run_dir / "metrics.csv"):
            entry = cases.setdefault(
                m["case"],
                {"group": m["group"], "endpoint": m["endpoint"], "step": m["step"], "statuses": [], "times": [], "results": []},
            )
            entry["statuses"].append(m["http_status"] or "error")
            if m["http_status"] != "200":
                continue
            entry["times"].append(float(m["total_seconds"]))
            body_file = run_dir / f"{m['case']}_r{m['repeat']}.json"
            rows = keyed_rows(json.loads(body_file.read_text())) if body_file.exists() else None
            if rows is not None:
                entry["results"].append((f"{run_dir.name}/r{m['repeat']}", rows))
    return cases


def consensus(results):
    """Return the result most requests agree on, the agreement count and the disagreeing sources."""
    clusters = []
    for source, rows in results:
        for cluster in clusters:
            if rows_equal(cluster["rows"], rows):
                cluster["sources"].append(source)
                break
        else:
            clusters.append({"rows": rows, "sources": [source]})
    if not clusters:
        return None, 0, []
    best = max(clusters, key=lambda c: len(c["sources"]))
    outliers = [s for c in clusters if c is not best for s in c["sources"]]
    return best["rows"], len(best["sources"]), outliers


def timing(times):
    return (
        round(statistics.median(times), 3) if times else None,
        min(times) if times else None,
        max(times) if times else None,
    )


def compare(before_dirs, after_dirs):
    before_side, after_side = load_side(before_dirs), load_side(after_dirs)
    report, by_tag, details = [], [], []

    for case in dict.fromkeys(list(before_side) + list(after_side)):
        b, a = before_side.get(case), after_side.get(case)
        info = b or a
        before_rows, b_agree, b_outliers = consensus(b["results"]) if b else (None, 0, [])
        after_rows, a_agree, a_outliers = consensus(a["results"]) if a else (None, 0, [])

        only_before = only_after = changed = None
        max_abs_diff = None
        case_details = []
        if before_rows is not None and after_rows is not None:
            only_before = len(before_rows.keys() - after_rows.keys())
            only_after = len(after_rows.keys() - before_rows.keys())
            changed, diffs = 0, []
            for key in sorted(before_rows.keys() | after_rows.keys(), key=str):
                in_before, in_after = key in before_rows, key in after_rows
                if in_before and in_after:
                    same, diff = compare_values(before_rows[key], after_rows[key])
                    if same:
                        continue
                    changed += 1
                    if diff is not None:
                        diffs.append(diff)
                    change = "value_changed"
                else:
                    diff = None
                    change = "only_before" if in_before else "only_after"
                case_details.append(
                    {
                        "case": case,
                        "group": info["group"],
                        "endpoint": info["endpoint"],
                        "step": info["step"],
                        "TagName": key[0],
                        "EventTime": key[1],
                        "occurrence": key[2],
                        "change": change,
                        "before_value": before_rows.get(key),
                        "after_value": after_rows.get(key),
                        "abs_diff": diff,
                    }
                )
            max_abs_diff = max(diffs) if diffs else 0.0
            details.extend(case_details)

            group_tags = REQUEST_GROUPS.get(info["group"], {}).get("tags", [])
            for tag in dict.fromkeys(group_tags + sorted({k[0] for k in before_rows.keys() | after_rows.keys()}, key=str)):
                tag_details = [d for d in case_details if d["TagName"] == tag]
                tag_diffs = [d["abs_diff"] for d in tag_details if d["abs_diff"] is not None]
                by_tag.append(
                    {
                        "case": case,
                        "group": info["group"],
                        "endpoint": info["endpoint"],
                        "step": info["step"],
                        "TagName": tag,
                        "before_rows": sum(1 for k in before_rows if k[0] == tag),
                        "after_rows": sum(1 for k in after_rows if k[0] == tag),
                        "rows_only_before": sum(1 for d in tag_details if d["change"] == "only_before"),
                        "rows_only_after": sum(1 for d in tag_details if d["change"] == "only_after"),
                        "values_changed": sum(1 for d in tag_details if d["change"] == "value_changed"),
                        "max_abs_value_diff": max(tag_diffs) if tag_diffs else 0.0,
                        "identical_results": not tag_details,
                    }
                )

        b_median, b_min, b_max = timing(b["times"] if b else [])
        a_median, a_min, a_max = timing(a["times"] if a else [])
        report.append(
            {
                "case": case,
                "group": info["group"],
                "endpoint": info["endpoint"],
                "step": info["step"],
                "before_statuses": ";".join(b["statuses"]) if b else None,
                "after_statuses": ";".join(a["statuses"]) if a else None,
                "before_agreeing": f"{b_agree}/{len(b['results'])}" if b else None,
                "after_agreeing": f"{a_agree}/{len(a['results'])}" if a else None,
                "before_outliers": ";".join(b_outliers),
                "after_outliers": ";".join(a_outliers),
                "before_rows": len(before_rows) if before_rows is not None else None,
                "after_rows": len(after_rows) if after_rows is not None else None,
                "rows_only_before": only_before,
                "rows_only_after": only_after,
                "values_changed": changed,
                "max_abs_value_diff": max_abs_diff,
                "identical_results": (
                    before_rows is not None
                    and after_rows is not None
                    and not (only_before or only_after or changed)
                ),
                "before_first_run_s": b.get("first_run_seconds"),
                "after_first_run_s": a.get("first_run_seconds"),
                "before_median_s": b_median,
                "after_median_s": a_median,
                "before_min_max_s": f"{b_min}-{b_max}" if b_median is not None else None,
                "after_min_max_s": f"{a_min}-{a_max}" if a_median is not None else None,
                "median_change_pct": (
                    round((a_median - b_median) / b_median * 100, 1)
                    if b_median and a_median is not None
                    else None
                ),
            }
        )

    out_dir = RESULTS_DIR / f"comparison_{datetime.now():%Y%m%d_%H%M%S}"
    out_dir.mkdir(parents=True)
    (out_dir / "inputs.json").write_text(
        json.dumps({"before": [str(d) for d in before_dirs], "after": [str(d) for d in after_dirs]}, indent=2)
    )
    output = out_dir / "comparison.csv"
    write_csv(output, report)
    by_tag_output = out_dir / "comparison_by_tag.csv"
    write_csv(by_tag_output, by_tag)
    details_output = out_dir / "comparison_rows.csv"
    write_csv(details_output, details)
    for row in report:
        print(
            f"{row['case']:<36} identical={str(row['identical_results']):<5} "
            f"changed={row['values_changed']} rows {row['before_rows']}->{row['after_rows']} "
            f"agree {row['before_agreeing']}|{row['after_agreeing']} "
            f"median {row['before_median_s']}s->{row['after_median_s']}s ({row['median_change_pct']}%)"
        )
    if any(r["before_outliers"] or r["after_outliers"] for r in report):
        print("\nWARNING: some requests disagreed with the majority result; see *_outliers columns.")
    print(f"\nSaved comparison to {output}")
    print(f"Saved per-tag comparison to {by_tag_output}")
    print(
        f"Saved {len(details)} differing rows to {details_output}"
        if details
        else "No row-level differences."
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    capture_parser = sub.add_parser("capture", help="Run all cases and save results")
    capture_parser.add_argument("--label", required=True, help='e.g. "before" or "after"')
    compare_parser = sub.add_parser("compare", help="Compare before runs with after runs")
    compare_parser.add_argument("--before", nargs="+", required=True, type=Path, help="One or more before run folders")
    compare_parser.add_argument("--after", nargs="+", required=True, type=Path, help="One or more after run folders")
    args = parser.parse_args()

    if args.command == "capture":
        capture(args.label)
    else:
        compare(args.before, args.after)


if __name__ == "__main__":
    main()
