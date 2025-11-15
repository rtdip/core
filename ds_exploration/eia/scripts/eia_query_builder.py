#!/usr/bin/env python3
"""
EIA API query builder & downloader
----------------------------------
Usage:
    export EIA_API_KEY="your_api_key_here"
    python scripts/eia_query_builder.py $EIA_API_URL
"""

import argparse
import os
import sys
import json
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import requests
from pathlib import Path
from datetime import datetime

# configuration
BASE_OUTDIR = Path(__file__).resolve().parent.parent / "data" / "raw"
BASE_OUTDIR.mkdir(parents=True, exist_ok=True)
CHUNK_YEARS = 1  # split per year

# helpers
def inject_api_key(url: str, api_key: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    query["api_key"] = [api_key]
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))

def download_json(full_url: str):
    print(f"→ Requesting {full_url}")
    resp = requests.get(full_url, timeout=60)
    print(f"HTTP {resp.status_code}")
    if not resp.ok:
        raise RuntimeError(f"EIA API error: {resp.status_code} {resp.text[:200]}")
    return resp.json()

def daterange_chunks(start, end, step_years=1):
    """Yield (start, end) date pairs split by year."""
    current = start
    while current < end:
        try:
            chunk_end = current.replace(year=current.year + step_years)
        except ValueError:
            # handle leap year
            chunk_end = current.replace(month=2, day=28, year=current.year + step_years)
        chunk_end = min(chunk_end, end)
        yield current, chunk_end
        current = chunk_end

def parse_eia_date(s: str) -> datetime:
    """Parse EIA-style dates (YYYY-MM or YYYY-MM-DD)."""
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return datetime.fromisoformat(f"{s}-01")

def main():
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        print("Missing EIA_API_KEY - please export it first")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="EIA API query builder with chunked downloads")
    parser.add_argument("url", help="EIA API URL (without api_key parameter)")
    args = parser.parse_args()

    base_url = args.url
    parsed = urlparse(base_url)
    query = parse_qs(parsed.query)

    start_str = query.get("start", [None])[0]
    end_str = query.get("end", [None])[0]
    if not (start_str and end_str):
        print("URL must contain 'start=' and 'end=' parameters")
        sys.exit(1)

    start_date = parse_eia_date(start_str)
    end_date = parse_eia_date(end_str)

    # create dataset-specific output directory
    parsed_path = parsed.path.strip("/")
    dataset_name = parsed_path.replace("/", "_") or "eia_dataset"
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    OUTDIR = BASE_OUTDIR / f"{dataset_name}_{run_ts}"
    OUTDIR.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {OUTDIR}")

    all_data = []
    for s, e in daterange_chunks(start_date, end_date, step_years=CHUNK_YEARS):
        # modify URL for this chunk
        chunk_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{urlencode(query, doseq=True)}"
        chunk_url = inject_api_key(chunk_url, api_key)
        chunk_url = chunk_url.replace(f"start={start_str}", f"start={s.date()}").replace(f"end={end_str}", f"end={e.date()}")

        try:
            data = download_json(chunk_url)
            records = data.get("response", {}).get("data", [])
            print(f"Retrieved {len(records)} rows ({s.date()} → {e.date()})")

            # save each chunk separately
            chunk_file = OUTDIR / f"chunk_{s.date()}_{e.date()}.json"
            with open(chunk_file, "w", encoding="utf-8") as f:
                json.dump(data, f)
            print(f"Saved {chunk_file.name}")

            all_data.extend(records)
        except Exception as ex:
            print(f"Failed chunk {s.date()} → {e.date()}: {ex}")

    # merged summary file
    merged_file = OUTDIR / "merged.json"
    with open(merged_file, "w", encoding="utf-8") as f:
        json.dump({"response": {"data": all_data}}, f)
    print(f"Saved merged file: {merged_file}")
    print(f"Total rows across chunks: {len(all_data)}")

if __name__ == "__main__":
    main()