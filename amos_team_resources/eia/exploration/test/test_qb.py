import json
import os
from pathlib import Path
from datetime import datetime
import subprocess
import pandas as pd
import pytest

import amos_team_resources.eia.exploration.scripts.eia_query_builder as qb


# ---------------------------------------------------------------------------
# Shared pytest fixture
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def setup_tmpdir(monkeypatch, tmp_path):
    """Redirect output dir to a temporary path."""
    monkeypatch.setattr(qb, "BASE_OUTDIR", tmp_path)
    yield


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


def test_inject_api_key():
    url = "https://api.eia.gov/v2/series/?series_id=XYZ"
    out = qb.inject_api_key(url, "abc123")
    assert "api_key=abc123" in out


def test_daterange_chunks_year_split():
    start = datetime(2020, 1, 1)
    end = datetime(2023, 1, 1)
    chunks = list(qb.daterange_chunks(start, end, step_years=1))
    assert len(chunks) == 3
    assert chunks[0][0].year == 2020
    assert chunks[-1][1].year == 2023


def test_parse_eia_date_month_only():
    d = qb.parse_eia_date("2020-05")
    assert isinstance(d, datetime)
    assert d.month == 5 and d.day == 1


def test_main_creates_chunks_and_merged(monkeypatch, tmp_path):
    """Basic smoke test for main() with mocked requests.get."""
    base_url = "https://api.eia.gov/v2/test?start=2020-01&end=2021-12"

    fake_resp = {"response": {"data": [{"period": "2020-01"}, {"period": "2020-02"}]}}

    class DummyResponse:
        ok = True
        status_code = 200
        text = "OK"

        def json(self):
            return fake_resp

    monkeypatch.setattr(qb.requests, "get", lambda *a, **kw: DummyResponse())
    monkeypatch.setenv("EIA_API_KEY", "TEST_KEY")
    monkeypatch.setattr("sys.argv", ["eia_query_builder.py", base_url])

    qb.main()

    outdirs = list(tmp_path.glob("v2_test_*"))
    assert len(outdirs) == 1
    outdir = outdirs[0]

    chunk_files = list(outdir.glob("chunk_*.json"))
    merged = outdir / "merged.json"
    assert merged.exists()
    assert chunk_files, "No chunk files created"

    with open(merged) as f:
        data = json.load(f)
    assert "response" in data and "data" in data["response"]
    assert isinstance(data["response"]["data"], list)


# ---------------------------------------------------------------------------
# Integration test for period coverage
# ---------------------------------------------------------------------------


def make_fake_response(start="2000-01", end="2005-01"):
    """Generate synthetic monthly records between start and end (inclusive)."""
    start_dt = pd.to_datetime(start, format="%Y-%m")
    end_dt = pd.to_datetime(end, format="%Y-%m")
    months = pd.date_range(start_dt, end_dt, freq="MS")
    data = [{"period": m.strftime("%Y-%m")} for m in months]
    return {"response": {"data": data}}


def test_period_coverage_real_api():
    """Integration test: query real EIA API and verify period count."""
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        pytest.skip("Real API test skipped (no EIA_API_KEY set)")

    # Test URL for monthly data from 2000-01 to 2005-01
    test_url = (
        "https://api.eia.gov/v2/petroleum/sum/crdsnd/data/"
        "?frequency=monthly&data[0]=value"
        "&start=2000-01&end=2005-01"
        "&sort[0][column]=period&sort[0][direction]=desc"
        "&offset=0&length=5000"
    )

    # use subprocess to mimic real CLI run
    env = os.environ.copy()
    env["EIA_API_KEY"] = api_key

    result = subprocess.run(
        ["python", "scripts/eia_query_builder.py", test_url],
        cwd=Path(__file__).resolve().parent.parent,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )

    print(result.stdout)
    assert result.returncode == 0, f"Script failed: {result.stderr}"

    # locate newest output directory
    data_raw = Path("data/raw")
    outdirs = sorted(data_raw.glob("v2_petroleum_sum_crdsnd_data_*"))
    assert outdirs, "No output directory found"
    merged = outdirs[-1] / "merged.json"
    assert merged.exists(), "merged.json not created"

    # verify period coverage
    with open(merged, "r", encoding="utf-8") as f:
        data = json.load(f)["response"]["data"]

    df = pd.DataFrame(data)
    unique_months = df["period"].nunique()

    # end date is exclusive
    expected_months = len(pd.date_range("2000-01", "2004-12", freq="MS"))

    assert (
        unique_months == expected_months
    ), f"Expected {expected_months} months, got {unique_months}"

    print(f"Real EIA API returned {unique_months} monthly periods (2000-01 → 2005-01).")
