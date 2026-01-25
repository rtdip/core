# Copyright 2025 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
import pandas as pd
import numpy as np
from src.sdk.python.rtdip_sdk.pipelines.decomposition.pandas.period_utils import (
    calculate_period_from_frequency,
    calculate_periods_from_frequency,
)


class TestCalculatePeriodFromFrequency:
    """Tests for calculate_period_from_frequency function."""

    def test_hourly_period_from_5_second_data(self):
        """Test calculating hourly period from 5-second sampling data."""
        # Create 5-second sampling data (1 day worth)
        n_samples = 24 * 60 * 12  # 24 hours * 60 min * 12 samples/min
        dates = pd.date_range("2024-01-01", periods=n_samples, freq="5s")
        df = pd.DataFrame({"timestamp": dates, "value": np.random.randn(n_samples)})

        period = calculate_period_from_frequency(
            df=df, timestamp_column="timestamp", period_name="hourly"
        )

        # Hourly period should be 3600 / 5 = 720
        assert period == 720

    def test_daily_period_from_5_second_data(self):
        """Test calculating daily period from 5-second sampling data."""
        n_samples = 3 * 24 * 60 * 12
        dates = pd.date_range("2024-01-01", periods=n_samples, freq="5s")
        df = pd.DataFrame({"timestamp": dates, "value": np.random.randn(n_samples)})

        period = calculate_period_from_frequency(
            df=df, timestamp_column="timestamp", period_name="daily"
        )

        assert period == 17280

    def test_weekly_period_from_daily_data(self):
        """Test calculating weekly period from daily data."""
        dates = pd.date_range("2024-01-01", periods=365, freq="D")
        df = pd.DataFrame({"timestamp": dates, "value": np.random.randn(365)})

        period = calculate_period_from_frequency(
            df=df, timestamp_column="timestamp", period_name="weekly"
        )

        assert period == 7

    def test_yearly_period_from_daily_data(self):
        """Test calculating yearly period from daily data."""
        dates = pd.date_range("2024-01-01", periods=365 * 3, freq="D")
        df = pd.DataFrame({"timestamp": dates, "value": np.random.randn(365 * 3)})

        period = calculate_period_from_frequency(
            df=df, timestamp_column="timestamp", period_name="yearly"
        )

        assert period == 365

    def test_insufficient_data_returns_none(self):
        """Test that insufficient data returns None."""
        # Only 10 samples at 1-second frequency - not enough for hourly (need 7200)
        dates = pd.date_range("2024-01-01", periods=10, freq="1s")
        df = pd.DataFrame({"timestamp": dates, "value": np.random.randn(10)})

        period = calculate_period_from_frequency(
            df=df, timestamp_column="timestamp", period_name="hourly"
        )

        assert period is None

    def test_period_too_small_returns_none(self):
        """Test that period < 2 returns None."""
        # Hourly data trying to get minutely period (1 hour / 1 hour = 1)
        dates = pd.date_range("2024-01-01", periods=100, freq="H")
        df = pd.DataFrame({"timestamp": dates, "value": np.random.randn(100)})

        period = calculate_period_from_frequency(
            df=df, timestamp_column="timestamp", period_name="minutely"
        )

        assert period is None

    def test_irregular_timestamps(self):
        """Test with irregular timestamps (uses median)."""
        dates = []
        current = pd.Timestamp("2024-01-01")
        for i in range(2000):
            dates.append(current)
            current += pd.Timedelta(seconds=5 if i % 2 == 0 else 10)

        df = pd.DataFrame({"timestamp": dates, "value": np.random.randn(2000)})

        period = calculate_period_from_frequency(
            df=df, timestamp_column="timestamp", period_name="hourly"
        )

        assert period == 720

    def test_invalid_period_name_raises_error(self):
        """Test that invalid period name raises ValueError."""
        dates = pd.date_range("2024-01-01", periods=100, freq="5s")
        df = pd.DataFrame({"timestamp": dates, "value": np.random.randn(100)})

        with pytest.raises(ValueError, match="Invalid period_name"):
            calculate_period_from_frequency(
                df=df, timestamp_column="timestamp", period_name="invalid"
            )

    def test_missing_timestamp_column_raises_error(self):
        """Test that missing timestamp column raises ValueError."""
        df = pd.DataFrame({"value": np.random.randn(100)})

        with pytest.raises(ValueError, match="not found in DataFrame"):
            calculate_period_from_frequency(
                df=df, timestamp_column="timestamp", period_name="hourly"
            )

    def test_non_datetime_column_raises_error(self):
        """Test that non-datetime timestamp column raises ValueError."""
        df = pd.DataFrame({"timestamp": range(100), "value": np.random.randn(100)})

        with pytest.raises(ValueError, match="must be datetime type"):
            calculate_period_from_frequency(
                df=df, timestamp_column="timestamp", period_name="hourly"
            )

    def test_insufficient_rows_raises_error(self):
        """Test that < 2 rows raises ValueError."""
        dates = pd.date_range("2024-01-01", periods=1, freq="H")
        df = pd.DataFrame({"timestamp": dates, "value": [1.0]})

        with pytest.raises(ValueError, match="at least 2 rows"):
            calculate_period_from_frequency(
                df=df, timestamp_column="timestamp", period_name="hourly"
            )

    def test_min_cycles_parameter(self):
        """Test min_cycles parameter."""
        # 10 days of hourly data
        dates = pd.date_range("2024-01-01", periods=10 * 24, freq="H")
        df = pd.DataFrame({"timestamp": dates, "value": np.random.randn(10 * 24)})

        # Weekly period (168 hours) needs at least 2 weeks (336 hours)
        period = calculate_period_from_frequency(
            df=df, timestamp_column="timestamp", period_name="weekly", min_cycles=2
        )
        assert period is None  # Only 10 days, need 14

        # But with min_cycles=1, should work
        period = calculate_period_from_frequency(
            df=df, timestamp_column="timestamp", period_name="weekly", min_cycles=1
        )
        assert period == 168


class TestCalculatePeriodsFromFrequency:
    """Tests for calculate_periods_from_frequency function."""

    def test_multiple_periods(self):
        """Test calculating multiple periods at once."""
        # 30 days of 5-second data
        n_samples = 30 * 24 * 60 * 12
        dates = pd.date_range("2024-01-01", periods=n_samples, freq="5s")
        df = pd.DataFrame({"timestamp": dates, "value": np.random.randn(n_samples)})

        periods = calculate_periods_from_frequency(
            df=df, timestamp_column="timestamp", period_names=["hourly", "daily"]
        )

        assert "hourly" in periods
        assert "daily" in periods
        assert periods["hourly"] == 720
        assert periods["daily"] == 17280

    def test_single_period_as_string(self):
        """Test passing single period name as string."""
        dates = pd.date_range("2024-01-01", periods=2000, freq="5s")
        df = pd.DataFrame({"timestamp": dates, "value": np.random.randn(2000)})

        periods = calculate_periods_from_frequency(
            df=df, timestamp_column="timestamp", period_names="hourly"
        )

        assert "hourly" in periods
        assert periods["hourly"] == 720

    def test_excludes_invalid_periods(self):
        """Test that invalid periods are excluded from results."""
        # Short dataset - weekly won't work
        dates = pd.date_range("2024-01-01", periods=100, freq="H")
        df = pd.DataFrame({"timestamp": dates, "value": np.random.randn(100)})

        periods = calculate_periods_from_frequency(
            df=df,
            timestamp_column="timestamp",
            period_names=["daily", "weekly", "monthly"],
        )

        # Daily should work (24 hours), but weekly and monthly need more data
        assert "daily" in periods
        assert "weekly" not in periods
        assert "monthly" not in periods

    def test_all_periods_available(self):
        """Test all supported period names."""
        dates = pd.date_range("2024-01-01", periods=3 * 365 * 24 * 60, freq="min")
        df = pd.DataFrame({"timestamp": dates, "value": np.random.randn(len(dates))})

        periods = calculate_periods_from_frequency(
            df=df,
            timestamp_column="timestamp",
            period_names=[
                "minutely",
                "hourly",
                "daily",
                "weekly",
                "monthly",
                "quarterly",
                "yearly",
            ],
        )

        assert "minutely" not in periods
        assert "hourly" in periods
        assert "daily" in periods
        assert "weekly" in periods
        assert "monthly" in periods
        assert "quarterly" in periods
        assert "yearly" in periods
