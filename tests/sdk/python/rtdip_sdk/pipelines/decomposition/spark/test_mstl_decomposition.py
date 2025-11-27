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
from pyspark.sql import SparkSession

from src.sdk.python.rtdip_sdk.pipelines.decomposition.spark.mstl_decomposition import (
    MSTLDecomposition,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    SystemType,
    Libraries,
)


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_time_series(spark):
    """Create a sample time series with trend, seasonality, and noise."""
    np.random.seed(42)
    n_points = 365
    dates = pd.date_range("2024-01-01", periods=n_points, freq="D")
    trend = np.linspace(10, 20, n_points)
    seasonal = 5 * np.sin(2 * np.pi * np.arange(n_points) / 7)
    noise = np.random.randn(n_points) * 0.5
    value = trend + seasonal + noise

    pdf = pd.DataFrame({"timestamp": dates, "value": value})
    return spark.createDataFrame(pdf)


@pytest.fixture
def multi_seasonal_time_series(spark):
    """Create a time series with multiple seasonal patterns."""
    np.random.seed(42)
    n_points = 24 * 60  # 60 days of hourly data
    dates = pd.date_range("2024-01-01", periods=n_points, freq="h")
    trend = np.linspace(10, 15, n_points)
    daily_seasonal = 5 * np.sin(2 * np.pi * np.arange(n_points) / 24)
    weekly_seasonal = 3 * np.sin(2 * np.pi * np.arange(n_points) / 168)
    noise = np.random.randn(n_points) * 0.5
    value = trend + daily_seasonal + weekly_seasonal + noise

    pdf = pd.DataFrame({"timestamp": dates, "value": value})
    return spark.createDataFrame(pdf)


def test_single_period(spark, sample_time_series):
    """Test MSTL with single period."""
    decomposer = MSTLDecomposition(
        df=sample_time_series,
        value_column="value",
        timestamp_column="timestamp",
        periods=7,
    )
    result = decomposer.decompose()

    assert "trend" in result.columns
    assert "seasonal_7" in result.columns
    assert "residual" in result.columns


def test_multiple_periods(spark, multi_seasonal_time_series):
    """Test MSTL with multiple periods."""
    decomposer = MSTLDecomposition(
        df=multi_seasonal_time_series,
        value_column="value",
        timestamp_column="timestamp",
        periods=[24, 168],  # Daily and weekly
        windows=[25, 169],
    )
    result = decomposer.decompose()

    assert "trend" in result.columns
    assert "seasonal_24" in result.columns
    assert "seasonal_168" in result.columns
    assert "residual" in result.columns


def test_list_period_input(spark, sample_time_series):
    """Test MSTL with list of periods."""
    decomposer = MSTLDecomposition(
        df=sample_time_series,
        value_column="value",
        timestamp_column="timestamp",
        periods=[7, 14],
    )
    result = decomposer.decompose()

    assert "seasonal_7" in result.columns
    assert "seasonal_14" in result.columns


def test_invalid_windows_length(spark, sample_time_series):
    """Test error handling for mismatched windows length."""
    with pytest.raises(ValueError, match="Length of windows"):
        MSTLDecomposition(
            df=sample_time_series,
            value_column="value",
            timestamp_column="timestamp",
            periods=[7, 14],
            windows=[9],  # Wrong length
        )


def test_invalid_column(spark, sample_time_series):
    """Test error handling for invalid column."""
    with pytest.raises(ValueError, match="Column 'invalid' not found"):
        MSTLDecomposition(
            df=sample_time_series,
            value_column="invalid",
            timestamp_column="timestamp",
            periods=7,
        )


def test_system_type():
    """Test that system_type returns SystemType.PYSPARK"""
    assert MSTLDecomposition.system_type() == SystemType.PYSPARK


def test_libraries():
    """Test that libraries returns a Libraries instance"""
    libraries = MSTLDecomposition.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    """Test that settings returns an empty dict"""
    settings = MSTLDecomposition.settings()
    assert isinstance(settings, dict)
    assert settings == {}
