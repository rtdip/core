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

from src.sdk.python.rtdip_sdk.pipelines.decomposition.spark.stl_decomposition import (
    STLDecomposition,
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


def test_basic_decomposition(spark, sample_time_series):
    """Test basic STL decomposition."""
    decomposer = STLDecomposition(
        df=sample_time_series,
        value_column="value",
        timestamp_column="timestamp",
        period=7,
    )
    result = decomposer.decompose()

    assert "trend" in result.columns
    assert "seasonal" in result.columns
    assert "residual" in result.columns
    assert result.count() == sample_time_series.count()


def test_robust_option(spark, sample_time_series):
    """Test STL with robust option."""
    pdf = sample_time_series.toPandas()
    pdf.loc[50, "value"] = pdf.loc[50, "value"] + 50  # Add outlier
    df = spark.createDataFrame(pdf)

    decomposer = STLDecomposition(
        df=df, value_column="value", timestamp_column="timestamp", period=7, robust=True
    )
    result = decomposer.decompose()

    assert "trend" in result.columns
    assert "seasonal" in result.columns
    assert "residual" in result.columns


def test_custom_parameters(spark, sample_time_series):
    """Test with custom seasonal and trend parameters."""
    decomposer = STLDecomposition(
        df=sample_time_series,
        value_column="value",
        timestamp_column="timestamp",
        period=7,
        seasonal=13,
        trend=15,
    )
    result = decomposer.decompose()

    assert "trend" in result.columns


def test_invalid_column(spark, sample_time_series):
    """Test error handling for invalid column."""
    with pytest.raises(ValueError, match="Column 'invalid' not found"):
        STLDecomposition(
            df=sample_time_series,
            value_column="invalid",
            timestamp_column="timestamp",
            period=7,
        )


def test_system_type():
    """Test that system_type returns SystemType.PYSPARK"""
    assert STLDecomposition.system_type() == SystemType.PYSPARK


def test_libraries():
    """Test that libraries returns a Libraries instance"""
    libraries = STLDecomposition.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    """Test that settings returns an empty dict"""
    settings = STLDecomposition.settings()
    assert isinstance(settings, dict)
    assert settings == {}
