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

from src.sdk.python.rtdip_sdk.pipelines.decomposition.spark.classical_decomposition import (
    ClassicalDecomposition,
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
def multiplicative_time_series(spark):
    """Create a time series suitable for multiplicative decomposition."""
    np.random.seed(42)
    n_points = 365
    dates = pd.date_range("2024-01-01", periods=n_points, freq="D")
    trend = np.linspace(10, 20, n_points)
    seasonal = 1 + 0.3 * np.sin(2 * np.pi * np.arange(n_points) / 7)
    noise = 1 + np.random.randn(n_points) * 0.05
    value = trend * seasonal * noise

    pdf = pd.DataFrame({"timestamp": dates, "value": value})
    return spark.createDataFrame(pdf)


@pytest.fixture
def multi_sensor_data(spark):
    """Create multi-sensor time series data."""
    np.random.seed(42)
    n_points = 100
    dates = pd.date_range("2024-01-01", periods=n_points, freq="D")

    data = []
    for sensor in ["A", "B", "C"]:
        trend = np.linspace(10, 20, n_points) + np.random.rand() * 5
        seasonal = 5 * np.sin(2 * np.pi * np.arange(n_points) / 7)
        noise = np.random.randn(n_points) * 0.5
        values = trend + seasonal + noise

        for i in range(n_points):
            data.append(
                {
                    "timestamp": dates[i],
                    "sensor": sensor,
                    "value": values[i],
                }
            )

    pdf = pd.DataFrame(data)
    return spark.createDataFrame(pdf)


def test_additive_decomposition(spark, sample_time_series):
    """Test additive decomposition."""
    decomposer = ClassicalDecomposition(
        df=sample_time_series,
        value_column="value",
        timestamp_column="timestamp",
        model="additive",
        period=7,
    )
    result = decomposer.decompose()

    assert "trend" in result.columns
    assert "seasonal" in result.columns
    assert "residual" in result.columns


def test_multiplicative_decomposition(spark, multiplicative_time_series):
    """Test multiplicative decomposition."""
    decomposer = ClassicalDecomposition(
        df=multiplicative_time_series,
        value_column="value",
        timestamp_column="timestamp",
        model="multiplicative",
        period=7,
    )
    result = decomposer.decompose()

    assert "trend" in result.columns
    assert "seasonal" in result.columns
    assert "residual" in result.columns


def test_invalid_model(spark, sample_time_series):
    """Test error handling for invalid model."""
    with pytest.raises(ValueError, match="Invalid model"):
        ClassicalDecomposition(
            df=sample_time_series,
            value_column="value",
            timestamp_column="timestamp",
            model="invalid",
            period=7,
        )


def test_invalid_column(spark, sample_time_series):
    """Test error handling for invalid column."""
    with pytest.raises(ValueError, match="Column 'invalid' not found"):
        ClassicalDecomposition(
            df=sample_time_series,
            value_column="invalid",
            timestamp_column="timestamp",
            model="additive",
            period=7,
        )


def test_system_type():
    """Test that system_type returns SystemType.PYSPARK"""
    assert ClassicalDecomposition.system_type() == SystemType.PYSPARK


def test_libraries():
    """Test that libraries returns a Libraries instance"""
    libraries = ClassicalDecomposition.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    """Test that settings returns an empty dict"""
    settings = ClassicalDecomposition.settings()
    assert isinstance(settings, dict)
    assert settings == {}


# =========================================================================
# Grouped Decomposition Tests
# =========================================================================


def test_grouped_single_column(spark, multi_sensor_data):
    """Test classical decomposition with single group column."""
    decomposer = ClassicalDecomposition(
        df=multi_sensor_data,
        value_column="value",
        timestamp_column="timestamp",
        group_columns=["sensor"],
        model="additive",
        period=7,
    )

    result = decomposer.decompose()
    result_pdf = result.toPandas()

    assert "trend" in result.columns
    assert "seasonal" in result.columns
    assert "residual" in result.columns
    assert set(result_pdf["sensor"].unique()) == {"A", "B", "C"}

    # Verify each group has correct number of observations
    for sensor in ["A", "B", "C"]:
        original_count = multi_sensor_data.filter(f"sensor = '{sensor}'").count()
        result_count = len(result_pdf[result_pdf["sensor"] == sensor])
        assert original_count == result_count


def test_grouped_multiplicative(spark):
    """Test multiplicative decomposition with grouped data."""
    np.random.seed(42)
    n_points = 100
    dates = pd.date_range("2024-01-01", periods=n_points, freq="D")

    data = []
    for sensor in ["A", "B"]:
        trend = np.linspace(10, 20, n_points)
        seasonal = 1 + 0.3 * np.sin(2 * np.pi * np.arange(n_points) / 7)
        noise = 1 + np.random.randn(n_points) * 0.05
        values = trend * seasonal * noise

        for i in range(n_points):
            data.append({"timestamp": dates[i], "sensor": sensor, "value": values[i]})

    pdf = pd.DataFrame(data)
    df = spark.createDataFrame(pdf)

    decomposer = ClassicalDecomposition(
        df=df,
        value_column="value",
        timestamp_column="timestamp",
        group_columns=["sensor"],
        model="multiplicative",
        period=7,
    )

    result = decomposer.decompose()

    assert "trend" in result.columns
    assert "seasonal" in result.columns
    assert "residual" in result.columns
