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


@pytest.fixture(scope="session")
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
                    "location": "Site1" if sensor in ["A", "B"] else "Site2",
                    "value": values[i],
                }
            )

    pdf = pd.DataFrame(data)
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


# =========================================================================
# Grouped Decomposition Tests
# =========================================================================


def test_single_group_column(spark, multi_sensor_data):
    """Test STL decomposition with single group column."""
    decomposer = STLDecomposition(
        df=multi_sensor_data,
        value_column="value",
        timestamp_column="timestamp",
        group_columns=["sensor"],
        period=7,
        robust=True,
    )

    result = decomposer.decompose()
    result_pdf = result.toPandas()

    assert "trend" in result.columns
    assert "seasonal" in result.columns
    assert "residual" in result.columns
    assert set(result_pdf["sensor"].unique()) == {"A", "B", "C"}

    # Check that each group has the correct number of observations
    for sensor in ["A", "B", "C"]:
        original_count = multi_sensor_data.filter(f"sensor = '{sensor}'").count()
        result_count = len(result_pdf[result_pdf["sensor"] == sensor])
        assert original_count == result_count


def test_multiple_group_columns(spark, multi_sensor_data):
    """Test STL decomposition with multiple group columns."""
    decomposer = STLDecomposition(
        df=multi_sensor_data,
        value_column="value",
        timestamp_column="timestamp",
        group_columns=["sensor", "location"],
        period=7,
    )

    result = decomposer.decompose()
    result_pdf = result.toPandas()

    original_pdf = multi_sensor_data.toPandas()
    original_groups = original_pdf.groupby(["sensor", "location"]).size()
    result_groups = result_pdf.groupby(["sensor", "location"]).size()

    assert len(original_groups) == len(result_groups)


def test_insufficient_data_per_group(spark):
    """Test that error is raised when a group has insufficient data."""
    np.random.seed(42)

    # Sensor A: Enough data
    dates_a = pd.date_range("2024-01-01", periods=100, freq="D")
    df_a = pd.DataFrame(
        {"timestamp": dates_a, "sensor": "A", "value": np.random.randn(100) + 10}
    )

    # Sensor B: Insufficient data
    dates_b = pd.date_range("2024-01-01", periods=10, freq="D")
    df_b = pd.DataFrame(
        {"timestamp": dates_b, "sensor": "B", "value": np.random.randn(10) + 10}
    )

    pdf = pd.concat([df_a, df_b], ignore_index=True)
    df = spark.createDataFrame(pdf)

    decomposer = STLDecomposition(
        df=df,
        value_column="value",
        timestamp_column="timestamp",
        group_columns=["sensor"],
        period=7,
    )

    with pytest.raises(ValueError, match="Group has .* observations"):
        decomposer.decompose()


def test_group_with_nans(spark):
    """Test that error is raised when a group contains NaN values."""
    np.random.seed(42)
    n_points = 100
    dates = pd.date_range("2024-01-01", periods=n_points, freq="D")

    # Sensor A: Clean data
    df_a = pd.DataFrame(
        {"timestamp": dates, "sensor": "A", "value": np.random.randn(n_points) + 10}
    )

    # Sensor B: Data with NaN
    values_b = np.random.randn(n_points) + 10
    values_b[10:15] = np.nan
    df_b = pd.DataFrame({"timestamp": dates, "sensor": "B", "value": values_b})

    pdf = pd.concat([df_a, df_b], ignore_index=True)
    df = spark.createDataFrame(pdf)

    decomposer = STLDecomposition(
        df=df,
        value_column="value",
        timestamp_column="timestamp",
        group_columns=["sensor"],
        period=7,
    )

    with pytest.raises(ValueError, match="contains NaN values"):
        decomposer.decompose()


def test_invalid_group_column(spark, multi_sensor_data):
    """Test that error is raised for invalid group column."""
    with pytest.raises(ValueError, match="Group columns .* not found"):
        STLDecomposition(
            df=multi_sensor_data,
            value_column="value",
            timestamp_column="timestamp",
            group_columns=["nonexistent_column"],
            period=7,
        )


def test_uneven_group_sizes(spark):
    """Test decomposition with groups of different sizes."""
    np.random.seed(42)

    # Sensor A: 100 points
    dates_a = pd.date_range("2024-01-01", periods=100, freq="D")
    df_a = pd.DataFrame(
        {"timestamp": dates_a, "sensor": "A", "value": np.random.randn(100) + 10}
    )

    # Sensor B: 50 points
    dates_b = pd.date_range("2024-01-01", periods=50, freq="D")
    df_b = pd.DataFrame(
        {"timestamp": dates_b, "sensor": "B", "value": np.random.randn(50) + 10}
    )

    pdf = pd.concat([df_a, df_b], ignore_index=True)
    df = spark.createDataFrame(pdf)

    decomposer = STLDecomposition(
        df=df,
        value_column="value",
        timestamp_column="timestamp",
        group_columns=["sensor"],
        period=7,
    )

    result = decomposer.decompose()
    result_pdf = result.toPandas()

    assert len(result_pdf[result_pdf["sensor"] == "A"]) == 100
    assert len(result_pdf[result_pdf["sensor"] == "B"]) == 50


def test_preserve_original_columns_grouped(spark, multi_sensor_data):
    """Test that original columns are preserved when using groups."""
    decomposer = STLDecomposition(
        df=multi_sensor_data,
        value_column="value",
        timestamp_column="timestamp",
        group_columns=["sensor"],
        period=7,
    )

    result = decomposer.decompose()
    original_cols = multi_sensor_data.columns
    result_cols = result.columns

    # All original columns should be present
    for col in original_cols:
        assert col in result_cols

    # Plus decomposition components
    assert "trend" in result_cols
    assert "seasonal" in result_cols
    assert "residual" in result_cols
