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

from src.sdk.python.rtdip_sdk.pipelines.decomposition.pandas.classical_decomposition import (
    ClassicalDecomposition,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    SystemType,
    Libraries,
)


@pytest.fixture
def sample_time_series():
    """Create a sample time series with trend, seasonality, and noise."""
    np.random.seed(42)
    n_points = 365
    dates = pd.date_range("2024-01-01", periods=n_points, freq="D")
    trend = np.linspace(10, 20, n_points)
    seasonal = 5 * np.sin(2 * np.pi * np.arange(n_points) / 7)
    noise = np.random.randn(n_points) * 0.5
    value = trend + seasonal + noise

    return pd.DataFrame({"timestamp": dates, "value": value})


@pytest.fixture
def multiplicative_time_series():
    """Create a time series suitable for multiplicative decomposition."""
    np.random.seed(42)
    n_points = 365
    dates = pd.date_range("2024-01-01", periods=n_points, freq="D")
    trend = np.linspace(10, 20, n_points)
    seasonal = 1 + 0.3 * np.sin(2 * np.pi * np.arange(n_points) / 7)
    noise = 1 + np.random.randn(n_points) * 0.05
    value = trend * seasonal * noise

    return pd.DataFrame({"timestamp": dates, "value": value})


def test_additive_decomposition(sample_time_series):
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


def test_multiplicative_decomposition(multiplicative_time_series):
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


def test_invalid_model(sample_time_series):
    """Test error handling for invalid model."""
    with pytest.raises(ValueError, match="Invalid model"):
        ClassicalDecomposition(
            df=sample_time_series,
            value_column="value",
            timestamp_column="timestamp",
            model="invalid",
            period=7,
        )


def test_invalid_column(sample_time_series):
    """Test error handling for invalid column."""
    with pytest.raises(ValueError, match="Column 'invalid' not found"):
        ClassicalDecomposition(
            df=sample_time_series,
            value_column="invalid",
            timestamp_column="timestamp",
            model="additive",
            period=7,
        )


def test_nan_values(sample_time_series):
    """Test error handling for NaN values."""
    df = sample_time_series.copy()
    df.loc[50, "value"] = np.nan

    decomposer = ClassicalDecomposition(
        df=df,
        value_column="value",
        timestamp_column="timestamp",
        model="additive",
        period=7,
    )

    with pytest.raises(ValueError, match="contains NaN values"):
        decomposer.decompose()


def test_insufficient_data():
    """Test error handling for insufficient data."""
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=10, freq="D"),
            "value": np.random.randn(10),
        }
    )

    with pytest.raises(ValueError, match="Time series length"):
        ClassicalDecomposition(
            df=df,
            value_column="value",
            timestamp_column="timestamp",
            model="additive",
            period=7,
        )


def test_preserves_original(sample_time_series):
    """Test that decomposition doesn't modify original DataFrame."""
    original_df = sample_time_series.copy()

    decomposer = ClassicalDecomposition(
        df=sample_time_series,
        value_column="value",
        timestamp_column="timestamp",
        model="additive",
        period=7,
    )
    decomposer.decompose()

    assert "trend" not in sample_time_series.columns
    pd.testing.assert_frame_equal(sample_time_series, original_df)


def test_system_type():
    """Test that system_type returns SystemType.PYTHON"""
    assert ClassicalDecomposition.system_type() == SystemType.PYTHON


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


def test_grouped_single_column():
    """Test Classical decomposition with single group column."""
    np.random.seed(42)
    n_points = 100
    dates = pd.date_range("2024-01-01", periods=n_points, freq="D")

    data = []
    for sensor in ["A", "B"]:
        trend = np.linspace(10, 20, n_points)
        seasonal = 5 * np.sin(2 * np.pi * np.arange(n_points) / 7)
        noise = np.random.randn(n_points) * 0.5
        values = trend + seasonal + noise

        for i in range(n_points):
            data.append({"timestamp": dates[i], "sensor": sensor, "value": values[i]})

    df = pd.DataFrame(data)

    decomposer = ClassicalDecomposition(
        df=df,
        value_column="value",
        timestamp_column="timestamp",
        group_columns=["sensor"],
        model="additive",
        period=7,
    )

    result = decomposer.decompose()

    assert "trend" in result.columns
    assert "seasonal" in result.columns
    assert set(result["sensor"].unique()) == {"A", "B"}


def test_grouped_multiplicative():
    """Test Classical multiplicative decomposition with groups."""
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

    df = pd.DataFrame(data)

    decomposer = ClassicalDecomposition(
        df=df,
        value_column="value",
        timestamp_column="timestamp",
        group_columns=["sensor"],
        model="multiplicative",
        period=7,
    )

    result = decomposer.decompose()
    assert len(result) == len(df)
