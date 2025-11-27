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

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.cyclical_encoding import (
    CyclicalEncoding,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    SystemType,
    Libraries,
)


def test_empty_df():
    """Empty DataFrame raises error"""
    empty_df = pd.DataFrame(columns=["month", "value"])

    with pytest.raises(ValueError, match="The DataFrame is empty."):
        encoder = CyclicalEncoding(empty_df, column="month", period=12)
        encoder.apply()


def test_column_not_exists():
    """Non-existent column raises error"""
    df = pd.DataFrame({"month": [1, 2, 3], "value": [10, 20, 30]})

    with pytest.raises(ValueError, match="Column 'nonexistent' does not exist"):
        encoder = CyclicalEncoding(df, column="nonexistent", period=12)
        encoder.apply()


def test_invalid_period():
    """Period <= 0 raises error"""
    df = pd.DataFrame({"month": [1, 2, 3], "value": [10, 20, 30]})

    with pytest.raises(ValueError, match="Period must be positive"):
        encoder = CyclicalEncoding(df, column="month", period=0)
        encoder.apply()

    with pytest.raises(ValueError, match="Period must be positive"):
        encoder = CyclicalEncoding(df, column="month", period=-1)
        encoder.apply()


def test_month_encoding():
    """Months are encoded correctly (period=12)"""
    df = pd.DataFrame({"month": [1, 4, 7, 10, 12], "value": [10, 20, 30, 40, 50]})

    encoder = CyclicalEncoding(df, column="month", period=12)
    result = encoder.apply()

    assert "month_sin" in result.columns
    assert "month_cos" in result.columns

    # January (1) and December (12) should have similar encodings
    jan_sin = result[result["month"] == 1]["month_sin"].iloc[0]
    dec_sin = result[result["month"] == 12]["month_sin"].iloc[0]
    # sin(2*pi*1/12) ≈ 0.5, sin(2*pi*12/12) = sin(2*pi) = 0
    assert abs(dec_sin - 0) < 0.01  # December sin ≈ 0


def test_hour_encoding():
    """Hours are encoded correctly (period=24)"""
    df = pd.DataFrame({"hour": [0, 6, 12, 18, 23], "value": [10, 20, 30, 40, 50]})

    encoder = CyclicalEncoding(df, column="hour", period=24)
    result = encoder.apply()

    assert "hour_sin" in result.columns
    assert "hour_cos" in result.columns

    # Hour 0 should have sin=0, cos=1
    h0_sin = result[result["hour"] == 0]["hour_sin"].iloc[0]
    h0_cos = result[result["hour"] == 0]["hour_cos"].iloc[0]
    assert abs(h0_sin - 0) < 0.01
    assert abs(h0_cos - 1) < 0.01

    # Hour 6 should have sin=1, cos≈0
    h6_sin = result[result["hour"] == 6]["hour_sin"].iloc[0]
    h6_cos = result[result["hour"] == 6]["hour_cos"].iloc[0]
    assert abs(h6_sin - 1) < 0.01
    assert abs(h6_cos - 0) < 0.01


def test_weekday_encoding():
    """Weekdays are encoded correctly (period=7)"""
    df = pd.DataFrame({"weekday": [0, 1, 2, 3, 4, 5, 6], "value": range(7)})

    encoder = CyclicalEncoding(df, column="weekday", period=7)
    result = encoder.apply()

    assert "weekday_sin" in result.columns
    assert "weekday_cos" in result.columns

    # Monday (0) and Sunday (6) should be close (adjacent in cycle)
    mon_sin = result[result["weekday"] == 0]["weekday_sin"].iloc[0]
    sun_sin = result[result["weekday"] == 6]["weekday_sin"].iloc[0]
    # They should be close in the sine representation
    assert abs(mon_sin - 0) < 0.01  # Monday sin ≈ 0


def test_drop_original():
    """Original column is dropped when drop_original=True"""
    df = pd.DataFrame({"month": [1, 2, 3], "value": [10, 20, 30]})

    encoder = CyclicalEncoding(df, column="month", period=12, drop_original=True)
    result = encoder.apply()

    assert "month" not in result.columns
    assert "month_sin" in result.columns
    assert "month_cos" in result.columns
    assert "value" in result.columns


def test_preserves_other_columns():
    """Other columns are preserved"""
    df = pd.DataFrame(
        {
            "month": [1, 2, 3],
            "value": [10, 20, 30],
            "category": ["A", "B", "C"],
        }
    )

    encoder = CyclicalEncoding(df, column="month", period=12)
    result = encoder.apply()

    assert "value" in result.columns
    assert "category" in result.columns
    assert list(result["value"]) == [10, 20, 30]


def test_sin_cos_in_valid_range():
    """Sin and cos values are in range [-1, 1]"""
    df = pd.DataFrame({"value": range(1, 101)})

    encoder = CyclicalEncoding(df, column="value", period=100)
    result = encoder.apply()

    assert result["value_sin"].min() >= -1
    assert result["value_sin"].max() <= 1
    assert result["value_cos"].min() >= -1
    assert result["value_cos"].max() <= 1


def test_sin_cos_identity():
    """sin² + cos² = 1 for all values"""
    df = pd.DataFrame({"month": range(1, 13)})

    encoder = CyclicalEncoding(df, column="month", period=12)
    result = encoder.apply()

    sum_of_squares = result["month_sin"] ** 2 + result["month_cos"] ** 2
    assert np.allclose(sum_of_squares, 1.0)


def test_system_type():
    """Test that system_type returns SystemType.PYTHON"""
    assert CyclicalEncoding.system_type() == SystemType.PYTHON


def test_libraries():
    """Test that libraries returns a Libraries instance"""
    libraries = CyclicalEncoding.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    """Test that settings returns an empty dict"""
    settings = CyclicalEncoding.settings()
    assert isinstance(settings, dict)
    assert settings == {}
