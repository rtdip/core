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

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.drop_columns_by_NaN_percentage import (
    DropByNaNPercentage,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    SystemType,
    Libraries,
)


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-drop-by-nan-percentage-wrapper")
        .getOrCreate()
    )
    yield spark
    spark.stop()

def test_negative_threshold(spark):
    """Negative NaN threshold should raise error"""
    pdf = pd.DataFrame({"a": [1, 2, 3]})
    sdf = spark.createDataFrame(pdf)

    with pytest.raises(ValueError, match="NaN Threshold is negative."):
        dropper = DropByNaNPercentage(sdf, nan_threshold=-0.1)
        dropper.filter_data()


def test_drop_columns_by_nan_percentage(spark):
    """Drop columns exceeding threshold"""
    data = {
        "a": [1, None, 3, 1, 0],          # keep
        "b": [None, None, None, None, 0], # drop
        "c": [7, 8, 9, 1, 0],             # keep
        "d": [1, None, None, None, 1],    # drop
    }
    pdf = pd.DataFrame(data)
    sdf = spark.createDataFrame(pdf)

    dropper = DropByNaNPercentage(sdf, nan_threshold=0.5)
    result_sdf = dropper.filter_data()
    result_pdf = result_sdf.toPandas()

    assert list(result_pdf.columns) == ["a", "c"]
    pd.testing.assert_series_equal(result_pdf["a"], pdf["a"], check_names=False)
    pd.testing.assert_series_equal(result_pdf["c"], pdf["c"], check_names=False)


def test_threshold_1_keeps_all_columns(spark):
    """Threshold = 1 means only 100% NaN columns removed"""
    data = {
        "a": [np.nan, 1, 2],              # 33% NaN -> keep
        "b": [np.nan, np.nan, np.nan],    # 100% -> drop
        "c": [3, 4, 5],                   # 0% -> keep
    }
    pdf = pd.DataFrame(data)
    sdf = spark.createDataFrame(pdf)

    dropper = DropByNaNPercentage(sdf, nan_threshold=1.0)
    result_pdf = dropper.filter_data().toPandas()

    assert list(result_pdf.columns) == ["a", "c"]


def test_threshold_0_removes_all_columns_with_any_nan(spark):
    """Threshold = 0 removes every column that has any NaN"""
    data = {
        "a": [1, np.nan, 3],           # contains NaN -> drop
        "b": [4, 5, 6],                # no NaN -> keep
        "c": [np.nan, np.nan, 9],      # contains NaN -> drop
    }
    pdf = pd.DataFrame(data)
    sdf = spark.createDataFrame(pdf)

    dropper = DropByNaNPercentage(sdf, nan_threshold=0.0)
    result_pdf = dropper.filter_data().toPandas()

    assert list(result_pdf.columns) == ["b"]


def test_no_columns_dropped(spark):
    """No column exceeds threshold -> expect identical DataFrame"""
    pdf = pd.DataFrame({
        "a": [1, 2, 3],
        "b": [4.0, 5.0, 6.0],
        "c": ["x", "y", "z"],
    })
    sdf = spark.createDataFrame(pdf)

    dropper = DropByNaNPercentage(sdf, nan_threshold=0.5)
    result_pdf = dropper.filter_data().toPandas()

    pd.testing.assert_frame_equal(result_pdf, pdf, check_dtype=False)


def test_original_df_not_modified(spark):
    """Ensure original DataFrame remains unchanged"""
    pdf = pd.DataFrame({
        "a": [1, None, 3],           # 33% NaN
        "b": [None, 1, None],        # 66% NaN -> drop
    })
    sdf = spark.createDataFrame(pdf)

    # Snapshot original input as pandas
    original_pdf = sdf.toPandas().copy(deep=True)

    dropper = DropByNaNPercentage(sdf, nan_threshold=0.5)
    _ = dropper.filter_data()

    # Re-read the original Spark DF; it should be unchanged
    after_pdf = sdf.toPandas()
    pd.testing.assert_frame_equal(after_pdf, original_pdf)


def test_system_type():
    """Test that system_type returns SystemType.PYTHON"""
    assert DropByNaNPercentage.system_type() == SystemType.PYTHON


def test_libraries():
    """Test that libraries returns a Libraries instance"""
    libraries = DropByNaNPercentage.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    """Test that settings returns an empty dict"""
    settings = DropByNaNPercentage.settings()
    assert isinstance(settings, dict)
    assert settings == {}
