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

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.drop_empty_columns import (
    DropEmptyAndUselessColumns,
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
        .appName("test-drop-empty-and-useless-columns-wrapper")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_drop_empty_and_constant_columns(spark):
    """Drops fully empty and constant columns"""
    data = {
        "a": [1, 2, 3],                 # informative
        "b": [np.nan, np.nan, np.nan],  # all NaN -> drop
        "c": [5, 5, 5],                 # constant -> drop
        "d": [np.nan, 7, 7],            # non-NaN all equal -> drop
        "e": [1, np.nan, 2],            # at least 2 unique non-NaN -> keep
    }
    pdf = pd.DataFrame(data)
    sdf = spark.createDataFrame(pdf)

    cleaner = DropEmptyAndUselessColumns(sdf)
    result_pdf = cleaner.filter_data().toPandas()

    # Expected kept columns
    assert list(result_pdf.columns) == ["a", "e"]

    # Check values preserved for kept columns
    pd.testing.assert_series_equal(result_pdf["a"], pdf["a"], check_names=False)
    pd.testing.assert_series_equal(result_pdf["e"], pdf["e"], check_names=False)


def test_mostly_nan_but_multiple_unique_values_kept(spark):
    """Keeps column with multiple unique non-NaN values even if many NaNs"""
    data = {
        "a": [np.nan, 1, np.nan, 2, np.nan],  # two unique non-NaN -> keep
        "b": [np.nan, np.nan, np.nan, np.nan, np.nan],  # all NaN -> drop
    }
    pdf = pd.DataFrame(data)
    sdf = spark.createDataFrame(pdf)

    cleaner = DropEmptyAndUselessColumns(sdf)
    result_pdf = cleaner.filter_data().toPandas()

    assert "a" in result_pdf.columns
    assert "b" not in result_pdf.columns
    assert result_pdf["a"].nunique(dropna=True) == 2


def test_no_columns_to_drop_returns_same_columns(spark):
    """No empty or constant columns -> DataFrame unchanged (column-wise)"""
    data = {
        "a": [1, 2, 3],
        "b": [1.0, 1.5, 2.0],
        "c": ["x", "y", "z"],
    }
    pdf = pd.DataFrame(data)
    sdf = spark.createDataFrame(pdf)

    cleaner = DropEmptyAndUselessColumns(sdf)
    result_pdf = cleaner.filter_data().toPandas()

    assert list(result_pdf.columns) == list(pdf.columns)
    pd.testing.assert_frame_equal(result_pdf, pdf, check_dtype=False)


def test_original_dataframe_not_modified_in_place(spark):
    """Ensure the original DataFrame is not modified in place"""
    data = {
        "a": [1, 2, 3],
        "b": [np.nan, np.nan, np.nan],  # will be dropped in result
    }
    pdf = pd.DataFrame(data)
    sdf = spark.createDataFrame(pdf)

    # Snapshot original input as pandas
    original_pdf = sdf.toPandas().copy(deep=True)

    cleaner = DropEmptyAndUselessColumns(sdf)
    result_pdf = cleaner.filter_data().toPandas()

    # Original Spark DF should remain unchanged
    after_pdf = sdf.toPandas()
    pd.testing.assert_frame_equal(after_pdf, original_pdf)

    # Result DataFrame has only the informative column
    assert list(result_pdf.columns) == ["a"]


def test_system_type():
    """Test that system_type returns SystemType.PYTHON"""
    assert DropEmptyAndUselessColumns.system_type() == SystemType.PYTHON


def test_libraries():
    """Test that libraries returns a Libraries instance"""
    libraries = DropEmptyAndUselessColumns.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    """Test that settings returns an empty dict"""
    settings = DropEmptyAndUselessColumns.settings()
    assert isinstance(settings, dict)
    assert settings == {}
