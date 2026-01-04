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

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.select_columns_by_correlation import (
    SelectColumnsByCorrelation,
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
        .appName("test-select-columns-by-correlation-wrapper")
        .getOrCreate()
    )
    yield spark
    spark.stop()

def test_missing_target_column_raises(spark):
    """Target column not present in DataFrame -> raises ValueError"""
    pdf = pd.DataFrame(
        {
            "feature_1": [1, 2, 3],
            "feature_2": [2, 3, 4],
        }
    )
    sdf = spark.createDataFrame(pdf)

    with pytest.raises(
        ValueError,
        match="Target column 'target' does not exist in the DataFrame.",
    ):
        selector = SelectColumnsByCorrelation(
            df=sdf,
            columns_to_keep=["feature_1"],
            target_col_name="target",
            correlation_threshold=0.5,
        )
        selector.filter_data()


def test_missing_columns_to_keep_raise(spark):
    """Columns in columns_to_keep not present in DataFrame -> raises ValueError"""
    pdf = pd.DataFrame(
        {
            "feature_1": [1, 2, 3],
            "target": [1, 2, 3],
        }
    )
    sdf = spark.createDataFrame(pdf)

    with pytest.raises(
        ValueError,
        match="missing in the DataFrame",
    ):
        selector = SelectColumnsByCorrelation(
            df=sdf,
            columns_to_keep=["feature_1", "non_existing_column"],
            target_col_name="target",
            correlation_threshold=0.5,
        )
        selector.filter_data()


def test_invalid_correlation_threshold_raises(spark):
    """Correlation threshold outside [0, 1] -> raises ValueError"""
    pdf = pd.DataFrame(
        {
            "feature_1": [1, 2, 3],
            "target": [1, 2, 3],
        }
    )
    sdf = spark.createDataFrame(pdf)

    # Negative threshold
    with pytest.raises(
        ValueError,
        match="correlation_threshold must be between 0.0 and 1.0",
    ):
        selector = SelectColumnsByCorrelation(
            df=sdf,
            columns_to_keep=["feature_1"],
            target_col_name="target",
            correlation_threshold=-0.1,
        )
        selector.filter_data()

    # Threshold > 1
    with pytest.raises(
        ValueError,
        match="correlation_threshold must be between 0.0 and 1.0",
    ):
        selector = SelectColumnsByCorrelation(
            df=sdf,
            columns_to_keep=["feature_1"],
            target_col_name="target",
            correlation_threshold=1.1,
        )
        selector.filter_data()


def test_target_column_not_numeric_raises(spark):
    """Non-numeric target column -> raises ValueError when building correlation matrix"""
    pdf = pd.DataFrame(
        {
            "feature_1": [1, 2, 3],
            "target": ["a", "b", "c"],  # non-numeric
        }
    )
    sdf = spark.createDataFrame(pdf)

    with pytest.raises(
        ValueError,
        match="is not numeric or cannot be used in the correlation matrix",
    ):
        selector = SelectColumnsByCorrelation(
            df=sdf,
            columns_to_keep=["feature_1"],
            target_col_name="target",
            correlation_threshold=0.5,
        )
        selector.filter_data()


def test_select_columns_by_correlation_basic(spark):
    """Selects numeric columns above correlation threshold and keeps columns_to_keep"""
    pdf = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=5, freq="h"),
            "feature_pos": [1, 2, 3, 4, 5],        # corr = 1.0 with target
            "feature_neg": [5, 4, 3, 2, 1],        # corr = -1.0 with target
            "feature_low": [0, 0, 1, 0, 0],        # low corr with target
            "constant": [10, 10, 10, 10, 10],      # no corr / NaN
            "target": [1, 2, 3, 4, 5],
        }
    )
    sdf = spark.createDataFrame(pdf)

    selector = SelectColumnsByCorrelation(
        df=sdf,
        columns_to_keep=["timestamp"],
        target_col_name="target",
        correlation_threshold=0.8,
    )
    result_pdf = selector.filter_data().toPandas()

    expected_columns = {"timestamp", "feature_pos", "feature_neg", "target"}
    assert set(result_pdf.columns) == expected_columns

    pd.testing.assert_series_equal(result_pdf["feature_pos"], pdf["feature_pos"], check_names=False)
    pd.testing.assert_series_equal(result_pdf["feature_neg"], pdf["feature_neg"], check_names=False)
    pd.testing.assert_series_equal(result_pdf["target"], pdf["target"], check_names=False)
    pd.testing.assert_series_equal(result_pdf["timestamp"], pdf["timestamp"], check_names=False)


def test_correlation_filter_includes_only_features_above_threshold(spark):
    """Features with high correlation are kept, weakly correlated ones are removed"""
    pdf = pd.DataFrame(
        {
            "keep_col": ["a", "b", "c", "d", "e"],
            "feature_strong": [1, 2, 3, 4, 5],
            "feature_weak": [0, 1, 0, 1, 0],
            "target": [2, 4, 6, 8, 10],
        }
    )
    sdf = spark.createDataFrame(pdf)

    selector = SelectColumnsByCorrelation(
        df=sdf,
        columns_to_keep=["keep_col"],
        target_col_name="target",
        correlation_threshold=0.8,
    )
    result_pdf = selector.filter_data().toPandas()

    assert "keep_col" in result_pdf.columns
    assert "target" in result_pdf.columns
    assert "feature_strong" in result_pdf.columns
    assert "feature_weak" not in result_pdf.columns


def test_correlation_filter_uses_absolute_value_for_negative_correlation(spark):
    """Features with strong negative correlation are included via absolute correlation"""
    pdf = pd.DataFrame(
        {
            "keep_col": [0, 1, 2, 3, 4],
            "feature_pos": [1, 2, 3, 4, 5],
            "feature_neg": [5, 4, 3, 2, 1],
            "target": [10, 20, 30, 40, 50],
        }
    )
    sdf = spark.createDataFrame(pdf)

    selector = SelectColumnsByCorrelation(
        df=sdf,
        columns_to_keep=["keep_col"],
        target_col_name="target",
        correlation_threshold=0.9,
    )
    result_pdf = selector.filter_data().toPandas()

    assert "keep_col" in result_pdf.columns
    assert "target" in result_pdf.columns
    assert "feature_pos" in result_pdf.columns
    assert "feature_neg" in result_pdf.columns


def test_correlation_threshold_zero_keeps_all_numeric_features(spark):
    """Threshold 0.0 -> all numeric columns are kept regardless of correlation strength"""
    pdf = pd.DataFrame(
        {
            "keep_col": ["x", "y", "z", "x"],
            "feature_1": [1, 2, 3, 4],
            "feature_2": [4, 3, 2, 1],
            "feature_weak": [0, 1, 0, 1],
            "target": [10, 20, 30, 40],
        }
    )
    sdf = spark.createDataFrame(pdf)

    selector = SelectColumnsByCorrelation(
        df=sdf,
        columns_to_keep=["keep_col"],
        target_col_name="target",
        correlation_threshold=0.0,
    )
    result_pdf = selector.filter_data().toPandas()

    expected_columns = {"keep_col", "feature_1", "feature_2", "feature_weak", "target"}
    assert set(result_pdf.columns) == expected_columns


def test_columns_to_keep_can_be_non_numeric(spark):
    """Non-numeric columns in columns_to_keep are preserved even if not in correlation matrix"""
    pdf = pd.DataFrame(
        {
            "id": ["a", "b", "c", "d"],
            "category": ["x", "x", "y", "y"],
            "feature_1": [1.0, 2.0, 3.0, 4.0],
            "target": [10.0, 20.0, 30.0, 40.0],
        }
    )
    sdf = spark.createDataFrame(pdf)

    selector = SelectColumnsByCorrelation(
        df=sdf,
        columns_to_keep=["id", "category"],
        target_col_name="target",
        correlation_threshold=0.1,
    )
    result_pdf = selector.filter_data().toPandas()

    assert "id" in result_pdf.columns
    assert "category" in result_pdf.columns
    assert "feature_1" in result_pdf.columns
    assert "target" in result_pdf.columns


def test_original_dataframe_not_modified_in_place(spark):
    """Ensure the original DataFrame is not modified in place"""
    pdf = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=3, freq="h"),
            "feature_1": [1, 2, 3],
            "feature_2": [3, 2, 1],
            "target": [1, 2, 3],
        }
    )
    sdf = spark.createDataFrame(pdf)

    original_pdf = sdf.toPandas().copy(deep=True)

    selector = SelectColumnsByCorrelation(
        df=sdf,
        columns_to_keep=["timestamp"],
        target_col_name="target",
        correlation_threshold=0.9,
    )
    _ = selector.filter_data()

    after_pdf = sdf.toPandas()
    pd.testing.assert_frame_equal(after_pdf, original_pdf)


def test_no_numeric_columns_except_target_results_in_keep_only(spark):
    """When no other numeric columns besides target exist, result contains only columns_to_keep + target"""
    pdf = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=4, freq="h"),
            "category": ["a", "b", "a", "b"],
            "target": [1, 2, 3, 4],
        }
    )
    sdf = spark.createDataFrame(pdf)

    selector = SelectColumnsByCorrelation(
        df=sdf,
        columns_to_keep=["timestamp"],
        target_col_name="target",
        correlation_threshold=0.5,
    )
    result_pdf = selector.filter_data().toPandas()

    expected_columns = {"timestamp", "target"}
    assert set(result_pdf.columns) == expected_columns


def test_system_type():
    """Test that system_type returns SystemType.PYTHON"""
    assert SelectColumnsByCorrelation.system_type() == SystemType.PYTHON


def test_libraries():
    """Test that libraries returns a Libraries instance"""
    libraries = SelectColumnsByCorrelation.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    """Test that settings returns an empty dict"""
    settings = SelectColumnsByCorrelation.settings()
    assert isinstance(settings, dict)
    assert settings == {}
