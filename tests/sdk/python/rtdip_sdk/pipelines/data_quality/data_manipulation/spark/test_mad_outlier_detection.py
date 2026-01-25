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
from pyspark.sql import SparkSession

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.mad_outlier_detection import (
    MADOutlierDetection,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    SystemType,
    Libraries,
)


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_none_df():
    with pytest.raises(ValueError, match="The DataFrame is None."):
        detector = MADOutlierDetection(None, column="Value")
        detector.filter_data()


def test_column_not_exists(spark):
    df = spark.createDataFrame([("A", 1.0), ("B", 2.0)], ["TagName", "Value"])

    with pytest.raises(ValueError, match="Column 'NonExistent' does not exist"):
        detector = MADOutlierDetection(df, column="NonExistent")
        detector.filter_data()


def test_invalid_action(spark):
    df = spark.createDataFrame([(1.0,), (2.0,), (3.0,)], ["Value"])

    with pytest.raises(ValueError, match="Invalid action"):
        detector = MADOutlierDetection(df, column="Value", action="invalid")
        detector.filter_data()


def test_invalid_n_sigma(spark):
    df = spark.createDataFrame([(1.0,), (2.0,), (3.0,)], ["Value"])

    with pytest.raises(ValueError, match="n_sigma must be positive"):
        detector = MADOutlierDetection(df, column="Value", n_sigma=-1)
        detector.filter_data()


def test_flag_action_detects_outlier(spark):
    df = spark.createDataFrame(
        [(10.0,), (11.0,), (12.0,), (10.5,), (11.5,), (1000000.0,)], ["Value"]
    )

    detector = MADOutlierDetection(df, column="Value", n_sigma=3.0, action="flag")
    result_df = detector.filter_data()

    assert "Value_is_outlier" in result_df.columns

    rows = result_df.orderBy("Value").collect()
    assert rows[-1]["Value_is_outlier"] == True
    assert rows[0]["Value_is_outlier"] == False


def test_flag_action_custom_column_name(spark):
    df = spark.createDataFrame([(10.0,), (11.0,), (1000000.0,)], ["Value"])

    detector = MADOutlierDetection(
        df, column="Value", action="flag", outlier_column="is_extreme"
    )
    result_df = detector.filter_data()

    assert "is_extreme" in result_df.columns
    assert "Value_is_outlier" not in result_df.columns


def test_replace_action(spark):
    df = spark.createDataFrame(
        [("A", 10.0), ("B", 11.0), ("C", 12.0), ("D", 1000000.0)],
        ["TagName", "Value"],
    )

    detector = MADOutlierDetection(
        df, column="Value", n_sigma=3.0, action="replace", replacement_value=-1.0
    )
    result_df = detector.filter_data()

    rows = result_df.orderBy("TagName").collect()
    assert rows[3]["Value"] == -1.0
    assert rows[0]["Value"] == 10.0


def test_replace_action_default_null(spark):
    df = spark.createDataFrame([(10.0,), (11.0,), (12.0,), (1000000.0,)], ["Value"])

    detector = MADOutlierDetection(df, column="Value", n_sigma=3.0, action="replace")
    result_df = detector.filter_data()

    rows = result_df.orderBy("Value").collect()
    assert any(row["Value"] is None for row in rows)


def test_remove_action(spark):
    df = spark.createDataFrame(
        [("A", 10.0), ("B", 11.0), ("C", 12.0), ("D", 1000000.0)],
        ["TagName", "Value"],
    )

    detector = MADOutlierDetection(df, column="Value", n_sigma=3.0, action="remove")
    result_df = detector.filter_data()

    assert result_df.count() == 3
    values = [row["Value"] for row in result_df.collect()]
    assert 1000000.0 not in values


def test_exclude_values(spark):
    df = spark.createDataFrame(
        [(10.0,), (11.0,), (12.0,), (-1.0,), (-1.0,), (1000000.0,)], ["Value"]
    )

    detector = MADOutlierDetection(
        df, column="Value", n_sigma=3.0, action="flag", exclude_values=[-1.0]
    )
    result_df = detector.filter_data()

    rows = result_df.collect()
    for row in rows:
        if row["Value"] == -1.0:
            assert row["Value_is_outlier"] == False
        elif row["Value"] == 1000000.0:
            assert row["Value_is_outlier"] == True


def test_no_outliers(spark):
    df = spark.createDataFrame([(10.0,), (10.5,), (11.0,), (10.2,), (10.8,)], ["Value"])

    detector = MADOutlierDetection(df, column="Value", n_sigma=3.0, action="flag")
    result_df = detector.filter_data()

    rows = result_df.collect()
    assert all(row["Value_is_outlier"] == False for row in rows)


def test_all_same_values(spark):
    df = spark.createDataFrame([(10.0,), (10.0,), (10.0,), (10.0,)], ["Value"])

    detector = MADOutlierDetection(df, column="Value", n_sigma=3.0, action="flag")
    result_df = detector.filter_data()

    rows = result_df.collect()
    assert all(row["Value_is_outlier"] == False for row in rows)


def test_negative_outliers(spark):
    df = spark.createDataFrame(
        [(10.0,), (11.0,), (12.0,), (10.5,), (-1000000.0,)], ["Value"]
    )

    detector = MADOutlierDetection(df, column="Value", n_sigma=3.0, action="flag")
    result_df = detector.filter_data()

    rows = result_df.collect()
    for row in rows:
        if row["Value"] == -1000000.0:
            assert row["Value_is_outlier"] == True


def test_both_direction_outliers(spark):
    df = spark.createDataFrame(
        [(-1000000.0,), (10.0,), (11.0,), (12.0,), (1000000.0,)], ["Value"]
    )

    detector = MADOutlierDetection(df, column="Value", n_sigma=3.0, action="flag")
    result_df = detector.filter_data()

    rows = result_df.collect()
    for row in rows:
        if row["Value"] in [-1000000.0, 1000000.0]:
            assert row["Value_is_outlier"] == True


def test_preserves_other_columns(spark):
    df = spark.createDataFrame(
        [
            ("A", "2024-01-01", 10.0),
            ("B", "2024-01-02", 11.0),
            ("C", "2024-01-03", 12.0),
            ("D", "2024-01-04", 1000000.0),
        ],
        ["TagName", "EventTime", "Value"],
    )

    detector = MADOutlierDetection(df, column="Value", action="flag")
    result_df = detector.filter_data()

    assert "TagName" in result_df.columns
    assert "EventTime" in result_df.columns

    rows = result_df.orderBy("TagName").collect()
    assert [row["TagName"] for row in rows] == ["A", "B", "C", "D"]


def test_with_null_values(spark):
    df = spark.createDataFrame(
        [(10.0,), (11.0,), (None,), (12.0,), (1000000.0,)], ["Value"]
    )

    detector = MADOutlierDetection(df, column="Value", n_sigma=3.0, action="flag")
    result_df = detector.filter_data()

    rows = result_df.collect()
    for row in rows:
        if row["Value"] is None:
            assert row["Value_is_outlier"] == False
        elif row["Value"] == 1000000.0:
            assert row["Value_is_outlier"] == True


def test_different_n_sigma_values(spark):
    df = spark.createDataFrame([(10.0,), (11.0,), (12.0,), (13.0,), (20.0,)], ["Value"])

    detector_strict = MADOutlierDetection(
        df, column="Value", n_sigma=1.0, action="flag"
    )
    result_strict = detector_strict.filter_data()

    detector_loose = MADOutlierDetection(
        df, column="Value", n_sigma=10.0, action="flag"
    )
    result_loose = detector_loose.filter_data()

    strict_count = sum(1 for row in result_strict.collect() if row["Value_is_outlier"])
    loose_count = sum(1 for row in result_loose.collect() if row["Value_is_outlier"])

    assert strict_count >= loose_count


def test_system_type():
    assert MADOutlierDetection.system_type() == SystemType.PYSPARK


def test_libraries():
    libraries = MADOutlierDetection.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    settings = MADOutlierDetection.settings()
    assert isinstance(settings, dict)
    assert settings == {}
