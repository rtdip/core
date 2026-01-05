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

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.rolling_statistics import (
    RollingStatistics,
    AVAILABLE_STATISTICS,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    SystemType,
    Libraries,
)


@pytest.fixture(scope="session")
def spark():
    spark_session = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    yield spark_session
    spark_session.stop()


def test_none_df():
    """None DataFrame raises error"""
    with pytest.raises(ValueError, match="The DataFrame is None."):
        roller = RollingStatistics(None, value_column="value")
        roller.filter_data()


def test_column_not_exists(spark):
    """Non-existent value column raises error"""
    df = spark.createDataFrame([(1, 10), (2, 20)], ["date", "value"])

    with pytest.raises(ValueError, match="Column 'nonexistent' does not exist"):
        roller = RollingStatistics(df, value_column="nonexistent")
        roller.filter_data()


def test_group_column_not_exists(spark):
    """Non-existent group column raises error"""
    df = spark.createDataFrame([(1, 10), (2, 20)], ["date", "value"])

    with pytest.raises(ValueError, match="Group column 'group' does not exist"):
        roller = RollingStatistics(df, value_column="value", group_columns=["group"])
        roller.filter_data()


def test_order_by_column_not_exists(spark):
    """Non-existent order by column raises error"""
    df = spark.createDataFrame([(1, 10), (2, 20)], ["date", "value"])

    with pytest.raises(
        ValueError, match="Order by column 'nonexistent' does not exist"
    ):
        roller = RollingStatistics(
            df, value_column="value", order_by_columns=["nonexistent"]
        )
        roller.filter_data()


def test_invalid_statistics(spark):
    """Invalid statistics raise error"""
    df = spark.createDataFrame([(10,), (20,), (30,)], ["value"])

    with pytest.raises(ValueError, match="Invalid statistics"):
        roller = RollingStatistics(df, value_column="value", statistics=["invalid"])
        roller.filter_data()


def test_invalid_windows(spark):
    """Invalid windows raise error"""
    df = spark.createDataFrame([(10,), (20,), (30,)], ["value"])

    with pytest.raises(ValueError, match="Windows must be a non-empty list"):
        roller = RollingStatistics(df, value_column="value", windows=[])
        roller.filter_data()

    with pytest.raises(ValueError, match="Windows must be a non-empty list"):
        roller = RollingStatistics(df, value_column="value", windows=[0])
        roller.filter_data()


def test_default_windows_and_statistics(spark):
    """Default windows are [3, 6, 12] and statistics are [mean, std]"""
    df = spark.createDataFrame([(i, i) for i in range(15)], ["id", "value"])

    roller = RollingStatistics(df, value_column="value", order_by_columns=["id"])
    result = roller.filter_data()

    assert "rolling_mean_3" in result.columns
    assert "rolling_std_3" in result.columns
    assert "rolling_mean_6" in result.columns
    assert "rolling_std_6" in result.columns
    assert "rolling_mean_12" in result.columns
    assert "rolling_std_12" in result.columns


def test_rolling_mean(spark):
    """Rolling mean is computed correctly"""
    df = spark.createDataFrame(
        [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)], ["id", "value"]
    )

    roller = RollingStatistics(
        df,
        value_column="value",
        windows=[3],
        statistics=["mean"],
        order_by_columns=["id"],
    )
    result = roller.filter_data()
    rows = result.orderBy("id").collect()

    # Window 3 rolling mean
    assert abs(rows[0]["rolling_mean_3"] - 10) < 0.01  # [10] -> mean=10
    assert abs(rows[1]["rolling_mean_3"] - 15) < 0.01  # [10, 20] -> mean=15
    assert abs(rows[2]["rolling_mean_3"] - 20) < 0.01  # [10, 20, 30] -> mean=20
    assert abs(rows[3]["rolling_mean_3"] - 30) < 0.01  # [20, 30, 40] -> mean=30
    assert abs(rows[4]["rolling_mean_3"] - 40) < 0.01  # [30, 40, 50] -> mean=40


def test_rolling_min_max(spark):
    """Rolling min and max are computed correctly"""
    df = spark.createDataFrame(
        [(1, 10), (2, 5), (3, 30), (4, 20), (5, 50)], ["id", "value"]
    )

    roller = RollingStatistics(
        df,
        value_column="value",
        windows=[3],
        statistics=["min", "max"],
        order_by_columns=["id"],
    )
    result = roller.filter_data()
    rows = result.orderBy("id").collect()

    # Window 3 rolling min and max
    assert rows[2]["rolling_min_3"] == 5  # min of [10, 5, 30]
    assert rows[2]["rolling_max_3"] == 30  # max of [10, 5, 30]


def test_rolling_std(spark):
    """Rolling std is computed correctly"""
    df = spark.createDataFrame(
        [(1, 10), (2, 10), (3, 10), (4, 10), (5, 10)], ["id", "value"]
    )

    roller = RollingStatistics(
        df,
        value_column="value",
        windows=[3],
        statistics=["std"],
        order_by_columns=["id"],
    )
    result = roller.filter_data()
    rows = result.orderBy("id").collect()

    # All same values -> std should be 0 or None
    assert rows[4]["rolling_std_3"] == 0 or rows[4]["rolling_std_3"] is None


def test_rolling_with_groups(spark):
    """Rolling statistics are computed within groups"""
    df = spark.createDataFrame(
        [
            ("A", 1, 10),
            ("A", 2, 20),
            ("A", 3, 30),
            ("B", 1, 100),
            ("B", 2, 200),
            ("B", 3, 300),
        ],
        ["group", "id", "value"],
    )

    roller = RollingStatistics(
        df,
        value_column="value",
        group_columns=["group"],
        windows=[2],
        statistics=["mean"],
        order_by_columns=["id"],
    )
    result = roller.filter_data()

    # Group A: rolling_mean_2 should be [10, 15, 25]
    group_a = result.filter(result["group"] == "A").orderBy("id").collect()
    assert abs(group_a[0]["rolling_mean_2"] - 10) < 0.01
    assert abs(group_a[1]["rolling_mean_2"] - 15) < 0.01
    assert abs(group_a[2]["rolling_mean_2"] - 25) < 0.01

    # Group B: rolling_mean_2 should be [100, 150, 250]
    group_b = result.filter(result["group"] == "B").orderBy("id").collect()
    assert abs(group_b[0]["rolling_mean_2"] - 100) < 0.01
    assert abs(group_b[1]["rolling_mean_2"] - 150) < 0.01
    assert abs(group_b[2]["rolling_mean_2"] - 250) < 0.01


def test_multiple_windows(spark):
    """Multiple windows create multiple columns"""
    df = spark.createDataFrame([(i, i) for i in range(10)], ["id", "value"])

    roller = RollingStatistics(
        df,
        value_column="value",
        windows=[2, 3],
        statistics=["mean"],
        order_by_columns=["id"],
    )
    result = roller.filter_data()

    assert "rolling_mean_2" in result.columns
    assert "rolling_mean_3" in result.columns


def test_all_statistics(spark):
    """All available statistics can be computed"""
    df = spark.createDataFrame([(i, i) for i in range(10)], ["id", "value"])

    roller = RollingStatistics(
        df,
        value_column="value",
        windows=[3],
        statistics=AVAILABLE_STATISTICS,
        order_by_columns=["id"],
    )
    result = roller.filter_data()

    for stat in AVAILABLE_STATISTICS:
        assert f"rolling_{stat}_3" in result.columns


def test_preserves_other_columns(spark):
    """Other columns are preserved"""
    df = spark.createDataFrame(
        [
            ("2024-01-01", "A", 10),
            ("2024-01-02", "B", 20),
            ("2024-01-03", "C", 30),
            ("2024-01-04", "D", 40),
            ("2024-01-05", "E", 50),
        ],
        ["date", "category", "value"],
    )

    roller = RollingStatistics(
        df,
        value_column="value",
        windows=[2],
        statistics=["mean"],
        order_by_columns=["date"],
    )
    result = roller.filter_data()

    assert "date" in result.columns
    assert "category" in result.columns
    rows = result.orderBy("date").collect()
    assert rows[0]["category"] == "A"
    assert rows[1]["category"] == "B"


def test_system_type():
    """Test that system_type returns SystemType.PYSPARK"""
    assert RollingStatistics.system_type() == SystemType.PYSPARK


def test_libraries():
    """Test that libraries returns a Libraries instance"""
    libraries = RollingStatistics.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    """Test that settings returns an empty dict"""
    settings = RollingStatistics.settings()
    assert isinstance(settings, dict)
    assert settings == {}
