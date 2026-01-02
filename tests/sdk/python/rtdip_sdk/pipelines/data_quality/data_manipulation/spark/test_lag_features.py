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

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.lag_features import (
    LagFeatures,
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
        lag_creator = LagFeatures(None, value_column="value")
        lag_creator.filter_data()


def test_column_not_exists(spark):
    """Non-existent value column raises error"""
    df = spark.createDataFrame([(1, 10), (2, 20)], ["date", "value"])

    with pytest.raises(ValueError, match="Column 'nonexistent' does not exist"):
        lag_creator = LagFeatures(df, value_column="nonexistent")
        lag_creator.filter_data()


def test_group_column_not_exists(spark):
    """Non-existent group column raises error"""
    df = spark.createDataFrame([(1, 10), (2, 20)], ["date", "value"])

    with pytest.raises(ValueError, match="Group column 'group' does not exist"):
        lag_creator = LagFeatures(df, value_column="value", group_columns=["group"])
        lag_creator.filter_data()


def test_order_by_column_not_exists(spark):
    """Non-existent order by column raises error"""
    df = spark.createDataFrame([(1, 10), (2, 20)], ["date", "value"])

    with pytest.raises(
        ValueError, match="Order by column 'nonexistent' does not exist"
    ):
        lag_creator = LagFeatures(
            df, value_column="value", order_by_columns=["nonexistent"]
        )
        lag_creator.filter_data()


def test_invalid_lags(spark):
    """Invalid lags raise error"""
    df = spark.createDataFrame([(10,), (20,), (30,)], ["value"])

    with pytest.raises(ValueError, match="Lags must be a non-empty list"):
        lag_creator = LagFeatures(df, value_column="value", lags=[])
        lag_creator.filter_data()

    with pytest.raises(ValueError, match="Lags must be a non-empty list"):
        lag_creator = LagFeatures(df, value_column="value", lags=[0])
        lag_creator.filter_data()

    with pytest.raises(ValueError, match="Lags must be a non-empty list"):
        lag_creator = LagFeatures(df, value_column="value", lags=[-1])
        lag_creator.filter_data()


def test_default_lags(spark):
    """Default lags are [1, 2, 3]"""
    df = spark.createDataFrame(
        [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)], ["id", "value"]
    )

    lag_creator = LagFeatures(df, value_column="value", order_by_columns=["id"])
    result = lag_creator.filter_data()

    assert "lag_1" in result.columns
    assert "lag_2" in result.columns
    assert "lag_3" in result.columns


def test_simple_lag(spark):
    """Simple lag without groups"""
    df = spark.createDataFrame(
        [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)], ["id", "value"]
    )

    lag_creator = LagFeatures(
        df, value_column="value", lags=[1, 2], order_by_columns=["id"]
    )
    result = lag_creator.filter_data()
    rows = result.orderBy("id").collect()

    # lag_1 should be [None, 10, 20, 30, 40]
    assert rows[0]["lag_1"] is None
    assert rows[1]["lag_1"] == 10
    assert rows[4]["lag_1"] == 40

    # lag_2 should be [None, None, 10, 20, 30]
    assert rows[0]["lag_2"] is None
    assert rows[1]["lag_2"] is None
    assert rows[2]["lag_2"] == 10


def test_lag_with_groups(spark):
    """Lags are computed within groups"""
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

    lag_creator = LagFeatures(
        df,
        value_column="value",
        group_columns=["group"],
        lags=[1],
        order_by_columns=["id"],
    )
    result = lag_creator.filter_data()

    # Group A: lag_1 should be [None, 10, 20]
    group_a = result.filter(result["group"] == "A").orderBy("id").collect()
    assert group_a[0]["lag_1"] is None
    assert group_a[1]["lag_1"] == 10
    assert group_a[2]["lag_1"] == 20

    # Group B: lag_1 should be [None, 100, 200]
    group_b = result.filter(result["group"] == "B").orderBy("id").collect()
    assert group_b[0]["lag_1"] is None
    assert group_b[1]["lag_1"] == 100
    assert group_b[2]["lag_1"] == 200


def test_multiple_group_columns(spark):
    """Lags with multiple group columns"""
    df = spark.createDataFrame(
        [
            ("R1", "A", 1, 10),
            ("R1", "A", 2, 20),
            ("R1", "B", 1, 100),
            ("R1", "B", 2, 200),
        ],
        ["region", "product", "id", "value"],
    )

    lag_creator = LagFeatures(
        df,
        value_column="value",
        group_columns=["region", "product"],
        lags=[1],
        order_by_columns=["id"],
    )
    result = lag_creator.filter_data()

    # R1-A group: lag_1 should be [None, 10]
    r1a = (
        result.filter((result["region"] == "R1") & (result["product"] == "A"))
        .orderBy("id")
        .collect()
    )
    assert r1a[0]["lag_1"] is None
    assert r1a[1]["lag_1"] == 10

    # R1-B group: lag_1 should be [None, 100]
    r1b = (
        result.filter((result["region"] == "R1") & (result["product"] == "B"))
        .orderBy("id")
        .collect()
    )
    assert r1b[0]["lag_1"] is None
    assert r1b[1]["lag_1"] == 100


def test_custom_prefix(spark):
    """Custom prefix for lag columns"""
    df = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "value"])

    lag_creator = LagFeatures(
        df, value_column="value", lags=[1], prefix="shifted", order_by_columns=["id"]
    )
    result = lag_creator.filter_data()

    assert "shifted_1" in result.columns
    assert "lag_1" not in result.columns


def test_preserves_other_columns(spark):
    """Other columns are preserved"""
    df = spark.createDataFrame(
        [("2024-01-01", "A", 10), ("2024-01-02", "B", 20), ("2024-01-03", "C", 30)],
        ["date", "category", "value"],
    )

    lag_creator = LagFeatures(
        df, value_column="value", lags=[1], order_by_columns=["date"]
    )
    result = lag_creator.filter_data()

    assert "date" in result.columns
    assert "category" in result.columns
    rows = result.orderBy("date").collect()
    assert rows[0]["category"] == "A"
    assert rows[1]["category"] == "B"


def test_system_type():
    """Test that system_type returns SystemType.PYSPARK"""
    assert LagFeatures.system_type() == SystemType.PYSPARK


def test_libraries():
    """Test that libraries returns a Libraries instance"""
    libraries = LagFeatures.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    """Test that settings returns an empty dict"""
    settings = LagFeatures.settings()
    assert isinstance(settings, dict)
    assert settings == {}
