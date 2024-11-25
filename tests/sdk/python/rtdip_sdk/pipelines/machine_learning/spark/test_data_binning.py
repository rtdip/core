# Copyright 2022 RTDIP
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
from pyspark.ml.linalg import Vectors
from src.sdk.python.rtdip_sdk.pipelines.machine_learning.spark.data_binning import (
    DataBinning,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[*]")
        .appName("Linear Regression Unit Test")
        .getOrCreate()
    )


@pytest.fixture(scope="function")
def sample_data(spark):
    data = [
        (Vectors.dense([1.0]),),
        (Vectors.dense([1.2]),),
        (Vectors.dense([1.5]),),
        (Vectors.dense([5.0]),),
        (Vectors.dense([5.2]),),
        (Vectors.dense([9.8]),),
        (Vectors.dense([10.0]),),
        (Vectors.dense([10.2]),),
    ]

    return spark.createDataFrame(data, ["features"])


def test_data_binning_kmeans(sample_data):
    binning = DataBinning(
        df=sample_data, column_name="features", bins=3, output_column_name="bin"
    )

    result_df = binning.train().predict()

    assert "bin" in result_df.columns
    assert result_df.count() == sample_data.count()

    bin_values = result_df.select("bin").distinct().collect()
    bin_numbers = [row.bin for row in bin_values]
    assert all(0 <= bin_num < 3 for bin_num in bin_numbers)

    for row in result_df.collect():
        if row["features"] in [1.0, 1.2, 1.5]:
            assert row["bin"] == 2
        elif row["features"] in [5.0, 5.2]:
            assert row["bin"] == 1
        elif row["features"] in [9.8, 10.0, 10.2]:
            assert row["bin"] == 0


def test_data_binning_invalid_method(sample_data):
    with pytest.raises(Exception) as exc_info:
        DataBinning(
            df=sample_data, column_name="features", bins=3, method="invalid_method"
        )
    assert "Unknown method" in str(exc_info.value)
