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
from pyspark.sql import SparkSession
import pytest
from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.k_sigma_anomaly_detection import (
    KSigmaAnomalyDetection,
)
import os

# Normal data mean=10 stddev=5 + 3 anomalies
# fmt: off
normal_input_values = [ 5.19811497,  8.34437927,  3.62104032, 10.02819525,  6.1183447 ,
                20.10067378, 10.32313075, 14.090119  , 21.43078927,  2.76624332,
                10.84089416,  1.90722629, 11.19750641, 13.70925639,  5.61011921,
                4.50072694, 13.79440311, 13.30173747,  7.07183589, 12.79853139, 100]

normal_expected_values = [ 5.19811497,  8.34437927,  3.62104032, 10.02819525,  6.1183447 ,
                   20.10067378, 10.32313075, 14.090119  , 21.43078927,  2.76624332,
                   10.84089416,  1.90722629, 11.19750641, 13.70925639,  5.61011921,
                   4.50072694, 13.79440311, 13.30173747,  7.07183589, 12.79853139]
# fmt: on

# These values are tricky for the mean method, as the anomaly has a big effect on the mean
input_values = [1, 2, 3, 4, 20]
expected_values = [1, 2, 3, 4]


def test_filter_with_mean(spark_session: SparkSession):
    # Test with normal data
    normal_input_df = spark_session.createDataFrame(
        [(float(num),) for num in normal_input_values], schema=["value"]
    )
    normal_expected_df = spark_session.createDataFrame(
        [(float(num),) for num in normal_expected_values], schema=["value"]
    )

    normal_filtered_df = KSigmaAnomalyDetection(
        spark_session,
        normal_input_df,
        column_names=["value"],
        k_value=3,
        use_median=False,
    ).filter_data()

    assert normal_expected_df.collect() == normal_filtered_df.collect()

    # Test with data that has an anomaly that shifts the mean significantly
    input_df = spark_session.createDataFrame(
        [(float(num),) for num in input_values], schema=["value"]
    )
    expected_df = spark_session.createDataFrame(
        [(float(num),) for num in expected_values], schema=["value"]
    )

    filtered_df = KSigmaAnomalyDetection(
        spark_session, input_df, column_names=["value"], k_value=3, use_median=False
    ).filter_data()

    assert expected_df.collect() != filtered_df.collect()


def test_filter_with_median(spark_session: SparkSession):
    # Test with normal data
    normal_input_df = spark_session.createDataFrame(
        [(float(num),) for num in normal_input_values], schema=["value"]
    )
    normal_expected_df = spark_session.createDataFrame(
        [(float(num),) for num in normal_expected_values], schema=["value"]
    )

    normal_filtered_df = KSigmaAnomalyDetection(
        spark_session,
        normal_input_df,
        column_names=["value"],
        k_value=3,
        use_median=True,
    ).filter_data()

    assert normal_expected_df.collect() == normal_filtered_df.collect()

    # Test with data that has an anomaly that shifts the mean significantly
    input_df = spark_session.createDataFrame(
        [(float(num),) for num in input_values], schema=["value"]
    )
    expected_df = spark_session.createDataFrame(
        [(float(num),) for num in expected_values], schema=["value"]
    )

    filtered_df = KSigmaAnomalyDetection(
        spark_session, input_df, column_names=["value"], k_value=3, use_median=True
    ).filter_data()

    assert expected_df.collect() == filtered_df.collect()


def test_filter_with_wrong_types(spark_session: SparkSession):
    wrong_column_type_df = spark_session.createDataFrame(
        [(f"string {i}",) for i in range(10)], schema=["value"]
    )

    # wrong value type
    with pytest.raises(ValueError):
        KSigmaAnomalyDetection(
            spark_session,
            wrong_column_type_df,
            column_names=["value"],
            k_value=3,
            use_median=True,
        ).filter_data()

    # missing column
    with pytest.raises(ValueError):
        KSigmaAnomalyDetection(
            spark_session,
            wrong_column_type_df,
            column_names=["$value"],
            k_value=3,
            use_median=True,
        ).filter_data()


def test_large_dataset(spark_session):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, "../../test_data.csv")
    df = spark_session.read.option("header", "true").csv(file_path)

    assert df.count() > 0, "Dataframe was not loaded correct"

    KSigmaAnomalyDetection(spark_session, df, column_names=["Value"]).filter_data()
