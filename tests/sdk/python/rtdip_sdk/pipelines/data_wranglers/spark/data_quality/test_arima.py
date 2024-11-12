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
from abc import ABCMeta

import numpy as np
import pandas
import pandas as pd
import pytest

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from src.sdk.python.rtdip_sdk.pipelines.data_wranglers import ArimaPrediction


@pytest.fixture(scope="session")
def spark_session():
    # Additional config needed since older PySpark <3.5 have troubles converting data with timestamps to pandas Dataframes
    return (
        SparkSession.builder.master("local[2]")
        .appName("test")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def test_nonexistent_column_arima(spark_session: SparkSession):
    input_df = spark_session.createDataFrame(
        [
            (1.0,),
            (2.0,),
        ],
        ["Value"],
    )

    with pytest.raises(ValueError):
        ArimaPrediction(input_df, column_name="NonexistingColumn")


def test_single_column_prediction_arima(spark_session: SparkSession):
    df = pandas.DataFrame()

    np.random.seed(0)
    arr_len = 40
    h_a_l = int(arr_len / 2)
    df["Value"] = np.random.rand(arr_len) + np.sin(
        np.linspace(0, arr_len / 10, num=arr_len)
    )
    df["Value2"] = (
        np.random.rand(arr_len) + np.cos(np.linspace(0, arr_len / 2, num=arr_len)) + 5
    )
    df["index"] = np.asarray(
        pandas.date_range(start="1/1/2024", end="2/1/2024", periods=arr_len)
    )
    df = df.set_index(pd.DatetimeIndex(df["index"]))

    learn_df = df.head(h_a_l)

    input_df = spark_session.createDataFrame(
        learn_df,
        ["Value", "Value2", "index"],
    )

    # input_df.show(n=50)

    arima_comp = ArimaPrediction(
        input_df,
        column_name="Value",
        number_of_data_points_to_analyze=h_a_l,
        number_of_data_points_to_predict=h_a_l,
        order=(3, 0, 0),
        seasonal_order=(3, 0, 0, 62),
    )
    forecasted_df = arima_comp.filter()

    assert isinstance(forecasted_df, DataFrame)

    assert forecasted_df.columns == forecasted_df.columns
    assert forecasted_df.count() == arr_len
