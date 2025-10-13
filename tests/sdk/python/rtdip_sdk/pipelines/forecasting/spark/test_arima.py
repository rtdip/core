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

import numpy as np
import pandas as pd
import pytest
import os

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    FloatType,
)

from src.sdk.python.rtdip_sdk._sdk_utils.pandas import (
    _prepare_pandas_to_convert_to_spark,
)
from src.sdk.python.rtdip_sdk.pipelines.forecasting.spark.arima import (
    ArimaPrediction,
)
from src.sdk.python.rtdip_sdk.pipelines.forecasting.spark.auto_arima import (
    ArimaAutoPrediction,
)

# Testcases to add:

# = TEST COLUMN NAME FINDER =
# Non-existing columns
# Wrong columns given
# correct columns given

# = COLUMN-BASED =

# = SOURCE-BASED =
# Pass additional future data -> should not be discarded

# = PMD-Arima =
# Column-based
# Source-based


@pytest.fixture(scope="session")
def spark_session():
    # Additional config needed since older PySpark <3.5 have troubles converting data with timestamps to pandas Dataframes
    return (
        SparkSession.builder.master("local[2]")
        .appName("test")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def historic_data():
    hist_data = [
        ("A2PS64V0J.:ZUX09R", "2024-01-01 03:29:21", "Good", "1.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 07:32:55", "Good", "2.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 11:36:29", "Good", "3.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 15:39:03", "Good", "4.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 19:42:37", "Good", "5.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 23:46:10", "Good", "6.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45", "Good", "7.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11", "Good", "8.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42", "Good", "9.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12", "Good", "10.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 20:13:46", "Good", "11.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-03 00:07:20", "Good", "12.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-03 04:10:50", "Good", "13.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-03 08:14:20", "Good", "14.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-03 12:18:02", "Good", "15.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-03 16:21:30", "Good", "16.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-03 20:25:10", "Good", "17.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-04 00:28:44", "Good", "18.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-04 04:32:18", "Good", "19.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-04 08:35:52", "Good", "20.0"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:01:43", "Good", "4686.26"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:02:44", "Good", "4691.1616"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:03:44", "Good", "4688.019"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:04:44", "Good", "4686.26"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:05:44", "Good", "4691.1616"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:06:44", "Good", "4694.203"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:07:44", "Good", "4693.92"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:08:44", "Good", "4691.6475"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:09:44", "Good", "4688.722"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:10:44", "Good", "4686.481"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:11:46", "Good", "4686.26"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:12:46", "Good", "4688.637"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:13:46", "Good", "4691.1616"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:14:46", "Good", "4691.4985"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:15:46", "Good", "4690.817"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:16:47", "Good", "4691.1616"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:17:47", "Good", "4693.7354"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:18:47", "Good", "4696.372"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:19:48", "Good", "4696.0635"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:20:48", "Good", "4691.1616"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:21:48", "Good", "4684.8516"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:22:48", "Good", "4679.2305"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:23:48", "Good", "4675.784"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:24:48", "Good", "4675.998"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:25:50", "Good", "4681.358"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:26:50", "Good", "4691.1616"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:27:50", "Good", "4696.0635"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:28:50", "Good", "4691.1616"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:29:50", "Good", "4691.056"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:30:50", "Good", "4694.813"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:31:51", "Good", "4696.0635"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:32:52", "Good", "4691.1616"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:33:52", "Good", "4685.6963"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:34:52", "Good", "4681.356"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:35:52", "Good", "4678.175"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:36:52", "Good", "4676.186"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:37:52", "Good", "4675.423"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:38:52", "Good", "4675.9185"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:39:52", "Good", "4677.707"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:40:52", "Good", "4680.8213"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:41:52", "Good", "4685.295"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:42:52", "Good", "4691.1616"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:42:54", "Good", "4696.0635"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:43:52", "Good", "4692.863"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:43:54", "Good", "4691.1616"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:44:54", "Good", "4696.0635"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:45:54", "Good", "4691.1616"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:46:55", "Good", "4696.0635"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:47:55", "Good", "4691.1616"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:48:55", "Good", "4689.178"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:49:55", "Good", "4692.111"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:50:55", "Good", "4695.794"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:51:56", "Good", "4696.0635"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:52:56", "Good", "4691.1616"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:53:56", "Good", "4687.381"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:54:56", "Good", "4687.1104"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:55:57", "Good", "4691.1616"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:56:58", "Good", "4696.0635"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:57:58", "Good", "4691.1616"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:58:58", "Good", "4693.161"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 00:59:59", "Good", "4696.0635"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:00:59", "Good", "4691.1616"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:01:59", "Good", "4688.2207"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:02:59", "Good", "4689.07"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:03:59", "Good", "4692.1904"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:05:01", "Good", "4696.0635"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:06:01", "Good", "4699.3506"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:07:01", "Good", "4701.433"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:08:01", "Good", "4701.872"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:09:01", "Good", "4700.228"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:10:02", "Good", "4696.0635"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:11:03", "Good", "4691.1616"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:12:03", "Good", "4692.6973"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:13:06", "Good", "4696.0635"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:14:06", "Good", "4695.113"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:15:06", "Good", "4691.5415"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:16:06", "Good", "4689.0054"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:17:07", "Good", "4691.1616"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:18:07", "Good", "4696.0635"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:19:07", "Good", "4688.7515"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:20:07", "Good", "4686.26"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:21:07", "Good", "4700.966"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:22:07", "Good", "4700.935"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:23:07", "Good", "4687.808"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:24:07", "Good", "4675.1323"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:25:09", "Good", "4676.456"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:26:09", "Good", "4696.0635"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:27:09", "Good", "4708.868"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:28:09", "Good", "4711.2476"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:29:09", "Good", "4707.2603"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:30:09", "Good", "4700.966"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:31:09", "Good", "4695.7764"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:32:09", "Good", "4692.5146"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:33:09", "Good", "4691.358"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:34:09", "Good", "4692.482"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:35:10", "Good", "4696.0635"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:36:10", "Good", "4700.966"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:37:10", "Good", "4702.4126"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:38:10", "Good", "4700.763"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:39:10", "Good", "4697.9897"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:40:11", "Good", "4696.0635"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:41:11", "Good", "4696.747"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:42:11", "Good", "4700.966"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:43:11", "Good", "4705.8677"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:44:11", "Good", "4700.966"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:45:11", "Good", "4695.9624"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:46:11", "Good", "4696.0635"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:47:11", "Good", "4700.966"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:48:11", "Good", "4702.187"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:49:11", "Good", "4699.401"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:50:11", "Good", "4695.0015"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:51:11", "Good", "4691.3823"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:52:11", "Good", "4690.9385"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:53:13", "Good", "4696.0635"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:54:13", "Good", "4700.966"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:55:13", "Good", "4686.26"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2023-12-31 01:56:13", "Good", "4700.966"),
    ]
    return hist_data


@pytest.fixture(scope="session")
def source_based_synthetic_data():
    output_object = {}

    df1 = pd.DataFrame()
    df2 = pd.DataFrame()
    np.random.seed(0)

    arr_len = 100
    h_a_l = int(arr_len / 2)
    df1["Value"] = np.random.rand(arr_len) + np.sin(
        np.linspace(0, arr_len / 2, num=arr_len)
    )
    df2["Value"] = (
        df1["Value"] * 2 + np.cos(np.linspace(0, arr_len / 2, num=arr_len)) + 5
    )
    df1["index"] = np.asarray(
        pd.date_range(start="1/1/2024", end="2/1/2024", periods=arr_len)
    ).astype(str)
    df2["index"] = np.asarray(
        pd.date_range(start="1/1/2024", end="2/1/2024", periods=arr_len)
    ).astype(str)
    df1["TagName"] = "PrimarySensor"
    df2["TagName"] = "SecondarySensor"
    df1["Status"] = "Good"
    df2["Status"] = "Good"

    output_object["df1"] = df1
    output_object["df2"] = df2
    output_object["arr_len"] = arr_len
    output_object["h_a_l"] = h_a_l
    output_object["half_df1_full_df2"] = _prepare_pandas_to_convert_to_spark(
        pd.concat([df1.head(h_a_l), df2])
    )
    output_object["full_df1_full_df2"] = _prepare_pandas_to_convert_to_spark(
        pd.concat([df1, df2])
    )
    output_object["full_df1_half_df2"] = _prepare_pandas_to_convert_to_spark(
        pd.concat([df1, df2.head(h_a_l)])
    )
    output_object["half_df1_half_df2"] = _prepare_pandas_to_convert_to_spark(
        pd.concat([df1.head(h_a_l), df2.head(h_a_l)])
    )
    return output_object


@pytest.fixture(scope="session")
def column_based_synthetic_data():
    output_object = {}

    df1 = pd.DataFrame()
    np.random.seed(0)

    arr_len = 100
    h_a_l = int(arr_len / 2)
    idx_start = "1/1/2024"
    idx_end = "2/1/2024"

    df1["PrimarySensor"] = np.random.rand(arr_len) + np.sin(
        np.linspace(0, arr_len / 2, num=arr_len)
    )
    df1["SecondarySensor"] = (
        df1["PrimarySensor"] * 2 + np.cos(np.linspace(0, arr_len / 2, num=arr_len)) + 5
    )
    df1["index"] = np.asarray(
        pd.date_range(start=idx_start, end=idx_end, periods=arr_len)
    ).astype(str)

    output_object["df"] = df1
    output_object["arr_len"] = arr_len
    output_object["h_a_l"] = h_a_l
    output_object["half_df1_full_df2"] = _prepare_pandas_to_convert_to_spark(df1.copy())
    output_object["half_df1_full_df2"].loc[h_a_l:, "PrimarySensor"] = None
    output_object["full_df1_full_df2"] = _prepare_pandas_to_convert_to_spark(df1.copy())
    output_object["full_df1_half_df2"] = _prepare_pandas_to_convert_to_spark(df1.copy())
    output_object["full_df1_half_df2"].loc[h_a_l:, "SecondarySensor"] = None
    output_object["half_df1_half_df2"] = _prepare_pandas_to_convert_to_spark(
        df1.copy().head(h_a_l)
    )
    return output_object


def test_nonexistent_column_arima(spark_session: SparkSession):
    input_df = spark_session.createDataFrame(
        [
            (1.0,),
            (2.0,),
        ],
        ["Value"],
    )

    with pytest.raises(ValueError):
        ArimaPrediction(input_df, to_extend_name="NonexistingColumn")


def test_invalid_size_arima(spark_session: SparkSession):
    input_df = spark_session.createDataFrame(
        [
            (1.0,),
            (2.0,),
        ],
        ["Value"],
    )

    with pytest.raises(ValueError):
        ArimaPrediction(
            input_df,
            to_extend_name="Value",
            order=(3, 0, 0),
            seasonal_order=(3, 0, 0, 62),
            number_of_data_points_to_analyze=62,
        )


def test_single_column_prediction_arima(spark_session: SparkSession, historic_data):
    schema = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", StringType(), True),
            StructField("Status", StringType(), True),
            StructField("Value", FloatType(), True),
        ]
    )

    # convert last column to float
    for idx, item in enumerate(historic_data):
        historic_data[idx] = item[0:3] + (float(item[3]),)

    input_df = spark_session.createDataFrame(historic_data, schema=schema)

    h_a_l = int(input_df.count() / 2)

    arima_comp = ArimaPrediction(
        input_df,
        value_name="Value",
        past_data_style=ArimaPrediction.InputStyle.SOURCE_BASED,
        to_extend_name="-4O7LSSAM_3EA02:2GT7E02I_R_MP",
        number_of_data_points_to_analyze=input_df.count(),
        number_of_data_points_to_predict=h_a_l,
        order=(3, 0, 0),
        seasonal_order=(3, 0, 0, 62),
        timestamp_name="EventTime",
        source_name="TagName",
        status_name="Status",
    )
    forecasted_df = arima_comp.filter_data()
    # print(forecasted_df.show(forecasted_df.count(), False))

    assert isinstance(forecasted_df, DataFrame)

    assert input_df.columns == forecasted_df.columns
    assert forecasted_df.count() == (input_df.count() + h_a_l)


def test_single_column_prediction_auto_arima(
    spark_session: SparkSession, historic_data
):

    schema = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", StringType(), True),
            StructField("Status", StringType(), True),
            StructField("Value", FloatType(), True),
        ]
    )

    # convert last column to float
    for idx, item in enumerate(historic_data):
        historic_data[idx] = item[0:3] + (float(item[3]),)

    input_df = spark_session.createDataFrame(historic_data, schema=schema)

    h_a_l = int(input_df.count() / 2)

    arima_comp = ArimaAutoPrediction(
        past_data=input_df,
        # past_data_style=ArimaPrediction.InputStyle.SOURCE_BASED,
        # value_name="Value",
        to_extend_name="-4O7LSSAM_3EA02:2GT7E02I_R_MP",
        number_of_data_points_to_analyze=input_df.count(),
        number_of_data_points_to_predict=h_a_l,
        # timestamp_name="EventTime",
        # source_name="TagName",
        # status_name="Status",
        seasonal=True,
    )
    forecasted_df = arima_comp.filter_data()
    # print(forecasted_df.show(forecasted_df.count(), False))

    assert isinstance(forecasted_df, DataFrame)

    assert input_df.columns == forecasted_df.columns
    assert forecasted_df.count() == (input_df.count() + h_a_l)
    assert arima_comp.value_name == "Value"
    assert arima_comp.past_data_style == ArimaPrediction.InputStyle.SOURCE_BASED
    assert arima_comp.timestamp_name == "EventTime"
    assert arima_comp.source_name == "TagName"
    assert arima_comp.status_name == "Status"


def test_column_based_prediction_arima(
    spark_session: SparkSession, column_based_synthetic_data
):

    schema = StructType(
        [
            StructField("PrimarySource", StringType(), True),
            StructField("SecondarySource", StringType(), True),
            StructField("EventTime", StringType(), True),
        ]
    )

    data = column_based_synthetic_data["half_df1_half_df2"]

    input_df = spark_session.createDataFrame(data, schema=schema)

    arima_comp = ArimaAutoPrediction(
        past_data=input_df,
        to_extend_name="PrimarySource",
        number_of_data_points_to_analyze=input_df.count(),
        number_of_data_points_to_predict=input_df.count(),
        seasonal=True,
    )
    forecasted_df = arima_comp.filter_data()

    # forecasted_df.show()

    assert isinstance(forecasted_df, DataFrame)

    assert input_df.columns == forecasted_df.columns
    assert forecasted_df.count() == (input_df.count() + input_df.count())
    assert arima_comp.value_name == None
    assert arima_comp.past_data_style == ArimaPrediction.InputStyle.COLUMN_BASED
    assert arima_comp.timestamp_name == "EventTime"
    assert arima_comp.source_name is None
    assert arima_comp.status_name is None


def test_arima_large_data_set(spark_session: SparkSession):
    test_path = os.path.dirname(__file__)
    data_path = os.path.join(test_path, "../../data_quality/test_data.csv")

    input_df = spark_session.read.option("header", "true").csv(data_path)

    expected_schema = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", TimestampType(), True),
            StructField("Status", StringType(), True),
            StructField("Value", FloatType(), True),
        ]
    )

    print((input_df.count(), len(input_df.columns)))

    count_signal = input_df.filter('TagName = "R0:Z24WVP.0S10L"').count()
    h_a_l = int(count_signal / 2)

    arima_comp = ArimaAutoPrediction(
        input_df,
        to_extend_name="R0:Z24WVP.0S10L",
        number_of_data_points_to_analyze=count_signal,
        number_of_data_points_to_predict=h_a_l,
    )

    result_df = arima_comp.filter_data()

    tolerance = 0.01

    assert isinstance(result_df, DataFrame)

    assert result_df.count() == pytest.approx((input_df.count() + h_a_l), rel=tolerance)


def test_arima_wrong_datatype(spark_session: SparkSession):

    expected_schema = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", TimestampType(), True),
            StructField("Status", StringType(), True),
            StructField("Value", FloatType(), True),
        ]
    )

    test_df = spark_session.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "invalid_data_type", "Good", "1.0"),
            ("A2PS64V0J.:ZUX09R", "invalid_data_type", "Good", "2.0"),
            ("A2PS64V0J.:ZUX09R", "invalid_data_type", "Good", "3.0"),
            ("A2PS64V0J.:ZUX09R", "invalid_data_type", "Good", "4.0"),
            ("A2PS64V0J.:ZUX09R", "invalid_data_type", "Good", "5.0"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

    count_signal = 5
    h_a_l = int(count_signal / 2)

    with pytest.raises(ValueError) as exc_info:
        arima_comp = ArimaAutoPrediction(
            test_df,
            to_extend_name="A2PS64V0J.:ZUX09R",
            number_of_data_points_to_analyze=count_signal,
            number_of_data_points_to_predict=h_a_l,
        )

        arima_comp.validate(expected_schema)
