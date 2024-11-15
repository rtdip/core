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
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType


from src.sdk.python.rtdip_sdk.pipelines.data_wranglers.spark.data_quality.missing_value_imputation import (
    MissingValueImputation,
)


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()


def test_missing_value_imputation(spark_session: SparkSession):

    schema = StructType([
        StructField("TagName", StringType(), True),
        StructField("EventTime", StringType(), True),
        StructField("Status", StringType(), True),
        StructField("Value", StringType(), True)
    ])

    data = [
        # Setup controlled Test
        ("A2PS64V0J.:ZUX09R", "2024-01-01 03:29:21.000", "Good", "1.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 07:32:55.000", "Good", "2.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 11:36:29.000", "Good", "3.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 15:39:03.000", "Good", "4.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 19:42:37.000", "Good", "5.0"),
        #("A2PS64V0J.:ZUX09R", "2024-01-01 23:46:11.000", "Good", "6.0"), # Test values
        ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", "7.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", "8.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", "9.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12.000", "Good", "10.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 20:13:46.000", "Good", "11.0"), # Tolerance Test
        ("A2PS64V0J.:ZUX09R", "2024-01-03 00:07:20.000", "Good", "12.0"),
        #("A2PS64V0J.:ZUX09R", "2024-01-03 04:10:54.000", "Good", "13.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-03 08:14:28.000", "Good", "14.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-03 12:18:02.000", "Good", "15.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-03 16:21:36.000", "Good", "16.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-03 20:25:10.000", "Good", "17.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-04 00:28:44.000", "Good", "18.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-04 04:32:18.000", "Good", "19.0"),
        # Real missing values
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:01:43", "Good", "4686.259766"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:02:44", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:04:44", "Good", "4686.259766"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:05:44", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:11:46", "Good", "4686.259766"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:13:46", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:16:47", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:19:48", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:20:48", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:25:50", "Good", "4681.35791"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:26:50", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:27:50", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:28:50", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:31:51", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:32:52", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:42:52", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:42:54", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:43:54", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:44:54", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:45:54", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:46:55", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:47:55", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:51:56", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:52:56", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:55:57", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:56:58", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:57:58", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:59:59", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:00:59", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:05:01", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:10:02", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:11:03", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:13:06", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:17:07", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:18:07", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:20:07", "Good", "4686.259766"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:21:07", "Good", "4700.96582"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:25:09", "Good", "4676.456055"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:26:09", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:30:09", "Good", "4700.96582"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:35:10", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:36:10", "Good", "4700.96582"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:40:11", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:42:11", "Good", "4700.96582"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:43:11", "Good", "4705.867676"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:44:11", "Good", "4700.96582"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:46:11", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:47:11", "Good", "4700.96582"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:53:13", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:54:13", "Good", "4700.96582"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:55:13", "Good", "4686.259766"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 01:56:13", "Good", "4700.96582"),
    ]

    df = spark_session.createDataFrame(data, schema=schema)

    missing_value_imputation = MissingValueImputation(spark_session, df)
    imputed_df = missing_value_imputation.filter()

    assert isinstance(imputed_df, DataFrame)
    #TODO

