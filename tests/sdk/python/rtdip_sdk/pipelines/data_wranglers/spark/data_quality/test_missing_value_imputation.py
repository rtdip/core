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
        ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", "0.340000004"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12.000", "Good", "0.150000006"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", "0.129999995"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", "0.119999997"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", "0.129999995"),
        #("A2PS64V0J.:ZUX09R", "2024-01-01 23:46:11.000", "Good", "0.340000004"), # Test value
        ("A2PS64V0J.:ZUX09R", "2024-01-01 19:42:37.000", "Good", "0.150000006"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 15:39:03.000", "Good", "0.129999995"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 11:36:29.000", "Good", "0.119999997"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 07:32:55.000", "Good", "0.129999995"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 03:29:21.000", "Good", "0.340000004"),
        #("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2024-01-02 06:08:00", "Good", "5921.549805"),
        #("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2024-01-02 05:14:00", "Good", "5838.216797"),
        #("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2024-01-02 01:37:00", "Good", "5607.825684"),
        #("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2024-01-02 00:26:00", "Good", "5563.708008"),
        #("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2024-01-02 06:08:00", "Good", "5921.549805"),
        #("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2024-01-02 05:14:00", "Good", "5838.216797"),
        #("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2024-01-02 01:37:00", "Good", "5607.825684"),
        #("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2024-01-02 00:26:00", "Good", "5563.708008"),
        #("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2024-01-02 06:08:00", "Good", "5921.549805"),
        #("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2024-01-02 05:14:00", "Good", "5838.216797"),
        #("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2024-01-02 01:37:00", "Good", "5607.825684"),
        #("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2024-01-02 00:26:00", "Good", "5563.708008"),
        #("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2024-01-02 06:08:00", "Good", "5921.549805"),
        #("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2024-01-02 05:14:00", "Good", "5838.216797"),
        #("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2024-01-02 01:37:00", "Good", "5607.825684"),
        #("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "2024-01-02 00:26:00", "Good", "5563.708008"),
    ]

    df = spark_session.createDataFrame(data, schema=schema)

    missing_value_imputation = MissingValueImputation(df)
    imputed_df = missing_value_imputation.filter()

    assert isinstance(imputed_df, DataFrame)
    #TODO

