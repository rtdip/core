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

from pyspark.sql import SparkSession, DataFrame as PySparkDataFrame, functions as F, Row
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, TimestampType, FloatType, ArrayType
from pyspark.sql.window import Window
from scipy.interpolate import UnivariateSpline
import numpy as np
from datetime import timedelta
from typing import List
from ...interfaces import DataManipulationBaseInterface
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)


class MissingValueImputation(DataManipulationBaseInterface):
    """
    Imputes missing values in a univariate time series creating a continuous curve of data points. For that, the
    time intervals of each individual source is calculated, to then insert empty records at the missing timestamps with
    NaN values. Through spline interpolation the missing NaN values are calculated resulting in a consistent data set
    and thus enhance your data quality.

    Example
    --------
    from pyspark.sql import SparkSession
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.types import StructType, StructField, StringType
    from src.sdk.python.rtdip_sdk.pipelines.data_wranglers.spark.data_manipulation.missing_value_imputation import (
        MissingValueImputation,
    )

    @pytest.fixture(scope="session")
    def spark_session():
        return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

    spark = spark_session()

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
        #("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", "7.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", "8.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", "9.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12.000", "Good", "10.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 20:13:46.000", "Good", "11.0"), # Tolerance Test
        ("A2PS64V0J.:ZUX09R", "2024-01-03 00:07:20.000", "Good", "10.0"),
        #("A2PS64V0J.:ZUX09R", "2024-01-03 04:10:54.000", "Good", "9.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-03 08:14:28.000", "Good", "8.0"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:01:43", "Good", "4686.259766"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:02:44", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:04:44", "Good", "4686.259766"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:05:44", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:11:46", "Good", "4686.259766"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:13:46", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:16:47", "Good", "4691.161621"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:19:48", "Good", "4696.063477"),
        ("-4O7LSSAM_3EA02:2GT7E02I_R_MP", "31.12.2023 00:20:48", "Good", "4691.161621"),
    ]

    df = spark.createDataFrame(data, schema=schema)

    missing_value_imputation = MissingValueImputation(spark, df)
    imputed_df = missing_value_imputation.filter()

    print(imputed_df.show(imputed_df.count(), False))

    ```

    Parameters:
        df (DataFrame): Dataframe containing the raw data.
        tolerance_percentage (int): Percentage value that indicates how much the time series data points may vary
            in each interval
    """

    df: PySparkDataFrame

    def __init__(
        self,
        spark: SparkSession,
        df: PySparkDataFrame,
        tolerance_percentage: int = 5,
    ) -> None:
        self.spark = spark
        self.df = df
        self.tolerance_percentage = tolerance_percentage

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYSPARK
        """
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    @staticmethod
    def _impute_missing_values_sp(df) -> PySparkDataFrame:
        """
        Imputes missing values by Spline Interpolation
        """
        data = np.array(
            df.select("Value").rdd.flatMap(lambda x: x).collect(), dtype=float
        )
        mask = np.isnan(data)

        x_data = np.arange(len(data))
        y_data = data[~mask]

        spline = UnivariateSpline(x_data[~mask], y_data, s=0)

        data_imputed = data.copy()
        data_imputed[mask] = spline(x_data[mask])
        data_imputed_list = data_imputed.tolist()

        imputed_rdd = df.rdd.zipWithIndex().map(
            lambda row: Row(
                TagName=row[0][0],
                EventTime=row[0][1],
                Status=row[0][2],
                Value=float(data_imputed_list[row[1]]),
            )
        )
        imputed_df = imputed_rdd.toDF(df.schema)

        return imputed_df

    @staticmethod
    def _flag_missing_values(df, tolerance_percentage) -> PySparkDataFrame:
        """
        Determines intervals of each respective source time series and inserts empty records at missing timestamps
        with NaN values
        """
        window_spec = Window.partitionBy("TagName").orderBy("EventTime")

        df = df.withColumn("prev_event_time", F.lag("EventTime").over(window_spec))
        df = df.withColumn(
            "time_diff_seconds",
            (F.unix_timestamp("EventTime") - F.unix_timestamp("prev_event_time")),
        )

        df_diff = df.filter(F.col("time_diff_seconds").isNotNull())
        interval_counts = df_diff.groupBy("time_diff_seconds").count()
        most_frequent_interval = interval_counts.orderBy(F.desc("count")).first()
        expected_interval = (
            most_frequent_interval["time_diff_seconds"]
            if most_frequent_interval
            else None
        )

        tolerance = (
            (expected_interval * tolerance_percentage) / 100 if expected_interval else 0
        )

        existing_timestamps = (
            df.select("TagName", "EventTime")
            .rdd.map(lambda row: (row["TagName"], row["EventTime"]))
            .groupByKey()
            .collectAsMap()
        )

        def generate_missing_timestamps(prev_event_time, event_time, tag_name):
            # Check for first row
            if (
                prev_event_time is None
                or event_time is None
                or expected_interval is None
            ):
                return []

            # Check against existing timestamps to avoid duplicates
            tag_timestamps = set(existing_timestamps.get(tag_name, []))
            missing_timestamps = []
            current_time = prev_event_time

            while current_time < event_time:
                next_expected_time = current_time + timedelta(seconds=expected_interval)
                time_diff = abs((next_expected_time - event_time).total_seconds())
                if time_diff <= tolerance:
                    break
                if next_expected_time not in tag_timestamps:
                    missing_timestamps.append(next_expected_time)
                current_time = next_expected_time

            return missing_timestamps

        generate_missing_timestamps_udf = udf(
            generate_missing_timestamps, ArrayType(TimestampType())
        )

        df_with_missing = df.withColumn(
            "missing_timestamps",
            generate_missing_timestamps_udf("prev_event_time", "EventTime", "TagName"),
        )

        df_missing_entries = df_with_missing.select(
            "TagName",
            F.explode("missing_timestamps").alias("EventTime"),
            F.lit("Good").alias("Status"),
            F.lit(float("nan")).cast(FloatType()).alias("Value"),
        )

        df_combined = (
            df.select("TagName", "EventTime", "Status", "Value")
            .union(df_missing_entries)
            .orderBy("EventTime")
        )

        return df_combined

    @staticmethod
    def _is_column_type(df, column_name, data_type):
        """
        Helper method for data type checking
        """
        type_ = df.schema[column_name]

        return isinstance(type_.dataType, data_type)

    def filter(self) -> PySparkDataFrame:
        """
        Imputate missing values based on [Spline Interpolation, ]
        """
        if not all(
            col_ in self.df.columns for col_ in ["TagName", "EventTime", "Value"]
        ):
            raise ValueError("Columns not as expected")

        if not self._is_column_type(self.df, "EventTime", TimestampType):
            if self._is_column_type(self.df, "EventTime", StringType):
                # Attempt to parse the first format, then fallback to the second
                self.df = self.df.withColumn(
                    "EventTime",
                    F.coalesce(
                        F.to_timestamp("EventTime", "yyyy-MM-dd HH:mm:ss.SSS"),
                        F.to_timestamp("EventTime", "dd.MM.yyyy HH:mm:ss"),
                    ),
                )
        if not self._is_column_type(self.df, "Value", FloatType):
            self.df = self.df.withColumn("Value", self.df["Value"].cast(FloatType()))

        dfs_by_source = self._split_by_source()

        imputed_dfs: List[PySparkDataFrame] = []

        for source, df in dfs_by_source.items():
            # Determine, insert and flag all the missing entries
            flagged_df = self._flag_missing_values(df, self.tolerance_percentage)

            # Impute the missing values of flagged entries
            imputed_df_sp = self._impute_missing_values_sp(flagged_df)

            imputed_df_sp = imputed_df_sp.withColumn(
                "EventTime", col("EventTime").cast("string")
            ).withColumn("Value", col("Value").cast("string"))

            imputed_dfs.append(imputed_df_sp)

        result_df = imputed_dfs[0]
        for df in imputed_dfs[1:]:
            result_df = result_df.unionByName(df)

        return result_df

    def _split_by_source(self) -> dict:
        """
        Helper method to separate individual time series based on their source
        """
        tag_names = self.df.select("TagName").distinct().collect()
        tag_names = [row["TagName"] for row in tag_names]
        source_dict = {
            tag: self.df.filter(col("TagName") == tag).orderBy("EventTime")
            for tag in tag_names
        }

        return source_dict
