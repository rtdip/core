# Copyright 2024 RTDIP
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
import logging

from pyspark.sql import DataFrame as PySparkDataFrame, SparkSession
from datetime import datetime


from pyspark.sql.types import StructField, TimestampType, StringType, StructType, Row


class DataFrameLogHandler(logging.Handler):
    """
    Handles logs from attached logger and stores them in a DataFrame at runtime
    Uses the following format: {Timestamp, Logger Name, Logging Level, Log Message}

     Args:
        logging.Handler: Inherits from logging.Handler

    Returns:
        returns a DataFrame with logs stored in it

    Example
    --------
    ```python
    import logging

    log_manager = logging.getLogger('log_manager')

    """

    logs_df: PySparkDataFrame = None
    spark: SparkSession

    def __init__(self, spark: SparkSession):
        self.spark = spark
        schema = StructType(
            [
                StructField("timestamp", TimestampType(), True),
                StructField("name", StringType(), True),
                StructField("level", StringType(), True),
                StructField("message", StringType(), True),
            ]
        )

        self.logs_df = self.spark.createDataFrame([], schema)
        super().__init__()

    def emit(self, record: logging.LogRecord) -> None:
        """Process and store a log record"""
        new_log_entry = Row(
            timestamp=datetime.fromtimestamp(record.created),
            name=record.name,
            level=record.levelname,
            message=record.msg,
        )

        self.logs_df = self.logs_df.union(self.spark.createDataFrame([new_log_entry]))

    def get_logs_as_df(self) -> PySparkDataFrame:
        return self.logs_df
