import logging
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    FloatType,
)

from src.sdk.python.rtdip_sdk.pipelines.data_quality.monitoring.interfaces import (
    MonitoringBaseInterface,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)
from ...input_validator import InputValidator


class MovingAverage(MonitoringBaseInterface, InputValidator):
    """
    Computes and logs the moving average over a specified window size for a given PySpark DataFrame.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to process.
        window_size (int): The size of the moving window.

    Example:
        ```python
        from pyspark.sql import SparkSession
        from rtdip_sdk.pipelines.monitoring.spark.data_quality.moving_average import MovingAverage

        spark = SparkSession.builder.master("local[1]").appName("MovingAverageExample").getOrCreate()

        data = [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", 1.0),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", 2.0),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", 3.0),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12.000", "Good", 4.0),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", 5.0),
        ]

        columns = ["TagName", "EventTime", "Status", "Value"]

        df = spark.createDataFrame(data, columns)

        moving_avg = MovingAverage(
            df=df,
            window_size=3,
        )

        moving_avg.check()
        ```
    """

    df: PySparkDataFrame
    window_size: int
    EXPECTED_SCHEMA = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", TimestampType(), True),
            StructField("Status", StringType(), True),
            StructField("Value", FloatType(), True),
        ]
    )

    def __init__(
        self,
        df: PySparkDataFrame,
        window_size: int,
    ) -> None:
        if not isinstance(window_size, int) or window_size <= 0:
            raise ValueError("window_size must be a positive integer.")

        self.df = df
        self.validate(self.EXPECTED_SCHEMA)
        self.window_size = window_size

        self.logger = logging.getLogger(self.__class__.__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

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

    def check(self) -> None:
        """
        Computes and logs the moving average using a specified window size.
        """

        self._validate_inputs()

        window_spec = (
            Window.partitionBy("TagName")
            .orderBy("EventTime")
            .rowsBetween(-(self.window_size - 1), 0)
        )

        self.logger.info("Computing moving averages:")

        for row in (
            self.df.withColumn("MovingAverage", avg(col("Value")).over(window_spec))
            .select("TagName", "EventTime", "Value", "MovingAverage")
            .collect()
        ):
            self.logger.info(
                f"Tag: {row.TagName}, Time: {row.EventTime}, Value: {row.Value}, Moving Avg: {row.MovingAverage}"
            )

    def _validate_inputs(self):
        if not isinstance(self.window_size, int) or self.window_size <= 0:
            raise ValueError("window_size must be a positive integer.")
