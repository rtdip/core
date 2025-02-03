from pyspark.sql import DataFrame as PySparkDataFrame

from ...monitoring.spark.flatline_detection import FlatlineDetection
from ..interfaces import DataManipulationBaseInterface
from ...._pipeline_utils.models import (
    Libraries,
    SystemType,
)


class FlatlineFilter(DataManipulationBaseInterface):
    """
    Removes and logs rows with flatlining detected in specified columns of a PySpark DataFrame.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame to process.
        watch_columns (list): List of column names to monitor for flatlining (null or zero values).
        tolerance_timespan (int): Maximum allowed consecutive flatlining period. Rows exceeding this period are removed.

    Example:
        ```python
        from pyspark.sql import SparkSession
        from rtdip_sdk.pipelines.data_manipulation.spark.data_quality.flatline_filter import FlatlineFilter

        spark = SparkSession.builder.master("local[1]").appName("FlatlineFilterExample").getOrCreate()

        # Example DataFrame
        data = [
            (1, "2024-01-02 03:49:45.000", 0.0),
            (1, "2024-01-02 03:50:45.000", 0.0),
            (1, "2024-01-02 03:51:45.000", 0.0),
            (2, "2024-01-02 03:49:45.000", 5.0),
        ]
        columns = ["TagName", "EventTime", "Value"]
        df = spark.createDataFrame(data, columns)

        filter_flatlining_rows = FlatlineFilter(
            df=df,
            watch_columns=["Value"],
            tolerance_timespan=2,
        )

        result_df = filter_flatlining_rows.filter()
        result_df.show()
        ```
    """

    def __init__(
        self, df: PySparkDataFrame, watch_columns: list, tolerance_timespan: int
    ) -> None:
        self.df = df
        self.flatline_detection = FlatlineDetection(
            df=df, watch_columns=watch_columns, tolerance_timespan=tolerance_timespan
        )

    @staticmethod
    def system_type():
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def filter(self) -> PySparkDataFrame:
        """
        Removes rows with flatlining detected.

        Returns:
            pyspark.sql.DataFrame: A DataFrame without rows with flatlining detected.
        """
        flatlined_rows = self.flatline_detection.check_for_flatlining()
        flatlined_rows = flatlined_rows.select(*self.df.columns)
        return self.df.subtract(flatlined_rows)
