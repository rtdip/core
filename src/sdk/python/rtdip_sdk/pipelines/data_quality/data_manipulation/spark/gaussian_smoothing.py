import numpy as np
from scipy.ndimage import gaussian_filter, gaussian_filter1d
from pyspark.sql import DataFrame as PySparkDataFrame
from enum import Enum
import pandas as pd

from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)
from ..interfaces import DataManipulationBaseInterface


class GaussianSmoothing(DataManipulationBaseInterface):
    """
    Applies Gaussian smoothing to specified columns of a PySpark DataFrame.

    This transformation smooths data to reduce noise and create a more continuous series of values.
    It supports both temporal smoothing (over time for each ID) and spatial smoothing (across IDs at each timestamp).
    The smoothing is applied using SciPys `gaussian_filter1d` function.

    Args:
        df (pyspark.sql.DataFrame): The input PySpark DataFrame to process.
        sigma (float): The standard deviation for the Gaussian kernel, controlling the amount of smoothing.
        mode (str): Smoothing mode, either 'temporal' or 'spatial'.
        id_col (str): Name of the column containing unique IDs for grouping (e.g., sensors, tags).
        timestamp_col (str): Name of the column containing timestamps.
        value_col (str): Name of the column containing the values to smooth.

    Example:
        ```python
        from pyspark.sql import SparkSession
        from rtdip_sdk.pipelines.data_manipulation.spark.data_quality.gaussian_smoothing import GaussianSmoothing

        spark = SparkSession.builder.master("local[1]").appName("GaussianSmoothingExample").getOrCreate()

        # Example DataFrame
        data = [
            ("Sensor1", "2024-01-02 03:49:45.000", 0.13),
            ("Sensor1", "2024-01-02 07:53:11.000", 0.12),
            ("Sensor1", "2024-01-02 11:56:42.000", 0.13),
            ("Sensor1", "2024-01-02 16:00:12.000", 0.15),
            ("Sensor1", "2024-01-02 20:03:46.000", 0.34),
        ]
        columns = ["TagName", "EventTime", "Value"]
        df = spark.createDataFrame(data, columns)

        smoother = GaussianSmoothing(
            df=df,
            sigma=2.0,
            mode="temporal",  # Choose between "temporal" and "spatial"
            id_col="TagName",
            timestamp_col="EventTime",
            value_col="Value"
        )

        result_df = smoother.filter()
        result_df.show()
        ```
    """

    def __init__(
        self,
        df: PySparkDataFrame,
        sigma: float,
        mode: str = "temporal",
        id_col: str = "id",
        timestamp_col: str = "timestamp",
        value_col: str = "value",
    ) -> None:

        if not isinstance(df, PySparkDataFrame):
            raise TypeError("df must be a PySpark DataFrame")
        if not isinstance(sigma, (int, float)) or sigma <= 0:
            raise ValueError("sigma must be a positive number")
        if mode not in ["temporal", "spatial"]:
            raise ValueError("mode must be either 'temporal' or 'spatial'")

        # Validate column existence
        if id_col not in df.columns:
            raise ValueError(f"Column {id_col} not found in DataFrame")
        if timestamp_col not in df.columns:
            raise ValueError(f"Column {timestamp_col} not found in DataFrame")
        if value_col not in df.columns:
            raise ValueError(f"Column {value_col} not found in DataFrame")

        self.df = df
        self.sigma = sigma
        self.mode = mode
        self.id_col = id_col
        self.timestamp_col = timestamp_col
        self.value_col = value_col

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

    def filter(self) -> PySparkDataFrame:

        pdf = self.df.toPandas()

        if self.mode == "temporal":
            pdf[self.value_col] = pdf.groupby(self.id_col)[self.value_col].transform(
                lambda x: gaussian_filter1d(x.astype(float).values, sigma=self.sigma)
            )
        else:  # spatial
            unique_timestamps = pdf[self.timestamp_col].unique()

            for timestamp in unique_timestamps:
                mask = pdf[self.timestamp_col] == timestamp
                ids = pdf.loc[mask, self.id_col].values
                values = pdf.loc[mask, self.value_col].astype(float).values

                sorted_indices = np.argsort(ids)
                ids_sorted = ids[sorted_indices]
                values_sorted = values[sorted_indices]

                smoothed_values = gaussian_filter1d(values_sorted, sigma=self.sigma)

                reverse_indices = np.argsort(sorted_indices)
                pdf.loc[mask, self.value_col] = smoothed_values[reverse_indices]

        spark = self.df.sparkSession
        result_df = spark.createDataFrame(pdf)

        return result_df
