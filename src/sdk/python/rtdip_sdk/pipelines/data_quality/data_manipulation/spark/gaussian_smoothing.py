import numpy as np
from pyspark.sql.types import FloatType
from scipy.ndimage import gaussian_filter1d
from pyspark.sql import DataFrame as PySparkDataFrame, Window
from pyspark.sql import functions as F

from ...._pipeline_utils.models import (
    Libraries,
    SystemType,
)
from ..interfaces import DataManipulationBaseInterface


class GaussianSmoothing(DataManipulationBaseInterface):
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
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    @staticmethod
    def create_gaussian_smoother(sigma_value):
        def apply_gaussian(values):
            if not values:
                return None
            values_array = np.array([float(v) for v in values])
            smoothed = gaussian_filter1d(values_array, sigma=sigma_value)
            return float(smoothed[-1])

        return apply_gaussian

    def filter(self) -> PySparkDataFrame:

        smooth_udf = F.udf(self.create_gaussian_smoother(self.sigma), FloatType())

        if self.mode == "temporal":
            window = (
                Window.partitionBy(self.id_col)
                .orderBy(self.timestamp_col)
                .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            )
        else:  # spatial mode
            window = (
                Window.partitionBy(self.timestamp_col)
                .orderBy(self.id_col)
                .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            )

        collect_list_expr = F.collect_list(F.col(self.value_col)).over(window)

        return self.df.withColumn(self.value_col, smooth_udf(collect_list_expr))
