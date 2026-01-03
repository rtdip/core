from ..interfaces import DataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType
from pyspark.sql import DataFrame
from pandas import DataFrame as PandasDataFrame

from ..pandas.drop_columns_by_NaN_percentage import DropByNaNPercentage

class DropByNaNPercentage(DataManipulationBaseInterface):

    df: DataFrame
    nan_threshold: float

    def __init__(
            self,
            df: DataFrame,
            nan_threshold: float
    ) -> None:
        self.df = df
        self.nan_threshold = nan_threshold
        self.pandas_DropByNaNPercentage = DropByNaNPercentage(df.toPandas(), nan_threshold)

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PANDAS
        """
        return SystemType.PYTHON

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def filter_data(self):
        result_pdf = self.pandas_DropByNaNPercentage.apply()

        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        result_df = spark.createDataFrame(result_pdf)
        return result_df
