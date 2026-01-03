
from ..interfaces import DataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType
from pyspark.sql import DataFrame
from pandas import DataFrame as PandasDataFrame

from ..pandas.drop_empty_columns import DropEmptyAndUselessColumns

class DropEmptyAndUselessColumns(DataManipulationBaseInterface):

    df: DataFrame

    def __init__(
            self,
            df: DataFrame,
    ) -> None:
        self.df = df
        self.pandas_DropEmptyAndUselessColumns = DropEmptyAndUselessColumns(df.toPandas())

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
        result_pdf = self.pandas_DropEmptyAndUselessColumns.apply()
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        result_df = spark.createDataFrame(result_pdf)
        return result_df

