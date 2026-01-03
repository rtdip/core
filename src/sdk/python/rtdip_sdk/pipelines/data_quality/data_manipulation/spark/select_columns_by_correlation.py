from ..interfaces import DataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType
from pyspark.sql import DataFrame
from pandas import DataFrame as PandasDataFrame

from ..pandas.select_columns_by_correlation import SelectColumnsByCorrelation

class SelectColumnsByCorrelation(DataManipulationBaseInterface):

    df: DataFrame
    columns_to_keep: list[str]
    target_col_name: str
    correlation_threshold: float

    def __init__(
            self,
            df: DataFrame,
            columns_to_keep: list[str],
            target_col_name: str,
            correlation_threshold: float = 0.6

    ) -> None:
        self.df = df
        self.columns_to_keep = columns_to_keep
        self.target_col_name = target_col_name
        self.correlation_threshold = correlation_threshold
        self.pandas_SelectColumnsByCorrelation = SelectColumnsByCorrelation(df.toPandas(),
                                                                            columns_to_keep, target_col_name,
                                                                            correlation_threshold)

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
        result_pdf = self.pandas_SelectColumnsByCorrelation.apply()

        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        result_df = spark.createDataFrame(result_pdf)
        return result_df
    