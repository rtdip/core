
from ..interfaces import DataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType
from pyspark.sql import DataFrame
from pandas import DataFrame as PandasDataFrame

from ..pandas.drop_empty_columns import DropEmptyAndUselessColumns as PandasDropEmptyAndUselessColumns


class DropEmptyAndUselessColumns(DataManipulationBaseInterface):
    """
    Removes columns that contain no meaningful information.

    This component scans all DataFrame columns and identifies those where
    - every value is NaN, **or**
    - all non-NaN entries are identical (i.e., the column has only one unique value).

    Such columns typically contain no informational value (empty placeholders,
    constant fields, or improperly loaded upstream data).

    The transformation returns a cleaned DataFrame containing only columns that
    provide variability or meaningful data.

    Example
    -------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.spark.drop_empty_columns import DropEmptyAndUselessColumns
    import pandas as pd

    df = pd.DataFrame({
        'a': [1, 2, 3],
        'b': [None, None, None],       # Empty column
        'c': [5, None, 7],
        'd': [NaN, NaN, NaN]           # Empty column
        'e': [7, 7, 7],                # Constant column
    })

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame(df)

    cleaner = DropEmptyAndUselessColumns(df)
    result_df = cleaner.filter_data()

    # result_df:
    #    a    c
    # 0  1  5.0
    # 1  2  NaN
    # 2  3  7.0
    ```

    Parameters
    ----------
    df : DataFrame
        The Spark DataFrame whose columns should be examined and cleaned.
    """

    df: DataFrame

    def __init__(
            self,
            df: DataFrame,
    ) -> None:
        self.df = df
        self.pandas_DropEmptyAndUselessColumns = PandasDropEmptyAndUselessColumns(df.toPandas())

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
        """
        Removes columns without values other than NaN from the DataFrame

        Returns:
            DataFrame: DataFrame without empty columns

        Raises:
            ValueError: If the DataFrame is empty or column doesn't exist.
        """
        result_pdf = self.pandas_DropEmptyAndUselessColumns.apply()
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        result_df = spark.createDataFrame(result_pdf)
        return result_df

