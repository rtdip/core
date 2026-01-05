from ..interfaces import DataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType
from pyspark.sql import DataFrame
from pandas import DataFrame as PandasDataFrame

from ..pandas.drop_columns_by_NaN_percentage import DropByNaNPercentage as PandasDropByNaNPercentage


class DropByNaNPercentage(DataManipulationBaseInterface):
    """
    Drops all DataFrame columns whose percentage of NaN values exceeds
    a user-defined threshold.

    This transformation is useful when working with wide datasets that contain
    many partially populated or sparsely filled columns. Columns with too many
    missing values tend to carry little predictive value and may negatively
    affect downstream analytics or machine learning tasks.

    The component analyzes each column, computes its NaN ratio, and removes
    any column where the ratio exceeds the configured threshold.

    Example
    -------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.drop_by_nan_percentage import DropByNaNPercentage
    import pandas as pd

    df = pd.DataFrame({
        'a': [1, None, 3],         # 33% NaN
        'b': [None, None, None],   # 100% NaN
        'c': [7, 8, 9],            # 0% NaN
        'd': [1, None, None],      # 66% NaN
    })

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame(df)

    dropper = DropByNaNPercentage(df, nan_threshold=0.5)
    cleaned_df = dropper.filter_data()

    # cleaned_df:
    #    a  c
    # 0  1  7
    # 1 NaN 8
    # 2  3  9
    ```

    Parameters
    ----------
    df : DataFrame
        The input DataFrame from which columns should be removed.
    nan_threshold : float
        Threshold between 0 and 1 indicating the minimum NaN ratio at which
        a column should be dropped (e.g., 0.3 = 30% or more NaN).
    """

    df: DataFrame
    nan_threshold: float

    def __init__(
            self,
            df: DataFrame,
            nan_threshold: float
    ) -> None:
        self.df = df
        self.nan_threshold = nan_threshold
        self.pandas_DropByNaNPercentage = PandasDropByNaNPercentage(df.toPandas(), nan_threshold)

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
        result_pdf = self.pandas_DropByNaNPercentage.apply()

        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        result_df = spark.createDataFrame(result_pdf)
        return result_df
