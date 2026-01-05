from ..interfaces import DataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType
from pyspark.sql import DataFrame
from pandas import DataFrame as PandasDataFrame

from ..pandas.select_columns_by_correlation import SelectColumnsByCorrelation as PandasSelectColumnsByCorrelation


class SelectColumnsByCorrelation(DataManipulationBaseInterface):
    """
    Selects columns based on their correlation with a target column.

    This transformation computes the pairwise correlation of all numeric
    columns in the DataFrame and selects those whose absolute correlation
    with a user-defined target column is greater than or equal to a specified
    threshold. In addition, a fixed set of columns can always be kept,
    regardless of their correlation.

    This is useful when you want to:
      - Reduce the number of features before training a model.
      - Keep only columns that have at least a minimum linear relationship
        with the target variable.
      - Ensure that certain key columns (IDs, timestamps, etc.) are always
        retained via `columns_to_keep`.

    Example
    -------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.select_columns_by_correlation import (
        SelectColumnsByCorrelation,
    )
    import pandas as pd

    df = pd.DataFrame({
        "timestamp": pd.date_range("2025-01-01", periods=5, freq="H"),
        "feature_1": [1, 2, 3, 4, 5],
        "feature_2": [5, 4, 3, 2, 1],
        "feature_3": [10, 10, 10, 10, 10],
        "target":    [1, 2, 3, 4, 5],
    })

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame(df)

    selector = SelectColumnsByCorrelation(
        df=df,
        columns_to_keep=["timestamp"],   # always keep timestamp
        target_col_name="target",
        correlation_threshold=0.8
    )
    reduced_df = selector.filter_data()

    # reduced_df contains:
    # - "timestamp" (from columns_to_keep)
    # - "feature_1" and "feature_2" (high absolute correlation with "target")
    # - "feature_3" is dropped (no variability / correlation)
    ```

    Parameters
    ----------
    df : DataFrame
        The input DataFrame containing the target column and candidate
        feature columns.
    columns_to_keep : list[str]
        List of column names that will always be kept in the output,
        regardless of their correlation with the target column.
    target_col_name : str
        Name of the target column against which correlations are computed.
        Must be present in `df` and have numeric dtype.
    correlation_threshold : float, optional
        Minimum absolute correlation value for a column to be selected.
        Should be between 0 and 1. Default is 0.6.
    """

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
        self.pandas_SelectColumnsByCorrelation = PandasSelectColumnsByCorrelation(df.toPandas(),
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
        """
        Selects DataFrame columns based on correlation with the target column.

        The method:
          1. Validates the input DataFrame and parameters.
          2. Computes the correlation matrix for all numeric columns.
          3. Extracts the correlation series for the target column.
          4. Filters columns whose absolute correlation is greater than or
             equal to `correlation_threshold`.
          5. Returns a copy of the original DataFrame restricted to:
             - `columns_to_keep`, plus
             - all columns passing the correlation threshold.

        Returns
        -------
        DataFrame: A DataFrame containing the selected columns.

        Raises
        ------
        ValueError
            If the DataFrame is empty.
        ValueError
            If the target column is missing in the DataFrame.
        ValueError
            If any column in `columns_to_keep` does not exist.
        ValueError
            If the target column is not numeric or cannot be found in the
            numeric correlation matrix.
        ValueError
            If `correlation_threshold` is outside the [0, 1] interval.
        """

        result_pdf = self.pandas_SelectColumnsByCorrelation.apply()

        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        result_df = spark.createDataFrame(result_pdf)
        return result_df
