# Copyright 2025 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pandas import DataFrame as PandasDataFrame
from ..interfaces import PandasDataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType


class SelectColumnsByCorrelation(PandasDataManipulationBaseInterface):
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

    selector = SelectColumnsByCorrelation(
        df=df,
        columns_to_keep=["timestamp"],   # always keep timestamp
        target_col_name="target",
        correlation_threshold=0.8
    )
    reduced_df = selector.apply()

    # reduced_df contains:
    # - "timestamp" (from columns_to_keep)
    # - "feature_1" and "feature_2" (high absolute correlation with "target")
    # - "feature_3" is dropped (no variability / correlation)
    ```

    Parameters
    ----------
    df : PandasDataFrame
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

    df: PandasDataFrame
    columns_to_keep: list[str]
    target_col_name: str
    correlation_threshold: float

    def __init__(
            self,
            df: PandasDataFrame,
            columns_to_keep: list[str],
            target_col_name: str,
            correlation_threshold: float = 0.6

    ) -> None:
        self.df = df
        self.columns_to_keep = columns_to_keep
        self.target_col_name = target_col_name
        self.correlation_threshold = correlation_threshold

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

    def apply(self) -> PandasDataFrame:
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
        PandasDataFrame
            A DataFrame containing the selected columns.

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
        # Basic validation: non-empty DataFrame
        if self.df is None or self.df.empty:
            raise ValueError("The DataFrame is empty.")

        # Validate target column presence
        if self.target_col_name not in self.df.columns:
            raise ValueError(
                f"Target column '{self.target_col_name}' does not exist in the DataFrame."
            )

        # Validate that all columns_to_keep exist in the DataFrame
        missing_keep_cols = [col for col in self.columns_to_keep if col not in self.df.columns]
        if missing_keep_cols:
            raise ValueError(
                f"The following columns from `columns_to_keep` are missing in the DataFrame: {missing_keep_cols}"
            )

        # Validate correlation_threshold range
        if not (0.0 <= self.correlation_threshold <= 1.0):
            raise ValueError(
                "correlation_threshold must be between 0.0 and 1.0 (inclusive)."
            )

        corr = self.df.select_dtypes(include='number').corr()

        # Ensure the target column is part of the numeric correlation matrix
        if self.target_col_name not in corr.columns:
            raise ValueError(
                f"Target column '{self.target_col_name}' is not numeric "
                "or cannot be used in the correlation matrix."
            )

        target_corr = corr[self.target_col_name]
        filtered_corr = target_corr[target_corr.abs() >= self.correlation_threshold]

        columns = []
        columns.extend(self.columns_to_keep)
        columns.extend(filtered_corr.keys())

        result_df = self.df.copy()
        result_df = result_df[columns]

        return result_df





