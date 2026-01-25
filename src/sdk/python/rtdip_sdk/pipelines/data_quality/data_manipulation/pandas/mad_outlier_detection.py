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

import pandas as pd
import numpy as np
from pandas import DataFrame as PandasDataFrame
from typing import Optional, Union
from ..interfaces import PandasDataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType


# Constant to convert MAD to standard deviation equivalent for normal distributions
MAD_TO_STD_CONSTANT = 1.4826


class MADOutlierDetection(PandasDataManipulationBaseInterface):
    """
    Detects and handles outliers using Median Absolute Deviation (MAD).

    MAD is a robust measure of variability that is less sensitive to extreme
    outliers compared to standard deviation. This makes it ideal for detecting
    outliers in sensor data that may contain extreme values or data corruption.

    The MAD is defined as: MAD = median(|X - median(X)|)

    Outliers are identified as values that fall outside:
    median ± (n_sigma * MAD * 1.4826)

    Where 1.4826 is a constant that makes MAD comparable to standard deviation
    for normally distributed data.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.mad_outlier_detection import MADOutlierDetection
    import pandas as pd

    df = pd.DataFrame({
        'sensor_id': ['A', 'B', 'C', 'D', 'E'],
        'value': [10.0, 12.0, 11.0, 1000000.0, 9.0]  # 1000000 is an outlier
    })

    detector = MADOutlierDetection(
        df,
        column="value",
        n_sigma=3.0,
        action="replace",
        replacement_value=-1
    )
    result_df = detector.apply()
    # Result will have the outlier replaced with -1
    ```

    Parameters:
        df (PandasDataFrame): The Pandas DataFrame containing the value column.
        column (str): The name of the column to check for outliers.
        n_sigma (float, optional): Number of MAD-based standard deviations for
            outlier threshold. Defaults to 3.0.
        action (str, optional): Action to take on outliers. Options:
            - "flag": Add a boolean column indicating outliers
            - "replace": Replace outliers with replacement_value
            - "remove": Remove rows containing outliers
            Defaults to "flag".
        replacement_value (Union[int, float], optional): Value to use when
            action="replace". Defaults to None (uses NaN).
        exclude_values (list, optional): Values to exclude from outlier detection
            (e.g., error codes like -1). Defaults to None.
        outlier_column (str, optional): Name for the outlier flag column when
            action="flag". Defaults to "{column}_is_outlier".
    """

    df: PandasDataFrame
    column: str
    n_sigma: float
    action: str
    replacement_value: Optional[Union[int, float]]
    exclude_values: Optional[list]
    outlier_column: Optional[str]

    def __init__(
        self,
        df: PandasDataFrame,
        column: str,
        n_sigma: float = 3.0,
        action: str = "flag",
        replacement_value: Optional[Union[int, float]] = None,
        exclude_values: Optional[list] = None,
        outlier_column: Optional[str] = None,
    ) -> None:
        self.df = df
        self.column = column
        self.n_sigma = n_sigma
        self.action = action
        self.replacement_value = replacement_value
        self.exclude_values = exclude_values
        self.outlier_column = (
            outlier_column if outlier_column else f"{column}_is_outlier"
        )

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

    def _compute_mad_bounds(self, values: pd.Series) -> tuple:
        """
        Compute lower and upper bounds based on MAD.

        Args:
            values: Series of numeric values (excluding any values to skip)

        Returns:
            Tuple of (lower_bound, upper_bound)
        """
        median = values.median()
        mad = (values - median).abs().median()

        # Convert MAD to standard deviation equivalent
        std_equivalent = mad * MAD_TO_STD_CONSTANT

        lower_bound = median - (self.n_sigma * std_equivalent)
        upper_bound = median + (self.n_sigma * std_equivalent)

        return lower_bound, upper_bound

    def apply(self) -> PandasDataFrame:
        """
        Detects and handles outliers using MAD-based thresholds.

        Returns:
            PandasDataFrame: DataFrame with outliers handled according to the
                specified action.

        Raises:
            ValueError: If the DataFrame is empty, column doesn't exist,
                or invalid action is specified.
        """
        if self.df is None or self.df.empty:
            raise ValueError("The DataFrame is empty.")

        if self.column not in self.df.columns:
            raise ValueError(f"Column '{self.column}' does not exist in the DataFrame.")

        valid_actions = ["flag", "replace", "remove"]
        if self.action not in valid_actions:
            raise ValueError(
                f"Invalid action '{self.action}'. Must be one of {valid_actions}."
            )

        if self.n_sigma <= 0:
            raise ValueError(f"n_sigma must be positive, got {self.n_sigma}.")

        result_df = self.df.copy()

        # Create mask for values to include in MAD calculation
        include_mask = pd.Series(True, index=result_df.index)

        # Exclude specified values from calculation
        if self.exclude_values is not None:
            include_mask = ~result_df[self.column].isin(self.exclude_values)

        # Also exclude NaN values
        include_mask = include_mask & result_df[self.column].notna()

        # Get values for MAD calculation
        valid_values = result_df.loc[include_mask, self.column]

        if len(valid_values) == 0:
            # No valid values to compute MAD, return original with appropriate columns
            if self.action == "flag":
                result_df[self.outlier_column] = False
            return result_df

        # Compute MAD-based bounds
        lower_bound, upper_bound = self._compute_mad_bounds(valid_values)

        # Identify outliers (only among included values)
        outlier_mask = include_mask & (
            (result_df[self.column] < lower_bound)
            | (result_df[self.column] > upper_bound)
        )

        # Apply the specified action
        if self.action == "flag":
            result_df[self.outlier_column] = outlier_mask

        elif self.action == "replace":
            replacement = (
                self.replacement_value if self.replacement_value is not None else np.nan
            )
            result_df.loc[outlier_mask, self.column] = replacement

        elif self.action == "remove":
            result_df = result_df[~outlier_mask].reset_index(drop=True)

        return result_df
