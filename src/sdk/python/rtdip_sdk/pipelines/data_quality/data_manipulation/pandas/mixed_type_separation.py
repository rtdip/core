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
from pandas import DataFrame as PandasDataFrame
from typing import Optional, Union
from ..interfaces import PandasDataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType


class MixedTypeSeparation(PandasDataManipulationBaseInterface):
    """
    Separates textual values from a mixed-type numeric column.

    This is useful when a column contains both numeric values and textual
    status indicators (e.g., "Bad", "Error", "N/A"). The component extracts
    non-numeric strings into a separate column and replaces them with a
    placeholder value in the original column.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.mixed_type_separation import MixedTypeSeparation
    import pandas as pd

    df = pd.DataFrame({
        'sensor_id': ['A', 'B', 'C', 'D'],
        'value': [3.14, 'Bad', 100, 'Error']
    })

    separator = MixedTypeSeparation(
        df,
        column="value",
        placeholder=-1,
        string_fill="NaN"
    )
    result_df = separator.apply()
    # Result:
    #   sensor_id  value value_str
    #   A          3.14  NaN
    #   B          -1    Bad
    #   C          100   NaN
    #   D          -1    Error
    ```

    Parameters:
        df (PandasDataFrame): The Pandas DataFrame containing the mixed-type column.
        column (str): The name of the column to separate.
        placeholder (Union[int, float], optional): Value to replace non-numeric entries.
            Defaults to -1.
        string_fill (str, optional): Value to fill in the string column for numeric entries.
            Defaults to "NaN".
        suffix (str, optional): Suffix for the new string column name.
            Defaults to "_str".
    """

    df: PandasDataFrame
    column: str
    placeholder: Union[int, float]
    string_fill: str
    suffix: str

    def __init__(
        self,
        df: PandasDataFrame,
        column: str,
        placeholder: Union[int, float] = -1,
        string_fill: str = "NaN",
        suffix: str = "_str",
    ) -> None:
        self.df = df
        self.column = column
        self.placeholder = placeholder
        self.string_fill = string_fill
        self.suffix = suffix

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

    def _is_numeric_string(self, x) -> bool:
        """Check if a value is a string that represents a number."""
        if not isinstance(x, str):
            return False
        try:
            float(x)
            return True
        except ValueError:
            return False

    def _is_non_numeric_string(self, x) -> bool:
        """Check if a value is a string that does not represent a number."""
        return isinstance(x, str) and not self._is_numeric_string(x)

    def apply(self) -> PandasDataFrame:
        """
        Separates textual values from the numeric column.

        Returns:
            PandasDataFrame: DataFrame with the original column containing only
                numeric values (non-numeric replaced with placeholder) and a new
                string column containing the extracted text values.

        Raises:
            ValueError: If the DataFrame is empty or column doesn't exist.
        """
        if self.df is None or self.df.empty:
            raise ValueError("The DataFrame is empty.")

        if self.column not in self.df.columns:
            raise ValueError(f"Column '{self.column}' does not exist in the DataFrame.")

        result_df = self.df.copy()
        string_col_name = f"{self.column}{self.suffix}"

        # Convert numeric-looking strings to actual numbers
        result_df[self.column] = result_df[self.column].apply(
            lambda x: float(x) if self._is_numeric_string(x) else x
        )

        # Create the string column with non-numeric values
        result_df[string_col_name] = result_df[self.column].where(
            result_df[self.column].apply(self._is_non_numeric_string)
        )
        result_df[string_col_name] = result_df[string_col_name].fillna(self.string_fill)

        # Replace non-numeric strings in the main column with placeholder
        result_df[self.column] = result_df[self.column].apply(
            lambda x: self.placeholder if self._is_non_numeric_string(x) else x
        )

        return result_df
