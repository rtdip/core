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
from typing import List, Optional
from ..interfaces import PandasDataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType


class ChronologicalSort(PandasDataManipulationBaseInterface):
    """
    Sorts a DataFrame chronologically by a datetime column.

    This component is essential for time series preprocessing to ensure
    data is in the correct temporal order before applying operations
    like lag features, rolling statistics, or time-based splits.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.chronological_sort import ChronologicalSort
    import pandas as pd

    df = pd.DataFrame({
        'sensor_id': ['A', 'B', 'C'],
        'timestamp': pd.to_datetime(['2024-01-03', '2024-01-01', '2024-01-02']),
        'value': [30, 10, 20]
    })

    sorter = ChronologicalSort(df, datetime_column="timestamp")
    result_df = sorter.apply()
    # Result will be sorted: 2024-01-01, 2024-01-02, 2024-01-03
    ```

    Parameters:
        df (PandasDataFrame): The Pandas DataFrame to sort.
        datetime_column (str): The name of the datetime column to sort by.
        ascending (bool, optional): Sort in ascending order (oldest first).
            Defaults to True.
        group_columns (List[str], optional): Columns to group by before sorting.
            If provided, sorting is done within each group. Defaults to None.
        na_position (str, optional): Position of NaT values after sorting.
            Options: "first" or "last". Defaults to "last".
        reset_index (bool, optional): Whether to reset the index after sorting.
            Defaults to True.
    """

    df: PandasDataFrame
    datetime_column: str
    ascending: bool
    group_columns: Optional[List[str]]
    na_position: str
    reset_index: bool

    def __init__(
        self,
        df: PandasDataFrame,
        datetime_column: str,
        ascending: bool = True,
        group_columns: Optional[List[str]] = None,
        na_position: str = "last",
        reset_index: bool = True,
    ) -> None:
        self.df = df
        self.datetime_column = datetime_column
        self.ascending = ascending
        self.group_columns = group_columns
        self.na_position = na_position
        self.reset_index = reset_index

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PANDAS
        """
        return SystemType.PANDAS

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def apply(self) -> PandasDataFrame:
        """
        Sorts the DataFrame chronologically by the datetime column.

        Returns:
            PandasDataFrame: Sorted DataFrame.

        Raises:
            ValueError: If the DataFrame is empty, column doesn't exist,
                or invalid na_position is specified.
        """
        if self.df is None or self.df.empty:
            raise ValueError("The DataFrame is empty.")

        if self.datetime_column not in self.df.columns:
            raise ValueError(
                f"Column '{self.datetime_column}' does not exist in the DataFrame."
            )

        if self.group_columns:
            for col in self.group_columns:
                if col not in self.df.columns:
                    raise ValueError(
                        f"Group column '{col}' does not exist in the DataFrame."
                    )

        valid_na_positions = ["first", "last"]
        if self.na_position not in valid_na_positions:
            raise ValueError(
                f"Invalid na_position '{self.na_position}'. "
                f"Must be one of {valid_na_positions}."
            )

        result_df = self.df.copy()

        if self.group_columns:
            # Sort by group columns first, then by datetime within groups
            sort_columns = self.group_columns + [self.datetime_column]
            result_df = result_df.sort_values(
                by=sort_columns,
                ascending=[True] * len(self.group_columns) + [self.ascending],
                na_position=self.na_position,
                kind="mergesort",  # Stable sort to preserve order of equal elements
            )
        else:
            result_df = result_df.sort_values(
                by=self.datetime_column,
                ascending=self.ascending,
                na_position=self.na_position,
                kind="mergesort",
            )

        if self.reset_index:
            result_df = result_df.reset_index(drop=True)

        return result_df
