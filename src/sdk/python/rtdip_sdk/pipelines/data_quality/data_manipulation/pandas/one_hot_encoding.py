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
from ..interfaces import PandasDataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType


class OneHotEncoding(PandasDataManipulationBaseInterface):
    """
    Performs One-Hot Encoding on a specified column of a Pandas DataFrame.

    One-Hot Encoding converts categorical variables into binary columns.
    For each unique value in the specified column, a new column is created
    with 1s and 0s indicating the presence of that value.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.one_hot_encoding import OneHotEncoding
    import pandas as pd

    df = pd.DataFrame({
        'color': ['red', 'blue', 'red', 'green'],
        'size': [1, 2, 3, 4]
    })

    encoder = OneHotEncoding(df, column="color")
    result_df = encoder.apply()
    # Result will have columns: size, color_red, color_blue, color_green
    ```

    Parameters:
        df (PandasDataFrame): The Pandas DataFrame to apply encoding on.
        column (str): The name of the column to apply the encoding to.
        sparse (bool, optional): Whether to return sparse matrix. Defaults to False.
    """

    df: PandasDataFrame
    column: str
    sparse: bool

    def __init__(self, df: PandasDataFrame, column: str, sparse: bool = False) -> None:
        self.df = df
        self.column = column
        self.sparse = sparse

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
        Performs one-hot encoding on the specified column.

        Returns:
            PandasDataFrame: DataFrame with the original column replaced by
                            binary columns for each unique value.

        Raises:
            ValueError: If the DataFrame is empty or column doesn't exist.
        """
        if self.df is None or self.df.empty:
            raise ValueError("The DataFrame is empty.")

        if self.column not in self.df.columns:
            raise ValueError(f"Column '{self.column}' does not exist in the DataFrame.")

        return pd.get_dummies(self.df, columns=[self.column], sparse=self.sparse)
