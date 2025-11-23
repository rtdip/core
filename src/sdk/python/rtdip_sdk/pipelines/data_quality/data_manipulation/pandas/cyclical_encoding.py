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

import numpy as np
import pandas as pd
from pandas import DataFrame as PandasDataFrame
from typing import Optional
from ..interfaces import PandasDataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType


class CyclicalEncoding(PandasDataManipulationBaseInterface):
    """
    Applies cyclical encoding to a periodic column using sine/cosine transformation.

    Cyclical encoding captures the circular nature of periodic features where
    the end wraps around to the beginning (e.g., December is close to January,
    hour 23 is close to hour 0).

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.cyclical_encoding import CyclicalEncoding
    import pandas as pd

    df = pd.DataFrame({
        'month': [1, 6, 12],
        'value': [100, 200, 300]
    })

    # Encode month cyclically (period=12 for months)
    encoder = CyclicalEncoding(df, column='month', period=12)
    result_df = encoder.apply()
    # Result will have columns: month, value, month_sin, month_cos
    ```

    Parameters:
        df (PandasDataFrame): The Pandas DataFrame containing the column to encode.
        column (str): The name of the column to encode cyclically.
        period (int): The period of the cycle (e.g., 12 for months, 24 for hours, 7 for weekdays).
        drop_original (bool, optional): Whether to drop the original column. Defaults to False.
    """

    df: PandasDataFrame
    column: str
    period: int
    drop_original: bool

    def __init__(
        self,
        df: PandasDataFrame,
        column: str,
        period: int,
        drop_original: bool = False,
    ) -> None:
        self.df = df
        self.column = column
        self.period = period
        self.drop_original = drop_original

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
        Applies cyclical encoding using sine and cosine transformations.

        Returns:
            PandasDataFrame: DataFrame with added {column}_sin and {column}_cos columns.

        Raises:
            ValueError: If the DataFrame is empty, column doesn't exist, or period <= 0.
        """
        if self.df is None or self.df.empty:
            raise ValueError("The DataFrame is empty.")

        if self.column not in self.df.columns:
            raise ValueError(
                f"Column '{self.column}' does not exist in the DataFrame."
            )

        if self.period <= 0:
            raise ValueError(f"Period must be positive, got {self.period}.")

        result_df = self.df.copy()

        # Apply sine/cosine transformation
        result_df[f"{self.column}_sin"] = np.sin(
            2 * np.pi * result_df[self.column] / self.period
        )
        result_df[f"{self.column}_cos"] = np.cos(
            2 * np.pi * result_df[self.column] / self.period
        )

        if self.drop_original:
            result_df = result_df.drop(columns=[self.column])

        return result_df
