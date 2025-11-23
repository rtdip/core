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


# Available datetime features that can be extracted
AVAILABLE_FEATURES = [
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "weekday",
    "day_name",
    "quarter",
    "week",
    "day_of_year",
    "is_weekend",
    "is_month_start",
    "is_month_end",
    "is_quarter_start",
    "is_quarter_end",
    "is_year_start",
    "is_year_end",
]


class DatetimeFeatures(PandasDataManipulationBaseInterface):
    """
    Extracts datetime/time-based features from a datetime column.

    This is useful for time series forecasting where temporal patterns
    (seasonality, day-of-week effects, etc.) are important predictors.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.datetime_features import DatetimeFeatures
    import pandas as pd

    df = pd.DataFrame({
        'timestamp': pd.date_range('2024-01-01', periods=5, freq='D'),
        'value': [1, 2, 3, 4, 5]
    })

    # Extract specific features
    extractor = DatetimeFeatures(
        df,
        datetime_column="timestamp",
        features=["year", "month", "weekday", "is_weekend"]
    )
    result_df = extractor.apply()
    # Result will have columns: timestamp, value, year, month, weekday, is_weekend
    ```

    Available features:
        - year: Year (e.g., 2024)
        - month: Month (1-12)
        - day: Day of month (1-31)
        - hour: Hour (0-23)
        - minute: Minute (0-59)
        - second: Second (0-59)
        - weekday: Day of week (0=Monday, 6=Sunday)
        - day_name: Name of day ("Monday", "Tuesday", etc.)
        - quarter: Quarter (1-4)
        - week: Week of year (1-52)
        - day_of_year: Day of year (1-366)
        - is_weekend: Boolean, True if Saturday or Sunday
        - is_month_start: Boolean, True if first day of month
        - is_month_end: Boolean, True if last day of month
        - is_quarter_start: Boolean, True if first day of quarter
        - is_quarter_end: Boolean, True if last day of quarter
        - is_year_start: Boolean, True if first day of year
        - is_year_end: Boolean, True if last day of year

    Parameters:
        df (PandasDataFrame): The Pandas DataFrame containing the datetime column.
        datetime_column (str): The name of the column containing datetime values.
        features (List[str], optional): List of features to extract.
            Defaults to ["year", "month", "day", "weekday"].
        prefix (str, optional): Prefix to add to new column names. Defaults to None.
    """

    df: PandasDataFrame
    datetime_column: str
    features: List[str]
    prefix: Optional[str]

    def __init__(
        self,
        df: PandasDataFrame,
        datetime_column: str,
        features: Optional[List[str]] = None,
        prefix: Optional[str] = None,
    ) -> None:
        self.df = df
        self.datetime_column = datetime_column
        self.features = features if features is not None else ["year", "month", "day", "weekday"]
        self.prefix = prefix

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
        Extracts the specified datetime features from the datetime column.

        Returns:
            PandasDataFrame: DataFrame with added datetime feature columns.

        Raises:
            ValueError: If the DataFrame is empty, column doesn't exist,
                       or invalid features are requested.
        """
        if self.df is None or self.df.empty:
            raise ValueError("The DataFrame is empty.")

        if self.datetime_column not in self.df.columns:
            raise ValueError(
                f"Column '{self.datetime_column}' does not exist in the DataFrame."
            )

        # Validate requested features
        invalid_features = set(self.features) - set(AVAILABLE_FEATURES)
        if invalid_features:
            raise ValueError(
                f"Invalid features: {invalid_features}. "
                f"Available features: {AVAILABLE_FEATURES}"
            )

        result_df = self.df.copy()

        # Ensure column is datetime type
        dt_col = pd.to_datetime(result_df[self.datetime_column])

        # Extract each requested feature
        for feature in self.features:
            col_name = f"{self.prefix}_{feature}" if self.prefix else feature

            if feature == "year":
                result_df[col_name] = dt_col.dt.year
            elif feature == "month":
                result_df[col_name] = dt_col.dt.month
            elif feature == "day":
                result_df[col_name] = dt_col.dt.day
            elif feature == "hour":
                result_df[col_name] = dt_col.dt.hour
            elif feature == "minute":
                result_df[col_name] = dt_col.dt.minute
            elif feature == "second":
                result_df[col_name] = dt_col.dt.second
            elif feature == "weekday":
                result_df[col_name] = dt_col.dt.weekday
            elif feature == "day_name":
                result_df[col_name] = dt_col.dt.day_name()
            elif feature == "quarter":
                result_df[col_name] = dt_col.dt.quarter
            elif feature == "week":
                result_df[col_name] = dt_col.dt.isocalendar().week
            elif feature == "day_of_year":
                result_df[col_name] = dt_col.dt.day_of_year
            elif feature == "is_weekend":
                result_df[col_name] = dt_col.dt.weekday >= 5
            elif feature == "is_month_start":
                result_df[col_name] = dt_col.dt.is_month_start
            elif feature == "is_month_end":
                result_df[col_name] = dt_col.dt.is_month_end
            elif feature == "is_quarter_start":
                result_df[col_name] = dt_col.dt.is_quarter_start
            elif feature == "is_quarter_end":
                result_df[col_name] = dt_col.dt.is_quarter_end
            elif feature == "is_year_start":
                result_df[col_name] = dt_col.dt.is_year_start
            elif feature == "is_year_end":
                result_df[col_name] = dt_col.dt.is_year_end

        return result_df
