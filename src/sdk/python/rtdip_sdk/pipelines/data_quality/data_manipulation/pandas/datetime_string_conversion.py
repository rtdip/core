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


# Default datetime formats to try when parsing
DEFAULT_FORMATS = [
    "%Y-%m-%d %H:%M:%S.%f",  # With microseconds
    "%Y-%m-%d %H:%M:%S",     # Without microseconds
    "%Y/%m/%d %H:%M:%S",     # Slash separator
    "%d-%m-%Y %H:%M:%S",     # DD-MM-YYYY format
    "%Y-%m-%dT%H:%M:%S",     # ISO format without microseconds
    "%Y-%m-%dT%H:%M:%S.%f",  # ISO format with microseconds
]


class DatetimeStringConversion(PandasDataManipulationBaseInterface):
    """
    Converts string-based timestamp columns to datetime with robust format handling.

    This component handles mixed datetime formats commonly found in industrial
    sensor data, including timestamps with and without microseconds, different
    separators, and various date orderings.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.datetime_string_conversion import DatetimeStringConversion
    import pandas as pd

    df = pd.DataFrame({
        'sensor_id': ['A', 'B', 'C'],
        'EventTime': ['2024-01-02 20:03:46.000', '2024-01-02 16:00:12.123', '2024-01-02 11:56:42']
    })

    converter = DatetimeStringConversion(
        df,
        column="EventTime",
        output_column="EventTime_DT"
    )
    result_df = converter.apply()
    # Result will have a new 'EventTime_DT' column with datetime objects
    ```

    Parameters:
        df (PandasDataFrame): The Pandas DataFrame containing the datetime string column.
        column (str): The name of the column containing datetime strings.
        output_column (str, optional): Name for the output datetime column.
            Defaults to "{column}_DT".
        formats (List[str], optional): List of datetime formats to try.
            Defaults to common formats including with/without microseconds.
        strip_trailing_zeros (bool, optional): Whether to strip trailing '.000'
            before parsing. Defaults to True.
        keep_original (bool, optional): Whether to keep the original string column.
            Defaults to True.
    """

    df: PandasDataFrame
    column: str
    output_column: Optional[str]
    formats: List[str]
    strip_trailing_zeros: bool
    keep_original: bool

    def __init__(
        self,
        df: PandasDataFrame,
        column: str,
        output_column: Optional[str] = None,
        formats: Optional[List[str]] = None,
        strip_trailing_zeros: bool = True,
        keep_original: bool = True,
    ) -> None:
        self.df = df
        self.column = column
        self.output_column = output_column if output_column else f"{column}_DT"
        self.formats = formats if formats is not None else DEFAULT_FORMATS
        self.strip_trailing_zeros = strip_trailing_zeros
        self.keep_original = keep_original

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
        Converts string timestamps to datetime objects.

        The conversion tries multiple formats and handles edge cases like
        trailing zeros in milliseconds. Failed conversions result in NaT.

        Returns:
            PandasDataFrame: DataFrame with added datetime column.

        Raises:
            ValueError: If the DataFrame is empty or column doesn't exist.
        """
        if self.df is None or self.df.empty:
            raise ValueError("The DataFrame is empty.")

        if self.column not in self.df.columns:
            raise ValueError(
                f"Column '{self.column}' does not exist in the DataFrame."
            )

        result_df = self.df.copy()

        # Convert column to string for consistent processing
        s = result_df[self.column].astype(str)

        # Initialize result with NaT
        result = pd.Series(pd.NaT, index=result_df.index, dtype="datetime64[ns]")

        if self.strip_trailing_zeros:
            # Handle timestamps ending with '.000' separately for better performance
            mask_trailing_zeros = s.str.endswith(".000")

            if mask_trailing_zeros.any():
                # Parse without fractional seconds after stripping '.000'
                result.loc[mask_trailing_zeros] = pd.to_datetime(
                    s.loc[mask_trailing_zeros].str[:-4],
                    format="%Y-%m-%d %H:%M:%S",
                    errors="coerce",
                )

            # Process remaining values
            remaining = ~mask_trailing_zeros
        else:
            remaining = pd.Series(True, index=result_df.index)

        # Try each format for remaining unparsed values
        for fmt in self.formats:
            still_nat = result.isna() & remaining
            if not still_nat.any():
                break

            try:
                parsed = pd.to_datetime(
                    s.loc[still_nat],
                    format=fmt,
                    errors="coerce",
                )
                # Update only successfully parsed values
                successfully_parsed = ~parsed.isna()
                result.loc[still_nat & successfully_parsed.reindex(still_nat.index, fill_value=False)] = parsed[successfully_parsed]
            except Exception:
                continue

        # Final fallback: try ISO8601 format for any remaining NaT values
        still_nat = result.isna()
        if still_nat.any():
            try:
                parsed = pd.to_datetime(
                    s.loc[still_nat],
                    format="ISO8601",
                    errors="coerce",
                )
                result.loc[still_nat] = parsed
            except Exception:
                pass

        # Last resort: infer format
        still_nat = result.isna()
        if still_nat.any():
            try:
                parsed = pd.to_datetime(
                    s.loc[still_nat],
                    format="mixed",
                    errors="coerce",
                )
                result.loc[still_nat] = parsed
            except Exception:
                pass

        result_df[self.output_column] = result

        if not self.keep_original:
            result_df = result_df.drop(columns=[self.column])

        return result_df
