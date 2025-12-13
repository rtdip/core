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

"""
Data validation and preparation utilities for RTDIP visualization components.

This module provides functions for:
- Column aliasing (mapping user column names to expected names)
- Input validation (checking required columns exist)
- Type coercion (converting columns to expected types)
- Descriptive error messages

Example
--------
```python
from rtdip_sdk.pipelines.visualization.validation import (
    apply_column_mapping,
    validate_dataframe,
    coerce_types,
)

# Apply column mapping
df = apply_column_mapping(my_df, {"my_time": "timestamp", "reading": "value"})

# Validate required columns exist
validate_dataframe(df, required_columns=["timestamp", "value"], df_name="historical_data")

# Coerce types
df = coerce_types(df, datetime_cols=["timestamp"], numeric_cols=["value"])
```
"""

import warnings
from typing import Dict, List, Optional, Union

import pandas as pd
from pandas import DataFrame as PandasDataFrame


class VisualizationDataError(Exception):
    """Exception raised for visualization data validation errors."""

    pass


def apply_column_mapping(
    df: PandasDataFrame,
    column_mapping: Optional[Dict[str, str]] = None,
    inplace: bool = False,
    strict: bool = False,
) -> PandasDataFrame:
    """
    Apply column name mapping to a DataFrame.

    Maps user-provided column names to the names expected by visualization classes.
    The mapping is from source column name to target column name.

    Args:
        df: Input DataFrame
        column_mapping: Dictionary mapping source column names to target names.
            Example: {"my_time_col": "timestamp", "sensor_reading": "value"}
        inplace: If True, modify DataFrame in place. Otherwise return a copy.
        strict: If True, raise error when source columns don't exist.
            If False (default), silently ignore missing source columns.
            This allows the same mapping to be used across multiple DataFrames
            where not all columns exist in all DataFrames.

    Returns:
        DataFrame with renamed columns

    Example
    --------
    ```python
    # User has columns "time" and "reading", but viz expects "timestamp" and "value"
    df = apply_column_mapping(df, {"time": "timestamp", "reading": "value"})
    ```

    Raises:
        VisualizationDataError: If strict=True and a source column doesn't exist
    """
    if column_mapping is None or len(column_mapping) == 0:
        return df if inplace else df.copy()

    if not inplace:
        df = df.copy()

    if strict:
        missing_sources = [col for col in column_mapping.keys() if col not in df.columns]
        if missing_sources:
            raise VisualizationDataError(
                f"Column mapping error: Source columns not found in DataFrame: {missing_sources}\n"
                f"Available columns: {list(df.columns)}\n"
                f"Mapping provided: {column_mapping}"
            )

    applicable_mapping = {
        src: tgt for src, tgt in column_mapping.items() if src in df.columns
    }

    df.rename(columns=applicable_mapping, inplace=True)

    return df


def validate_dataframe(
    df: PandasDataFrame,
    required_columns: List[str],
    df_name: str = "DataFrame",
    optional_columns: Optional[List[str]] = None,
) -> Dict[str, bool]:
    """
    Validate that a DataFrame contains required columns.

    Args:
        df: DataFrame to validate
        required_columns: List of column names that must be present
        df_name: Name of the DataFrame (for error messages)
        optional_columns: List of optional column names to check for presence

    Returns:
        Dictionary with column names as keys and True/False for presence

    Raises:
        VisualizationDataError: If any required columns are missing

    Example
    --------
    ```python
    validate_dataframe(
        historical_df,
        required_columns=["timestamp", "value"],
        df_name="historical_data"
    )
    ```
    """
    if df is None:
        raise VisualizationDataError(
            f"{df_name} is None. Please provide a valid DataFrame."
        )

    if not isinstance(df, pd.DataFrame):
        raise VisualizationDataError(
            f"{df_name} must be a pandas DataFrame, got {type(df).__name__}"
        )

    if len(df) == 0:
        raise VisualizationDataError(
            f"{df_name} is empty. Please provide a DataFrame with data."
        )

    missing_required = [col for col in required_columns if col not in df.columns]
    if missing_required:
        raise VisualizationDataError(
            f"{df_name} is missing required columns: {missing_required}\n"
            f"Required columns: {required_columns}\n"
            f"Available columns: {list(df.columns)}\n"
            f"Hint: Use the 'column_mapping' parameter to map your column names. "
            f"Example: column_mapping={{'{missing_required[0]}': 'your_column_name'}}"
        )

    column_presence = {col: True for col in required_columns}
    if optional_columns:
        for col in optional_columns:
            column_presence[col] = col in df.columns

    return column_presence


def coerce_datetime(
    df: PandasDataFrame,
    columns: List[str],
    errors: str = "coerce",
    inplace: bool = False,
) -> PandasDataFrame:
    """
    Convert columns to datetime type.

    Args:
        df: Input DataFrame
        columns: List of column names to convert
        errors: How to handle errors - 'raise', 'coerce' (invalid become NaT), or 'ignore'
        inplace: If True, modify DataFrame in place

    Returns:
        DataFrame with converted columns

    Example
    --------
    ```python
    df = coerce_datetime(df, columns=["timestamp", "event_time"])
    ```
    """
    if not inplace:
        df = df.copy()

    for col in columns:
        if col not in df.columns:
            continue

        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue  

        try:
            original_na_count = df[col].isna().sum()
            df[col] = pd.to_datetime(df[col], errors=errors)
            new_na_count = df[col].isna().sum()

            failed_conversions = new_na_count - original_na_count
            if failed_conversions > 0:
                warnings.warn(
                    f"Column '{col}': {failed_conversions} values could not be "
                    f"converted to datetime and were set to NaT",
                    UserWarning,
                )
        except Exception as e:
            if errors == "raise":
                raise VisualizationDataError(
                    f"Failed to convert column '{col}' to datetime: {e}\n"
                    f"Sample values: {df[col].head(3).tolist()}"
                )

    return df


def coerce_numeric(
    df: PandasDataFrame,
    columns: List[str],
    errors: str = "coerce",
    inplace: bool = False,
) -> PandasDataFrame:
    """
    Convert columns to numeric type.

    Args:
        df: Input DataFrame
        columns: List of column names to convert
        errors: How to handle errors - 'raise', 'coerce' (invalid become NaN), or 'ignore'
        inplace: If True, modify DataFrame in place

    Returns:
        DataFrame with converted columns

    Example
    --------
    ```python
    df = coerce_numeric(df, columns=["value", "mean", "0.1", "0.9"])
    ```
    """
    if not inplace:
        df = df.copy()

    for col in columns:
        if col not in df.columns:
            continue

        if pd.api.types.is_numeric_dtype(df[col]):
            continue 

        try:
            original_na_count = df[col].isna().sum()
            df[col] = pd.to_numeric(df[col], errors=errors)
            new_na_count = df[col].isna().sum()

            failed_conversions = new_na_count - original_na_count
            if failed_conversions > 0:
                warnings.warn(
                    f"Column '{col}': {failed_conversions} values could not be "
                    f"converted to numeric and were set to NaN",
                    UserWarning,
                )
        except Exception as e:
            if errors == "raise":
                raise VisualizationDataError(
                    f"Failed to convert column '{col}' to numeric: {e}\n"
                    f"Sample values: {df[col].head(3).tolist()}"
                )

    return df


def coerce_types(
    df: PandasDataFrame,
    datetime_cols: Optional[List[str]] = None,
    numeric_cols: Optional[List[str]] = None,
    errors: str = "coerce",
    inplace: bool = False,
) -> PandasDataFrame:
    """
    Convert multiple columns to their expected types.

    Combines datetime and numeric coercion in a single call.

    Args:
        df: Input DataFrame
        datetime_cols: Columns to convert to datetime
        numeric_cols: Columns to convert to numeric
        errors: How to handle errors - 'raise', 'coerce', or 'ignore'
        inplace: If True, modify DataFrame in place

    Returns:
        DataFrame with converted columns

    Example
    --------
    ```python
    df = coerce_types(
        df,
        datetime_cols=["timestamp"],
        numeric_cols=["value", "mean", "0.1", "0.9"]
    )
    ```
    """
    if not inplace:
        df = df.copy()

    if datetime_cols:
        df = coerce_datetime(df, datetime_cols, errors=errors, inplace=True)

    if numeric_cols:
        df = coerce_numeric(df, numeric_cols, errors=errors, inplace=True)

    return df


def prepare_dataframe(
    df: PandasDataFrame,
    required_columns: List[str],
    df_name: str = "DataFrame",
    column_mapping: Optional[Dict[str, str]] = None,
    datetime_cols: Optional[List[str]] = None,
    numeric_cols: Optional[List[str]] = None,
    optional_columns: Optional[List[str]] = None,
    sort_by: Optional[str] = None,
) -> PandasDataFrame:
    """
    Prepare a DataFrame for visualization with full validation and coercion.

    This is a convenience function that combines column mapping, validation,
    and type coercion in a single call.

    Args:
        df: Input DataFrame
        required_columns: Columns that must be present
        df_name: Name for error messages
        column_mapping: Optional mapping from source to target column names
        datetime_cols: Columns to convert to datetime
        numeric_cols: Columns to convert to numeric
        optional_columns: Optional columns to check for
        sort_by: Column to sort by after preparation

    Returns:
        Prepared DataFrame ready for visualization

    Example
    --------
    ```python
    historical_df = prepare_dataframe(
        my_df,
        required_columns=["timestamp", "value"],
        df_name="historical_data",
        column_mapping={"time": "timestamp", "reading": "value"},
        datetime_cols=["timestamp"],
        numeric_cols=["value"],
        sort_by="timestamp"
    )
    ```
    """
    df = apply_column_mapping(df, column_mapping, inplace=False)

    validate_dataframe(
        df,
        required_columns=required_columns,
        df_name=df_name,
        optional_columns=optional_columns,
    )

    df = coerce_types(
        df,
        datetime_cols=datetime_cols,
        numeric_cols=numeric_cols,
        inplace=True,
    )

    if sort_by and sort_by in df.columns:
        df = df.sort_values(sort_by)

    return df


def check_data_overlap(
    df1: PandasDataFrame,
    df2: PandasDataFrame,
    on: str,
    df1_name: str = "DataFrame1",
    df2_name: str = "DataFrame2",
    min_overlap: int = 1,
) -> int:
    """
    Check if two DataFrames have overlapping values in a column.

    Useful for checking if forecast and actual data have overlapping timestamps.

    Args:
        df1: First DataFrame
        df2: Second DataFrame
        on: Column name to check for overlap
        df1_name: Name of first DataFrame for messages
        df2_name: Name of second DataFrame for messages
        min_overlap: Minimum required overlap count

    Returns:
        Number of overlapping values

    Raises:
        VisualizationDataError: If overlap is less than min_overlap
    """
    if on not in df1.columns or on not in df2.columns:
        raise VisualizationDataError(
            f"Column '{on}' must exist in both DataFrames for overlap check"
        )

    overlap = set(df1[on]).intersection(set(df2[on]))
    overlap_count = len(overlap)

    if overlap_count < min_overlap:
        warnings.warn(
            f"Low data overlap: {df1_name} and {df2_name} have only "
            f"{overlap_count} matching values in column '{on}'. "
            f"This may result in incomplete visualizations.",
            UserWarning,
        )

    return overlap_count
