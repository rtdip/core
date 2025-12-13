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

"""Tests for visualization validation module."""

import numpy as np
import pandas as pd
import pytest

from src.sdk.python.rtdip_sdk.pipelines.visualization.validation import (
    VisualizationDataError,
    apply_column_mapping,
    validate_dataframe,
    coerce_datetime,
    coerce_numeric,
    coerce_types,
    prepare_dataframe,
    check_data_overlap,
)


class TestApplyColumnMapping:
    """Tests for apply_column_mapping function."""

    def test_no_mapping(self):
        """Test that data is returned unchanged when no mapping provided."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        result = apply_column_mapping(df, column_mapping=None)
        assert list(result.columns) == ["a", "b"]

    def test_empty_mapping(self):
        """Test that data is returned unchanged when empty mapping provided."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        result = apply_column_mapping(df, column_mapping={})
        assert list(result.columns) == ["a", "b"]

    def test_valid_mapping(self):
        """Test that columns are renamed correctly."""
        df = pd.DataFrame({"my_time": [1, 2, 3], "reading": [4, 5, 6]})
        result = apply_column_mapping(
            df, column_mapping={"my_time": "timestamp", "reading": "value"}
        )
        assert list(result.columns) == ["timestamp", "value"]

    def test_partial_mapping(self):
        """Test that partial mapping works."""
        df = pd.DataFrame({"my_time": [1, 2, 3], "value": [4, 5, 6]})
        result = apply_column_mapping(df, column_mapping={"my_time": "timestamp"})
        assert list(result.columns) == ["timestamp", "value"]

    def test_missing_source_column_ignored(self):
        """Test that missing source columns are ignored by default (non-strict mode)."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        result = apply_column_mapping(df, column_mapping={"nonexistent": "timestamp", "a": "x"})
        assert list(result.columns) == ["x", "b"]

    def test_invalid_source_column_strict_mode(self):
        """Test that error is raised when source column doesn't exist in strict mode."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        with pytest.raises(VisualizationDataError) as exc_info:
            apply_column_mapping(df, column_mapping={"nonexistent": "timestamp"}, strict=True)
        assert "Source columns not found" in str(exc_info.value)

    def test_inplace_false(self):
        """Test that inplace=False returns a copy."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = apply_column_mapping(df, column_mapping={"a": "b"}, inplace=False)
        assert list(df.columns) == ["a"]
        assert list(result.columns) == ["b"]

    def test_inplace_true(self):
        """Test that inplace=True modifies the original."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = apply_column_mapping(df, column_mapping={"a": "b"}, inplace=True)
        assert list(df.columns) == ["b"]
        assert result is df


class TestValidateDataframe:
    """Tests for validate_dataframe function."""

    def test_valid_dataframe(self):
        """Test validation passes for valid DataFrame."""
        df = pd.DataFrame({"timestamp": [1, 2], "value": [3, 4]})
        result = validate_dataframe(df, required_columns=["timestamp", "value"])
        assert result == {"timestamp": True, "value": True}

    def test_missing_required_column(self):
        """Test error raised when required column missing."""
        df = pd.DataFrame({"timestamp": [1, 2]})
        with pytest.raises(VisualizationDataError) as exc_info:
            validate_dataframe(
                df, required_columns=["timestamp", "value"], df_name="test_df"
            )
        assert "test_df is missing required columns" in str(exc_info.value)
        assert "['value']" in str(exc_info.value)

    def test_none_dataframe(self):
        """Test error raised when DataFrame is None."""
        with pytest.raises(VisualizationDataError) as exc_info:
            validate_dataframe(None, required_columns=["timestamp"])
        assert "is None" in str(exc_info.value)

    def test_empty_dataframe(self):
        """Test error raised when DataFrame is empty."""
        df = pd.DataFrame({"timestamp": [], "value": []})
        with pytest.raises(VisualizationDataError) as exc_info:
            validate_dataframe(df, required_columns=["timestamp"])
        assert "is empty" in str(exc_info.value)

    def test_not_dataframe(self):
        """Test error raised when input is not a DataFrame."""
        with pytest.raises(VisualizationDataError) as exc_info:
            validate_dataframe([1, 2, 3], required_columns=["timestamp"])
        assert "must be a pandas DataFrame" in str(exc_info.value)

    def test_optional_columns(self):
        """Test optional columns are reported correctly."""
        df = pd.DataFrame({"timestamp": [1, 2], "value": [3, 4], "optional": [5, 6]})
        result = validate_dataframe(
            df,
            required_columns=["timestamp", "value"],
            optional_columns=["optional", "missing_optional"],
        )
        assert result["timestamp"] is True
        assert result["value"] is True
        assert result["optional"] is True
        assert result["missing_optional"] is False


class TestCoerceDatetime:
    """Tests for coerce_datetime function."""

    def test_string_to_datetime(self):
        """Test converting string timestamps to datetime."""
        df = pd.DataFrame({"timestamp": ["2024-01-01", "2024-01-02", "2024-01-03"]})
        result = coerce_datetime(df, columns=["timestamp"])
        assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])

    def test_already_datetime(self):
        """Test that datetime columns are unchanged."""
        df = pd.DataFrame({"timestamp": pd.date_range("2024-01-01", periods=3)})
        result = coerce_datetime(df, columns=["timestamp"])
        assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])

    def test_missing_column_ignored(self):
        """Test that missing columns are silently ignored."""
        df = pd.DataFrame({"timestamp": ["2024-01-01"]})
        result = coerce_datetime(df, columns=["timestamp", "nonexistent"])
        assert "nonexistent" not in result.columns

    def test_invalid_values_coerced_to_nat(self):
        """Test that invalid values become NaT with errors='coerce'."""
        df = pd.DataFrame({"timestamp": ["2024-01-01", "invalid", "2024-01-03"]})
        result = coerce_datetime(df, columns=["timestamp"], errors="coerce")
        assert pd.isna(result["timestamp"].iloc[1])


class TestCoerceNumeric:
    """Tests for coerce_numeric function."""

    def test_string_to_numeric(self):
        """Test converting string numbers to numeric."""
        df = pd.DataFrame({"value": ["1.5", "2.5", "3.5"]})
        result = coerce_numeric(df, columns=["value"])
        assert pd.api.types.is_numeric_dtype(result["value"])
        assert result["value"].iloc[0] == 1.5

    def test_already_numeric(self):
        """Test that numeric columns are unchanged."""
        df = pd.DataFrame({"value": [1.5, 2.5, 3.5]})
        result = coerce_numeric(df, columns=["value"])
        assert pd.api.types.is_numeric_dtype(result["value"])

    def test_invalid_values_coerced_to_nan(self):
        """Test that invalid values become NaN with errors='coerce'."""
        df = pd.DataFrame({"value": ["1.5", "invalid", "3.5"]})
        result = coerce_numeric(df, columns=["value"], errors="coerce")
        assert pd.isna(result["value"].iloc[1])


class TestCoerceTypes:
    """Tests for coerce_types function."""

    def test_combined_coercion(self):
        """Test coercing both datetime and numeric columns."""
        df = pd.DataFrame(
            {
                "timestamp": ["2024-01-01", "2024-01-02"],
                "value": ["1.5", "2.5"],
                "other": ["a", "b"],
            }
        )
        result = coerce_types(
            df, datetime_cols=["timestamp"], numeric_cols=["value"]
        )
        assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])
        assert pd.api.types.is_numeric_dtype(result["value"])
        assert result["other"].dtype == object


class TestPrepareDataframe:
    """Tests for prepare_dataframe function."""

    def test_full_preparation(self):
        """Test complete DataFrame preparation."""
        df = pd.DataFrame(
            {
                "my_time": ["2024-01-02", "2024-01-01", "2024-01-03"],
                "reading": ["1.5", "2.5", "3.5"],
            }
        )
        result = prepare_dataframe(
            df,
            required_columns=["timestamp", "value"],
            column_mapping={"my_time": "timestamp", "reading": "value"},
            datetime_cols=["timestamp"],
            numeric_cols=["value"],
            sort_by="timestamp",
        )

        assert "timestamp" in result.columns
        assert "value" in result.columns

        assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])
        assert pd.api.types.is_numeric_dtype(result["value"])

        assert result["value"].iloc[0] == 2.5

    def test_missing_column_error(self):
        """Test error when required column missing after mapping."""
        df = pd.DataFrame({"timestamp": [1, 2, 3]})
        with pytest.raises(VisualizationDataError) as exc_info:
            prepare_dataframe(df, required_columns=["timestamp", "value"])
        assert "missing required columns" in str(exc_info.value)


class TestCheckDataOverlap:
    """Tests for check_data_overlap function."""

    def test_full_overlap(self):
        """Test with full overlap."""
        df1 = pd.DataFrame({"timestamp": [1, 2, 3]})
        df2 = pd.DataFrame({"timestamp": [1, 2, 3]})
        result = check_data_overlap(df1, df2, on="timestamp")
        assert result == 3

    def test_partial_overlap(self):
        """Test with partial overlap."""
        df1 = pd.DataFrame({"timestamp": [1, 2, 3]})
        df2 = pd.DataFrame({"timestamp": [2, 3, 4]})
        result = check_data_overlap(df1, df2, on="timestamp")
        assert result == 2

    def test_no_overlap_warning(self):
        """Test warning when no overlap."""
        df1 = pd.DataFrame({"timestamp": [1, 2, 3]})
        df2 = pd.DataFrame({"timestamp": [4, 5, 6]})
        with pytest.warns(UserWarning, match="Low data overlap"):
            result = check_data_overlap(df1, df2, on="timestamp")
        assert result == 0

    def test_missing_column_error(self):
        """Test error when column missing."""
        df1 = pd.DataFrame({"timestamp": [1, 2, 3]})
        df2 = pd.DataFrame({"other": [1, 2, 3]})
        with pytest.raises(VisualizationDataError) as exc_info:
            check_data_overlap(df1, df2, on="timestamp")
        assert "must exist in both DataFrames" in str(exc_info.value)


class TestColumnMappingIntegration:
    """Integration tests for column mapping with visualization classes."""

    def test_forecast_plot_with_column_mapping(self):
        """Test ForecastPlot works with column mapping."""
        from src.sdk.python.rtdip_sdk.pipelines.visualization.matplotlib.forecasting import (
            ForecastPlot,
        )

        historical_df = pd.DataFrame(
            {
                "time": pd.date_range("2024-01-01", periods=10, freq="h"),
                "reading": np.random.randn(10),
            }
        )
        forecast_df = pd.DataFrame(
            {
                "time": pd.date_range("2024-01-01T10:00:00", periods=5, freq="h"),
                "prediction": np.random.randn(5),
            }
        )

        plot = ForecastPlot(
            historical_data=historical_df,
            forecast_data=forecast_df,
            forecast_start=pd.Timestamp("2024-01-01T10:00:00"),
            column_mapping={
                "time": "timestamp",
                "reading": "value",
                "prediction": "mean",
            },
        )

        fig = plot.plot()
        assert fig is not None
        import matplotlib.pyplot as plt
        plt.close(fig)

    def test_error_message_with_hint(self):
        """Test that error messages include helpful hints."""
        from src.sdk.python.rtdip_sdk.pipelines.visualization.matplotlib.forecasting import (
            ForecastPlot,
        )

        historical_df = pd.DataFrame(
            {
                "time": pd.date_range("2024-01-01", periods=10, freq="h"),
                "reading": np.random.randn(10),
            }
        )
        forecast_df = pd.DataFrame(
            {
                "time": pd.date_range("2024-01-01T10:00:00", periods=5, freq="h"),
                "mean": np.random.randn(5),
            }
        )

        with pytest.raises(VisualizationDataError) as exc_info:
            ForecastPlot(
                historical_data=historical_df,
                forecast_data=forecast_df,
                forecast_start=pd.Timestamp("2024-01-01T10:00:00"),
            )

        error_message = str(exc_info.value)
        assert "missing required columns" in error_message
        assert "column_mapping" in error_message
