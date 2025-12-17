"""
Shell Data Preprocessing Script
This script preprocesses Shell sensor data using RTDIP pipeline components.
Usage:
    python preprocess_shell_data.py --input ShellData.parquet --output ShellData_preprocessed.parquet
    python preprocess_shell_data.py --input ShellData.csv --output ShellData_preprocessed.parquet

Steps:
    1. Separate text values from numeric Value column
    2. Convert EventTime strings to datetime
    3. One-hot encode Status column
    4. Extract datetime features (day, week, weekday, month)
    5. Handle missing values (drop rows with NaT timestamps or missing values)
    6. Sort chronologically
    7. Detect and handle outliers using MAD
    8. Save final dataset
"""

import argparse
import gc
import json
import os
import sys
from pathlib import Path

import pandas as pd

SDK_PATH = Path(__file__).resolve().parents[3] / "src" / "sdk" / "python"
sys.path.insert(0, str(SDK_PATH))

from rtdip_sdk.pipelines.data_quality.data_manipulation.pandas import (
    MixedTypeSeparation,
    DatetimeStringConversion,
    OneHotEncoding,
    DatetimeFeatures,
    ChronologicalSort,
    MADOutlierDetection,
)


def print_step(step_num: int, description: str) -> None:
    """Print a formatted step header."""
    print(f"Step {step_num}: {description}")


def print_stats(df: pd.DataFrame, description: str = "") -> None:
    """Print DataFrame statistics."""
    if description:
        print(f"\n{description}")
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Memory: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")


def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize DataFrame memory usage by downcasting numeric types."""
    float_cols = df.select_dtypes(include=['float64']).columns
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], downcast='float')

    int_cols = df.select_dtypes(include=['int64']).columns
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], downcast='integer')

    return df


def load_data(input_path: str) -> pd.DataFrame:
    """Load data from parquet or CSV file (auto-detected by extension)."""
    print(f"Loading data from {input_path}")

    input_path_obj = Path(input_path)
    extension = input_path_obj.suffix.lower()

    if extension == ".parquet":
        df = pd.read_parquet(input_path)
    elif extension == ".csv":
        df = pd.read_csv(input_path, dtype={
            'Value': 'float32',  
        })
    else:
        raise ValueError(f"Unsupported file format: {extension}. Use .parquet or .csv")

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    print_stats(df, "Loaded data:")

    df = optimize_dtypes(df)

    return df


def sample_data(df: pd.DataFrame, sample_fraction: float) -> pd.DataFrame:
    """Sample a fraction of the data for memory-constrained systems."""
    if sample_fraction is None or sample_fraction >= 1.0:
        return df

    print(f"\nSampling {sample_fraction*100:.1f}% of data for memory efficiency (or for showcasing)")
    original_len = len(df)
    df_sampled = df.sample(frac=sample_fraction, random_state=42)
    print(f"Reduced from {original_len:,} to {len(df_sampled):,} rows")

    return df_sampled


def step1_separate_mixed_types(df: pd.DataFrame) -> pd.DataFrame:
    """Step 1: Separate text values from numeric Value column."""
    print_step(1, "Separating text values from numeric Value column")

    separator = MixedTypeSeparation(
        df,
        column="Value",
        placeholder=-1,
        string_fill="NaN",
        suffix="_str"
    )
    result = separator.apply()

    text_count = (result["Value_str"] != "NaN").sum()
    print(f"  Text values found: {text_count:,}")

    print_stats(result, "After separation:")
    return result


def step2_convert_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Step 2: Convert EventTime strings to datetime."""
    print_step(2, "Converting EventTime to datetime")

    converter = DatetimeStringConversion(
        df,
        column="EventTime",
        output_column="EventTime_DT",
        strip_trailing_zeros=True,
        keep_original=True
    )
    result = converter.apply()

    nat_count = result["EventTime_DT"].isna().sum()
    print(f"Failed conversions (NaT): {nat_count:,}")
    print_stats(result, "After datetime conversion:")
    return result


def step3_one_hot_encode(df: pd.DataFrame) -> pd.DataFrame:
    """Step 3: One-hot encode Status column."""
    print_step(3, "One-hot encoding Status column")

    encoder = OneHotEncoding(
        df,
        column="Status",
        sparse=False
    )
    result = encoder.apply()

    status_cols = [col for col in result.columns if col.startswith("Status_")]
    print(f"Status columns created: {status_cols}")

    print_stats(result, "After one-hot encoding:")
    return result

def step4_extract_datetime_features(df: pd.DataFrame) -> pd.DataFrame:
    """Step 4: Extract datetime features."""
    print_step(4, "Extracting datetime features")

    extractor = DatetimeFeatures(
        df,
        datetime_column="EventTime_DT",
        features=["day", "week", "weekday", "day_name"],
        prefix="EventTime"
    )
    result = extractor.apply()

    result["EventTime_month"] = result["EventTime_DT"].dt.month_name()

    dt = result["EventTime_DT"].dt
    result["EventTime_seconds"] = (dt.hour * 3600 + dt.minute * 60 + dt.second).astype("Int32")

    print(f"Features added: day, week, weekday, day_name, month, seconds")
    print_stats(result, "After feature extraction:")
    return result


def step5_handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Step 5: Handle missing values."""
    print_step(5, "Handling missing values")

    initial_rows = len(df)

    nat_count = df["EventTime_DT"].isna().sum()
    df_clean = df.dropna(subset=["EventTime_DT"]).copy()
    print(f"Dropped {nat_count:,} rows with NaT timestamps ({nat_count/initial_rows*100:.4f}%)")

    value_missing = df_clean["Value"].isna().sum()
    if value_missing > 0:
        if value_missing / len(df_clean) < 0.001:
            df_clean = df_clean.dropna(subset=["Value"])
            print(f"Dropped {value_missing:,} rows with missing Value ({value_missing/initial_rows*100:.4f}%)")
        else:
            df_clean["Value"] = df_clean["Value"].fillna(-1)
            print(f"Filled {value_missing:,} missing Values with -1")
    else:
        print(f"No missing Values to handle")

    total_dropped = initial_rows - len(df_clean)
    print(f"Total dropped: {total_dropped:,} ({total_dropped/initial_rows*100:.2f}%)")
    print(f"Retention rate: {len(df_clean)/initial_rows*100:.2f}%")

    print_stats(df_clean, "After missing value handling:")
    return df_clean


def step6_sort_chronologically(df: pd.DataFrame) -> pd.DataFrame:
    """Step 6: Sort data chronologically."""
    print_step(6, "Sorting chronologically")

    sorter = ChronologicalSort(
        df,
        datetime_column="EventTime_DT",
        ascending=True,
        na_position="last",
        reset_index=True
    )
    result = sorter.apply()

    print(f"Date range: {result['EventTime_DT'].min()} to {result['EventTime_DT'].max()}")
    print(f"Is sorted: {result['EventTime_DT'].is_monotonic_increasing}")

    print_stats(result, "After sorting:")
    return result


def step7_detect_outliers(df: pd.DataFrame, n_sigma: float = 10.0) -> pd.DataFrame:
    """Step 7: Detect and handle outliers using MAD."""
    print_step(7, f"Detecting outliers using MAD (n_sigma={n_sigma})")

    detector = MADOutlierDetection(
        df,
        column="Value",
        n_sigma=n_sigma,
        action="replace",
        replacement_value=-1,
        exclude_values=[-1]  # Exclude existing error values
    )
    result = detector.apply()

    original_errors = (df["Value"] == -1).sum()
    new_errors = (result["Value"] == -1).sum()
    outliers_found = new_errors - original_errors

    print(f"Outliers detected and replaced: {outliers_found:,} ({outliers_found/len(df)*100:.4f}%)")

    outlier_mask = (df["Value"] != -1) & (result["Value"] == -1)
    result.loc[outlier_mask, "Value_str"] = "Extreme Outlier"

    valid_values = result[result["Value"] != -1]["Value"]
    if len(valid_values) > 0:
        print(f"Valid values after outlier removal: {len(valid_values):,}")
        print(f"Value range: {valid_values.min():.2f} to {valid_values.max():.2f}")
        print(f"Mean: {valid_values.mean():.2f}, Median: {valid_values.median():.2f}")

    print_stats(result, "After outlier detection:")
    return result


def save_results(
    df: pd.DataFrame,
    output_path: str,
    save_sample: bool = True,
    save_metadata: bool = True
) -> None:
    """Save the preprocessed data and optional metadata."""
    print_step(8, "Saving results")

    output_path = Path(output_path)
    output_dir = output_path.parent
    output_stem = output_path.stem
    
    print(f"Saving to {output_path}")
    df.to_parquet(output_path, index=False, compression="snappy")
    file_size = os.path.getsize(output_path) / 1024 / 1024
    print(f"Saved ({file_size:.2f} MB)")

    if save_sample:
        sample_path = output_dir / f"{output_stem}_sample.csv"
        sample_size = min(100_000, len(df))
        sample = df.sample(n=sample_size, random_state=42)
        sample.to_csv(sample_path, index=False)
        print(f"Saved {sample_size:,} row sample to {sample_path}")

    # Save metadata
    if save_metadata:
        metadata_path = output_dir / f"{output_stem}_metadata.json"

        valid_values = df[df["Value"] != -1]["Value"]
        error_breakdown = df[df["Value"] == -1]["Value_str"].value_counts().to_dict()

        metadata = {
            "dataset_info": {
                "total_rows": int(len(df)),
                "total_columns": int(len(df.columns)),
                "date_range_start": str(df["EventTime_DT"].min()),
                "date_range_end": str(df["EventTime_DT"].max()),
                "unique_sensors": int(df["TagName"].nunique()),
                "memory_usage_mb": float(df.memory_usage(deep=True).sum() / 1024 / 1024)
            },
            "value_statistics": {
                "valid_values_count": int((df["Value"] != -1).sum()),
                "valid_values_percent": float((df["Value"] != -1).sum() / len(df) * 100),
                "error_values_count": int((df["Value"] == -1).sum()),
                "error_values_percent": float((df["Value"] == -1).sum() / len(df) * 100),
                "mean": float(valid_values.mean()) if len(valid_values) > 0 else None,
                "median": float(valid_values.median()) if len(valid_values) > 0 else None,
                "std": float(valid_values.std()) if len(valid_values) > 0 else None,
                "min": float(valid_values.min()) if len(valid_values) > 0 else None,
                "max": float(valid_values.max()) if len(valid_values) > 0 else None
            },
            "error_breakdown": {str(k): int(v) for k, v in error_breakdown.items()},
            "status_distribution": {
                col: int(df[col].sum())
                for col in df.columns if col.startswith("Status_")
            },
            "columns": list(df.columns),
            "column_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "preprocessing_steps": [
                "1. Separated text values from numeric Value column (MixedTypeSeparation)",
                "2. Converted EventTime strings to datetime (DatetimeStringConversion)",
                "3. One-hot encoded Status column (OneHotEncoding)",
                "4. Extracted datetime features (DatetimeFeatures)",
                "5. Handled missing values (dropped NaT timestamps and missing values)",
                "6. Sorted chronologically (ChronologicalSort)",
                "7. Detected and handled outliers using MAD (MADOutlierDetection)"
            ]
        }

        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)
        print(f"Saved metadata to {metadata_path}")


def preprocess(
    input_path: str,
    output_path: str,
    n_sigma: float = 10.0,
    save_sample: bool = True,
    save_metadata: bool = True,
    sample_fraction: float = None
) -> pd.DataFrame:
    """
    Run the full preprocessing pipeline.

    Args:
        input_path: Path to input file (.parquet or .csv)
        output_path: Path for output parquet file
        n_sigma: Number of MAD-based standard deviations for outlier detection
        save_sample: Whether to save a CSV sample
        save_metadata: Whether to save metadata JSON

    Returns:
        Preprocessed DataFrame
    """
    print("SHELL DATA PREPROCESSING PIPELINE")
    # Load data
    df = load_data(input_path)

    # Sample data if requested for memory-constrained systems or showcase
    df = sample_data(df, sample_fraction)

    # Step 1: Separate mixed types
    df = step1_separate_mixed_types(df)
    gc.collect()

    # Step 2: Convert datetime
    df = step2_convert_datetime(df)
    gc.collect()

    # Step 3: One-hot encode
    df = step3_one_hot_encode(df)
    gc.collect()

    # Step 4: Extract datetime features
    df = step4_extract_datetime_features(df)
    gc.collect()

    # Step 5: Handle missing values
    df = step5_handle_missing_values(df)
    gc.collect()

    # Step 6: Sort chronologically
    df = step6_sort_chronologically(df)
    gc.collect()

    # Step 7: Detect outliers
    df = step7_detect_outliers(df, n_sigma=n_sigma)
    gc.collect()

    # Save results
    save_results(df, output_path, save_sample=save_sample, save_metadata=save_metadata)

    print("PREPROCESSING COMPLETE")
    print_stats(df, "Final dataset:")

    return df


def main():
    parser = argparse.ArgumentParser(
        description="Preprocess Shell sensor data using RTDIP pipeline components."
    )
    parser.add_argument(
        "--input", "-i",
        type=str,
        default="ShellData.parquet",
        help="Input file path, supports .parquet or .csv (default: ShellData.parquet)"
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default="ShellData_preprocessed.parquet",
        help="Output parquet file path (default: ShellData_preprocessed.parquet)"
    )
    parser.add_argument(
        "--n-sigma",
        type=float,
        default=10.0,
        help="Number of MAD-based std deviations for outlier detection (default: 10.0)"
    )
    parser.add_argument(
        "--no-sample",
        action="store_true",
        help="Skip saving CSV sample"
    )
    parser.add_argument(
        "--no-metadata",
        action="store_true",
        help="Skip saving metadata JSON"
    )
    parser.add_argument(
        "--sample",
        type=float,
        default=None,
        help="Sample a fraction of data for testing (e.g., 0.1 for 10%%). Use for memory-constrained systems."
    )

    args = parser.parse_args()

    preprocess(
        input_path=args.input,
        output_path=args.output,
        n_sigma=args.n_sigma,
        save_sample=not args.no_sample,
        save_metadata=not args.no_metadata,
        sample_fraction=args.sample
    )


if __name__ == "__main__":
    main()
