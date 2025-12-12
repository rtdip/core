"""
Time Series Decomposition - 1-Minute Resampling with Full Interpolation

This script performs MSTL decomposition with:
- 1-minute resampling (average of all values in each minute)
- Full interpolation of all gaps (no limit on gap size)
- Optional extreme outlier capping (only absurdly extreme values)
- Configurable seasonality patterns (daily, weekly, or both)
- All available data for the selected sensor

Usage:
    # Basic usage with both daily and weekly patterns
    python decomposition_1min_resample_interpolated.py --sensor "0SP116X5V:NXR.0"

    # Only daily pattern
    python decomposition_1min_resample_interpolated.py --sensor "0SP116X5V:NXR.0" --patterns daily

    # Only weekly pattern
    python decomposition_1min_resample_interpolated.py --sensor "0SP116X5V:NXR.0" --patterns weekly

    # With extreme outlier capping (20 std deviations) - THIS IS RECOMMENDED
    python decomposition_1min_resample_interpolated.py --sensor "0SP116X5V:NXR.0" --cap-outliers --outlier-threshold 20

    # Custom outlier threshold
    python decomposition_1min_resample_interpolated.py --sensor "0SP116X5V:NXR.0" --cap-outliers --outlier-threshold 10

    # With robust mode
    python decomposition_1min_resample_interpolated.py --sensor "0SP116X5V:NXR.0" --robust

Output files:
    - decomposition_{sensor}_1min_full.parquet: Full decomposed dataset
    - decomposition_{sensor}_1min_full.json: Metadata and statistics
"""

import argparse
import sys
import json
from pathlib import Path
import pandas as pd
import numpy as np

# Add SDK to path
SDK_PATH = Path(__file__).resolve().parents[3] / "src" / "sdk" / "python"
sys.path.insert(0, str(SDK_PATH))

from rtdip_sdk.pipelines.decomposition.pandas import MSTLDecomposition


def load_and_filter_data(data_path: Path, sensor: str) -> pd.DataFrame:
    """Load data and filter to selected sensor."""
    print("Loading data...")
    print(f"  Path: {data_path}")
    print(f"  Sensor: {sensor}")
    print()

    df = pd.read_parquet(data_path, columns=['TagName', 'EventTime', 'Value'])

    print(f"Loaded {len(df):,} total rows")

    # Filter to selected sensor
    df_sensor = df[df['TagName'] == sensor].copy()

    if len(df_sensor) == 0:
        raise ValueError(f"No data found for sensor '{sensor}'")

    print(f"Filtered to sensor '{sensor}': {len(df_sensor):,} rows ({len(df_sensor)/len(df)*100:.2f}%)")

    return df_sensor


def preprocess_values(df: pd.DataFrame) -> pd.DataFrame:
    """Convert and clean value column."""
    print("\nPreprocessing values...")

    # Convert EventTime
    df['EventTime'] = pd.to_datetime(df['EventTime'], errors='coerce')
    invalid_times = df['EventTime'].isna().sum()
    if invalid_times > 0:
        df = df.dropna(subset=['EventTime']).copy()
        print(f"  Dropped {invalid_times:,} invalid timestamps")

    # Convert Value to numeric
    if not pd.api.types.is_numeric_dtype(df['Value']):
        df['Value'] = pd.to_numeric(df['Value'], errors='coerce')

    # Remove invalid values
    before_len = len(df)
    df = df[(df['Value'].notna()) & (df['Value'] != -1)].copy()
    removed = before_len - len(df)
    print(f"  Removed {removed:,} invalid values ({removed/before_len*100:.2f}%)")

    # Sort by time
    df = df.sort_values('EventTime').reset_index(drop=True)

    print(f"  Time range: {df['EventTime'].min()} to {df['EventTime'].max()}")
    print(f"  Duration: {df['EventTime'].max() - df['EventTime'].min()}")

    return df


def resample_to_1min(df: pd.DataFrame) -> pd.DataFrame:
    """Resample to 1-minute intervals using mean aggregation."""
    print("\nResampling to 1-minute intervals...")

    df_resampled = df.set_index('EventTime').resample('1min')['Value'].mean().reset_index()

    missing_count = df_resampled['Value'].isna().sum()

    print(f"  Original points: {len(df):,}")
    print(f"  Resampled to: {len(df_resampled):,} 1-minute intervals")
    print(f"  Missing values: {missing_count:,} ({missing_count/len(df_resampled)*100:.2f}%)")

    return df_resampled


def interpolate_gaps(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Interpolate all missing values using time-based method."""
    print("\nInterpolating missing values...")

    missing_before = df['Value'].isna().sum()

    # Set index for time-based interpolation
    df_interpolated = df.set_index('EventTime')
    df_interpolated['Value'] = df_interpolated['Value'].interpolate(
        method='time',
        limit_direction='both'
    )
    df_interpolated = df_interpolated.reset_index()

    missing_after = df_interpolated['Value'].isna().sum()
    interpolated_count = missing_before - missing_after

    print(f"  Interpolated: {interpolated_count:,} values ({interpolated_count/len(df_interpolated)*100:.2f}%)")

    if missing_after > 0:
        print(f"  Removing {missing_after} remaining NaN values...")
        df_interpolated = df_interpolated.dropna(subset=['Value']).reset_index(drop=True)

    return df_interpolated, interpolated_count


def cap_extreme_outliers(df: pd.DataFrame, n_std: float = 20.0) -> tuple[pd.DataFrame, int, dict]:
    """
    Cap only extremely absurd outliers that would destroy decomposition.

    Uses a two-stage approach:
    1. Only consider capping values beyond n_std standard deviations (default: 20)
    2. Among those extreme values, cap to 0.1st/99.9th percentiles

    This ensures we only touch truly absurd outliers, not legitimate high/low values.
    """
    print("\nChecking for extreme outliers...")

    mean = df['Value'].mean()
    std = df['Value'].std()

    # Stage 1: Identify absurdly extreme values (beyond n_std standard deviations)
    lower_extreme = mean - n_std * std
    upper_extreme = mean + n_std * std

    extreme_mask = (df['Value'] < lower_extreme) | (df['Value'] > upper_extreme)
    extreme_count = extreme_mask.sum()

    print(f"  Data statistics:")
    print(f"    Mean: {mean:.4f}")
    print(f"    Std: {std:.4f}")
    print(f"  Extreme outlier threshold ({n_std} std):")
    print(f"    Lower: {lower_extreme:.4f}")
    print(f"    Upper: {upper_extreme:.4f}")
    print(f"  Found {extreme_count} extreme outliers ({extreme_count/len(df)*100:.3f}%)")

    if extreme_count == 0:
        print("  No extreme outliers to cap")
        return df, 0, {
            'method': 'std_deviation + percentile',
            'std_threshold': n_std,
            'outliers_capped': 0
        }

    # Stage 2: For extreme outliers, cap to percentiles
    q99_9 = df['Value'].quantile(0.999)
    q00_1 = df['Value'].quantile(0.001)

    # Only cap values that are BOTH extreme (beyond n_std) AND beyond percentiles
    cap_lower = (df['Value'] < lower_extreme) & (df['Value'] < q00_1)
    cap_upper = (df['Value'] > upper_extreme) & (df['Value'] > q99_9)
    outliers_total = cap_lower.sum() + cap_upper.sum()

    print(f"  Capping thresholds:")
    print(f"    Lower (0.1st percentile): {q00_1:.4f}")
    print(f"    Upper (99.9th percentile): {q99_9:.4f}")
    print(f"  Values to cap: {outliers_total}")

    if outliers_total > 0:
        # Show most extreme values
        extreme_values = df[extreme_mask].copy()
        extreme_high = extreme_values.nlargest(min(5, len(extreme_values)), 'Value')
        extreme_low = extreme_values.nsmallest(min(5, len(extreme_values)), 'Value')

        if len(extreme_high) > 0:
            print(f"  Most extreme high values:")
            for idx, row in extreme_high.iterrows():
                print(f"    {row['EventTime']}: {row['Value']:.4f} (>{upper_extreme:.1f})")

        if len(extreme_low) > 0 and extreme_low['Value'].min() < lower_extreme:
            print(f"  Most extreme low values:")
            for idx, row in extreme_low.iterrows():
                if row['Value'] < lower_extreme:
                    print(f"    {row['EventTime']}: {row['Value']:.4f} (<{lower_extreme:.1f})")

        # Cap the values
        df['Value'] = df['Value'].clip(lower=q00_1, upper=q99_9)

        print(f"  After capping:")
        print(f"    Min: {df['Value'].min():.4f}")
        print(f"    Max: {df['Value'].max():.4f}")

    metadata = {
        'method': 'std_deviation + percentile',
        'std_threshold': n_std,
        'lower_percentile': 0.001,
        'upper_percentile': 0.999,
        'outliers_capped': int(outliers_total)
    }

    return df, outliers_total, metadata


def perform_decomposition(
    df: pd.DataFrame,
    patterns: list[str],
    use_robust: bool = False
) -> tuple[pd.DataFrame, list[int], bool]:
    """Perform MSTL decomposition with specified patterns."""
    print("\nApplying MSTL decomposition...")

    # Calculate periods for 1-minute resampling
    daily_period = 60 * 24  # 1440 minutes per day
    weekly_period = 60 * 24 * 7  # 10080 minutes per week

    # Map pattern names to periods
    pattern_map = {
        'daily': daily_period,
        'weekly': weekly_period
    }

    # Build periods list from requested patterns
    periods = []
    use_weekly = False

    for pattern in patterns:
        if pattern not in pattern_map:
            raise ValueError(f"Unknown pattern '{pattern}'. Valid: {list(pattern_map.keys())}")

        period = pattern_map[pattern]

        # Check data sufficiency
        min_required = period * 2
        if len(df) < min_required:
            print(f"  Warning: Insufficient data for {pattern} pattern")
            print(f"    Need: {min_required:,} points, Have: {len(df):,} points")
            print(f"    Skipping {pattern} pattern")
            continue

        periods.append(period)
        if pattern == 'weekly':
            use_weekly = True

    if len(periods) == 0:
        raise ValueError("No valid patterns available for decomposition (insufficient data)")

    pattern_names = [k for k, v in pattern_map.items() if v in periods]
    print(f"  Using patterns: {', '.join(pattern_names)}")
    print(f"  Data points: {len(df):,}")
    print(f"  Periods: {periods}")
    print(f"  Robust mode: {use_robust}")

    mstl = MSTLDecomposition(
        df=df,
        value_column='Value',
        timestamp_column='EventTime',
        periods=periods,
        iterate=2,
        stl_kwargs={'robust': use_robust}
    )

    df_decomposed = mstl.decompose()

    print(f"  Decomposition complete")
    print(f"  Components: {[col for col in df_decomposed.columns if col not in ['EventTime', 'Value']]}")

    return df_decomposed, periods, use_weekly


def save_results(
    df_decomposed: pd.DataFrame,
    sensor: str,
    interpolated_count: int,
    periods: list[int],
    outlier_metadata: dict,
    output_dir: Path
) -> None:
    """Save decomposed data and metadata."""
    print("\nSaving results...")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Create safe filename
    safe_tag = sensor.replace('/', '_').replace('\\', '_').replace(':', '_')

    # Save parquet
    parquet_file = output_dir / f'decomposition_{safe_tag}_1min_full.parquet'
    df_decomposed.to_parquet(parquet_file, index=False)
    print(f"  Saved: {parquet_file}")

    # Calculate variance breakdown
    seasonal_cols = [col for col in df_decomposed.columns if col.startswith('seasonal_')]

    total_var = df_decomposed['Value'].var()
    trend_var = df_decomposed['trend'].dropna().var()
    residual_var = df_decomposed['residual'].dropna().var()

    variance_dict = {
        'trend_pct': float(trend_var / total_var * 100),
        'residual_pct': float(residual_var / total_var * 100)
    }

    # Add seasonal variances
    total_seasonal_var = 0
    for col in seasonal_cols:
        period = int(col.split('_')[1])
        var = df_decomposed[col].dropna().var()
        variance_dict[f'seasonal_{period}_pct'] = float(var / total_var * 100)
        total_seasonal_var += var

    variance_dict['combined_seasonal_pct'] = float(total_seasonal_var / total_var * 100)

    # Create metadata
    metadata = {
        'sensor': sensor,
        'duration': f'{(df_decomposed["EventTime"].max() - df_decomposed["EventTime"].min()).days:.1f} days',
        'data_points': len(df_decomposed),
        'resampling': {
            'frequency': '1 minute',
            'aggregation': 'mean'
        },
        'interpolation': {
            'method': 'time',
            'limit': 'none (all gaps interpolated)',
            'values_interpolated': int(interpolated_count)
        },
        'outlier_handling': outlier_metadata,
        'periods': {f'period_{p}': p for p in periods},
        'time_range': {
            'start': str(df_decomposed['EventTime'].min()),
            'end': str(df_decomposed['EventTime'].max())
        },
        'variance_explained': variance_dict
    }

    # Save metadata
    metadata_file = output_dir / f'decomposition_{safe_tag}_1min_full.json'
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"  Saved: {metadata_file}")

    # Print summary
    print("\nSummary:")
    print(f"  Sensor: {sensor}")
    print(f"  Duration: {metadata['duration']} days")
    for col in seasonal_cols:
        period = int(col.split('_')[1])
        pct = variance_dict[f'seasonal_{period}_pct']
        pattern_name = 'daily' if period == 1440 else 'weekly' if period == 10080 else f'{period}min'
        print(f"  {pattern_name.capitalize()} seasonality: {pct:.2f}%")
    print(f"  Residual: {variance_dict['residual_pct']:.2f}%")


def main():
    parser = argparse.ArgumentParser(
        description="Perform MSTL decomposition on Shell sensor data with 1-minute resampling and interpolation.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with both patterns
  python decomposition_1min_resample_interpolated.py --sensor "0SP116X5V:NXR.0"

  # Only daily pattern
  python decomposition_1min_resample_interpolated.py --sensor "0SP116X5V:NXR.0" --patterns daily

  # With extreme outlier capping
  python decomposition_1min_resample_interpolated.py --sensor "0SP116X5V:NXR.0" --cap-outliers

  # Custom outlier threshold (10 std instead of 20)
  python decomposition_1min_resample_interpolated.py --sensor "0SP116X5V:NXR.0" --cap-outliers --outlier-threshold 10
        """
    )

    script_dir = Path(__file__).parent
    default_input = script_dir.parent / "data" / "ShellData.parquet"

    parser.add_argument(
        "--sensor",
        "-s",
        type=str,
        required=True,
        help="Sensor TagName to decompose (e.g., '0SP116X5V:NXR.0')"
    )
    parser.add_argument(
        "--input",
        "-i",
        type=str,
        default=str(default_input),
        help="Input parquet file path (default: ../data/ShellData.parquet)"
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        type=str,
        default=str(script_dir),
        help="Output directory for results (default: current directory)"
    )
    parser.add_argument(
        "--patterns",
        "-p",
        nargs='+',
        choices=['daily', 'weekly'],
        default=['daily', 'weekly'],
        help="Seasonality patterns to extract (default: daily weekly)"
    )
    parser.add_argument(
        "--cap-outliers",
        action="store_true",
        help="Enable extreme outlier capping (only absurdly extreme values)"
    )
    parser.add_argument(
        "--outlier-threshold",
        type=float,
        default=20.0,
        help="Number of standard deviations to consider as extreme outlier (default: 20.0)"
    )
    parser.add_argument(
        "--robust",
        action="store_true",
        help="Use robust mode for MSTL (resistant to outliers, slower)"
    )

    args = parser.parse_args()

    data_path = Path(args.input)
    output_dir = Path(args.output_dir)

    if not data_path.exists():
        raise FileNotFoundError(f"Input file not found: {data_path}")

    print("="*80)
    print("MSTL DECOMPOSITION - 1-MINUTE RESAMPLING WITH INTERPOLATION")
    print("="*80)

    # Step 1: Load and filter data
    df = load_and_filter_data(data_path, args.sensor)

    # Step 2: Preprocess values
    df = preprocess_values(df)

    # Step 3: Resample to 1-minute intervals
    df = resample_to_1min(df)

    # Step 4: Interpolate gaps
    df, interpolated_count = interpolate_gaps(df)

    # Step 5: Cap extreme outliers (if enabled)
    if args.cap_outliers:
        df, outliers_capped, outlier_metadata = cap_extreme_outliers(df, n_std=args.outlier_threshold)
    else:
        outlier_metadata = {
            'method': 'none',
            'outliers_capped': 0
        }
        print("\nSkipping outlier capping (use --cap-outliers to enable)")

    # Step 6: Perform decomposition
    df_decomposed, periods, use_weekly = perform_decomposition(df, args.patterns, use_robust=args.robust)

    # Step 7: Save results
    save_results(df_decomposed, args.sensor, interpolated_count, periods, outlier_metadata, output_dir)

    print("\n" + "="*80)
    print("DECOMPOSITION COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()
