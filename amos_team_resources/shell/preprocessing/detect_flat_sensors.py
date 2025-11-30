"""
Script to detect flat sensors in the Shell dataset.

A "flat" sensor is one where the values show very little or no variation,
which can negatively impact time series forecasting models.

This script analyzes sensors using multiple criteria:
1. Standard deviation (low std = flat)
2. Unique value count (few unique values = flat)
3. Coefficient of variation (CV = std/mean)
4. Range of values (max - min)
5. Percentage of most common value
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json


def load_data(data_path):
    """Load the preprocessed Shell data."""
    print(f"Loading data from: {data_path}")

    if str(data_path).endswith('.parquet'):
        df = pd.read_parquet(data_path)
    else:
        df = pd.read_csv(data_path)

    print(f"Loaded {len(df):,} rows with {len(df.columns)} columns")
    print(f"Unique sensors: {df['TagName'].nunique()}")

    return df


def analyze_sensor_variation(df, output_path='flat_sensor_analysis.json'):
    """
    Analyze each sensor for flatness using multiple metrics.

    Args:
        df: DataFrame with columns [TagName, EventTime, Value, ...]
        output_path: Path to save the analysis results

    Returns:
        DataFrame with analysis results for each sensor
    """
    print("\nAnalyzing sensor variation...")

    # Filter out error values (-1 markers)
    df_clean = df[df['Value'] != -1].copy()

    # Remove NaN values
    df_clean = df_clean.dropna(subset=['Value'])

    print(f"After filtering: {len(df_clean):,} rows")

    # Group by sensor
    results = []

    sensors = df_clean['TagName'].unique()
    print(f"Analyzing {len(sensors)} sensors...")

    for i, sensor in enumerate(sensors, 1):
        if i % 100 == 0:
            print(f"  Progress: {i}/{len(sensors)} sensors analyzed")

        sensor_data = df_clean[df_clean['TagName'] == sensor]['Value']

        if len(sensor_data) < 2:
            continue

        # Calculate various metrics
        values = sensor_data.values
        n_points = len(values)

        # Basic statistics
        mean_val = np.mean(values)
        std_val = np.std(values)
        min_val = np.min(values)
        max_val = np.max(values)
        value_range = max_val - min_val

        # Unique values
        n_unique = len(np.unique(values))
        unique_ratio = n_unique / n_points

        # Most common value percentage
        if n_unique > 0:
            unique_vals, counts = np.unique(values, return_counts=True)
            most_common_pct = (np.max(counts) / n_points) * 100
        else:
            most_common_pct = 100.0

        # Coefficient of variation (normalized standard deviation)
        if abs(mean_val) > 1e-10:
            cv = abs(std_val / mean_val)
        else:
            cv = 0.0 if std_val < 1e-10 else float('inf')

        # Calculate sequential differences (how much values change)
        if len(values) > 1:
            diffs = np.diff(values)
            mean_abs_diff = np.mean(np.abs(diffs))
            pct_zero_diff = (np.sum(diffs == 0) / len(diffs)) * 100
        else:
            mean_abs_diff = 0.0
            pct_zero_diff = 100.0

        results.append({
            'sensor': sensor,
            'n_points': n_points,
            'mean': mean_val,
            'std': std_val,
            'min': min_val,
            'max': max_val,
            'range': value_range,
            'n_unique': n_unique,
            'unique_ratio': unique_ratio,
            'most_common_pct': most_common_pct,
            'cv': cv if not np.isinf(cv) else 999.0,
            'mean_abs_diff': mean_abs_diff,
            'pct_zero_diff': pct_zero_diff
        })

    results_df = pd.DataFrame(results)

    print(f"\nCompleted analysis of {len(results_df)} sensors")

    return results_df


def identify_flat_sensors(results_df,
                          std_threshold=0.01,
                          unique_ratio_threshold=0.01,
                          cv_threshold=0.01,
                          range_threshold=0.01,
                          most_common_threshold=95.0,
                          zero_diff_threshold=95.0):
    """
    Identify flat sensors based on multiple criteria.

    Args:
        results_df: DataFrame with sensor analysis results
        std_threshold: Maximum standard deviation for flat sensor
        unique_ratio_threshold: Maximum ratio of unique values
        cv_threshold: Maximum coefficient of variation
        range_threshold: Maximum range (max - min)
        most_common_threshold: Minimum percentage of most common value
        zero_diff_threshold: Minimum percentage of zero differences

    Returns:
        Dictionary with flat sensor classifications
    """
    print("\nIdentifying flat sensors...")
    print(f"Criteria:")
    print(f"  - Standard deviation <= {std_threshold}")
    print(f"  - Unique value ratio <= {unique_ratio_threshold}")
    print(f"  - Coefficient of variation <= {cv_threshold}")
    print(f"  - Value range <= {range_threshold}")
    print(f"  - Most common value >= {most_common_threshold}%")
    print(f"  - Zero differences >= {zero_diff_threshold}%")

    # Create boolean masks for each criterion
    masks = {
        'low_std': results_df['std'] <= std_threshold,
        'low_unique_ratio': results_df['unique_ratio'] <= unique_ratio_threshold,
        'low_cv': results_df['cv'] <= cv_threshold,
        'low_range': results_df['range'] <= range_threshold,
        'high_common_value': results_df['most_common_pct'] >= most_common_threshold,
        'high_zero_diff': results_df['pct_zero_diff'] >= zero_diff_threshold
    }

    # Combine criteria (sensor is flat if it meets ANY of these criteria)
    flat_mask = (
        masks['low_std'] |
        masks['low_unique_ratio'] |
        masks['low_cv'] |
        masks['low_range'] |
        masks['high_common_value'] |
        masks['high_zero_diff']
    )

    flat_sensors = results_df[flat_mask].copy()
    non_flat_sensors = results_df[~flat_mask].copy()

    # Determine which criteria each flat sensor meets
    flat_sensors['flatness_reasons'] = ''
    for criterion, mask in masks.items():
        matched = flat_sensors[mask[flat_mask]]
        flat_sensors.loc[matched.index, 'flatness_reasons'] += criterion + ', '

    flat_sensors['flatness_reasons'] = flat_sensors['flatness_reasons'].str.rstrip(', ')

    print(f"\nResults:")
    print(f"  Total sensors: {len(results_df)}")
    print(f"  Flat sensors: {len(flat_sensors)} ({len(flat_sensors)/len(results_df)*100:.1f}%)")
    print(f"  Non-flat sensors: {len(non_flat_sensors)} ({len(non_flat_sensors)/len(results_df)*100:.1f}%)")

    # Count sensors by criterion
    print(f"\nBreakdown by criterion:")
    for criterion, mask in masks.items():
        count = mask.sum()
        print(f"  {criterion}: {count} sensors ({count/len(results_df)*100:.1f}%)")

    return {
        'flat_sensors': flat_sensors,
        'non_flat_sensors': non_flat_sensors,
        'criteria': masks
    }


def save_results(results_df, flat_info, output_dir='preprocessing'):
    """Save analysis results to files."""
    output_dir = Path(output_dir)

    # Save full analysis
    full_results_path = output_dir / 'sensor_variation_analysis.csv'
    results_df.to_csv(full_results_path, index=False)
    print(f"\nFull analysis saved to: {full_results_path}")

    # Save flat sensors list
    flat_sensors_path = output_dir / 'flat_sensors.csv'
    flat_info['flat_sensors'].to_csv(flat_sensors_path, index=False)
    print(f"Flat sensors list saved to: {flat_sensors_path}")

    # Save non-flat sensors list
    non_flat_sensors_path = output_dir / 'non_flat_sensors.csv'
    flat_info['non_flat_sensors'].to_csv(non_flat_sensors_path, index=False)
    print(f"Non-flat sensors list saved to: {non_flat_sensors_path}")

    # Save summary JSON
    summary = {
        'total_sensors': len(results_df),
        'flat_sensors_count': len(flat_info['flat_sensors']),
        'non_flat_sensors_count': len(flat_info['non_flat_sensors']),
        'flat_percentage': len(flat_info['flat_sensors']) / len(results_df) * 100,
        'flat_sensor_names': flat_info['flat_sensors']['sensor'].tolist(),
        'criterion_counts': {
            criterion: int(mask.sum())
            for criterion, mask in flat_info['criteria'].items()
        }
    }

    summary_path = output_dir / 'flat_sensor_summary.json'
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"Summary saved to: {summary_path}")

    # Print top 10 flattest sensors
    print("\n" + "="*80)
    print("TOP 10 FLATTEST SENSORS (by standard deviation):")
    print("="*80)
    top_flat = flat_info['flat_sensors'].nsmallest(10, 'std')
    for i, row in top_flat.iterrows():
        print(f"\n{row['sensor']}:")
        print(f"  Data points: {row['n_points']:,}")
        print(f"  Mean: {row['mean']:.4f}, Std: {row['std']:.6f}")
        print(f"  Range: [{row['min']:.4f}, {row['max']:.4f}] (span: {row['range']:.6f})")
        print(f"  Unique values: {row['n_unique']} ({row['unique_ratio']*100:.2f}%)")
        print(f"  Most common value: {row['most_common_pct']:.1f}% of data")
        print(f"  Zero differences: {row['pct_zero_diff']:.1f}%")
        print(f"  Flatness reasons: {row['flatness_reasons']}")


def main():
    """Main execution function."""
    print("="*80)
    print("FLAT SENSOR DETECTION FOR SHELL DATASET")
    print("="*80)

    # Configuration
    data_path = 'amos_team_resources/shell/preprocessing/ShellData_preprocessed.parquet'
    output_dir = 'amos_team_resources/shell/preprocessing'

    # Load data
    df = load_data(data_path)

    # Analyze variation
    results_df = analyze_sensor_variation(df)

    # Identify flat sensors
    flat_info = identify_flat_sensors(results_df)

    # Save results
    save_results(results_df, flat_info, output_dir)

    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)


if __name__ == '__main__':
    main()
