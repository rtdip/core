"""
Filter flat sensors from the Shell dataset.
This script removes sensors identified as "flat" (showing minimal variation)
and creates a cleaned dataset.
"""

import pandas as pd
import json
from pathlib import Path


def load_flat_sensor_list(flat_sensors_path='amos_team_resources/shell/preprocessing/flat_sensors.csv'):
    """Load the list of identified flat sensors."""
    flat_df = pd.read_csv(flat_sensors_path)
    flat_sensor_names = flat_df['sensor'].tolist()
    print(f"Loaded {len(flat_sensor_names)} flat sensors to filter")
    return flat_sensor_names

def filter_dataset(data_path, flat_sensors, output_path=None):
    """
    Filter out flat sensors from the dataset.

    Args:
        data_path: Path to preprocessed data file
        flat_sensors: List of sensor names to filter out
        output_path: Path to save filtered data (default: adds '_filtered' suffix)

    Returns:
        Filtered DataFrame
    """
    print(f"\nLoading data from: {data_path}")

    if str(data_path).endswith('.parquet'):
        df = pd.read_parquet(data_path)
        file_format = 'parquet'
    else:
        df = pd.read_csv(data_path)
        file_format = 'csv'

    print(f"Original dataset: {len(df):,} rows, {df['TagName'].nunique()} unique sensors")

    df_filtered = df[~df['TagName'].isin(flat_sensors)].copy()

    print(f"Filtered dataset: {len(df_filtered):,} rows, {df_filtered['TagName'].nunique()} unique sensors")
    print(f"Removed {len(df) - len(df_filtered):,} rows ({(len(df) - len(df_filtered))/len(df)*100:.1f}%)")

    if output_path is None:
        base_path = Path(data_path)
        if file_format == 'parquet':
            output_path = base_path.with_name(base_path.stem + '_filtered.parquet')
        else:
            output_path = base_path.with_name(base_path.stem + '_filtered.csv')

    print(f"\nSaving filtered data to: {output_path}")

    if file_format == 'parquet':
        df_filtered.to_parquet(output_path, index=False)
    else:
        df_filtered.to_csv(output_path, index=False)

    print("Filtered dataset saved")

    return df_filtered


def create_metadata(df_filtered, output_path, flat_sensors_removed):
    """Create metadata file for the filtered dataset."""
    metadata = {
        'dataset_info': {
            'total_rows': len(df_filtered),
            'total_columns': len(df_filtered.columns),
            'date_range_start': str(df_filtered['EventTime'].min()) if 'EventTime' in df_filtered.columns else str(df_filtered['EventTime_DT'].min()),
            'date_range_end': str(df_filtered['EventTime'].max()) if 'EventTime' in df_filtered.columns else str(df_filtered['EventTime_DT'].max()),
            'unique_sensors': df_filtered['TagName'].nunique(),
            'flat_sensors_removed': len(flat_sensors_removed)
        },
        'value_statistics': {
            'mean': float(df_filtered['Value'].mean()),
            'median': float(df_filtered['Value'].median()),
            'std': float(df_filtered['Value'].std()),
            'min': float(df_filtered['Value'].min()),
            'max': float(df_filtered['Value'].max())
        },
        'filtering_info': {
            'description': 'Flat sensors have been removed',
            'flat_sensors_removed_count': len(flat_sensors_removed),
            'filtering_date': str(pd.Timestamp.now())
        }
    }

    metadata_path = Path(output_path).with_suffix('.json').with_name(
        Path(output_path).stem + '_metadata.json'
    )
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)

    print(f"Metadata saved to: {metadata_path}")

    print("FILTERING SUMMARY")
    print(f"Total rows: {metadata['dataset_info']['total_rows']:,}")
    print(f"Unique sensors: {metadata['dataset_info']['unique_sensors']}")
    print(f"Flat sensors removed: {metadata['dataset_info']['flat_sensors_removed']}")
    print(f"Date range: {metadata['dataset_info']['date_range_start']} to {metadata['dataset_info']['date_range_end']}")
    print(f"\nValue statistics:")
    print(f"  Mean: {metadata['value_statistics']['mean']:.2f}")
    print(f"  Median: {metadata['value_statistics']['median']:.2f}")
    print(f"  Std: {metadata['value_statistics']['std']:.2f}")
    print(f"  Range: [{metadata['value_statistics']['min']:.2f}, {metadata['value_statistics']['max']:.2f}]")


def main():
    """Main execution function."""
    print("FILTER FLAT SENSORS")
    # Configuration
    data_path = 'amos_team_resources/shell/preprocessing/ShellData_preprocessed.parquet'
    flat_sensors_path = 'amos_team_resources/shell/preprocessing/flat_sensors.csv'
    output_path = 'amos_team_resources/shell/preprocessing/ShellData_preprocessed_filtered.parquet'

    # Load flat sensor list
    flat_sensors = load_flat_sensor_list(flat_sensors_path)

    # Filter dataset
    df_filtered = filter_dataset(data_path, flat_sensors, output_path)

    # Create metadata
    create_metadata(df_filtered, output_path, flat_sensors)

    print("FILTERING COMPLETE")
    print(f"  {output_path}")
    print(f"\nTo use this in AutoGluon training, update the DATA_PATH in train_autogluon_shell.py:")
    print(f'  DATA_PATH = "{output_path}"')


if __name__ == '__main__':
    main()
