"""
Generalized Forecast Visualization Tool

Creates visualizations for time series forecasting results
from any parquet files. Supports customizable column mappings and data sources.

Usage:
    # Basic usage with command-line arguments:
    python visualize_forecasts.py --predictions path/to/predictions.parquet \\
                                   --historical path/to/historical.parquet \\
                                   --output my_visualizations

    # With actuals for error analysis:
    python visualize_forecasts.py --predictions path/to/predictions.parquet \\
                                   --historical path/to/historical.parquet \\
                                   --actuals path/to/actuals.parquet \\
                                   --output my_visualizations

    # With custom column mappings:
    python visualize_forecasts.py --predictions predictions.parquet \\
                                   --historical historical.parquet \\
                                   --pred-timestamp timestamp \\
                                   --pred-sensor item_id \\
                                   --pred-value mean \\
                                   --hist-timestamp EventTime \\
                                   --hist-sensor TagName \\
                                   --hist-value Value

    # Using a configuration file:
    python visualize_forecasts.py --config my_config.json

Configuration File Format (JSON):
{
    "paths": {
        "predictions": "path/to/predictions.parquet",
        "historical": "path/to/historical.parquet",
        "actuals": "path/to/actuals.parquet",  // Optional
        "output": "output_directory"
    },
    "columns": {
        "predictions": {
            "timestamp": "timestamp",
            "sensor_id": "item_id",
            "mean": "mean",
            "quantile_10": "0.1",
            "quantile_20": "0.2",
            "quantile_80": "0.8",
            "quantile_90": "0.9"
        },
        "historical": {
            "timestamp": "EventTime",
            "sensor_id": "TagName",
            "value": "Value"
        },
        "actuals": {
            "timestamp": "timestamp",
            "sensor_id": "item_id",
            "value": "target"
        }
    },
    "settings": {
        "lookback_hours": 168,
        "max_sensors_overview": 9,
        "detailed_sensors": 3,
        "datetime_format": "mixed"  // "mixed", "iso", or specific format string
    }
}
"""

import argparse
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from visualization import forecasting, comparison, utils, config


class ForecastVisualizer:
    """Generalized forecast visualization tool"""

    def __init__(self, config_dict):
        """
        Initialize visualizer with configuration

        Args:
            config_dict: Dictionary containing paths, column mappings, and settings
        """
        self.config = config_dict
        self.predictions_df = None
        self.historical_df = None
        self.actuals_df = None
        self.sensors = None

    def load_data(self):
        """Load all data files"""
        print("\n1. Loading data")

        pred_path = Path(self.config['paths']['predictions'])
        if not pred_path.exists():
            raise FileNotFoundError(f"Predictions file not found: {pred_path}")

        self.predictions_df = pd.read_parquet(pred_path)

        ts_col = self.config['columns']['predictions']['timestamp']
        dt_format = self.config['settings'].get('datetime_format', 'mixed')
        self.predictions_df[ts_col] = pd.to_datetime(
            self.predictions_df[ts_col],
            format=dt_format if dt_format != 'mixed' else 'mixed'
        )

        print(f"   [OK] Loaded predictions: {len(self.predictions_df)} points")
        print(f"        Columns: {list(self.predictions_df.columns)}")

        hist_path = Path(self.config['paths']['historical'])
        if not hist_path.exists():
            raise FileNotFoundError(f"Historical file not found: {hist_path}")

        self.historical_df = pd.read_parquet(hist_path)

        hist_ts_col = self.config['columns']['historical']['timestamp']
        self.historical_df[hist_ts_col] = pd.to_datetime(
            self.historical_df[hist_ts_col],
            format=dt_format if dt_format != 'mixed' else 'mixed'
        )

        print(f"   [OK] Loaded historical: {len(self.historical_df):,} points")
        print(f"        Columns: {list(self.historical_df.columns)}")

        if 'actuals' in self.config['paths'] and self.config['paths']['actuals']:
            actuals_path = Path(self.config['paths']['actuals'])
            if actuals_path.exists():
                self.actuals_df = pd.read_parquet(actuals_path)

                act_ts_col = self.config['columns']['actuals']['timestamp']
                self.actuals_df[act_ts_col] = pd.to_datetime(
                    self.actuals_df[act_ts_col],
                    format=dt_format if dt_format != 'mixed' else 'mixed'
                )

                print(f"   [OK] Loaded actuals: {len(self.actuals_df)} points")
                print(f"        Columns: {list(self.actuals_df.columns)}")
            else:
                print(f"   [WARNING] Actuals file not found: {actuals_path}")
                self.actuals_df = None
        else:
            print("   [INFO] No actuals file provided (error analysis will be skipped)")
            self.actuals_df = None

        sensor_col = self.config['columns']['predictions']['sensor_id']
        self.sensors = self.predictions_df[sensor_col].unique()
        print(f"   [OK] Found {len(self.sensors)} sensors")

    def prepare_sensor_data(self, sensor_id):
        """
        Prepare data for a specific sensor (wrapper around forecasting.prepare_sensor_data)

        Returns:
            tuple: (historical_data, forecast_data, forecast_start)
        """
        pred_cols = self.config['columns']['predictions']
        hist_cols = self.config['columns']['historical']

        pred_temp = self.predictions_df.copy()
        pred_temp = pred_temp.rename(columns={
            pred_cols['timestamp']: 'timestamp',
            pred_cols['sensor_id']: 'item_id',
            pred_cols['mean']: 'mean'
        })

        quantile_mapping = {}
        for q in ['quantile_10', 'quantile_20', 'quantile_80', 'quantile_90']:
            if q in pred_cols and pred_cols[q] in self.predictions_df.columns:
                quantile_mapping[pred_cols[q]] = q.replace('quantile_', '0.')

        if quantile_mapping:
            pred_temp = pred_temp.rename(columns=quantile_mapping)

        hist_temp = self.historical_df.copy()
        hist_temp = hist_temp.rename(columns={
            hist_cols['timestamp']: 'EventTime',
            hist_cols['sensor_id']: 'TagName',
            hist_cols['value']: 'Value'
        })


        return forecasting.prepare_sensor_data(
            sensor_id=sensor_id,
            predictions_df=pred_temp,
            historical_df=hist_temp,
            lookback_hours=self.config['settings']['lookback_hours']
        )

    def create_overview(self, output_dir):
        """Create multi-sensor overview plot"""
        max_sensors = self.config['settings']['max_sensors_overview']
        print(f"\n2. Creating multi-sensor overview ({min(len(self.sensors), max_sensors)} sensors)")

        pred_cols = self.config['columns']['predictions']
        hist_cols = self.config['columns']['historical']

        pred_temp = self.predictions_df.rename(columns={
            pred_cols['timestamp']: 'timestamp',
            pred_cols['sensor_id']: 'item_id',
            pred_cols['mean']: 'mean'
        })

        hist_temp = self.historical_df.rename(columns={
            hist_cols['timestamp']: 'EventTime',
            hist_cols['sensor_id']: 'TagName',
            hist_cols['value']: 'Value'
        })

        fig = forecasting.plot_multi_sensor_overview(
            predictions_df=pred_temp,
            historical_df=hist_temp,
            lookback_hours=self.config['settings']['lookback_hours'],
            max_sensors=max_sensors,
            output_path=output_dir / 'overview_all_sensors.png'
        )
        plt.close(fig)

        print(f"   [OK] Saved: {output_dir / 'overview_all_sensors.png'}")

    def create_dashboards(self, output_dir):
        """Create detailed dashboards for top sensors"""
        n_detailed = self.config['settings']['detailed_sensors']
        print(f"\n3. Creating detailed dashboards for {n_detailed} sensors")

        for idx, sensor_id in enumerate(self.sensors[:n_detailed], 1):
            print(f"   {idx}. {sensor_id}", end=" ")

            historical_data, forecast_data, forecast_start = self.prepare_sensor_data(sensor_id)

            if historical_data is None or len(historical_data) == 0:
                print("[SKIP - No data]")
                continue

            actual_data = None
            if self.actuals_df is not None:
                act_cols = self.config['columns']['actuals']
                actual_temp = self.actuals_df[
                    self.actuals_df[act_cols['sensor_id']] == sensor_id
                ].copy()

                if len(actual_temp) > 0:
                    actual_data = actual_temp.rename(columns={
                        act_cols['timestamp']: 'timestamp',
                        act_cols['value']: 'value'
                    })
                    actual_data = actual_data[['timestamp', 'value']].sort_values('timestamp')

            safe_sensor_name = sensor_id.replace('/', '_').replace('\\', '_').replace(':', '_')[:50]
            output_path = output_dir / f'dashboard_{idx}_{safe_sensor_name}.png'

            fig = forecasting.create_forecast_dashboard(
                historical_data=historical_data,
                forecast_data=forecast_data,
                actual_data=actual_data,
                forecast_start=forecast_start,
                sensor_id=sensor_id,
                output_path=output_path
            )
            plt.close(fig)

            print("[OK]")

    def create_detailed_plots(self, output_dir):
        """Create individual detailed error analysis plots"""
        if self.actuals_df is None:
            print("\n4. Skipping detailed error analysis (no actuals provided)")
            return

        print("\n4. Creating detailed error analysis plots")

        sensor_id = self.sensors[0]
        print(f"   Analyzing: {sensor_id}")

        historical_data, forecast_data, forecast_start = self.prepare_sensor_data(sensor_id)

        act_cols = self.config['columns']['actuals']
        actual_data = self.actuals_df[
            self.actuals_df[act_cols['sensor_id']] == sensor_id
        ].copy()
        actual_data = actual_data.rename(columns={
            act_cols['timestamp']: 'timestamp',
            act_cols['value']: 'value'
        })
        actual_data = actual_data[['timestamp', 'value']].sort_values('timestamp')

        merged = pd.merge(
            forecast_data[['timestamp', 'mean']],
            actual_data[['timestamp', 'value']],
            on='timestamp',
            how='inner'
        )

        if len(merged) > 0:
            plots = [
                ('Forecast with CI', 'detailed_01_forecast_ci.png',
                 lambda: forecasting.plot_forecast_with_confidence(
                     historical_data, forecast_data, forecast_start, sensor_id)),
                ('Forecast vs Actual', 'detailed_02_vs_actual.png',
                 lambda: forecasting.plot_forecast_with_actual(
                     historical_data, forecast_data, actual_data, forecast_start, sensor_id)),
                ('Residuals over time', 'detailed_03_residuals.png',
                 lambda: forecasting.plot_residuals_over_time(
                     merged['value'], merged['mean'], merged['timestamp'], sensor_id)),
                ('Error distribution', 'detailed_04_error_dist.png',
                 lambda: forecasting.plot_error_distribution(
                     merged['value'], merged['mean'], sensor_id)),
                ('Actual vs Predicted scatter', 'detailed_05_scatter.png',
                 lambda: forecasting.plot_scatter_actual_vs_predicted(
                     merged['value'], merged['mean'], sensor_id))
            ]

            for name, filename, plot_func in plots:
                print(f"   - {name}", end=" ")
                ax = plot_func()
                utils.save_plot(ax.figure, output_dir / filename)
                print("[OK]")
        else:
            print("   [WARNING] No overlapping timestamps for error analysis")

    def calculate_statistics(self):
        """Calculate and display overall statistics"""
        if self.actuals_df is None:
            print("\n5. Skipping statistics (no actuals provided)")
            return

        print("\n5. Calculating overall statistics")

        try:
            from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
        except ImportError:
            print("   [WARNING] scikit-learn not installed, skipping statistics")
            return

        # Merge predictions with actuals
        pred_cols = self.config['columns']['predictions']
        act_cols = self.config['columns']['actuals']

        all_merged = pd.merge(
            self.predictions_df[[pred_cols['sensor_id'], pred_cols['timestamp'], pred_cols['mean']]].rename(columns={
                pred_cols['sensor_id']: 'item_id',
                pred_cols['timestamp']: 'timestamp',
                pred_cols['mean']: 'mean'
            }),
            self.actuals_df[[act_cols['sensor_id'], act_cols['timestamp'], act_cols['value']]].rename(columns={
                act_cols['sensor_id']: 'item_id',
                act_cols['timestamp']: 'timestamp',
                act_cols['value']: 'value'
            }),
            on=['item_id', 'timestamp'],
            how='inner'
        )

        if len(all_merged) > 0:
            overall_mae = mean_absolute_error(all_merged['value'], all_merged['mean'])
            overall_rmse = np.sqrt(mean_squared_error(all_merged['value'], all_merged['mean']))
            overall_mape = np.mean(np.abs((all_merged['value'] - all_merged['mean']) / all_merged['value'])) * 100
            overall_r2 = r2_score(all_merged['value'], all_merged['mean'])

            print("\n   Overall Performance Metrics:")
            print(f"   {'Metric':<15} {'Value':>12}")
            print("   " + "-" * 30)
            print(f"   {'MAE':<15} {overall_mae:>12.4f}")
            print(f"   {'RMSE':<15} {overall_rmse:>12.4f}")
            print(f"   {'MAPE':<15} {overall_mape:>12.2f}%")
            print(f"   {'R²':<15} {overall_r2:>12.4f}")

            print("\n   Per-Sensor Statistics:")
            print(f"   {'Sensor':<50} {'MAE':>10} {'RMSE':>10} {'R²':>8}")

            for sensor_id in self.sensors[:5]:
                sensor_merged = all_merged[all_merged['item_id'] == sensor_id]
                if len(sensor_merged) > 0:
                    mae = mean_absolute_error(sensor_merged['value'], sensor_merged['mean'])
                    rmse = np.sqrt(mean_squared_error(sensor_merged['value'], sensor_merged['mean']))
                    r2 = r2_score(sensor_merged['value'], sensor_merged['mean'])

                    print(f"   {sensor_id[:50]:<50} {mae:>10.3f} {rmse:>10.3f} {r2:>8.4f}")

            if len(self.sensors) > 5:
                print(f"   ... and {len(self.sensors) - 5} more sensors")
        else:
            print("   [WARNING] No matching timestamps between predictions and actuals")

    def run(self):
        """Run complete visualization pipeline"""
        self.load_data()

        output_dir = Path(self.config['paths']['output'])
        output_dir.mkdir(exist_ok=True, parents=True)
        utils.setup_plot_style()

        self.create_overview(output_dir)
        self.create_dashboards(output_dir)
        self.create_detailed_plots(output_dir)
        self.calculate_statistics()
        
        print("VISUALIZATION COMPLETE")

        generated_files = sorted(output_dir.glob('*.png'))
        print(f"\nGenerated {len(generated_files)} visualization files in:")
        print(f"  {output_dir.absolute()}")

        print("\nFiles created:")
        for i, filepath in enumerate(generated_files, 1):
            print(f"  {i:2d}. {filepath.name}")
        print()


def create_default_config():
    """Create default configuration template"""
    return {
        "paths": {
            "predictions": "predictions.parquet",
            "historical": "historical.parquet",
            "actuals": "actuals.parquet",
            "output": "forecast_visualizations"
        },
        "columns": {
            "predictions": {
                "timestamp": "timestamp",
                "sensor_id": "item_id",
                "mean": "mean",
                "quantile_10": "0.1",
                "quantile_20": "0.2",
                "quantile_80": "0.8",
                "quantile_90": "0.9"
            },
            "historical": {
                "timestamp": "EventTime",
                "sensor_id": "TagName",
                "value": "Value"
            },
            "actuals": {
                "timestamp": "timestamp",
                "sensor_id": "item_id",
                "value": "target"
            }
        },
        "settings": {
            "lookback_hours": 168,
            "max_sensors_overview": 9,
            "detailed_sensors": 3,
            "datetime_format": "mixed"
        }
    }


def main():
    parser = argparse.ArgumentParser(
        description='Generalized Time Series Forecast Visualization Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument('--config', type=str, help='Path to JSON configuration file')

    # Path arguments
    parser.add_argument('--predictions', type=str, help='Path to predictions parquet file')
    parser.add_argument('--historical', type=str, help='Path to historical data parquet file')
    parser.add_argument('--actuals', type=str, help='Path to actuals/ground truth parquet file (optional)')
    parser.add_argument('--output', type=str, default='forecast_visualizations',
                       help='Output directory for visualizations (default: forecast_visualizations)')

    # Predictions column mappings
    parser.add_argument('--pred-timestamp', type=str, default='timestamp',
                       help='Predictions timestamp column name (default: timestamp)')
    parser.add_argument('--pred-sensor', type=str, default='item_id',
                       help='Predictions sensor ID column name (default: item_id)')
    parser.add_argument('--pred-mean', type=str, default='mean',
                       help='Predictions mean value column name (default: mean)')
    parser.add_argument('--pred-q10', type=str, default='0.1',
                       help='Predictions 10th quantile column (default: 0.1)')
    parser.add_argument('--pred-q20', type=str, default='0.2',
                       help='Predictions 20th quantile column (default: 0.2)')
    parser.add_argument('--pred-q80', type=str, default='0.8',
                       help='Predictions 80th quantile column (default: 0.8)')
    parser.add_argument('--pred-q90', type=str, default='0.9',
                       help='Predictions 90th quantile column (default: 0.9)')

    # Historical column mappings
    parser.add_argument('--hist-timestamp', type=str, default='EventTime',
                       help='Historical timestamp column name (default: EventTime)')
    parser.add_argument('--hist-sensor', type=str, default='TagName',
                       help='Historical sensor ID column name (default: TagName)')
    parser.add_argument('--hist-value', type=str, default='Value',
                       help='Historical value column name (default: Value)')

    # Actuals column mappings
    parser.add_argument('--act-timestamp', type=str, default='timestamp',
                       help='Actuals timestamp column name (default: timestamp)')
    parser.add_argument('--act-sensor', type=str, default='item_id',
                       help='Actuals sensor ID column name (default: item_id)')
    parser.add_argument('--act-value', type=str, default='target',
                       help='Actuals value column name (default: target)')

    # Settings
    parser.add_argument('--lookback-hours', type=int, default=168,
                       help='Hours of historical context to show (default: 168 = 1 week)')
    parser.add_argument('--max-sensors', type=int, default=9,
                       help='Maximum sensors in overview grid (default: 9)')
    parser.add_argument('--detailed-sensors', type=int, default=3,
                       help='Number of detailed dashboards to create (default: 3)')
    parser.add_argument('--datetime-format', type=str, default='mixed',
                       help='DateTime format: "mixed", "iso", or format string (default: mixed)')

    # Utility options
    parser.add_argument('--create-template', type=str, metavar='FILENAME',
                       help='Create a template configuration file and exit')

    args = parser.parse_args()

    if args.create_template:
        template_path = Path(args.create_template)
        with open(template_path, 'w') as f:
            json.dump(create_default_config(), f, indent=2)
        print(f"Created template configuration file: {template_path}")
        print("Edit this file and run with: python visualize_forecasts.py --config " + args.create_template)
        return

    if args.config:
        config_path = Path(args.config)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, 'r') as f:
            config_dict = json.load(f)
    else:
        if not args.predictions or not args.historical:
            parser.error("--predictions and --historical are required (or use --config)")

        config_dict = {
            "paths": {
                "predictions": args.predictions,
                "historical": args.historical,
                "actuals": args.actuals,
                "output": args.output
            },
            "columns": {
                "predictions": {
                    "timestamp": args.pred_timestamp,
                    "sensor_id": args.pred_sensor,
                    "mean": args.pred_mean,
                    "quantile_10": args.pred_q10,
                    "quantile_20": args.pred_q20,
                    "quantile_80": args.pred_q80,
                    "quantile_90": args.pred_q90
                },
                "historical": {
                    "timestamp": args.hist_timestamp,
                    "sensor_id": args.hist_sensor,
                    "value": args.hist_value
                },
                "actuals": {
                    "timestamp": args.act_timestamp,
                    "sensor_id": args.act_sensor,
                    "value": args.act_value
                }
            },
            "settings": {
                "lookback_hours": args.lookback_hours,
                "max_sensors_overview": args.max_sensors,
                "detailed_sensors": args.detailed_sensors,
                "datetime_format": args.datetime_format
            }
        }

    visualizer = ForecastVisualizer(config_dict)
    visualizer.run()


if __name__ == '__main__':
    main()
