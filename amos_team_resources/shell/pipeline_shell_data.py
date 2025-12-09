"""
Shell Data End-to-End Pipeline

This script orchestrates the complete pipeline for Shell sensor data:
0. Data Ingestion: Download data from Azure Blob Storage
1. Preprocessing: Load raw data and apply RTDIP transformations
2. Flat Sensor Detection: Analyze sensors for minimal variation
3. Flat Sensor Filtering: Remove flat sensors from dataset
4. Training: Train AutoGluon forecasting models
5. Model Optimization: Tag best model as 'optimized' for deployment
6. Visualization: Generate comprehensive forecast visualizations

Each step can be skipped if already completed (checkpointing).

Usage:
    # Run full pipeline (requires Azure credentials for ingestion)
    export AZURE_ACCOUNT_URL="https://azassaexdseq00039ewduni.blob.core.windows.net"
    export AZURE_CONTAINER_NAME="rtimedata"
    export AZURE_SAS_TOKEN="?sv=2020-10-02&ss=btqf&srt=sco&st=..."
    python pipeline_shell_data.py --ingest-format parquet --raw-data data/ShellData.parquet

    # Use CSV format instead of Parquet
    python pipeline_shell_data.py --ingest-format csv --raw-data data/ShellData.csv

    # Skip preprocessing if already done
    python pipeline_shell_data.py --skip-preprocessing

    # Skip flat sensor detection/filtering
    python pipeline_shell_data.py --skip-flat-detection --skip-flat-filtering

    # Skip training if already done
    python pipeline_shell_data.py --skip-training

    # Skip visualization
    python pipeline_shell_data.py --skip-visualization

    # Use Plotly for interactive HTML visualizations
    python pipeline_shell_data.py --use-plotly

    # Custom paths
    python pipeline_shell_data.py --raw-data data/ShellData.parquet --preprocessed-data output.parquet

    # Run pipeline on a sample of the data
    python pipeline_shell_data.py --sample 0.1

    # Customize flat sensor detection thresholds
    python pipeline_shell_data.py --std-threshold 0.05 --unique-ratio-threshold 0.02
"""

import argparse
import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime


class ShellDataPipeline:
    """End-to-end pipeline for Shell sensor data preprocessing and training."""

    def __init__(
        self,
        raw_data_path: str = "data/ShellData.csv",
        preprocessed_data_path: str = "preprocessing/ShellData_preprocessed.parquet",
        filtered_data_path: str = "preprocessing/ShellData_preprocessed_filtered.parquet",
        flat_sensors_path: str = "preprocessing/flat_sensors.csv",
        training_output_dir: str = "training/autogluon_results",
        visualization_output_dir: str = "training/forecast_visualizations",
        n_sigma: float = 10.0,
        sample_fraction: float = None,
        ingest_format: str = "csv",
        skip_ingestion: bool = False,
        skip_preprocessing: bool = False,
        skip_flat_detection: bool = False,
        skip_flat_filtering: bool = False,
        skip_training: bool = False,
        skip_visualization: bool = False,
        use_plotly: bool = False,
        std_threshold: float = 0.01,
        unique_ratio_threshold: float = 0.01,
        cv_threshold: float = 0.01,
        range_threshold: float = 0.01,
        most_common_threshold: float = 95.0,
        zero_diff_threshold: float = 95.0,
    ):
        self.script_dir = Path(__file__).parent
        self.raw_data_path = self.script_dir / raw_data_path
        self.preprocessed_data_path = self.script_dir / preprocessed_data_path
        self.filtered_data_path = self.script_dir / filtered_data_path
        self.flat_sensors_path = self.script_dir / flat_sensors_path
        self.training_output_dir = self.script_dir / training_output_dir
        self.visualization_output_dir = self.script_dir / visualization_output_dir
        self.n_sigma = n_sigma
        self.sample_fraction = sample_fraction
        self.ingest_format = ingest_format
        self.skip_ingestion = skip_ingestion
        self.skip_preprocessing = skip_preprocessing
        self.skip_flat_detection = skip_flat_detection
        self.skip_flat_filtering = skip_flat_filtering
        self.skip_training = skip_training
        self.skip_visualization = skip_visualization
        self.use_plotly = use_plotly

        self.std_threshold = std_threshold
        self.unique_ratio_threshold = unique_ratio_threshold
        self.cv_threshold = cv_threshold
        self.range_threshold = range_threshold
        self.most_common_threshold = most_common_threshold
        self.zero_diff_threshold = zero_diff_threshold

        self.ingest_script = self.script_dir / "ingest" / "ingest_shell_data.py"
        self.preprocess_script = (
            self.script_dir / "preprocessing" / "preprocess_shell_data.py"
        )
        self.detect_flat_script = (
            self.script_dir / "preprocessing" / "detect_flat_sensors.py"
        )
        self.filter_flat_script = (
            self.script_dir / "preprocessing" / "filter_flat_sensors.py"
        )
        self.training_script = self.script_dir / "training" / "train_autogluon_shell.py"
        self.tag_model_script = self.script_dir / "training" / "tag_optimized_model.py"
        self.optimized_model_dir = self.training_output_dir / "optimized_model"

    def print_header(self, text: str):
        """Print header."""
        print("\n" + "=" * 80)
        print(text)
        print("=" * 80)

    def print_step(self, step_num: int, text: str):
        """Print step."""
        print(f"\nStep {step_num}: {text}")
        print("-" * 80)

    def check_file_exists(self, path: Path) -> bool:
        """Check if file exists and print status."""
        exists = path.exists()
        if exists:
            size_mb = path.stat().st_size / (1024 * 1024)
            print(f"Found: {path.name} ({size_mb:.2f} MB)")
        else:
            print(f"Not found: {path.name}")
        return exists

    def run_ingestion(self):
        """Run data ingestion step."""
        self.print_step(0, "DATA INGESTION")

        if self.skip_ingestion:
            print("Skipping data ingestion (--skip-ingestion flag)")
            if not self.check_file_exists(self.raw_data_path):
                print("Warning: Raw data not found but skipping anyway")
            return True

        if self.raw_data_path.exists():
            print(f"Raw data already exists: {self.raw_data_path.name}")
            print("Using existing data (delete file to re-download)")
            return True

        if not self.ingest_script.exists():
            print(f"Error: Ingestion script not found at {self.ingest_script}")
            return False

        print(f"\nDownloading data from Azure Blob Storage")
        print(f"Output format: {self.ingest_format}")

        cmd = [
            sys.executable,
            str(self.ingest_script),
            "--output-format",
            self.ingest_format,
        ]

        try:
            result = subprocess.run(cmd, check=True, capture_output=False)
            print("\nData ingestion completed successfully")
            return True
        except subprocess.CalledProcessError as e:
            print(f"\nData ingestion failed with exit code {e.returncode}")
            print("Ensure Azure credentials are set:")
            print("  export AZURE_ACCOUNT_URL=...")
            print("  export AZURE_CONTAINER_NAME=...")
            print("  export AZURE_SAS_TOKEN=...")
            return False

    def run_preprocessing(self):
        """Run preprocessing step."""
        self.print_step(1, "PREPROCESSING")

        # Preprocessing already done?
        if self.skip_preprocessing:
            print("Skipping preprocessing (--skip-preprocessing flag)")
            if not self.check_file_exists(self.preprocessed_data_path):
                print("Warning: Preprocessed data not found but skipping anyway")
            return True

        if self.preprocessed_data_path.exists():
            print(
                f"Preprocessed data already exists: {self.preprocessed_data_path.name}"
            )
            response = input("Rerun preprocessing? (y/N): ").strip().lower()
            if response != "y":
                print("Using existing preprocessed data")
                return True

        # Check if raw data exists
        if not self.check_file_exists(self.raw_data_path):
            print(f"Error: Raw data not found at {self.raw_data_path}")
            return False

        # Check if preprocessing script exists
        if not self.preprocess_script.exists():
            print(f"Error: Preprocessing script not found at {self.preprocess_script}")
            return False

        # Run preprocessing
        print(f"\nRunning preprocessing script")
        print(f"Input: {self.raw_data_path}")
        print(f"Output: {self.preprocessed_data_path}")
        print(f"n_sigma: {self.n_sigma}")
        if self.sample_fraction:
            print(f"Sample: {self.sample_fraction*100:.1f}% of data")

        cmd = [
            sys.executable,
            str(self.preprocess_script),
            "--input",
            str(self.raw_data_path),
            "--output",
            str(self.preprocessed_data_path),
            "--n-sigma",
            str(self.n_sigma),
        ]

        if self.sample_fraction:
            cmd.extend(["--sample", str(self.sample_fraction)])

        try:
            result = subprocess.run(cmd, check=True, capture_output=False)
            print("\nPreprocessing completed successfully")
            return True
        except subprocess.CalledProcessError as e:
            print(f"\nPreprocessing failed with exit code {e.returncode}")
            return False

    def run_flat_detection(self):
        """Run flat sensor detection step."""
        self.print_step(2, "FLAT SENSOR DETECTION")
        if self.skip_flat_detection:
            print("Skipping flat sensor detection (--skip-flat-detection flag)")
            if not self.check_file_exists(self.flat_sensors_path):
                print("Warning: Flat sensors list not found but skipping anyway")
            return True

        if self.flat_sensors_path.exists():
            print(f"Flat sensors list already exists: {self.flat_sensors_path.name}")
            response = input("Rerun flat sensor detection? (y/N): ").strip().lower()
            if response != "y":
                print("Using existing flat sensors list")
                return True

        if not self.check_file_exists(self.preprocessed_data_path):
            print(
                f"Error: Preprocessed data not found at {self.preprocessed_data_path}"
            )
            print("Run preprocessing first or provide correct path")
            return False

        if not self.detect_flat_script.exists():
            print(f"Error: Detection script not found at {self.detect_flat_script}")
            return False

        print(f"\nRunning flat sensor detection script")
        print(f"Input: {self.preprocessed_data_path}")
        print(f"Output: {self.flat_sensors_path}")
        print(f"Thresholds:")
        print(f"  - std_threshold: {self.std_threshold}")
        print(f"  - unique_ratio_threshold: {self.unique_ratio_threshold}")
        print(f"  - cv_threshold: {self.cv_threshold}")
        print(f"  - range_threshold: {self.range_threshold}")
        print(f"  - most_common_threshold: {self.most_common_threshold}")
        print(f"  - zero_diff_threshold: {self.zero_diff_threshold}")

        try:
            sys.path.insert(0, str(self.detect_flat_script.parent))
            from detect_flat_sensors import (
                load_data,
                analyze_sensor_variation,
                identify_flat_sensors,
                save_results,
            )

            df = load_data(self.preprocessed_data_path)
            results_df = analyze_sensor_variation(df)
            flat_info = identify_flat_sensors(
                results_df,
                std_threshold=self.std_threshold,
                unique_ratio_threshold=self.unique_ratio_threshold,
                cv_threshold=self.cv_threshold,
                range_threshold=self.range_threshold,
                most_common_threshold=self.most_common_threshold,
                zero_diff_threshold=self.zero_diff_threshold,
            )

            save_results(
                results_df, flat_info, output_dir=self.flat_sensors_path.parent
            )

            print("\nFlat sensor detection completed successfully")
            return True
        except Exception as e:
            print(f"\nFlat sensor detection failed with error: {e}")
            import traceback

            traceback.print_exc()
            return False
        finally:
            sys.path.pop(0)

    def run_flat_filtering(self):
        """Run flat sensor filtering step."""
        self.print_step(3, "FLAT SENSOR FILTERING")

        if self.skip_flat_filtering:
            print("Skipping flat sensor filtering (--skip-flat-filtering flag)")
            if not self.check_file_exists(self.filtered_data_path):
                print("Warning: Filtered data not found but skipping anyway")
            return True

        if self.filtered_data_path.exists():
            print(f"Filtered data already exists: {self.filtered_data_path.name}")
            response = input("Rerun flat sensor filtering? (y/N): ").strip().lower()
            if response != "y":
                print("Using existing filtered data")
                return True

        if not self.check_file_exists(self.preprocessed_data_path):
            print(
                f"Error: Preprocessed data not found at {self.preprocessed_data_path}"
            )
            print("Run preprocessing first or provide correct path")
            return False

        if not self.check_file_exists(self.flat_sensors_path):
            print(f"Error: Flat sensors list not found at {self.flat_sensors_path}")
            print("Run flat sensor detection first")
            return False

        if not self.filter_flat_script.exists():
            print(f"Error: Filtering script not found at {self.filter_flat_script}")
            return False

        print(f"\nRunning flat sensor filtering script")
        print(f"Input: {self.preprocessed_data_path}")
        print(f"Flat sensors list: {self.flat_sensors_path}")
        print(f"Output: {self.filtered_data_path}")

        try:
            sys.path.insert(0, str(self.filter_flat_script.parent))
            from filter_flat_sensors import (
                load_flat_sensor_list,
                filter_dataset,
                create_metadata,
            )

            flat_sensors = load_flat_sensor_list(self.flat_sensors_path)

            df_filtered = filter_dataset(
                self.preprocessed_data_path,
                flat_sensors,
                output_path=self.filtered_data_path,
            )

            create_metadata(df_filtered, self.filtered_data_path, flat_sensors)

            print("\nFlat sensor filtering completed successfully")
            return True
        except Exception as e:
            print(f"\nFlat sensor filtering failed with error: {e}")
            import traceback

            traceback.print_exc()
            return False
        finally:
            sys.path.pop(0)

    def run_training(self):
        """Run training step."""
        self.print_step(4, "TRAINING")

        # Check if training already done
        if self.skip_training:
            print("Skipping training (--skip-training flag)")
            model_path = self.training_output_dir / "autogluon_model"
            if not model_path.exists():
                print("Warning: Model not found but skipping anyway")
            return True

        if self.training_output_dir.exists():
            print(f"Training results already exist: {self.training_output_dir.name}")
            response = input("Rerun training? (y/N): ").strip().lower()
            if response != "y":
                print("Using existing training results")
                return True

        # Determine which data file to use (filtered if available, otherwise preprocessed)
        training_data_path = (
            self.filtered_data_path
            if self.filtered_data_path.exists()
            else self.preprocessed_data_path
        )

        # Check if data exists
        if not self.check_file_exists(training_data_path):
            print(f"Error: Training data not found at {training_data_path}")
            if training_data_path == self.filtered_data_path:
                print(
                    "Filtered data not found. Run flat sensor detection and filtering first."
                )
            else:
                print("Preprocessed data not found. Run preprocessing first.")
            return False

        # Check if training script exists
        if not self.training_script.exists():
            print(f"Error: Training script not found at {self.training_script}")
            return False

        # Update training script to use the training data path
        print(f"\nRunning training script")
        print(f"Input: {training_data_path}")
        if training_data_path == self.filtered_data_path:
            print(f"  Using filtered data (flat sensors removed)")
        else:
            print(f"  Using preprocessed data (no filtering applied)")
        print(f"Output: {self.training_output_dir}")

        # Change to training directory and run script
        original_cwd = os.getcwd()
        training_dir = self.training_script.parent

        try:
            os.chdir(training_dir)

            env = os.environ.copy()
            env["SHELL_DATA_PATH"] = str(training_data_path)

            cmd = [sys.executable, str(self.training_script)]

            result = subprocess.run(cmd, check=True, capture_output=False, env=env)
            print("\nTraining completed successfully")
            return True
        except subprocess.CalledProcessError as e:
            print(f"\nTraining failed with exit code {e.returncode}")
            return False
        finally:
            os.chdir(original_cwd)

    def run_model_optimization(self):
        """Run model optimization (tagging) step."""
        self.print_step(5, "MODEL OPTIMIZATION")
        if not self.training_output_dir.exists():
            print(
                f"Error: Training output directory not found at {self.training_output_dir}"
            )
            print("Run training first")
            return False

        if not self.tag_model_script.exists():
            print(f"Error: Model tagging script not found at {self.tag_model_script}")
            return False
        training_data_path = (
            self.filtered_data_path if self.filtered_data_path.exists() else None
        )

        print(f"\nTagging best-performing model as 'optimized'")
        print(f"Training output: {self.training_output_dir}")
        if training_data_path:
            print(f"Training data: {training_data_path}")
        try:
            sys.path.insert(0, str(self.tag_model_script.parent))
            from tag_optimized_model import tag_optimized_model

            deployment_dir = tag_optimized_model(
                self.training_output_dir, filtered_data_path=training_data_path
            )

            if deployment_dir:
                print("\nModel optimization completed successfully")
                print(f"Optimized model ready at: {deployment_dir}")
                return True
            else:
                print("\nModel optimization failed")
                return False

        except Exception as e:
            print(f"\nModel optimization failed with error: {e}")
            import traceback

            traceback.print_exc()
            return False
        finally:
            sys.path.pop(0)

    def run_visualization(self):
        """Run visualization step."""
        self.print_step(6, "VISUALIZATION")

        if self.skip_visualization:
            print("Skipping visualization (--skip-visualization flag)")
            return True

        predictions_path = self.training_output_dir / "predictions.parquet"
        actuals_path = self.training_output_dir / "test_actuals.parquet"

        if not predictions_path.exists():
            print(f"Error: Predictions not found at {predictions_path}")
            print("Run training first to generate forecasts")
            return False

        if not actuals_path.exists():
            print(f"Warning: Test actuals not found at {actuals_path}")
            print("Continuing with predictions-only visualization")

        historical_data_path = self.filtered_data_path if self.filtered_data_path.exists() else self.preprocessed_data_path

        if not historical_data_path.exists():
            print(f"Error: Historical data not found at {historical_data_path}")
            return False

        backend = "Plotly (interactive HTML)" if self.use_plotly else "Matplotlib (static PNG)"
        print(f"\nGenerating forecast visualizations using {backend}")
        print(f"Predictions: {predictions_path.relative_to(self.script_dir)}")
        print(f"Historical:  {historical_data_path.relative_to(self.script_dir)}")
        if actuals_path.exists():
            print(f"Actuals:     {actuals_path.relative_to(self.script_dir)}")
        print(f"Output:      {self.visualization_output_dir.relative_to(self.script_dir)}")

        viz_module_dir = self.script_dir.parent / "visualization"

        try:
            sys.path.insert(0, str(viz_module_dir.parent))

            if self.use_plotly:
                return self._run_plotly_visualization(
                    predictions_path,
                    historical_data_path,
                    actuals_path
                )
            else:
                from visualization.visualize_forecasts import ForecastVisualizer

                config = {
                    "paths": {
                        "predictions": str(predictions_path),
                        "historical": str(historical_data_path),
                        "actuals": str(actuals_path) if actuals_path.exists() else None,
                        "output": str(self.visualization_output_dir)
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

                visualizer = ForecastVisualizer(config)
                visualizer.run()

                print("\nVisualization completed successfully")
                return True

        except Exception as e:
            print(f"\nVisualization failed with error: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            sys.path.pop(0)

    def _run_plotly_visualization(self, predictions_path: Path, historical_data_path: Path, actuals_path: Path):
        """Run Plotly interactive visualization with configurable column mappings."""
        import pandas as pd
        from visualization import forecasting_plotly

        self.visualization_output_dir.mkdir(parents=True, exist_ok=True)

        column_mapping = {
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
        }

        print("\nLoading data for Plotly visualization")
        predictions_df = pd.read_parquet(predictions_path)
        historical_df = pd.read_parquet(historical_data_path)
        actuals_df = pd.read_parquet(actuals_path) if actuals_path.exists() else None

        pred_sensor_col = column_mapping["predictions"]["sensor_id"]
        sensors = predictions_df[pred_sensor_col].unique()[:5]
        print(f"Generating interactive plots for {len(sensors)} sensors")

        for i, sensor_id in enumerate(sensors, 1):
            print(f"  [{i}/{len(sensors)}] Processing {sensor_id}")

            pred_cols = column_mapping["predictions"]
            hist_cols = column_mapping["historical"]
            actual_cols = column_mapping["actuals"]

            sensor_predictions = predictions_df[predictions_df[pred_cols["sensor_id"]] == sensor_id].copy()
            sensor_historical = historical_df[historical_df[hist_cols["sensor_id"]] == sensor_id].copy()

            sensor_historical['timestamp'] = pd.to_datetime(sensor_historical[hist_cols["timestamp"]])
            sensor_historical['value'] = sensor_historical[hist_cols["value"]]

            sensor_predictions['timestamp'] = pd.to_datetime(sensor_predictions[pred_cols["timestamp"]])

            forecast_start = sensor_predictions['timestamp'].min()

            forecast_data = pd.DataFrame({
                'timestamp': sensor_predictions['timestamp'],
                'mean': sensor_predictions[pred_cols["mean"]]
            })

            if pred_cols["quantile_10"] in sensor_predictions.columns and pred_cols["quantile_90"] in sensor_predictions.columns:
                forecast_data['lower_80'] = sensor_predictions[pred_cols["quantile_10"]]
                forecast_data['upper_80'] = sensor_predictions[pred_cols["quantile_90"]]
            if pred_cols["quantile_20"] in sensor_predictions.columns and pred_cols["quantile_80"] in sensor_predictions.columns:
                forecast_data['lower_60'] = sensor_predictions[pred_cols["quantile_20"]]
                forecast_data['upper_60'] = sensor_predictions[pred_cols["quantile_80"]]

            fig1 = forecasting_plotly.plot_forecast_with_confidence(
                historical_data=sensor_historical[['timestamp', 'value']],
                forecast_data=forecast_data,
                forecast_start=forecast_start,
                sensor_id=sensor_id,
                ci_levels=[60, 80]
            )

            output_file = self.visualization_output_dir / f"forecast_{sensor_id.replace(':', '_')}.html"
            forecasting_plotly.save_plotly_figure(fig1, output_file, format='html')

            if actuals_df is not None:
                sensor_actuals = actuals_df[actuals_df[actual_cols["sensor_id"]] == sensor_id].copy()
                if not sensor_actuals.empty:
                    sensor_actuals['timestamp'] = pd.to_datetime(sensor_actuals[actual_cols["timestamp"]])
                    sensor_actuals['value'] = sensor_actuals[actual_cols["value"]]

                    fig2 = forecasting_plotly.plot_forecast_with_actual(
                        historical_data=sensor_historical[['timestamp', 'value']],
                        forecast_data=forecast_data,
                        actual_data=sensor_actuals[['timestamp', 'value']],
                        forecast_start=forecast_start,
                        sensor_id=sensor_id
                    )

                    output_file_actual = self.visualization_output_dir / f"forecast_vs_actual_{sensor_id.replace(':', '_')}.html"
                    forecasting_plotly.save_plotly_figure(fig2, output_file_actual, format='html')

        print(f"\nPlotly visualization completed successfully")
        print(f"Open HTML files in browser for interactive exploration")
        return True
    def print_summary(
        self,
        ingestion_success: bool,
        preprocessing_success: bool,
        detection_success: bool,
        filtering_success: bool,
        training_success: bool,
        optimization_success: bool,
        visualization_success: bool):
    ):
        """Print pipeline summary."""
        self.print_header("PIPELINE SUMMARY")

        print("\nSteps completed:")

        print(
            f"0. Data Ingestion:          {'Success' if ingestion_success else 'Failed' if not self.skip_ingestion else 'Skipped'}"
        )
        print(
            f"1. Preprocessing:           {'Success' if preprocessing_success else 'Failed'}"
        )
        print(
            f"2. Flat Sensor Detection:   {'Success' if detection_success else 'Failed' if not self.skip_flat_detection else 'Skipped'}"
        )
        print(
            f"3. Flat Sensor Filtering:   {'Success' if filtering_success else 'Failed' if not self.skip_flat_filtering else 'Skipped'}"
        )
        print(
            f"4. Training:                {'Success' if training_success else 'Failed'}"
        )
        print(
            f"5. Model Optimization:      {'Success' if optimization_success else 'Failed'}"
        )
        print(f"6. Visualization:           {'Success' if visualization_success else 'Failed' if not self.skip_visualization else 'Skipped'}")
        all_success = (
            ingestion_success
            and preprocessing_success
            and detection_success
            and filtering_success
            and training_success
            and optimization_success
            and visualization_success
        )
        if all_success:
            print("\nPipeline completed successfully")

            print("\nGenerated files:")
            if self.preprocessed_data_path.exists():
                size_mb = self.preprocessed_data_path.stat().st_size / (1024 * 1024)
                print(
                    f"  • {self.preprocessed_data_path.relative_to(self.script_dir)} ({size_mb:.2f} MB)"
                )

            if self.flat_sensors_path.exists():
                size_kb = self.flat_sensors_path.stat().st_size / 1024
                print(
                    f"  • {self.flat_sensors_path.relative_to(self.script_dir)} ({size_kb:.2f} KB)"
                )

            if self.filtered_data_path.exists():
                size_mb = self.filtered_data_path.stat().st_size / (1024 * 1024)
                print(
                    f"  • {self.filtered_data_path.relative_to(self.script_dir)} ({size_mb:.2f} MB)"
                )

            if self.training_output_dir.exists():
                print(f"  • {self.training_output_dir.relative_to(self.script_dir)}/")
                for item in self.training_output_dir.iterdir():
                    if item.is_file():
                        size_mb = item.stat().st_size / (1024 * 1024)
                        print(f"    - {item.name} ({size_mb:.2f} MB)")
            if self.optimized_model_dir.exists():
                print(f"\n  Optimized Model for Deployment:")
                print(f"  • {self.optimized_model_dir.relative_to(self.script_dir)}/")
                print(f"    Tag: optimized")
                print(f"    Status: Ready for deployment")
                metadata_path = self.optimized_model_dir / "deployment_metadata.json"
                if metadata_path.exists():
                    import json

                    with open(metadata_path, "r") as f:
                        metadata = json.load(f)
                    if "model" in metadata and "best_model" in metadata["model"]:
                        best = metadata["model"]["best_model"]
                        print(f"    Best Model: {best.get('name', 'N/A')}")
                        print(f"    Score: {best.get('score_val', 'N/A')}")

            if self.visualization_output_dir.exists():
                print(f"\n  Forecast Visualizations:")
                print(f"  • {self.visualization_output_dir.relative_to(self.script_dir)}/")
                viz_files = list(self.visualization_output_dir.glob('*.png'))
                if viz_files:
                    print(f"    Generated {len(viz_files)} visualization files:")
                    for viz_file in sorted(viz_files)[:5]:  # Show first 5
                        size_kb = viz_file.stat().st_size / 1024
                        print(f"    - {viz_file.name} ({size_kb:.1f} KB)")
                    if len(viz_files) > 5:
                        print(f"    ... and {len(viz_files) - 5} more files")
        else:
            print("\nPipeline completed with errors")

    def run(self):
        """Run the complete pipeline."""
        self.print_header("SHELL DATA END-TO-END PIPELINE")
        print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        print("\nConfiguration:")

        print(f"  Raw data:            {self.raw_data_path.relative_to(self.script_dir)}")
        print(f"  Preprocessed:        {self.preprocessed_data_path.relative_to(self.script_dir)}")
        print(f"  Filtered data:       {self.filtered_data_path.relative_to(self.script_dir)}")
        print(f"  Flat sensors:        {self.flat_sensors_path.relative_to(self.script_dir)}")
        print(f"  Training output:     {self.training_output_dir.relative_to(self.script_dir)}")
        print(f"  Visualization output: {self.visualization_output_dir.relative_to(self.script_dir)}")
        print(f"  Outlier n_sigma:     {self.n_sigma}")

        print(f"  Raw data:        {self.raw_data_path.relative_to(self.script_dir)}")
        print(
            f"  Preprocessed:    {self.preprocessed_data_path.relative_to(self.script_dir)}"
        )
        print(
            f"  Filtered data:   {self.filtered_data_path.relative_to(self.script_dir)}"
        )
        print(
            f"  Flat sensors:    {self.flat_sensors_path.relative_to(self.script_dir)}"
        )
        print(
            f"  Training output: {self.training_output_dir.relative_to(self.script_dir)}"
        )
        print(f"  Visualization output: {self.visualization_output_dir.relative_to(self.script_dir)}")
        print(f"  Outlier n_sigma: {self.n_sigma}")
        if self.sample_fraction:
            print(f"  Sample fraction:     {self.sample_fraction*100:.1f}%")

        # Step 0: Data Ingestion
        ingestion_success = self.run_ingestion()

        # Step 1: Preprocessing (only if ingestion succeeds)
        if ingestion_success:
            preprocessing_success = self.run_preprocessing()
        else:
            print("\nSkipping preprocessing due to ingestion failure")
            preprocessing_success = False

        # Step 2: Flat Sensor Detection (only if preprocessing succeeds)
        if preprocessing_success:
            detection_success = self.run_flat_detection()
        else:
            if ingestion_success:
                print("\nSkipping flat sensor detection due to preprocessing failure")
            detection_success = False

        # Step 3: Flat Sensor Filtering (only if detection succeeds)
        if detection_success:
            filtering_success = self.run_flat_filtering()
        else:
            if preprocessing_success and not self.skip_flat_detection:
                print("\nSkipping flat sensor filtering due to detection failure")
            filtering_success = False

        # Step 4: Training (only if previous steps succeed or are skipped)
        if (
            preprocessing_success
            and (detection_success or self.skip_flat_detection)
            and (filtering_success or self.skip_flat_filtering)
        ):
            training_success = self.run_training()
        else:
            print("\nSkipping training due to previous failures")
            training_success = False

        # Step 5: Model Optimization (only if training succeeds)
        if training_success:
            optimization_success = self.run_model_optimization()
        else:
            print("\nSkipping model optimization due to training failure")
            optimization_success = False

        # Step 6: Visualization (only if training succeeds)
        if training_success:
            visualization_success = self.run_visualization()
        else:
            print("\nSkipping visualization due to training failure")
            visualization_success = False

        # Summary
        self.print_summary(
            ingestion_success,
            preprocessing_success,
            detection_success,
            filtering_success,
            training_success,
            optimization_success,
            visualization_success
        )

        print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        all_success = (
            ingestion_success
            and preprocessing_success
            and detection_success
            and filtering_success
            and training_success
            and optimization_success
            and visualization_success
        )
        return 0 if all_success else 1


def main():
    parser = argparse.ArgumentParser(
        description="End-to-end pipeline for Shell sensor data preprocessing and training."
    )

    # Data paths
    parser.add_argument(
        "--raw-data",
        type=str,
        default="data/ShellData.csv",
        help="Path to raw data file (default: data/ShellData.csv)",
    )
    parser.add_argument(
        "--preprocessed-data",
        type=str,
        default="preprocessing/ShellData_preprocessed.parquet",
        help="Path for preprocessed data (relative to script directory, default: preprocessing/ShellData_preprocessed.parquet)",
    )
    parser.add_argument(
        "--filtered-data",
        type=str,
        default="preprocessing/ShellData_preprocessed_filtered.parquet",
        help="Path for filtered data (relative to script directory, default: preprocessing/ShellData_preprocessed_filtered.parquet)",
    )
    parser.add_argument(
        "--flat-sensors",
        type=str,
        default="preprocessing/flat_sensors.csv",
        help="Path for flat sensors list (relative to script directory, default: preprocessing/flat_sensors.csv)",
    )
    parser.add_argument(
        "--training-output",
        type=str,
        default="training/autogluon_results",
        help="Directory for training outputs (relative to script directory, default: training/autogluon_results)",
    )
    parser.add_argument(
        "--visualization-output",
        type=str,
        default="training/forecast_visualizations",
        help="Directory for visualization outputs (relative to script directory, default: training/forecast_visualizations)"
    )

    # Preprocessing options
    parser.add_argument(
        "--n-sigma",
        type=float,
        default=10.0,
        help="Number of MAD-based std deviations for outlier detection (default: 10.0)",
    )
    parser.add_argument(
        "--sample",
        type=float,
        default=None,
        help="Sample a fraction of data for memory efficiency (e.g., 0.1 for 10%%)",
    )

    # Flat sensor detection thresholds
    parser.add_argument(
        "--std-threshold",
        type=float,
        default=0.01,
        help="Maximum standard deviation for flat sensor (default: 0.01)",
    )
    parser.add_argument(
        "--unique-ratio-threshold",
        type=float,
        default=0.01,
        help="Maximum ratio of unique values for flat sensor (default: 0.01)",
    )
    parser.add_argument(
        "--cv-threshold",
        type=float,
        default=0.01,
        help="Maximum coefficient of variation for flat sensor (default: 0.01)",
    )
    parser.add_argument(
        "--range-threshold",
        type=float,
        default=0.01,
        help="Maximum value range for flat sensor (default: 0.01)",
    )
    parser.add_argument(
        "--most-common-threshold",
        type=float,
        default=95.0,
        help="Minimum percentage of most common value for flat sensor (default: 95.0)",
    )
    parser.add_argument(
        "--zero-diff-threshold",
        type=float,
        default=95.0,
        help="Minimum percentage of zero differences for flat sensor (default: 95.0)",
    )

    # Ingestion options
    parser.add_argument(
        "--ingest-format",
        type=str,
        choices=["parquet", "csv", "both"],
        default="csv",
        help="Format to download from Azure: parquet, csv, or both (default: csv)",
    )

    # Skip options
    parser.add_argument(
        "--skip-ingestion",
        action="store_true",
        help="Skip data ingestion step (use existing raw data)",
    )
    parser.add_argument(
        "--skip-preprocessing",
        action="store_true",
        help="Skip preprocessing step (use existing preprocessed data)",
    )
    parser.add_argument(
        "--skip-flat-detection",
        action="store_true",
        help="Skip flat sensor detection step (use existing flat sensors list)",
    )
    parser.add_argument(
        "--skip-flat-filtering",
        action="store_true",
        help="Skip flat sensor filtering step (use existing filtered data)",
    )
    parser.add_argument(
        "--skip-training",
        action="store_true",
        help="Skip training step (use existing model)",
    )
    parser.add_argument(
        "--skip-visualization",
        action="store_true",
        help="Skip visualization step (use existing visualizations)"
    )
    parser.add_argument(
        "--use-plotly",
        action="store_true",
        help="Use Plotly for interactive HTML visualizations instead of Matplotlib static PNGs"
    )

    args = parser.parse_args()

    pipeline = ShellDataPipeline(
        raw_data_path=args.raw_data,
        preprocessed_data_path=args.preprocessed_data,
        filtered_data_path=args.filtered_data,
        flat_sensors_path=args.flat_sensors,
        training_output_dir=args.training_output,
        visualization_output_dir=args.visualization_output,
        n_sigma=args.n_sigma,
        sample_fraction=args.sample,
        ingest_format=args.ingest_format,
        skip_ingestion=args.skip_ingestion,
        skip_preprocessing=args.skip_preprocessing,
        skip_flat_detection=args.skip_flat_detection,
        skip_flat_filtering=args.skip_flat_filtering,
        skip_training=args.skip_training,
        skip_visualization=args.skip_visualization,
        use_plotly=args.use_plotly,
        std_threshold=args.std_threshold,
        unique_ratio_threshold=args.unique_ratio_threshold,
        cv_threshold=args.cv_threshold,
        range_threshold=args.range_threshold,
        most_common_threshold=args.most_common_threshold,
        zero_diff_threshold=args.zero_diff_threshold,
    )

    return pipeline.run()


if __name__ == "__main__":
    sys.exit(main())
