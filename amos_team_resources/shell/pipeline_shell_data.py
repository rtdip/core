"""
Shell Data End-to-End Pipeline

This script orchestrates the complete pipeline for Shell sensor data:
1. Preprocessing: Load raw data and apply RTDIP transformations
2. Flat Sensor Detection: Analyze sensors for minimal variation
3. Flat Sensor Filtering: Remove flat sensors from dataset
4. Training: Train AutoGluon forecasting models
5. Model Optimization: Tag best model as 'optimized' for deployment

Each step can be skipped if already completed (checkpointing).

Usage (CSV needed for now, want to refactor to Parquet later):
    # Run full pipeline
    python pipeline_shell_data.py

    # Skip preprocessing if already done
    python pipeline_shell_data.py --skip-preprocessing

    # Skip flat sensor detection/filtering
    python pipeline_shell_data.py --skip-flat-detection --skip-flat-filtering

    # Skip training if already done
    python pipeline_shell_data.py --skip-training

    # Custom paths
    python pipeline_shell_data.py --raw-data ShellData.csv --preprocessed-data output.parquet

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
        raw_data_path: str = "preprocessing/ShellData.csv",
        preprocessed_data_path: str = "preprocessing/ShellData_preprocessed.parquet",
        filtered_data_path: str = "preprocessing/ShellData_preprocessed_filtered.parquet",
        flat_sensors_path: str = "preprocessing/flat_sensors.csv",
        training_output_dir: str = "training/autogluon_results",
        n_sigma: float = 10.0,
        sample_fraction: float = None,
        skip_preprocessing: bool = False,
        skip_flat_detection: bool = False,
        skip_flat_filtering: bool = False,
        skip_training: bool = False,
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
        self.n_sigma = n_sigma
        self.sample_fraction = sample_fraction
        self.skip_preprocessing = skip_preprocessing
        self.skip_flat_detection = skip_flat_detection
        self.skip_flat_filtering = skip_flat_filtering
        self.skip_training = skip_training

        self.std_threshold = std_threshold
        self.unique_ratio_threshold = unique_ratio_threshold
        self.cv_threshold = cv_threshold
        self.range_threshold = range_threshold
        self.most_common_threshold = most_common_threshold
        self.zero_diff_threshold = zero_diff_threshold

        self.preprocess_script = self.script_dir / "preprocessing" / "preprocess_shell_data.py"
        self.detect_flat_script = self.script_dir / "preprocessing" / "detect_flat_sensors.py"
        self.filter_flat_script = self.script_dir / "preprocessing" / "filter_flat_sensors.py"
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
            print(f"Preprocessed data already exists: {self.preprocessed_data_path.name}")
            response = input("Rerun preprocessing? (y/N): ").strip().lower()
            if response != 'y':
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
            "--input", str(self.raw_data_path),
            "--output", str(self.preprocessed_data_path),
            "--n-sigma", str(self.n_sigma),
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
            if response != 'y':
                print("Using existing flat sensors list")
                return True
            
        if not self.check_file_exists(self.preprocessed_data_path):
            print(f"Error: Preprocessed data not found at {self.preprocessed_data_path}")
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
            from detect_flat_sensors import load_data, analyze_sensor_variation, identify_flat_sensors, save_results

            df = load_data(self.preprocessed_data_path)
            results_df = analyze_sensor_variation(df)
            flat_info = identify_flat_sensors(
                results_df,
                std_threshold=self.std_threshold,
                unique_ratio_threshold=self.unique_ratio_threshold,
                cv_threshold=self.cv_threshold,
                range_threshold=self.range_threshold,
                most_common_threshold=self.most_common_threshold,
                zero_diff_threshold=self.zero_diff_threshold
            )

            save_results(results_df, flat_info, output_dir=self.flat_sensors_path.parent)

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
            if response != 'y':
                print("Using existing filtered data")
                return True

        if not self.check_file_exists(self.preprocessed_data_path):
            print(f"Error: Preprocessed data not found at {self.preprocessed_data_path}")
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
            from filter_flat_sensors import load_flat_sensor_list, filter_dataset, create_metadata

            flat_sensors = load_flat_sensor_list(self.flat_sensors_path)

            df_filtered = filter_dataset(
                self.preprocessed_data_path,
                flat_sensors,
                output_path=self.filtered_data_path
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
            if response != 'y':
                print("Using existing training results")
                return True

        # Determine which data file to use (filtered if available, otherwise preprocessed)
        training_data_path = self.filtered_data_path if self.filtered_data_path.exists() else self.preprocessed_data_path

        # Check if data exists
        if not self.check_file_exists(training_data_path):
            print(f"Error: Training data not found at {training_data_path}")
            if training_data_path == self.filtered_data_path:
                print("Filtered data not found. Run flat sensor detection and filtering first.")
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
            env['SHELL_DATA_PATH'] = str(training_data_path)

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
            print(f"Error: Training output directory not found at {self.training_output_dir}")
            print("Run training first")
            return False

        if not self.tag_model_script.exists():
            print(f"Error: Model tagging script not found at {self.tag_model_script}")
            return False
        training_data_path = self.filtered_data_path if self.filtered_data_path.exists() else None

        print(f"\nTagging best-performing model as 'optimized'")
        print(f"Training output: {self.training_output_dir}")
        if training_data_path:
            print(f"Training data: {training_data_path}")
        try:
            sys.path.insert(0, str(self.tag_model_script.parent))
            from tag_optimized_model import tag_optimized_model
            deployment_dir = tag_optimized_model(
                self.training_output_dir,
                filtered_data_path=training_data_path
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

    def print_summary(self, preprocessing_success: bool, detection_success: bool,
                      filtering_success: bool, training_success: bool, optimization_success: bool):
        """Print pipeline summary."""
        self.print_header("PIPELINE SUMMARY")

        print("\nSteps completed:")
        print(f"1. Preprocessing:           {'Success' if preprocessing_success else 'Failed'}")
        print(f"2. Flat Sensor Detection:   {'Success' if detection_success else 'Failed' if not self.skip_flat_detection else 'Skipped'}")
        print(f"3. Flat Sensor Filtering:   {'Success' if filtering_success else 'Failed' if not self.skip_flat_filtering else 'Skipped'}")
        print(f"4. Training:                {'Success' if training_success else 'Failed'}")
        print(f"5. Model Optimization:      {'Success' if optimization_success else 'Failed'}")
        all_success = preprocessing_success and detection_success and filtering_success and training_success and optimization_success

        if all_success:
            print("\nPipeline completed successfully")

            print("\nGenerated files:")
            if self.preprocessed_data_path.exists():
                size_mb = self.preprocessed_data_path.stat().st_size / (1024 * 1024)
                print(f"  • {self.preprocessed_data_path.relative_to(self.script_dir)} ({size_mb:.2f} MB)")

            if self.flat_sensors_path.exists():
                size_kb = self.flat_sensors_path.stat().st_size / 1024
                print(f"  • {self.flat_sensors_path.relative_to(self.script_dir)} ({size_kb:.2f} KB)")

            if self.filtered_data_path.exists():
                size_mb = self.filtered_data_path.stat().st_size / (1024 * 1024)
                print(f"  • {self.filtered_data_path.relative_to(self.script_dir)} ({size_mb:.2f} MB)")

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
                    with open(metadata_path, 'r') as f:
                        metadata = json.load(f)
                    if 'model' in metadata and 'best_model' in metadata['model']:
                        best = metadata['model']['best_model']
                        print(f"    Best Model: {best.get('name', 'N/A')}")
                        print(f"    Score: {best.get('score_val', 'N/A')}")
        else:
            print("\nPipeline completed with errors")

    def run(self):
        """Run the complete pipeline."""
        self.print_header("SHELL DATA END-TO-END PIPELINE")
        print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        print("\nConfiguration:")
        print(f"  Raw data:        {self.raw_data_path.relative_to(self.script_dir)}")
        print(f"  Preprocessed:    {self.preprocessed_data_path.relative_to(self.script_dir)}")
        print(f"  Filtered data:   {self.filtered_data_path.relative_to(self.script_dir)}")
        print(f"  Flat sensors:    {self.flat_sensors_path.relative_to(self.script_dir)}")
        print(f"  Training output: {self.training_output_dir.relative_to(self.script_dir)}")
        print(f"  Outlier n_sigma: {self.n_sigma}")
        if self.sample_fraction:
            print(f"  Sample fraction: {self.sample_fraction*100:.1f}%")

        # Step 1: Preprocessing
        preprocessing_success = self.run_preprocessing()

        # Step 2: Flat Sensor Detection (only if preprocessing succeeds)
        if preprocessing_success:
            detection_success = self.run_flat_detection()
        else:
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
        if preprocessing_success and (detection_success or self.skip_flat_detection) and (filtering_success or self.skip_flat_filtering):
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

        # Summary
        self.print_summary(preprocessing_success, detection_success, filtering_success, training_success, optimization_success)

        print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        all_success = preprocessing_success and detection_success and filtering_success and training_success and optimization_success
        return 0 if all_success else 1


def main():
    parser = argparse.ArgumentParser(
        description="End-to-end pipeline for Shell sensor data preprocessing and training."
    )

    # Data paths
    parser.add_argument(
        "--raw-data",
        type=str,
        default="preprocessing/ShellData.csv",
        help="Path to raw data file (csv for now, might be refactored into parquet) (default: preprocessing/ShellData.csv)"
    )
    parser.add_argument(
        "--preprocessed-data",
        type=str,
        default="preprocessing/ShellData_preprocessed.parquet",
        help="Path for preprocessed data (relative to script directory, default: preprocessing/ShellData_preprocessed.parquet)"
    )
    parser.add_argument(
        "--filtered-data",
        type=str,
        default="preprocessing/ShellData_preprocessed_filtered.parquet",
        help="Path for filtered data (relative to script directory, default: preprocessing/ShellData_preprocessed_filtered.parquet)"
    )
    parser.add_argument(
        "--flat-sensors",
        type=str,
        default="preprocessing/flat_sensors.csv",
        help="Path for flat sensors list (relative to script directory, default: preprocessing/flat_sensors.csv)"
    )
    parser.add_argument(
        "--training-output",
        type=str,
        default="training/autogluon_results",
        help="Directory for training outputs (relative to script directory, default: training/autogluon_results)"
    )

    # Preprocessing options
    parser.add_argument(
        "--n-sigma",
        type=float,
        default=10.0,
        help="Number of MAD-based std deviations for outlier detection (default: 10.0)"
    )
    parser.add_argument(
        "--sample",
        type=float,
        default=None,
        help="Sample a fraction of data for memory efficiency (e.g., 0.1 for 10%%)"
    )

    # Flat sensor detection thresholds
    parser.add_argument(
        "--std-threshold",
        type=float,
        default=0.01,
        help="Maximum standard deviation for flat sensor (default: 0.01)"
    )
    parser.add_argument(
        "--unique-ratio-threshold",
        type=float,
        default=0.01,
        help="Maximum ratio of unique values for flat sensor (default: 0.01)"
    )
    parser.add_argument(
        "--cv-threshold",
        type=float,
        default=0.01,
        help="Maximum coefficient of variation for flat sensor (default: 0.01)"
    )
    parser.add_argument(
        "--range-threshold",
        type=float,
        default=0.01,
        help="Maximum value range for flat sensor (default: 0.01)"
    )
    parser.add_argument(
        "--most-common-threshold",
        type=float,
        default=95.0,
        help="Minimum percentage of most common value for flat sensor (default: 95.0)"
    )
    parser.add_argument(
        "--zero-diff-threshold",
        type=float,
        default=95.0,
        help="Minimum percentage of zero differences for flat sensor (default: 95.0)"
    )

    # Skip options
    parser.add_argument(
        "--skip-preprocessing",
        action="store_true",
        help="Skip preprocessing step (use existing preprocessed data)"
    )
    parser.add_argument(
        "--skip-flat-detection",
        action="store_true",
        help="Skip flat sensor detection step (use existing flat sensors list)"
    )
    parser.add_argument(
        "--skip-flat-filtering",
        action="store_true",
        help="Skip flat sensor filtering step (use existing filtered data)"
    )
    parser.add_argument(
        "--skip-training",
        action="store_true",
        help="Skip training step (use existing model)"
    )

    args = parser.parse_args()

    pipeline = ShellDataPipeline(
        raw_data_path=args.raw_data,
        preprocessed_data_path=args.preprocessed_data,
        filtered_data_path=args.filtered_data,
        flat_sensors_path=args.flat_sensors,
        training_output_dir=args.training_output,
        n_sigma=args.n_sigma,
        sample_fraction=args.sample,
        skip_preprocessing=args.skip_preprocessing,
        skip_flat_detection=args.skip_flat_detection,
        skip_flat_filtering=args.skip_flat_filtering,
        skip_training=args.skip_training,
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
