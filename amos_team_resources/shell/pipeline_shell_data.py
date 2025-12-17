"""
Shell Data End-to-End Pipeline

This script orchestrates the complete pipeline for Shell sensor data:
1. Preprocessing: Load raw data and apply RTDIP transformations
2. Training: Train AutoGluon forecasting models

Each step can be skipped if already completed (checkpointing).

Usage (CSV needed for now, want to refactor to Parquet later):
    # Run full pipeline
    python pipeline_shell_data.py

    # Skip preprocessing if already done
    python pipeline_shell_data.py --skip-preprocessing

    # Skip training if already done
    python pipeline_shell_data.py --skip-training

    # Custom paths
    python pipeline_shell_data.py --raw-data ShellData.csv --preprocessed-data output.parquet
    
    # Run pipeline on a sample of the data 
    python pipeline_shell_data.py --sample 0.1
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
        training_output_dir: str = "training/autogluon_results",
        n_sigma: float = 10.0,
        sample_fraction: float = None,
        skip_preprocessing: bool = False,
        skip_training: bool = False,
    ):
        self.script_dir = Path(__file__).parent
        self.raw_data_path = self.script_dir / raw_data_path
        self.preprocessed_data_path = self.script_dir / preprocessed_data_path
        self.training_output_dir = self.script_dir / training_output_dir
        self.n_sigma = n_sigma
        self.sample_fraction = sample_fraction
        self.skip_preprocessing = skip_preprocessing
        self.skip_training = skip_training

        self.preprocess_script = self.script_dir / "preprocessing" / "preprocess_shell_data.py"
        self.training_script = self.script_dir / "training" / "train_autogluon_shell.py"

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

    def run_training(self):
        """Run training step."""
        self.print_step(2, "TRAINING")

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

        # Check if preprocessed data exists
        if not self.check_file_exists(self.preprocessed_data_path):
            print(f"Error: Preprocessed data not found at {self.preprocessed_data_path}")
            print("Run preprocessing first or provide correct path")
            return False

        # Check if training script exists
        if not self.training_script.exists():
            print(f"Error: Training script not found at {self.training_script}")
            return False

        # Update training script to use the preprocessed data path
        print(f"\nRunning training script")
        print(f"Input: {self.preprocessed_data_path}")
        print(f"Output: {self.training_output_dir}")

        # Change to training directory and run script
        original_cwd = os.getcwd()
        training_dir = self.training_script.parent

        try:
            os.chdir(training_dir)

            env = os.environ.copy()
            env['SHELL_DATA_PATH'] = str(self.preprocessed_data_path)

            cmd = [sys.executable, str(self.training_script)]

            result = subprocess.run(cmd, check=True, capture_output=False, env=env)
            print("\nTraining completed successfully")
            return True
        except subprocess.CalledProcessError as e:
            print(f"\nTraining failed with exit code {e.returncode}")
            return False
        finally:
            os.chdir(original_cwd)

    def print_summary(self, preprocessing_success: bool, training_success: bool):
        """Print pipeline summary."""
        self.print_header("PIPELINE SUMMARY")

        print("\nSteps completed:")
        print(f"1. Preprocessing: {'Success' if preprocessing_success else 'Failed'}")
        print(f"2. Training:      {'Success' if training_success else 'Failed'}")

        if preprocessing_success and training_success:
            print("\nPipeline completed successfully")

            print("\nGenerated files:")
            if self.preprocessed_data_path.exists():
                size_mb = self.preprocessed_data_path.stat().st_size / (1024 * 1024)
                print(f"  • {self.preprocessed_data_path.relative_to(self.script_dir)} ({size_mb:.2f} MB)")

            if self.training_output_dir.exists():
                print(f"  • {self.training_output_dir.relative_to(self.script_dir)}/")
                for item in self.training_output_dir.iterdir():
                    if item.is_file():
                        size_mb = item.stat().st_size / (1024 * 1024)
                        print(f"    - {item.name} ({size_mb:.2f} MB)")
        else:
            print("\nPipeline completed with errors")

    def run(self):
        """Run the complete pipeline."""
        self.print_header("SHELL DATA END-TO-END PIPELINE")
        print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        print("\nConfiguration:")
        print(f"  Raw data:        {self.raw_data_path.relative_to(self.script_dir)}")
        print(f"  Preprocessed:    {self.preprocessed_data_path.relative_to(self.script_dir)}")
        print(f"  Training output: {self.training_output_dir.relative_to(self.script_dir)}")
        print(f"  Outlier n_sigma: {self.n_sigma}")
        if self.sample_fraction:
            print(f"  Sample fraction: {self.sample_fraction*100:.1f}%")

        # Step 1: Preprocessing
        preprocessing_success = self.run_preprocessing()

        # Step 2: Training (only if preprocessing succeeds)
        if preprocessing_success:
            training_success = self.run_training()
        else:
            print("\nSkipping training due to preprocessing failure")
            training_success = False

        # Summary
        self.print_summary(preprocessing_success, training_success)

        print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        return 0 if (preprocessing_success and training_success) else 1


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

    # Skip options
    parser.add_argument(
        "--skip-preprocessing",
        action="store_true",
        help="Skip preprocessing step (use existing preprocessed data)"
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
        training_output_dir=args.training_output,
        n_sigma=args.n_sigma,
        sample_fraction=args.sample,
        skip_preprocessing=args.skip_preprocessing,
        skip_training=args.skip_training,
    )

    return pipeline.run()


if __name__ == "__main__":
    sys.exit(main())
