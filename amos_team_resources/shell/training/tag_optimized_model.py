"""
Tag and prepare the best-performing model for deployment.

1. Identifies the best model from AutoGluon training results
2. Extracts model performance metrics
3. Creates a deployment-ready directory structure
4. Tags the model as 'optimized' with metadata
5. Prepares model for production deployment
"""

import json
import shutil
from pathlib import Path
from datetime import datetime
import pandas as pd


def find_autogluon_model_dir(training_output_dir):
    """
    Find the AutoGluon model directory from training output.

    Args:
        training_output_dir: Path to training output directory

    Returns:
        Path to AutoGluon model directory or None if not found
    """
    training_path = Path(training_output_dir)

    # Try multiple possible locations for AutogluonModels
    search_paths = [
        training_path / "AutogluonModels",  # Inside training_output_dir
        training_path.parent / "AutogluonModels",  # Sibling to training_output_dir
    ]

    for autogluon_models in search_paths:
        if autogluon_models.exists() and autogluon_models.is_dir():
            # Find valid model directories (must contain predictor.pkl or leaderboard.csv)
            valid_model_dirs = []
            for d in autogluon_models.iterdir():
                if d.is_dir():
                    # Check for essential AutoGluon files
                    has_predictor = (d / "predictor.pkl").exists()
                    has_leaderboard = (d / "leaderboard.csv").exists()

                    if has_predictor or has_leaderboard:
                        valid_model_dirs.append(d)

            if valid_model_dirs:
                # Sort by modification time, most recent first
                valid_model_dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                print(f"Found AutoGluon models at: {autogluon_models}")
                print(f"Selected model: {valid_model_dirs[0].name}")
                return valid_model_dirs[0]

    return None


def extract_model_metadata(model_dir, training_output_dir=None):
    """
    Extract metadata from the AutoGluon model directory.

    Args:
        model_dir: Path to AutoGluon model directory
        training_output_dir: Optional path to training output directory (for leaderboard)

    Returns:
        Dictionary with model metadata
    """
    metadata = {
        'model_dir': str(model_dir),
        'model_name': model_dir.name,
        'training_date': datetime.fromtimestamp(model_dir.stat().st_mtime).isoformat()
    }

    # Try multiple locations for leaderboard.csv
    leaderboard_paths = [
        model_dir / "leaderboard.csv",  # Inside model directory
    ]

    # Also check training output directory if provided
    if training_output_dir:
        leaderboard_paths.append(Path(training_output_dir) / "leaderboard.csv")

    leaderboard_path = None
    for path in leaderboard_paths:
        if path.exists():
            leaderboard_path = path
            break

    if leaderboard_path:
        leaderboard = pd.read_csv(leaderboard_path)

        if len(leaderboard) > 0:
            best_model = leaderboard.iloc[0]
            metadata['best_model'] = {
                'name': best_model['model'],
                'score_val': float(best_model['score_val']),
                'pred_time_val': float(best_model.get('pred_time_val', 0)),
                'fit_time': float(best_model.get('fit_time', 0))
            }

            for col in leaderboard.columns:
                if col not in ['model', 'score_val', 'pred_time_val', 'fit_time']:
                    try:
                        metadata['best_model'][col] = float(best_model[col])
                    except (ValueError, TypeError):
                        metadata['best_model'][col] = str(best_model[col])

    predictor_info_path = model_dir / "predictor_info.json"
    if predictor_info_path.exists():
        try:
            with open(predictor_info_path, 'r') as f:
                predictor_info = json.load(f)
                metadata['predictor_info'] = {
                    'eval_metric': predictor_info.get('eval_metric'),
                    'problem_type': predictor_info.get('problem_type'),
                    'num_models_trained': len(predictor_info.get('model_info', {}))
                }
        except Exception as e:
            print(f"Warning: Could not read predictor info: {e}")

    return metadata


def create_deployment_package(model_dir, output_dir, metadata):
    """
    Create a deployment package with the optimized model.

    Args:
        model_dir: Path to AutoGluon model directory
        output_dir: Path to output directory for deployment package
        metadata: Model metadata dictionary

    Returns:
        Path to deployment package directory
    """
    deployment_dir = Path(output_dir) / "optimized_model"

    deployment_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nCreating deployment package at: {deployment_dir}")
    model_link = deployment_dir / "model"

    if model_link.exists():
        if model_link.is_symlink():
            model_link.unlink()
        else:
            shutil.rmtree(model_link)

    try:
        model_link.symlink_to(model_dir.resolve(), target_is_directory=True)
        print(f"  Created symbolic link to model directory")
    except (OSError, NotImplementedError):
        print(f"  Copying model directory (symbolic links not supported)")
        shutil.copytree(model_dir, model_link)
        print(f"  Model directory copied")
    return deployment_dir


def create_deployment_metadata(deployment_dir, metadata, filtered_data_info=None):
    """
    Create deployment metadata file.

    Args:
        deployment_dir: Path to deployment directory
        metadata: Model metadata dictionary
        filtered_data_info: Optional info about filtered data used for training

    Returns:
        Path to metadata file
    """
    deployment_metadata = {
        'version': '1.0',
        'tag': 'optimized',
        'deployment_ready': True,
        'created_at': datetime.now().isoformat(),
        'model': metadata,
        'deployment_info': {
            'environment': 'production',
            'status': 'ready',
            'validation_required': False
        }
    }

    if filtered_data_info:
        deployment_metadata['training_data'] = filtered_data_info

    metadata_path = deployment_dir / "deployment_metadata.json"
    with open(metadata_path, 'w') as f:
        json.dump(deployment_metadata, f, indent=2)

    print(f"  Created deployment metadata: {metadata_path.name}")

    return metadata_path


def create_model_card(deployment_dir, metadata):
    """
    Create a model card (README) for the deployment package.
    Args:
        deployment_dir: Path to deployment directory
        metadata: Model metadata dictionary

    Returns:
        Path to model card file
    """
    model_card_content = f"""# Optimized Model for Deployment

**Tag:** `optimized`
**Status:** Ready for deployment
**Created:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Model Information

- **Model Name:** {metadata.get('model_name', 'N/A')}
- **Training Date:** {metadata.get('training_date', 'N/A')}
- **Model Directory:** {metadata.get('model_dir', 'N/A')}

## Best Performing Model

"""
    if 'best_model' in metadata:
        best = metadata['best_model']
        model_card_content += f"""- **Model:** {best.get('name', 'N/A')}
- **Validation Score:** {best.get('score_val', 'N/A')}
- **Prediction Time:** {best.get('pred_time_val', 'N/A')}s
- **Training Time:** {best.get('fit_time', 'N/A')}s

### Performance Metrics

"""
        for key, value in best.items():
            if key not in ['name', 'score_val', 'pred_time_val', 'fit_time']:
                model_card_content += f"- **{key}:** {value}\n"

    model_card_content += f"""
## Deployment Instructions

1. **Load the model:**
   ```python
   from autogluon.timeseries import TimeSeriesPredictor

   predictor = TimeSeriesPredictor.load('optimized_model/model')
   ```

2. **Make predictions:**
   ```python
   predictions = predictor.predict(test_data)
   ```

3. **Model location:** `{deployment_dir / 'model'}`

## Files

- `model/` - AutoGluon model directory (symbolic link)
- `deployment_metadata.json` - Deployment metadata and configuration
- `MODEL_CARD.md` - This file

## Notes

- This model has been tagged as 'optimized' and is ready for production deployment
- Ensure all dependencies are installed before loading the model
- Review the deployment metadata for additional configuration details
"""

    model_card_path = deployment_dir / "MODEL_CARD.md"
    with open(model_card_path, 'w') as f:
        f.write(model_card_content)

    print(f"  Created model card: {model_card_path.name}")

    return model_card_path


def tag_optimized_model(training_output_dir, filtered_data_path=None):
    """
    Tag the best-performing model as optimized and prepare for deployment.

    Args:
        training_output_dir: Path to training output directory
        filtered_data_path: Optional path to filtered data used for training

    Returns:
        Path to deployment package or None if failed
    """
    print("TAGGING OPTIMIZED MODEL FOR DEPLOYMENT")
    print(f"\nSearching for trained model in: {training_output_dir}")
    model_dir = find_autogluon_model_dir(training_output_dir)
    if not model_dir:
        print("Error: No AutoGluon model directory found")
        return None

    print(f"Found model directory: {model_dir.name}")

    print("\nExtracting model metadata")
    metadata = extract_model_metadata(model_dir, training_output_dir)

    if 'best_model' in metadata:
        best = metadata['best_model']
        print(f"\nBest performing model:")
        print(f"  Model: {best['name']}")
        print(f"  Score: {best['score_val']}")
        print(f"  Prediction time: {best.get('pred_time_val', 'N/A')}s")
    filtered_data_info = None
    if filtered_data_path and Path(filtered_data_path).exists():
        filtered_metadata_path = Path(filtered_data_path).with_name(
            Path(filtered_data_path).stem + '_metadata.json'
        )
        if filtered_metadata_path.exists():
            with open(filtered_metadata_path, 'r') as f:
                filtered_data_info = json.load(f)
            print(f"\nTraining data information:")
            print(f"  Data source: {filtered_data_path}")
            print(f"  Sensors: {filtered_data_info['dataset_info']['unique_sensors']}")
            print(f"  Flat sensors removed: {filtered_data_info['filtering_info']['flat_sensors_removed_count']}")
            
    deployment_dir = create_deployment_package(model_dir, training_output_dir, metadata)
    create_deployment_metadata(deployment_dir, metadata, filtered_data_info)
    create_model_card(deployment_dir, metadata)

    print("MODEL TAGGING COMPLETE")
    print(f"\nDeployment package created at: {deployment_dir}")
    print(f"Tag: optimized")
    print(f"Status: Ready for deployment")

    return deployment_dir

def main():
    """Main execution function."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Tag best-performing model as optimized for deployment"
    )
    parser.add_argument(
        "--training-output",
        type=str,
        default="../training",
        help="Path to training output directory"
    )
    parser.add_argument(
        "--filtered-data",
        type=str,
        default="../preprocessing/ShellData_preprocessed_filtered.parquet",
        help="Path to filtered data used for training"
    )

    args = parser.parse_args()

    deployment_dir = tag_optimized_model(args.training_output, args.filtered_data)

    if deployment_dir:
        return 0
    else:
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
