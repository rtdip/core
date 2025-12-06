"""
Shell Data Ingestion Script

Downloads Shell sensor data from Azure Blob Storage and saves locally as CSV/Parquet.

Usage:
    export AZURE_ACCOUNT_URL="https://azassaexdseq00039ewduni.blob.core.windows.net"
    export AZURE_CONTAINER_NAME="rtimedata"
    export AZURE_SAS_TOKEN="?sv=2020-10-02&ss=btqf&srt=sco&st=..."

    python ingest_shell_data.py --output-format parquet
    python ingest_shell_data.py --output-format csv
    python ingest_shell_data.py --output-format both

"""

import argparse
import os
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src" / "sdk" / "python"))
from rtdip_sdk.pipelines.sources import PythonAzureBlobSource

DEFAULT_OUTPUT_DIR = Path(__file__).parent / ".." / "data"


def print_header(text: str):
    """Print formatted header."""
    print("\n" + "=" * 80)
    print(text)
    print("=" * 80)


def print_step(step: str):
    """Print formatted step."""
    print(f"\n{step}")
    print("-" * 80)


def get_config_value(arg_value, env_var_name, config_name):
    """Get configuration value from args or environment variable."""
    if arg_value:
        print(f"Using {config_name} from command-line argument")
        return arg_value

    env_value = os.environ.get(env_var_name)
    if env_value:
        print(f"Using {config_name} from {env_var_name} environment variable")
        return env_value

    return None


def get_credentials(args) -> str:
    """Get Azure credentials from args or environment variables."""
    if args.sas_token:
        print("Using SAS token from command-line argument")
        return args.sas_token
    if args.account_key:
        print("Using account key from command-line argument")
        return args.account_key

    sas_token = os.environ.get("AZURE_SAS_TOKEN")
    if sas_token:
        print("Using SAS token from AZURE_SAS_TOKEN environment variable")
        return sas_token

    account_key = os.environ.get("AZURE_ACCOUNT_KEY")
    if account_key:
        print("Using account key from AZURE_ACCOUNT_KEY environment variable")
        return account_key

    raise ValueError(
        "No Azure credentials provided. Use --sas-token/--account-key or set "
        "AZURE_SAS_TOKEN/AZURE_ACCOUNT_KEY environment variable"
    )


def download_data(account_url: str, container_name: str, credential: str):
    """Download data from Azure Blob Storage."""
    print_step("Downloading data from Azure Blob Storage")
    print(f"Account URL: {account_url}")
    print(f"Container: {container_name}")

    source = PythonAzureBlobSource(
        account_url=account_url,
        container_name=container_name,
        credential=credential,
        file_pattern="*.parquet",
        combine_blobs=True,
        eager=True,
    )

    print("\nReading data...")
    df = source.read_batch()

    print(f"Shape: {df.shape}")
    print(f"Columns: {df.columns}")

    return df


def save_data(df, output_format: str, output_dir: Path):
    """Save DataFrame to specified format(s)."""
    print_step("Saving data")
    output_dir.mkdir(parents=True, exist_ok=True)

    formats = []
    if output_format in ["parquet", "both"]:
        formats.append("parquet")
    if output_format in ["csv", "both"]:
        formats.append("csv")

    saved_files = []

    for fmt in formats:
        output_path = output_dir / f"ShellData.{fmt}"
        print(f"\nSaving to {output_path}")

        if fmt == "parquet":
            if hasattr(df, "write_parquet"):
                df.write_parquet(output_path)
            else:
                df.to_parquet(output_path, index=False)
        elif fmt == "csv":
            if hasattr(df, "write_csv"):
                df.write_csv(output_path)
            else:
                df.to_csv(output_path, index=False)

        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"Saved {file_size_mb:.2f} MB")
        saved_files.append(output_path)

    return saved_files


def print_summary(saved_files: list, total_time: float):
    """Print ingestion summary."""
    print_header("INGESTION SUMMARY")

    print("\nFiles created:")
    for file_path in saved_files:
        size_mb = file_path.stat().st_size / (1024 * 1024)
        print(
            f"  • {file_path.relative_to(file_path.parent.parent)} ({size_mb:.2f} MB)"
        )

    print(f"\nTotal time: {total_time:.2f} seconds")
    print("\nData is now ready for preprocessing!")
    print(f"Location: {saved_files[0].parent}")


def main():
    parser = argparse.ArgumentParser(
        description="Ingest Shell sensor data from Azure Blob Storage using RTDIP SDK"
    )

    # Azure configuration (can be provided via CLI or environment variables)
    parser.add_argument(
        "--account-url",
        type=str,
        help="Azure Storage account URL (or use AZURE_ACCOUNT_URL env var)",
    )
    parser.add_argument(
        "--container-name",
        type=str,
        help="Azure Blob container name (or use AZURE_CONTAINER_NAME env var)",
    )

    # Credentials
    parser.add_argument(
        "--sas-token",
        type=str,
        help="Azure SAS token for authentication (should start with '?')",
    )
    parser.add_argument(
        "--account-key", type=str, help="Azure Storage account key for authentication"
    )

    # Output configuration
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory for downloaded data (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--output-format",
        type=str,
        choices=["parquet", "csv", "both"],
        default="both",
        help="Output format: parquet, csv, or both (default: both)",
    )

    args = parser.parse_args()

    # Start ingestion
    print_header("SHELL DATA INGESTION")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    start_time = datetime.now()

    try:
        # Get configuration from args or environment variables
        account_url = get_config_value(
            args.account_url, "AZURE_ACCOUNT_URL", "account URL"
        )
        container_name = get_config_value(
            args.container_name, "AZURE_CONTAINER_NAME", "container name"
        )
        credential = get_credentials(args)

        # Validate required parameters
        if not account_url:
            raise ValueError(
                "Azure account URL is required. Provide via --account-url or AZURE_ACCOUNT_URL environment variable.\n"
                "Example: export AZURE_ACCOUNT_URL='https://azassaexdseq00039ewduni.blob.core.windows.net'"
            )
        if not container_name:
            raise ValueError(
                "Azure container name is required. Provide via --container-name or AZURE_CONTAINER_NAME environment variable.\n"
                "Example: export AZURE_CONTAINER_NAME='rtimedata'"
            )

        # Download data
        df = download_data(
            account_url=account_url,
            container_name=container_name,
            credential=credential,
        )

        # Save to file(s)
        saved_files = save_data(df, args.output_format, args.output_dir)

        # Summary
        total_time = (datetime.now() - start_time).total_seconds()
        print_summary(saved_files, total_time)

        return 0

    except Exception as e:
        print(f"\nERROR: Ingestion failed with error: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
