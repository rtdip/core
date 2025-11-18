"""
USAGE:
    python create_csv.py
Note: Make sure to set the AZURE_URL environment variable (.env) with your Azure Blob Storage connection string.
"""

from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import BytesIO

from dotenv import load_dotenv
import os

OUTPUT_CSV_FILE = "ShellData.csv"


def load_shell_data():

    load_dotenv()

    connection_string = os.getenv("AZURE_URL")

    print(connection_string)

    container_name = "rtimedata"

    bsc = BlobServiceClient.from_connection_string(connection_string)
    cc = bsc.get_container_client(container_name)

    dfs = []
    ok, fail = 0, 0

    for blob in cc.list_blobs(name_starts_with="Data/2024/"):
        print("Current blob:", blob.name)
        name = blob.name
        if "parquet" in name.lower():
            try:
                data = cc.get_blob_client(name).download_blob().readall()
                df = pd.read_parquet(BytesIO(data))
                dfs.append(df)
                ok += 1
            except Exception as e:
                print(f"Überspringe {name}: {e}")
                fail += 1

    print(f"Read: {ok}, Failed: {fail}")

    all_data = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    all_data.to_csv(OUTPUT_CSV_FILE, index=False)

    print(f"Saved combined data to {OUTPUT_CSV_FILE}")


def main():
    load_shell_data()


if __name__ == "__main__":
    exit(main())
