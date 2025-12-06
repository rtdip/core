# Copyright 2025 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from io import BytesIO
from typing import Optional, List, Union
import polars as pl
from polars import LazyFrame, DataFrame

from ..interfaces import SourceInterface
from ..._pipeline_utils.models import Libraries, SystemType


class PythonAzureBlobSource(SourceInterface):
    """
    The Python Azure Blob Storage Source is used to read parquet files from Azure Blob Storage without using Apache Spark, returning a Polars LazyFrame.

    Example
    --------
    === "SAS Token Authentication"

        ```python
        from rtdip_sdk.pipelines.sources import PythonAzureBlobSource

        azure_blob_source = PythonAzureBlobSource(
            account_url="https://{ACCOUNT-NAME}.blob.core.windows.net",
            container_name="rtimedata",
            credential="?sv=2020-10-02&ss=btqf...",  # SAS token
            file_pattern="*.parquet",
            combine_blobs=True
        )

        azure_blob_source.read_batch()
        ```

    === "Account Key Authentication"

        ```python
        from rtdip_sdk.pipelines.sources import PythonAzureBlobSource

        azure_blob_source = PythonAzureBlobSource(
            account_url="https://{ACCOUNT-NAME}.blob.core.windows.net",
            container_name="rtimedata",
            credential="{ACCOUNT-KEY}",
            file_pattern="*.parquet",
            combine_blobs=True
        )

        azure_blob_source.read_batch()
        ```

    === "Specific Blob Names"

        ```python
        from rtdip_sdk.pipelines.sources import PythonAzureBlobSource

        azure_blob_source = PythonAzureBlobSource(
            account_url="https://{ACCOUNT-NAME}.blob.core.windows.net",
            container_name="rtimedata",
            credential="{SAS-TOKEN-OR-KEY}",
            blob_names=["data_2024_01.parquet", "data_2024_02.parquet"],
            combine_blobs=True
        )

        azure_blob_source.read_batch()
        ```

    Parameters:
        account_url (str): Azure Storage account URL (e.g., "https://{account-name}.blob.core.windows.net")
        container_name (str): Name of the blob container
        credential (str): SAS token (with leading '?') or account key for authentication
        blob_names (optional List[str]): List of specific blob names to read. If provided, file_pattern is ignored
        file_pattern (optional str): Pattern to match blob names (e.g., "*.parquet", "data/*.parquet"). Defaults to "*.parquet"
        combine_blobs (optional bool): If True, combines all matching blobs into a single LazyFrame. If False, returns list of LazyFrames. Defaults to True
        eager (optional bool): If True, returns eager DataFrame instead of LazyFrame. Defaults to False

    !!! note "Note"
        - Requires `azure-storage-blob` package
        - Currently only supports parquet files
        - SAS token should include the leading '?' character
        - When combine_blobs=False, returns a list of LazyFrames instead of a single LazyFrame
    """

    account_url: str
    container_name: str
    credential: str
    blob_names: Optional[List[str]]
    file_pattern: str
    combine_blobs: bool
    eager: bool

    def __init__(
        self,
        account_url: str,
        container_name: str,
        credential: str,
        blob_names: Optional[List[str]] = None,
        file_pattern: str = "*.parquet",
        combine_blobs: bool = True,
        eager: bool = False,
    ):
        self.account_url = account_url
        self.container_name = container_name
        self.credential = credential
        self.blob_names = blob_names
        self.file_pattern = file_pattern
        self.combine_blobs = combine_blobs
        self.eager = eager

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYTHON
        """
        return SystemType.PYTHON

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def pre_read_validation(self):
        return True

    def post_read_validation(self):
        return True

    def _get_blob_list(self, container_client) -> List[str]:
        """Get list of blobs to read based on blob_names or file_pattern."""
        if self.blob_names:
            return self.blob_names
        else:
            import fnmatch

            all_blobs = container_client.list_blobs()
            matching_blobs = []

            for blob in all_blobs:
                # Match pattern directly using fnmatch
                if fnmatch.fnmatch(blob.name, self.file_pattern):
                    matching_blobs.append(blob.name)
                # Handle patterns like "*.parquet" - check if pattern keyword appears in filename
                elif self.file_pattern.startswith("*"):
                    pattern_keyword = self.file_pattern[1:].lstrip(".")
                    if pattern_keyword and pattern_keyword.lower() in blob.name.lower():
                        matching_blobs.append(blob.name)

            return matching_blobs

    def _read_blob_to_polars(
        self, container_client, blob_name: str
    ) -> Union[LazyFrame, DataFrame]:
        """Read a single blob into a Polars LazyFrame or DataFrame."""
        try:
            blob_client = container_client.get_blob_client(blob_name)
            logging.info(f"Reading blob: {blob_name}")

            # Download blob data
            stream = blob_client.download_blob()
            data = stream.readall()

            # Read into Polars
            if self.eager:
                df = pl.read_parquet(BytesIO(data))
            else:
                # For lazy reading, we need to read eagerly first, then convert to lazy
                # This is a limitation of reading from in-memory bytes
                df = pl.read_parquet(BytesIO(data)).lazy()

            return df

        except Exception as e:
            logging.error(f"Failed to read blob {blob_name}: {e}")
            raise e

    def read_batch(
        self,
    ) -> Union[LazyFrame, DataFrame, List[Union[LazyFrame, DataFrame]]]:
        """
        Reads parquet files from Azure Blob Storage into Polars LazyFrame(s).

        Returns:
            Union[LazyFrame, DataFrame, List]: Single LazyFrame/DataFrame if combine_blobs=True,
                                               otherwise list of LazyFrame/DataFrame objects
        """
        try:
            from azure.storage.blob import BlobServiceClient

            # Create blob service client
            blob_service_client = BlobServiceClient(
                account_url=self.account_url, credential=self.credential
            )
            container_client = blob_service_client.get_container_client(
                self.container_name
            )

            # Get list of blobs to read
            blob_list = self._get_blob_list(container_client)

            if not blob_list:
                raise ValueError(
                    f"No blobs found matching pattern '{self.file_pattern}' in container '{self.container_name}'"
                )

            logging.info(
                f"Found {len(blob_list)} blob(s) to read from container '{self.container_name}'"
            )

            # Read all blobs
            dataframes = []
            for blob_name in blob_list:
                df = self._read_blob_to_polars(container_client, blob_name)
                dataframes.append(df)

            # Combine or return list
            if self.combine_blobs:
                if len(dataframes) == 1:
                    return dataframes[0]
                else:
                    # Concatenate all dataframes
                    logging.info(f"Combining {len(dataframes)} dataframes")
                    if self.eager:
                        combined = pl.concat(dataframes, how="vertical_relaxed")
                    else:
                        combined = pl.concat(dataframes, how="vertical_relaxed")
                    return combined
            else:
                return dataframes

        except Exception as e:
            logging.exception(str(e))
            raise e

    def read_stream(self):
        """
        Raises:
            NotImplementedError: Reading from Azure Blob Storage using Python is only possible for batch reads. To perform a streaming read, use a Spark-based source component.
        """
        raise NotImplementedError(
            "Reading from Azure Blob Storage using Python is only possible for batch reads. To perform a streaming read, use a Spark-based source component"
        )
