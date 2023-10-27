# Copyright 2022 RTDIP
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

from typing import Literal
import polars as pl
from polars import LazyFrame
from ..interfaces import DestinationInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package


class PythonDeltaDestination(DestinationInterface):
    """
    The Python Delta Destination is used to write data to a Delta table from a Polars LazyFrame.

     Example
    --------
    === "Azure"

        ```python
        from rtdip_sdk.pipelines.destinations import PythonDeltaDestination

        path = "abfss://{FILE-SYSTEM}@{ACCOUNT-NAME}.dfs.core.windows.net/{PATH}/{FILE-NAME}

        python_delta_destination = PythonDeltaDestination(
            data=LazyFrame
            path=path,
            storage_options={
                "azure_storage_account_name": "{AZURE-STORAGE-ACCOUNT-NAME}",
                "azure_storage_account_key": "{AZURE-STORAGE-ACCOUNT-KEY}"
            },
            mode=:error",
            overwrite_schema=False,
            delta_write_options=None
        )

        python_delta_destination.read_batch()

        ```
    === "AWS"

        ```python
        from rtdip_sdk.pipelines.destinations import PythonDeltaDestination

        path = "https://s3.{REGION-CODE}.amazonaws.com/{BUCKET-NAME}/{KEY-NAME}"

        python_delta_destination = PythonDeltaDestination(
            data=LazyFrame
            path=path,
            options={
                "aws_access_key_id": "{AWS-ACCESS-KEY-ID}",
                "aws_secret_access_key": "{AWS-SECRET-ACCESS-KEY}"
            },
            mode=:error",
            overwrite_schema=False,
            delta_write_options=None
        )

        python_delta_destination.read_batch()
        ```

    Parameters:
        data (LazyFrame): Polars LazyFrame to be written to Delta
        path (str): Path to Delta table to be written to; either local or [remote](https://delta-io.github.io/delta-rs/python/usage.html#loading-a-delta-table){ target="_blank" }. **Locally** if the Table does't exist one will be created, but to write to AWS or Azure, you must have an existing Delta Table
        options (Optional dict): Used if writing to a remote location. For AWS use format {"aws_access_key_id": "<>", "aws_secret_access_key": "<>"}. For Azure use format {"azure_storage_account_name": "storageaccountname", "azure_storage_access_key": "<>"}
        mode (Literal['error', 'append', 'overwrite', 'ignore']): Defaults to error if table exists, 'ignore' won't write anything if table exists
        overwrite_schema (bool): If True will allow for the table schema to be overwritten
        delta_write_options (dict): Options when writing to a Delta table. See [here](https://delta-io.github.io/delta-rs/python/api_reference.html#writing-deltatables){ target="_blank" } for all options
    """

    data: LazyFrame
    path: str
    options: dict
    mode: Literal["error", "append", "overwrite", "ignore"]
    overwrite_schema: bool
    delta_write_options: dict

    def __init__(
        self,
        data: LazyFrame,
        path: str,
        options: dict = None,
        mode: Literal["error", "append", "overwrite", "ignore"] = "error",
        overwrite_schema: bool = False,
        delta_write_options: dict = None,
    ) -> None:
        self.data = data
        self.path = path
        self.options = options
        self.mode = mode
        self.overwrite_schema = overwrite_schema
        self.delta_write_options = delta_write_options

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

    def pre_write_validation(self):
        return True

    def post_write_validation(self):
        return True

    def write_batch(self):
        """
        Writes batch data to Delta without using Spark.
        """
        if isinstance(self.data, pl.LazyFrame):
            df = self.data.collect()
            df.write_delta(
                self.path,
                mode=self.mode,
                overwrite_schema=self.overwrite_schema,
                storage_options=self.options,
                delta_write_options=self.delta_write_options,
            )
        else:
            raise ValueError(
                "Data must be a Polars LazyFrame. See https://pola-rs.github.io/polars/py-polars/html/reference/lazyframe/index.html"
            )

    def write_stream(self):
        """
        Raises:
            NotImplementedError: Writing to a Delta table using Python is only possible for batch writes. To perform a streaming read, use the write_stream method of the SparkDeltaDestination component.
        """
        raise NotImplementedError(
            "Writing to a Delta table using Python is only possible for batch writes. To perform a streaming read, use the write_stream method of the SparkDeltaDestination component"
        )
