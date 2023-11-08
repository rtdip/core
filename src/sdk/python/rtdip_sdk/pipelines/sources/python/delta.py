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

from ..interfaces import SourceInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package
import polars as pl
from polars import LazyFrame


class PythonDeltaSource(SourceInterface):
    """
    The Python Delta Source is used to read data from a Delta table without using Apache Spark, returning a Polars LazyFrame.

     Example
    --------
    === "Azure"

        ```python
        from rtdip_sdk.pipelines.sources import PythonDeltaSource

        path = "abfss://{FILE-SYSTEM}@{ACCOUNT-NAME}.dfs.core.windows.net/{PATH}/{FILE-NAME}

        python_delta_source = PythonDeltaSource(
            path=path,
            version=None,
            storage_options={
                "azure_storage_account_name": "{AZURE-STORAGE-ACCOUNT-NAME}",
                "azure_storage_account_key": "{AZURE-STORAGE-ACCOUNT-KEY}"
            },
            pyarrow_options=None,
            without_files=False
        )

        python_delta_source.read_batch()
        ```
    === "AWS"

        ```python
        from rtdip_sdk.pipelines.sources import PythonDeltaSource

        path = "https://s3.{REGION-CODE}.amazonaws.com/{BUCKET-NAME}/{KEY-NAME}"

        python_delta_source = PythonDeltaSource(
            path=path,
            version=None,
            storage_options={
                "aws_access_key_id": "{AWS-ACCESS-KEY-ID}",
                "aws_secret_access_key": "{AWS-SECRET-ACCESS-KEY}"
            },
            pyarrow_options=None,
            without_files=False
        )

        python_delta_source.read_batch()
        ```

    Parameters:
        path (str): Path to the Delta table. Can be local or in S3/Azure storage
        version (optional int): Specify the Delta table version to read from. Defaults to the latest version
        storage_options (optional dict): Used to read from AWS/Azure storage. For AWS use format {"aws_access_key_id": "<>", "aws_secret_access_key":"<>"}. For Azure use format {"azure_storage_account_name": "<>", "azure_storage_account_key": "<>"}.
        pyarrow_options (optional dict): Data Access and Efficiency options when reading from Delta. See [to_pyarrow_dataset](https://delta-io.github.io/delta-rs/python/api_reference.html#deltalake.table.DeltaTable.to_pyarrow_dataset){ target="_blank" }.
        without_files (optional bool): If True loads the table without tracking files
    """

    path: str
    version: int
    storage_options: dict
    pyarrow_options: dict
    without_files: bool

    def __init__(
        self,
        path: str,
        version: int = None,
        storage_options: dict = None,
        pyarrow_options: dict = None,
        without_files: bool = False,
    ):
        self.path = path
        self.version = version
        self.storage_options = storage_options
        self.pyarrow_options = pyarrow_options
        self.without_files = without_files

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

    def read_batch(self) -> LazyFrame:
        """
        Reads data from a Delta table into a Polars LazyFrame
        """
        without_files_dict = {"without_files": self.without_files}
        lf = pl.scan_delta(
            source=self.path,
            version=self.version,
            storage_options=self.storage_options,
            delta_table_options=without_files_dict,
            pyarrow_options=self.pyarrow_options,
        )
        return lf

    def read_stream(self):
        """
        Raises:
            NotImplementedError: Reading from a Delta table using Python is only possible for batch reads. To perform a streaming read, use the read_stream method of the SparkDeltaSource component.
        """
        raise NotImplementedError(
            "Reading from a Delta table using Python is only possible for batch reads. To perform a streaming read, use the read_stream method of the SparkDeltaSource component"
        )
