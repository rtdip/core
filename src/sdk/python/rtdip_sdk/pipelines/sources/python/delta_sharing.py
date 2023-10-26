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
import delta_sharing
import polars as pl
from polars import LazyFrame


class PythonDeltaSharingSource(SourceInterface):
    """
    The Python Delta Sharing Source is used to read data from a Delta table with Delta Sharing configured, without using Apache Spark.

    Example
    -------
    ```python
    from rtdip_sdk.pipelines.sources import PythonDeltaSharingSource

    python_delta_sharing_source = PythonDeltaSharingSource(
        profile_path="{CREDENTIAL-FILE-LOCATION}",
        share_name="{SHARE-NAME}",
        schema_name="{SCHEMA-NAME}",
        table_name="{TABLE-NAME}"
    )

    python_delta_sharing_source.read_batch()
    ```

    Parameters:
        profile_path (str): Location of the credential file. Can be any URL supported by [FSSPEC](https://filesystem-spec.readthedocs.io/en/latest/index.html){ target="_blank" }
        share_name (str): The value of 'share=' for the table
        schema_name (str): The value of 'schema=' for the table
        table_name (str): The value of 'name=' for the table
    """

    profile_path: str
    share_name: str
    schema_name: str
    table_name: str

    def __init__(
        self, profile_path: str, share_name: str, schema_name: str, table_name: str
    ):
        self.profile_path = profile_path
        self.share_name = share_name
        self.schema_name = schema_name
        self.table_name = table_name

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
        Reads data from a Delta table with Delta Sharing into a Polars LazyFrame.
        """
        pandas_df = delta_sharing.load_as_pandas(
            f"{self.profile_path}#{self.share_name}.{self.schema_name}.{self.table_name}"
        )
        polars_lazyframe = pl.from_pandas(pandas_df).lazy()
        return polars_lazyframe

    def read_stream(self):
        """
        Raises:
            NotImplementedError: Reading from a Delta table with Delta Sharing using Python is only possible for batch reads.
        """
        raise NotImplementedError(
            "Reading from a Delta table with Delta Sharing using Python is only possible for batch reads."
        )
