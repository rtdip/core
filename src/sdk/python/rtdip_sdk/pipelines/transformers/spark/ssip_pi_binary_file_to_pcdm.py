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

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from pyspark.sql import DataFrame

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package


class SSIPPIBinaryFileToPCDMTransformer(TransformerInterface):
    """
    Converts a Spark DataFrame column containing binaryFile parquet data to the Process Control Data Model.

    This DataFrame should contain a path and the binary data. Typically this can be done using the Autoloader source component and specify "binaryFile" as the format.

    For more information about the SSIP PI Batch Connector, please see [here.](https://bakerhughesc3.ai/oai-solution/shell-sensor-intelligence-platform/)

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.transformers import SSIPPIBinaryFileToPCDMTransformer

    ssip_pi_binary_file_to_pcdm_transformer = SSIPPIBinaryFileToPCDMTransformer(
        data=df
    )

    result = ssip_pi_binary_file_to_pcdm_transformer.transform()
    ```

    Parameters:
        data (DataFrame): DataFrame containing the path and binaryFile data
    """

    data: DataFrame

    def __init__(self, data: DataFrame) -> None:
        self.data = data

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYSPARK
        """
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        libraries.add_pypi_library(get_default_package("pyarrow"))
        libraries.add_pypi_library(get_default_package("pandas"))
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def pre_transform_validation(self):
        return True

    def post_transform_validation(self):
        return True

    @staticmethod
    def _convert_binary_to_pandas(pdf):
        try:
            binary_list = pdf.values.tolist()
            binary_data = binary_list[0][3]
            buf = pa.py_buffer(binary_data)
            table = pq.read_table(buf)
        except Exception as e:
            print(str(e))
            return pd.DataFrame(
                {
                    "EventDate": pd.Series([], dtype="datetime64[ns]"),
                    "TagName": pd.Series([], dtype="str"),
                    "EventTime": pd.Series([], dtype="datetime64[ns]"),
                    "Status": pd.Series([], dtype="str"),
                    "Value": pd.Series([], dtype="str"),
                    "ValueType": pd.Series([], dtype="str"),
                    "ChangeType": pd.Series([], dtype="str"),
                }
            )

        output_pdf = table.to_pandas()

        if "ValueType" not in output_pdf.columns:
            value_type = str(table.schema.field("Value").type)
            if value_type == "int16" or value_type == "int32":
                value_type = "integer"
            output_pdf["ValueType"] = value_type

        if "ChangeType" not in output_pdf.columns:
            output_pdf["ChangeType"] = "insert"

        output_pdf["EventDate"] = output_pdf["EventTime"].dt.date
        output_pdf["Value"] = output_pdf["Value"].astype(str)
        output_pdf = output_pdf[
            [
                "EventDate",
                "TagName",
                "EventTime",
                "Status",
                "Value",
                "ValueType",
                "ChangeType",
            ]
        ]
        return output_pdf

    def transform(self) -> DataFrame:
        """
        Returns:
            DataFrame: A dataframe with the provided Binary data convert to PCDM
        """
        return self.data.groupBy("path").applyInPandas(
            SSIPPIBinaryFileToPCDMTransformer._convert_binary_to_pandas,
            schema="EventDate DATE, TagName STRING, EventTime TIMESTAMP, Status STRING, Value STRING, ValueType STRING, ChangeType STRING",
        )
