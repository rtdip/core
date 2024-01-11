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

from pyspark.sql import DataFrame, SparkSession

from ..base_raw_to_mdm import BaseRawToMDMTransformer
from ...._pipeline_utils.iso import CAISO_SCHEMA
from .....data_models.timeseries import SeriesType, ModelType, ValueType


class CAISOToMDMTransformer(BaseRawToMDMTransformer):
    """
    Converts CAISO Raw data into Meters Data Model.

    Please check the BaseRawToMDMTransformer for the required arguments and methods.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.transformers import CAISOToMDMTransformer
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    caiso_to_mdm_transformer = CAISOToMDMTransformer(
        spark=spark,
        data=df,
        output_type="usage",
        name=None,
        description=None,
        value_type=None,
        version=None,
        series_id=None,
        series_parent_id=None
    )

    result = caiso_to_mdm_transformer.transform()
    ```
    """

    spark: SparkSession
    data: DataFrame
    input_schema = CAISO_SCHEMA
    uid_col = "TacAreaName"
    series_id_col = "'series_std_001'"
    timestamp_col = "to_timestamp(StartTime)"
    interval_timestamp_col = "Timestamp + INTERVAL 1 HOURS"
    value_col = "Load"
    series_parent_id_col = "'series_parent_std_001'"
    name_col = "'CAISO API'"
    uom_col = "'mwh'"
    description_col = "'CAISO data pulled from CAISO ISO API'"
    timestamp_start_col = "StartTime"
    timestamp_end_col = "StartTime + INTERVAL 1 HOURS"
    time_zone_col = "'PST'"
    version_col = "'1'"
    series_type = SeriesType.Hour
    model_type = ModelType.Default
    value_type = ValueType.Usage
    properties_col = "null"
