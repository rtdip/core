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

from .....data_models.meters.ami_meters import SeriesType, ModelType, ValueType
from ..base_raw_to_mdm import BaseRawToMDMTransformer
from ...._pipeline_utils.iso import melt, PJM_SCHEMA


class PJMToMDMTransformer(BaseRawToMDMTransformer):
    """
    Converts PJM Raw data into Meters Data Model.

    Please check the [BaseRawToMDMTransformer](../base_raw_to_mdm.md) for more info.
    """

    spark: SparkSession
    data: DataFrame
    input_schema = PJM_SCHEMA
    uid_col = "Zone"
    series_id_col = "'series_std_001'"
    timestamp_col = "to_utc_timestamp(StartTime, 'America/New_York')"
    interval_timestamp_col = "Timestamp + INTERVAL 1 HOURS"
    value_col = "bround(Load, 2)"
    series_parent_id_col = "'series_parent_std_001'"
    name_col = "'PJM API'"
    uom_col = "'mwh'"
    description_col = "'PJM data pulled from PJM ISO API'"
    timestamp_start_col = "StartTime"
    timestamp_end_col = "StartTime + INTERVAL 1 HOURS"
    time_zone_col = "'America/New_York'"
    version_col = "'1'"
    series_type = SeriesType.Hour
    model_type = ModelType.Default
    value_type = ValueType.Usage
    properties_col = "null"

