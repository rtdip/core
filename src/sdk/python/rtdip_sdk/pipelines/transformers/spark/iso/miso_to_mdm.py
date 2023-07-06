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
from ...._pipeline_utils.iso import melt, MISO_SCHEMA


class MISOToMDMTransformer(BaseRawToMDMTransformer):
    """
    Converts MISO Raw data into Meters Data Model.

    Please check the [BaseRawToMDMTransformer](../base_raw_to_mdm.md) for more info.
    """

    spark: SparkSession
    data: DataFrame
    input_schema = MISO_SCHEMA
    uid_col = "variable"
    series_id_col = "'series_std_001'"
    timestamp_col = "to_utc_timestamp(Datetime, 'US/Central')"
    interval_timestamp_col = "Timestamp + INTERVAL 1 HOURS"
    value_col = "bround(value, 2)"
    series_parent_id_col = "'series_parent_std_001'"
    name_col = "'Miso API'"
    uom_col = "'mwh'"
    description_col = "'Miso data pulled from Miso ISO API'"
    timestamp_start_col = "Datetime"
    timestamp_end_col = "Datetime + INTERVAL 1 HOURS"
    time_zone_col = "'US/Central'"
    version_col = "'1'"
    series_type = SeriesType.Hour
    model_type = ModelType.Default
    value_type = ValueType.Usage
    properties_col = "null"

    def _pre_process(self) -> DataFrame:
        df: DataFrame = super(MISOToMDMTransformer, self)._pre_process()
        df = melt(df,
                  id_vars=["Datetime"],
                  value_vars=["Lrz1", "Lrz2_7", "Lrz3_5", "Lrz4", "Lrz6", "Lrz8_9_10", "Miso"]
                  )
        return df
