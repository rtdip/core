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
from abc import abstractmethod

from pyspark.sql import DataFrame

from ..base_raw_to_smdm_transformer import BaseRawToSMDMTransformer
from ...._pipeline_utils.iso import melt, MISO_SCHEMA


class MISOISOTransformer(BaseRawToSMDMTransformer):

    input_schema = MISO_SCHEMA
    uid_col = "variable"
    series_id_col = "'series_std_001'"
    timestamp_col = "to_utc_timestamp(DATE_TIME, 'US/Central')"
    interval_timestamp_col = "timestamp + INTERVAL 1 HOURS"
    value_col = "bround(value, 2)"
    series_parent_id_col = "'series_parent_std_001'"
    name_col = "'MISO API'"
    uom_col = "'mwh'"
    description_col = "'MISO data pulled from MISO ISO API'"
    timestamp_start_col = "DATE_TIME"
    timestamp_end_col = "DATE_TIME + INTERVAL 1 HOURS"
    time_zone_col = "'US/Central'"
    version_col = "'1'"
    series_type_col = "'hour'"
    model_type_col = "null"
    value_type_col = "'usage'"
    properties_col = "null"

    def _pre_process(self) -> DataFrame:
        df = melt(self.data,
                  id_vars=["DATE_TIME"],
                  value_vars=["LRZ1", "LRZ2_7", "LRZ3_5", "LRZ4", "LRZ6", "LRZ8_9_10", "MISO"]
                  )
        return df

