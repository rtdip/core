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


from ..timeseries import SeriesType
from ..timeseries import MetaData
from ..timeseries import ValueType
from ..timeseries import ModelType
from ..timeseries import Uom
import logging


def create_timeseries_vo(   # NOSONAR
    uid: str,
    series_id: str,
    series_parent_id: str,
    name: str,
    uom: Uom,
    description: str,
    timestamp_start: int,
    timestamp_end: int,
    time_zone: str,
    version: str,
    series_type: SeriesType,
    model_type: ModelType,
    value_type: ValueType,
    properties: dict,
):
    try:
        return MetaData(
            Uid=uid,
            SeriesId=series_id,
            SeriesParentId=series_parent_id,
            Name=name,
            Uom=uom,
            Description=description,
            TimestampStart=timestamp_start,
            TimestampEnd=timestamp_end,
            Timezone=time_zone,
            Version=version,
            SeriesType=series_type,
            ModelType=model_type,
            ValueType=value_type,
            Properties=properties,
        )
    except Exception as e:
        error_msg_str: str = "Could not create Metadata Value Object: {}".format(e)
        logging.exception(e)
        raise SystemError(error_msg_str)
