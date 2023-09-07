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


def create_timeseries_vo(**kwargs):
    try:
        return MetaData(
            Uid=kwargs["uid"],
            SeriesId=kwargs["series_id"],
            SeriesParentId=kwargs["series_parent_id"],
            Name=kwargs["name"],
            Uom=kwargs["uom"],
            Description=kwargs["description"],
            TimestampStart=kwargs["timestamp_start"],
            TimestampEnd=kwargs["timestamp_end"],
            Timezone=kwargs["time_zone"],
            Version=kwargs["version"],
            SeriesType=kwargs["series_type"],
            ModelType=kwargs["model_type"],
            ValueType=kwargs["value_type"],
            Properties=kwargs["properties"],
        )
    except Exception as e:
        error_msg_str: str = "Could not create Metadata Value Object: {}".format(e)
        logging.exception(e)
        raise SystemError(error_msg_str)
