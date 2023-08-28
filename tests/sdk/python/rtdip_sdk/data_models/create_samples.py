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

import sys
import os

sys.path.insert(0, ".")
cwd: str = os.getcwd()
rtdip_root: str = os.path.join(cwd, "..", "..", "..", "..", "..")
sys.path.insert(0, rtdip_root)

from src.sdk.python.rtdip_sdk.data_models.utils import CreateTimeSeriesObject
from src.sdk.python.rtdip_sdk.data_models.meters.utils import CreateUsageObject
from src.sdk.python.rtdip_sdk.data_models.timeseries import SeriesType
from src.sdk.python.rtdip_sdk.data_models.timeseries import MetaData
from src.sdk.python.rtdip_sdk.data_models.timeseries import ValueType
from src.sdk.python.rtdip_sdk.data_models.timeseries import ModelType
from src.sdk.python.rtdip_sdk.data_models.timeseries import Uom
from src.sdk.python.rtdip_sdk.data_models.meters.ami_meter import Usage
from src.sdk.python.rtdip_sdk.data_models.utils import timeseries_utils
from fastapi.encoders import jsonable_encoder
from uuid import uuid4
import pandas as pd
import datetime
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def generate_random_timeserie_instance():
    #
    meter_1_uid_str: str = str(uuid4())
    series_1_id_str: str = str(uuid4())
    series_1_parent_id_str: str = "parent_id_" + str(uuid4())
    description_str: str = "description_" + str(uuid4())

    version_str: str = "Version_" + str(uuid4())

    timestamp_start_int: int = int(timeseries_utils.get_utc_timestamp())
    timestamp_end_int: int = timestamp_start_int
    timezone_str: str = str(
        datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
    )
    name_str: str = "name_" + str(uuid4())
    uom = Uom.KWH

    series_type = SeriesType.Minutes10
    model_type = ModelType.AMI_USAGE
    value_type = ValueType.Usage

    properties_dict: dict = dict()
    key_str: str = "key_" + str(uuid4())
    value_str: str = "value_" + str(uuid4())
    properties_dict[key_str] = value_str

    metadata_instance: MetaData = CreateTimeSeriesObject.create_timeseries_vo(
        meter_1_uid_str,
        series_1_id_str,
        series_1_parent_id_str,
        name_str,
        uom,
        description_str,
        timestamp_start_int,
        timestamp_end_int,
        timezone_str,
        version_str,
        series_type,
        model_type,
        value_type,
        properties_dict,
    )

    return metadata_instance


def generate_usage_instance(
    meter_id: str, series_id: str, timestamp: int, timestamp_interval: int, value: float
):
    return CreateUsageObject.create_usage_VO(
        meter_id, series_id, timestamp, timestamp_interval, value
    )


if __name__ == "__main__":
    number_of_samples: int = 100
    usage_list: list = list()

    timeseries_metadata_instance = generate_random_timeserie_instance()
    meter_id: str = str(uuid4())
    for i in range(number_of_samples):
        usage_timestamp: int = timeseries_metadata_instance.TimestampStart
        usage_value: float = timeseries_utils.generate_random_int_number(0, 1000) * 0.1
        usage_instance = generate_usage_instance(
            meter_id,
            timeseries_metadata_instance.SeriesId,
            usage_timestamp,
            usage_timestamp,
            usage_value,
        )
        usage_df = pd.DataFrame(jsonable_encoder(usage_instance), index=[0])
        usage_list.append(usage_df)
    timeseries_metadata_instance_df = pd.DataFrame(
        jsonable_encoder(timeseries_metadata_instance), index=[0]
    )
    logger.debug(timeseries_metadata_instance_df)
    usage_df = pd.concat(usage_list)
    print(str(usage_df))
