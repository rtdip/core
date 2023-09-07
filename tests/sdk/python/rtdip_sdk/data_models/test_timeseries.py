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

sys.path.insert(0, ".")


from src.sdk.python.rtdip_sdk.data_models.utils import CreateTimeSeriesObject
from src.sdk.python.rtdip_sdk.data_models.meters.utils import CreateUsageObject
from src.sdk.python.rtdip_sdk.data_models.timeseries import SeriesType
from src.sdk.python.rtdip_sdk.data_models.timeseries import MetaData
from src.sdk.python.rtdip_sdk.data_models.timeseries import ValueType
from src.sdk.python.rtdip_sdk.data_models.timeseries import ModelType
from src.sdk.python.rtdip_sdk.data_models.timeseries import Uom
from src.sdk.python.rtdip_sdk.data_models.meters.ami_meter import Usage
from src.sdk.python.rtdip_sdk.data_models.utils import timeseries_utils
from uuid import uuid4
import datetime
import logging


def test_generate_timeseries_objects_creation():
    #
    meter_uid: str = str(uuid4())
    series_id: str = str(uuid4())
    series_parent_id: str = "parent_id_" + str(uuid4())
    description_str: str = "description_" + str(uuid4())

    version: str = "Version_0_0_1"

    timestamp_start: int = int(timeseries_utils.get_utc_timestamp())
    timestamp_end: int = int(timeseries_utils.get_utc_timestamp())
    time_zone: str = str(
        datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
    )
    name: str = "name_" + str(uuid4())
    uom = Uom.KWH

    series_type = SeriesType.Minutes10
    model_type = ModelType.Default
    value_type = ValueType.Usage

    series_properties: dict = dict()
    key_str: str = "key_" + str(uuid4())
    value_str: str = "value_" + str(uuid4())
    series_properties[key_str] = value_str

    metadata_vo: MetaData = CreateTimeSeriesObject.create_timeseries_vo(
        uid=meter_uid,
        series_id=series_id,
        series_parent_id=series_parent_id,
        name=name,
        uom=uom,
        description=description_str,
        timestamp_start=timestamp_start,
        timestamp_end=timestamp_end,
        time_zone=time_zone,
        version=version,
        series_type=series_type,
        model_type=model_type,
        value_type=value_type,
        properties=series_properties,
    )

    logging.debug(metadata_vo)

    # Test for json serialization/deser. Usage
    usage_vo: Usage = CreateUsageObject.create_usage_vo(
        meter_uid,
        series_id,
        timestamp_start,
        timestamp_start,
        timeseries_utils.generate_random_int_number(0, 1000) * 0.1,
    )
    logging.debug(usage_vo)

    assert metadata_vo.SeriesId == usage_vo.SeriesId
