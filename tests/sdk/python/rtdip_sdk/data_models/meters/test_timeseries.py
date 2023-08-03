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
sys.path.insert(0, '.')


from src.sdk.python.rtdip_sdk.data_models.meters.utils import CreateMetaDataObject
from src.sdk.python.rtdip_sdk.data_models.meters.utils import CreateUsageObject
from src.sdk.python.rtdip_sdk.data_models.meters.ami_meters import SeriesType
from src.sdk.python.rtdip_sdk.data_models.meters.ami_meters import MetaData
from src.sdk.python.rtdip_sdk.data_models.meters.ami_meters import ValueType
from src.sdk.python.rtdip_sdk.data_models.meters.ami_meters import ModelType
from src.sdk.python.rtdip_sdk.data_models.meters.ami_meters import UomUsage
from src.sdk.python.rtdip_sdk.data_models.meters.ami_meters import Usage
from src.sdk.python.rtdip_sdk.data_models.meters.utils import utils
from uuid import uuid4
import datetime
import logging
import pytest


def test_generate_timeseries_objects_creation():

    #
    meter_1_uid_str: str = str(uuid4())
    series_1_id_str: str = str(uuid4())
    series_1_parent_id_str: str = 'parent_id_' + str(uuid4())
    description_str: str = 'description_' + str(uuid4())

    version_str: str = 'Version_0_0_1'

    timestamp_start_int: int = int(utils.get_utc_timestamp())
    timestamp_end_int: int = timestamp_start_int
    timezone_str: str = str(datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo)
    name_str: str = 'name_' + str(uuid4())
    uom = UomUsage.KWH

    series_type = SeriesType.Minutes10
    model_type = ModelType.Default
    value_type = ValueType.Usage

    properties_dict: dict = dict()
    key_str: str = 'key_' + str(uuid4())
    value_str: str = 'value_' + str(uuid4())
    properties_dict[key_str] = value_str

    metadata_vo: MetaData = CreateMetaDataObject.create_metadata_VO(meter_1_uid_str,
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
                                                                            properties_dict)
    
    logging.debug(metadata_vo)


    # Test for json serialization/deser. Usage
    timestamp_interval_int: int = timestamp_start_int
    usage_vo: Usage = CreateUsageObject.create_usage_VO(meter_1_uid_str,
                                                        series_1_id_str,
                                                        timestamp_start_int,
                                                        timestamp_interval_int,
                                                        utils.generate_random_int_number(0,1000) * 0.1)
    logging.debug(usage_vo)

    assert (metadata_vo.SeriesId == usage_vo.SeriesId)
  

