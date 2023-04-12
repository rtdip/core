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

from src.sdk.python.rtdip_sdk.data_models.weather.utils import CreateWeatherObject
from src.sdk.python.rtdip_sdk.data_models.meters.utils import utils

import pytest


def test_create_AtmosphericG215minForecastV1():
    # Random values. Not value/type checking. 
    # Will check if this value is set properly in the object attributes
    temp_value_int: int = utils.generate_random_int_number(0, 100)

    # Create the object via the factory
    a_g2_15min_for_v1_instance = CreateWeatherObject.create_AtmosphericG215minForecastV1_VO(clas=utils.generate_random_alpha_num_string(),
        clds=utils.generate_random_int_number(0, 100),
        day_ind=utils.generate_random_alpha_num_string(),
        dewpt=utils.generate_random_int_number(0, 100),
        dow=utils.generate_random_alpha_num_string(),
        expire_time_gmt=utils.get_utc_timestamp(),
        fcst_valid=utils.get_utc_timestamp(),
        fcst_valid_local=utils.get_datetime_from_utc_timestamp(utils.get_utc_timestamp()),
        feels_like=utils.generate_random_int_number(0, 100),
        golf_category=utils.generate_random_alpha_num_string(),
        golf_index=utils.generate_random_int_number(0, 100),
        gust=utils.generate_random_int_number(0, 100),
        hi=utils.generate_random_int_number(0, 100),
        icon_code=utils.generate_random_int_number(0, 100),
        icon_extd=utils.generate_random_int_number(0, 100),
        mslp=utils.generate_random_int_number(0, 100) / 100,
        num=utils.generate_random_int_number(0, 100),
        phrase_12char=utils.generate_random_alpha_num_string(),
        phrase_22char=utils.generate_random_alpha_num_string(),
        phrase_32char=utils.generate_random_alpha_num_string(),
        pop=utils.generate_random_alpha_num_string(),
        precip_type=utils.generate_random_alpha_num_string(),
        qpf=utils.generate_random_int_number(0, 100)* 1.0,
        rh=utils.generate_random_int_number(0, 100),
        severity = utils.generate_random_int_number(0, 100),
        snow_qpf= utils.generate_random_int_number(0, 100) * 1.0,
        subphrase_pt1=utils.generate_random_alpha_num_string(),
        subphrase_pt2=utils.generate_random_alpha_num_string(),
        subphrase_pt3=utils.generate_random_alpha_num_string(),
        temp=temp_value_int,
        uv_desc=utils.generate_random_alpha_num_string(),
        uv_index=utils.generate_random_int_number(0, 100),
        uv_index_raw=utils.generate_random_int_number(0, 100)*1.0,
        uv_warning=utils.generate_random_int_number(0, 100),
        vis=utils.generate_random_int_number(0, 100) * 1.0,
        wc=utils.generate_random_int_number(0, 100),
        wdir=utils.generate_random_int_number(0, 100),
        wdir_cardinal=utils.generate_random_alpha_num_string(),
        wspd=utils.generate_random_int_number(0, 100),
        wxman=utils.generate_random_alpha_num_string())

    assert a_g2_15min_for_v1_instance.temp == temp_value_int

 
