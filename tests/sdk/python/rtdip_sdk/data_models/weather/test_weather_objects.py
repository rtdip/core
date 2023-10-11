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

from src.sdk.python.rtdip_sdk.data_models.weather.utils import CreateWeatherObject
from src.sdk.python.rtdip_sdk.data_models.utils import timeseries_utils

import pytest


def test_create_AtmosphericG215minForecastV1():
    # Random values. Not value/type checking.
    # Will check if this value is set properly in the object attributes
    temp_value_int: int = timeseries_utils.generate_random_int_number(0, 100)

    # Create the object via the factory
    a_g2_15min_for_v1_instance = (
        CreateWeatherObject.create_AtmosphericG215minForecastV1_VO(
            clas=timeseries_utils.generate_random_alpha_num_string(),
            clds=timeseries_utils.generate_random_int_number(0, 100),
            day_ind=timeseries_utils.generate_random_alpha_num_string(),
            dewpt=timeseries_utils.generate_random_int_number(0, 100),
            dow=timeseries_utils.generate_random_alpha_num_string(),
            expire_time_gmt=timeseries_utils.get_utc_timestamp(),
            fcst_valid=timeseries_utils.get_utc_timestamp(),
            fcst_valid_local=timeseries_utils.get_datetime_from_utc_timestamp(
                timeseries_utils.get_utc_timestamp()
            ),
            feels_like=timeseries_utils.generate_random_int_number(0, 100),
            golf_category=timeseries_utils.generate_random_alpha_num_string(),
            golf_index=timeseries_utils.generate_random_int_number(0, 100),
            gust=timeseries_utils.generate_random_int_number(0, 100),
            hi=timeseries_utils.generate_random_int_number(0, 100),
            icon_code=timeseries_utils.generate_random_int_number(0, 100),
            icon_extd=timeseries_utils.generate_random_int_number(0, 100),
            mslp=timeseries_utils.generate_random_int_number(0, 100) / 100,
            num=timeseries_utils.generate_random_int_number(0, 100),
            phrase_12char=timeseries_utils.generate_random_alpha_num_string(),
            phrase_22char=timeseries_utils.generate_random_alpha_num_string(),
            phrase_32char=timeseries_utils.generate_random_alpha_num_string(),
            pop=timeseries_utils.generate_random_alpha_num_string(),
            precip_type=timeseries_utils.generate_random_alpha_num_string(),
            qpf=timeseries_utils.generate_random_int_number(0, 100) * 1.0,
            rh=timeseries_utils.generate_random_int_number(0, 100),
            severity=timeseries_utils.generate_random_int_number(0, 100),
            snow_qpf=timeseries_utils.generate_random_int_number(0, 100) * 1.0,
            subphrase_pt1=timeseries_utils.generate_random_alpha_num_string(),
            subphrase_pt2=timeseries_utils.generate_random_alpha_num_string(),
            subphrase_pt3=timeseries_utils.generate_random_alpha_num_string(),
            temp=temp_value_int,
            uv_desc=timeseries_utils.generate_random_alpha_num_string(),
            uv_index=timeseries_utils.generate_random_int_number(0, 100),
            uv_index_raw=timeseries_utils.generate_random_int_number(0, 100) * 1.0,
            uv_warning=timeseries_utils.generate_random_int_number(0, 100),
            vis=timeseries_utils.generate_random_int_number(0, 100) * 1.0,
            wc=timeseries_utils.generate_random_int_number(0, 100),
            wdir=timeseries_utils.generate_random_int_number(0, 100),
            wdir_cardinal=timeseries_utils.generate_random_alpha_num_string(),
            wspd=timeseries_utils.generate_random_int_number(0, 100),
            wxman=timeseries_utils.generate_random_alpha_num_string(),
        )
    )

    assert a_g2_15min_for_v1_instance.temp == temp_value_int
