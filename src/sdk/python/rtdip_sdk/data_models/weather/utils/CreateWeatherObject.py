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

from ..weather_models import AtmosphericG215minForecastV1
from datetime import datetime


def create_AtmosphericG215minForecastV1_VO(
    clas: str,
    clds: int,
    day_ind: str,
    dewpt: int,
    dow: str,
    expire_time_gmt: float,
    fcst_valid: float,
    fcst_valid_local: datetime,
    feels_like: int,
    golf_category: str,
    golf_index: int,
    gust: int,
    hi: int,
    icon_code: int,
    icon_extd: int,
    mslp: float,
    num: int,
    phrase_12char: str,
    phrase_22char: str,
    phrase_32char: str,
    pop: str,
    precip_type: str,
    qpf: float,
    rh: int,
    severity: int,
    snow_qpf: float,
    subphrase_pt1: str,
    subphrase_pt2: str,
    subphrase_pt3: str,
    temp: int,
    uv_desc: str,
    uv_index: int,
    uv_index_raw: float,
    uv_warning: int,
    vis: float,
    wc: int,
    wdir: int,
    wdir_cardinal: str,
    wspd: int,
    wxman: str,
):
    try:
        return AtmosphericG215minForecastV1(
            clas=clas,
            clds=clds,
            day_ind=day_ind,
            dewpt=dewpt,
            dow=dow,
            expire_time_gmt=expire_time_gmt,
            fcst_valid=fcst_valid,
            fcst_valid_local=fcst_valid_local,
            feels_like=feels_like,
            golf_category=golf_category,
            golf_index=golf_index,
            gust=gust,
            hi=hi,
            icon_code=icon_code,
            icon_extd=icon_extd,
            mslp=mslp,
            num=num,
            phrase_12char=phrase_12char,
            phrase_22char=phrase_22char,
            phrase_32char=phrase_32char,
            pop=pop,
            precip_type=precip_type,
            qpf=qpf,
            rh=rh,
            severity=severity,
            snow_qpf=snow_qpf,
            subphrase_pt1=subphrase_pt1,
            subphrase_pt2=subphrase_pt2,
            subphrase_pt3=subphrase_pt3,
            temp=temp,
            uv_desc=uv_desc,
            uv_index=uv_index,
            uv_index_raw=uv_index_raw,
            uv_warning=uv_warning,
            vis=vis,
            wc=wc,
            wdir=wdir,
            wdir_cardinal=wdir_cardinal,
            wspd=wspd,
            wxman=wxman,
        )
    except Exception as ex:
        error_msg_str: str = (
            "Could not create AtmosphericG215minForecastV1 Value Object: {}".format(ex)
        )
        raise SystemError(error_msg_str)
