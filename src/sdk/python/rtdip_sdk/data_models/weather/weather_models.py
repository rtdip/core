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


from pydantic import BaseModel
from datetime import datetime

class AtmosphericG215minForecastV1(BaseModel):
    clas: str  # Data identifier: example: fod_long_range_hourly
    clds: int  # Cloud Cover: Hourly average cloud cover expressed as a percentage: range: 0 to 100
    day_ind: str  # DayOrNight: range: D, N, X   X=Missing
    dewpt: int  # Dew Point: example: 63
    dow: str  # dayOfWeek: range: Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday
    expire_time_gmt: float  # expirationTimeUtc [epoch] Expiration time in UNIX seconds: example: 1369252800
    fcst_valid: float  # Time forecast is valid in UNIX seconds [epoch]: example: 1369306800
    fcst_valid_local: datetime  # Time forecast is valid in local apparent time [ISO]: example: 2013-08-06T07:00:00-0400
    feels_like: int  # example: 84
    golf_category: str  # example: very good
    golf_index: int  # range: 1-10: enum: 0-2=Very Poor, 3=Poor, 4-5=Fair, 6-7=Good, 8-9=Very Good, 10=Excellent
    gust: int
    hi: int  # Hourly maximum heat index
    icon_code: int  # exmaple: 26
    icon_extd: int  # example 5560
    mslp: float  # Hourly mean sea level pressure
    num: int  # range: 1-15
    phrase_12char: str
    phrase_22char: str
    phrase_32char: str
    pop: str  # Hourly maximum probability of precipitation: range 0-100
    precip_type: str  # enum: rain,snow, precip
    qpf: float  #
    rh: int  # enum 0-100
    severity: int  # severity example: 0 = no threat 6 = dangerous / life threatening
    snow_qpf: float
    subphrase_pt1: str
    subphrase_pt2: str
    subphrase_pt3: str
    temp: int  # range -140 to 140
    uv_desc: str  # enum: -2 is Not Available-1 is No Report 0 to 2 is Low 3 to 5 is Moderate 6 to 7 is High 8 to 10 is Very High 11 to 16 is Extreme
    uv_index: int
    uv_index_raw: float
    uv_warning: int
    vis: float  # range 0 - 999
    wc: int
    wdir: int  # range 0 - 359
    wdir_cardinal: str  # enum: N , NNE , NE, ENE, E, ESE, SE, SSE, S, SSW, SW, WSW, W, WNW, NW, NNW, CALM, VAR
    wspd: int
    wxman: str
    



