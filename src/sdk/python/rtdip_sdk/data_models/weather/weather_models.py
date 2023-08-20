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
    """
    The Hourly Forecast API is sourced from the The Weather Company Forecast system.
    """

    clas: str
    """Data identifier. Example: fod_long_range_hourly"""
    clds: int
    """Cloud Cover: Hourly average cloud cover expressed as a percentage. Range: 0 to 100"""
    day_ind: str
    """This data field indicates whether it is daytime or nighttime based on the Local Apparent Time of the location. Range: D, N, X   X=Missing"""
    dewpt: int
    """Dew Point. The temperature which air must be cooled at constant pressure to reach saturation. The Dew Point is also an indirect measure of the humidity of the air. The Dew Point will never exceed the Temperature. When the Dew Point and Temperature are equal, clouds or fog will typically form. The closer the values of Temperature and Dew Point, the higher the relative humidity"""
    dow: str
    """Day of week. Range: Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday"""
    expire_time_gmt: float
    """Expiration time in UNIX seconds."""
    fcst_valid: float
    """Time forecast is valid in UNIX seconds."""
    fcst_valid_local: datetime
    """Time forecast is valid in local apparent time [ISO]"""
    feels_like: int
    """Hourly feels like temperature. An apparent temperature. It represents what the air temperature “feels like” on exposed human skin due to the combined effect of the wind chill or heat index"""
    golf_category: str
    """The Golf Index Category expressed as a worded phrase the weather conditions for playing golf"""
    golf_index: int
    """The Golf Index expresses on a scale of 0 to 10 the weather conditions for playing golf. Not applicable at night. Enum: 0-2=Very Poor, 3=Poor, 4-5=Fair, 6-7=Good, 8-9=Very Good, 10=Excellent"""
    gust: int
    """The maximum expected wind gust speed"""
    hi: int
    """Hourly maximum heat index"""
    icon_code: int
    """This number is the key to the weather icon lookup. The data field shows the icon number that is matched to represent the observed weather conditions. Please refer to the Forecast Icon Code, Weather Phrases and Images document"""
    icon_extd: int
    """Code representing explicit full set sensible weather"""
    mslp: float
    """Hourly mean sea level pressure"""
    num: int
    """This data field is the sequential number that identifies each of the forecasted days in the API. They start on day 1, which is the forecast for the current day. Then the forecast for tomorrow uses number 2, then number 3 for the day after tomorrow, and so forth. Range: 1-15"""
    phrase_12char: str
    """Hourly sensible weather phrase"""
    phrase_22char: str
    """Hourly sensible weather phrase"""
    phrase_32char: str
    """Hourly sensible weather phrase"""
    pop: str
    """Hourly maximum probability of precipitation.  Range 0-100"""
    precip_type: str
    """The short text describing the expected type accumulation associated with the Probability of Precipitation (POP) display for the hou. Enum: rain,snow, precip"""
    qpf: float
    """The forecasted measurable precipitation (liquid or liquid equivalent) during the hour"""
    rh: int
    """The relative humidity of the aire. Range 0-100"""
    severity: int
    """A number denoting how impactful is the forecasted weather for this hour. Range: 0 = no threat,  6 = dangerous / life threatening"""
    snow_qpf: float
    """The forecasted hourly snow accumulation during the hour"""
    subphrase_pt1: str
    """Part 1 of 3-part hourly sensible weather phrase"""
    subphrase_pt2: str
    """Part 2 of 3-part hourly sensible weather phrase"""
    subphrase_pt3: str
    """Part 3 of 3-part hourly sensible weather phrase"""
    temp: int
    """The temperature of the air, measured by a thermometer 1.5 meters (4.5 feet) above the ground that is shaded from the other element"""
    uv_desc: str
    """The UV Index Description which complements the UV Index value by providing an associated level of risk of skin damage due to exposur. Eenum: -2 is Not Available-1 is No Report 0 to 2 is Low 3 to 5 is Moderate 6 to 7 is High 8 to 10 is Very High 11 to 16 is Extreme"""
    uv_index: int
    """Hourly maximum UV index"""
    uv_index_raw: float
    """The non-truncated UV Index which is the intensity of the solar radiation based on a number of factors"""
    uv_warning: int
    """TWC-created UV warning based on UV index of 11 or greater"""
    vis: float
    """Prevailing hourly visibility"""
    wc: int
    """Hourly minimum wind chill"""
    wdir: int
    """Hourly average wind direction in magnetic notation. Range 0 - 359"""
    wdir_cardinal: str
    """Hourly average wind direction in cardinal notation. Enum: N , NNE , NE, ENE, E, ESE, SE, SSE, S, SSW, SW, WSW, W, WNW, NW, NNW, CALM, VAR"""
    wspd: int
    """The maximum forecasted hourly wind speed"""
    wxman: str
    """Code combining Hourly sensible weather and temperature conditions"""


class WeatherForecastV1(BaseModel):
    """
    This model is used to represent the standardised weather forecast for a given location.
    """

    Tagname: str
    """Unique identifier for the data point"""
    Longitude: float
    """Longitude of the location"""
    Latitude: float
    """Latitude of the location"""
    EventDate: datetime
    """Event date of forecast"""
    EventTime: datetime
    """Event time of forecast"""
    Source: str
    """Forecast API source i.e. ECMWF"""
    Status: str
    """Forecast API status i.e. Success"""
    Value: float
    Value: str
    """Value of forecast measurement"""
    EnqueuedTime: datetime
    """Time Forecast API was called"""
    Latest: bool
    """Latest forecast Identifier"""
