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

import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DoubleType, IntegerType

from .base_weather import SparkWeatherCompanyBaseWeatherSource
from ...._pipeline_utils.weather import WEATHER_FORECAST_SCHEMA


class SparkWeatherCompanyForecastAPIV1Source(SparkWeatherCompanyBaseWeatherSource):
    """
    The Weather Forecast API V1 Source is used to read 15 days forecast from the Weather API.

    URL: <a href="https://api.weather.com/v1/geocode/32.3667/-95.4/forecast/hourly/360hour.json">
    https://api.weather.com/v1/geocode/32.3667/-95.4/forecast/hourly/360hour.json</a>

    Parameters:
        spark (SparkSession): Spark Session instance
        options (dict): A dictionary of ISO Source specific configurations (See Attributes table below).

    Attributes:
        lat (str): Latitude of the Weather Station.
        lon (str): Longitude of the Weather Station.
        api_key (str): Weather API key.
        language (str): API response language. Defaults to `en-US`.
        units (str): Unit of measurements. Defaults to `e`.
    """

    spark: SparkSession
    spark_schema = WEATHER_FORECAST_SCHEMA
    options: dict
    weather_url: str = "https://api.weather.com/v1/geocode/"
    required_options = ["lat", "lon", "api_key"]

    def __init__(self, spark: SparkSession, options: dict) -> None:
        super(SparkWeatherCompanyForecastAPIV1Source, self).__init__(spark, options)
        self.spark = spark
        self.options = options
        self.lat = self.options.get("lat", "").strip()
        self.lon = self.options.get("lon", "").strip()
        self.api_key = self.options.get("api_key", "").strip()
        self.language = self.options.get("language", "en-US").strip()
        self.units = self.options.get("units", "e").strip()

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepares weather data for the use.

        Args:
            df: Data received after preparation.

        Returns:
            Final data after all the transformations.

        """

        rename_cols = {
            "latitude": "Latitude",
            "longitude": "Longitude",
            "class": "Class",
            "expire_time_gmt": "ExpireTimeGmt",
            "fcst_valid": "FcstValid",
            "fcst_valid_local": "FcstValidLocal",
            "num": "Num",
            "day_ind": "DayInd",
            "temp": "Temp",
            "dewpt": "Dewpt",
            "hi": "Hi",
            "wc": "Wc",
            "feels_like": "FeelsLike",
            "icon_extd": "IconExtd",
            "wxman": "Wxman",
            "icon_code": "IconCode",
            "dow": "Dow",
            "phrase_12char": "Phrase12Char",
            "phrase_22char": "Phrase22Char",
            "phrase_32char": "Phrase32Char",
            "subphrase_pt1": "SubphrasePt1",
            "subphrase_pt2": "SubphrasePt2",
            "subphrase_pt3": "SubphrasePt3",
            "pop": "Pop",
            "precip_type": "PrecipType",
            "qpf": "Qpf",
            "snow_qpf": "SnowQpf",
            "rh": "Rh",
            "wspd": "Wspd",
            "wdir": "Wdir",
            "wdir_cardinal": "WdirCardinal",
            "gust": "Gust",
            "clds": "Clds",
            "vis": "Vis",
            "mslp": "Mslp",
            "uv_index_raw": "UvIndexRaw",
            "uv_index": "UvIndex",
            "uv_warning": "UvWarning",
            "uv_desc": "UvDesc",
            "golf_index": "GolfIndex",
            "golf_category": "GolfCategory",
            "severity": "Severity",
        }

        df = df.rename(columns=rename_cols)

        fields = self.spark_schema.fields

        str_cols = list(
            map(
                lambda x: x.name,
                filter(lambda x: isinstance(x.dataType, StringType), fields),
            )
        )
        double_cols = list(
            map(
                lambda x: x.name,
                filter(lambda x: isinstance(x.dataType, DoubleType), fields),
            )
        )
        int_cols = list(
            map(
                lambda x: x.name,
                filter(lambda x: isinstance(x.dataType, IntegerType), fields),
            )
        )

        df[str_cols] = df[str_cols].astype(str)
        df[double_cols] = df[double_cols].astype(float)
        df[int_cols] = df[int_cols].astype(int)

        df.reset_index(inplace=True, drop=True)

        return df

    def _get_api_params(self):
        params = {
            "language": self.language,
            "units": self.units,
            "apiKey": self.api_key,
        }
        return params

    def _pull_for_weather_station(self, lat: str, lon: str) -> pd.DataFrame:
        response = json.loads(
            self._fetch_from_url(f"{lat}/{lon}/forecast/hourly/360hour.json").decode(
                "utf-8"
            )
        )
        return pd.DataFrame(response["forecasts"])

    def _pull_data(self) -> pd.DataFrame:
        """
        Pulls data from the Weather API and parses the JSON file.

        Returns:
            Raw form of data.
        """

        df = self._pull_for_weather_station(self.lat, self.lon)
        df["latitude"] = self.lat
        df["longitude"] = self.lon

        return df
