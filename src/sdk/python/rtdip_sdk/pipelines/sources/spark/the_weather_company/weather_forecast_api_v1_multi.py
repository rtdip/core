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

import pandas as pd
from pyspark.sql import SparkSession

from ...._pipeline_utils.weather import WEATHER_FORECAST_SCHEMA
from .weather_forecast_api_v1 import SparkWeatherCompanyForecastAPIV1Source


class SparkWeatherCompanyForecastAPIV1MultiSource(
    SparkWeatherCompanyForecastAPIV1Source
):
    """
    The Weather Forecast API V1 Multi Source is used to read 15 days forecast from the Weather API. It allows to
    pull weather data for multiple stations and returns all of them in a single DataFrame.

    URL for one station: <a href="https://api.weather.com/v1/geocode/32.3667/-95.4/forecast/hourly/360hour.json">
    https://api.weather.com/v1/geocode/32.3667/-95.4/forecast/hourly/360hour.json</a>

    It takes a list of Weather Stations. Each station item must contain comma separated Latitude & Longitude.

    Examples
    --------
    `["32.3667,-95.4", "51.52,-0.11"]`

    Parameters:
        spark (SparkSession): Spark Session instance
        options (dict): A dictionary of ISO Source specific configurations (See Attributes table below).

    Attributes:
        stations (list[str]): List of Weather Stations.
        api_key (str): Weather API key.
        language (str): API response language. Defaults to `en-US`.
        units (str): Unit of measurements. Defaults to `e`.
    """

    spark: SparkSession
    options: dict
    spark_schema = WEATHER_FORECAST_SCHEMA
    required_options = ["stations", "api_key"]

    def __init__(self, spark: SparkSession, options: dict) -> None:
        super(SparkWeatherCompanyForecastAPIV1MultiSource, self).__init__(
            spark, options
        )
        self.spark = spark
        self.options = options
        self.stations = self.options.get("stations", [])
        self.api_key = self.options.get("api_key", "").strip()
        self.language = self.options.get("language", "en-US").strip()
        self.units = self.options.get("units", "e").strip()

    def _pull_data(self) -> pd.DataFrame:
        """
        Pulls data from the Weather API and parses the JSON file for multiple stations

        Returns:
            Raw form of data.
        """

        result_df = None
        for station in self.stations:
            parts = station.split(",")
            lat, lon = parts

            df = self._pull_for_weather_station(lat, lon)
            df["latitude"] = lat
            df["longitude"] = lon

            if result_df is not None:
                result_df = pd.concat([result_df, df])
            else:
                result_df = df

        return result_df

    def _validate_options(self) -> bool:
        for station in self.stations:
            parts = station.split(",")

            if len(parts) != 2 or parts[0].strip() == "" or parts[1].strip() == "":
                raise ValueError(
                    f"Each station item must contain comma separated Latitude & Longitude. Eg: 10.23:45.2"
                )

        return True
