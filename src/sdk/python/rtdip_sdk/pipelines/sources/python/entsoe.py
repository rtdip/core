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
from pandas import DataFrame
from entsoe import EntsoePandasClient

from ..interfaces import SourceInterface
from ..._pipeline_utils.models import Libraries, SystemType


class PythonEntsoeSource(SourceInterface):
    """
    The Python ENTSO-E Source is used to read day-ahead prices from ENTSO-E.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.sources import PythonEntsoeSource

    entsoe_source = PythonEntsoeSource(
        api_key={API_KEY},
        start='20230101',
        end='20231001',
        country_code='NL'
    )

    entsoe_source.read_batch()
    ```

    Args:
        api_key (str): API token for ENTSO-E, to request access see documentation [here](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_authentication_and_authorisation)
        start (str): Start time in the format YYYYMMDD
        end (str): End time in the format YYYYMMDD
        country_code (str): Country code to query from. A full list of country codes can be found [here](https://github.com/EnergieID/entsoe-py/blob/master/entsoe/mappings.py#L48)
        resolution (optional str): Frequency of values; '60T' for hourly values, '30T' for half-hourly values or '15T' for quarterly values
    """

    api_key: str
    start: str
    end: str
    country_code: str
    resolution: str

    def __init__(
        self, api_key, start, end, country_code, resolution: str = "60T"
    ) -> None:
        self.key = api_key
        self.start = pd.Timestamp(start, tz="UTC")
        self.end = pd.Timestamp(end, tz="UTC")
        self.country = country_code
        self.resolution = resolution

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires Python
        """
        return SystemType.PYTHON

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def pre_read_validation(self):
        return True

    def post_read_validation(self):
        return True

    def read_batch(self) -> DataFrame:
        """
        Reads batch from ENTSO-E API.
        """
        client = EntsoePandasClient(api_key=self.key)
        df = client.query_day_ahead_prices(self.country, start=self.start, end=self.end)
        df = pd.DataFrame(df, columns=["Price"])
        df["Name"] = "APX"
        return df

    def read_stream(self):
        """
        Raises:
            NotImplementedError: ENTSO-E connector does not support the stream operation.
        """
        raise NotImplementedError(
            "ENTSO-E connector does not support the stream operation."
        )
