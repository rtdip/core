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
import requests
import logging
from pandas import DataFrame
from requests import HTTPError

from ..interfaces import SourceInterface
from ..._pipeline_utils.models import Libraries, SystemType


class PythonMFFBASSource(SourceInterface):
    """
    The Python MFFBAS Source is used to read the Standaard Jaar Verbruiksprofielen (Standard Consumption Profiles) from the MFFBAS API. More information on the Standard Consumption Profiles can be found [here](https://www.mffbas.nl/documenten/).

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.sources import PythonMFFBASSource

    sjv_source = PythonMFFBASSource(
       start="2024-01-01",
       end="2024-01-02"
    )

    sjv_source.read_batch()
    ```

    Args:
       start (str): Start date in the format YYYY-MM-DD
       end (str): End date in the format YYYY-MM-DD

    !!! note "Note"
        It is not possible to collect fractions over a period before 2023-04-01 with this API. Requests are limited to a maximum of 31 days at a time.

    """

    start: str
    end: str

    def __init__(self, start, end) -> None:
        self.start = start
        self.end = end

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

    def _pull_data(self):
        url = "https://gateway.edsn.nl/energyvalues/profile-fractions-series/v1/profile-fractions"

        parameters = {
            "startdate": self.start,
            "enddate": self.end,
            "pftype": "STANDARD",
            "product": "023",
        }

        response = requests.request("GET", url, params=parameters)

        code = response.status_code

        if code != 200:
            raise HTTPError(
                f"Unable to access URL `{url}`."
                f" Received status code {code} with message {response.content}"
            )

        data = response.json()

        return data

    def _prepare_data(self):
        data = self._pull_data()
        df = pd.DataFrame.from_dict(data["Detail_SeriesList"])

        df.rename(columns={"calendar_date": "Versienr"}, inplace=True)
        df = df.explode("PointList")
        df = pd.concat(
            [df.drop(["PointList"], axis=1), df["PointList"].apply(pd.Series)], axis=1
        )
        df["direction"] = df["direction"].map({"E17": "A", "E18": "I"})
        df["profiles"] = df[
            ["profileCategory", "determinedConsumption", "direction"]
        ].agg(lambda x: "_".join(x.dropna()), axis=1)
        df["Versienr"] = pd.to_datetime(df["Versienr"]) + pd.to_timedelta(
            df["pos"] * 15, unit="min"
        )
        df = df[df["pos"] < 96]
        drop = [
            "direction",
            "pFdate_version",
            "profileCategory",
            "determinedConsumption",
            "pos",
            "resolution",
            "profileStatus_quality",
        ]
        df.drop(columns=drop, axis=1, inplace=True)

        result = df.pivot(index="Versienr", columns="profiles", values="qnt")
        result["year_created"] = result.index.strftime("%Y-%m-%d")

        return result

    def read_batch(self) -> DataFrame:
        """
        Reads batch from the MFFBAS API.
        """
        try:
            df = self._prepare_data()
            return df
        except Exception as e:
            logging.exception(str(e))
            raise e

    def read_stream(self):
        """
        Raises:
              NotImplementedError: MFFBAS connector does not support the stream operation.
        """
        raise NotImplementedError(
            "MFFBAS connector does not support the stream operation."
        )
