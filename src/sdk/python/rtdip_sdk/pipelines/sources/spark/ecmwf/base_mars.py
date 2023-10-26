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
import numpy as np
import os

from ecmwfapi import ECMWFService
from joblib import Parallel, delayed


class SparkECMWFBaseMarsSource:
    """
    Download nc files from ECMWF MARS server using the ECMWF python API.
    Data is downloaded in parallel using joblib from ECMWF MARS server using the ECMWF python API.

    Parameters:
        save_path (str): Path to local directory where the nc files will be stored, in format "yyyy-mm-dd_HH.nc"
        date_start (str): Start date of extraction in "YYYY-MM-DD HH:MM:SS" format
        date_end (str): End date of extraction in "YYYY-MM-DD HH:MM:SS" format
        ecmwf_api_key (str): API key for ECMWF MARS server
        ecmwf_api_email (str): Email for ECMWF MARS server
        ecmwf_api_url (str): URL for ECMWF MARS server
        run_frequency (str):Frequency format of runs to download, e.g. "H"
        run_interval (str): Interval of runs, e.g. a run_frequency of "H" and run_interval of "12" will extract the data of the 00 and 12 run for each day.
    """

    def __init__(
        self,
        date_start: str,
        date_end: str,
        save_path: str,
        ecmwf_api_key: str,
        ecmwf_api_email: str,
        ecmwf_api_url: str = "https://api.ecmwf.int/v1",
        run_interval: str = "12",
        run_frequency: str = "H",
    ):
        self.retrieve_ran = False
        self.date_start = date_start
        self.date_end = date_end
        self.save_path = save_path
        self.format = format
        self.run_interval = run_interval
        self.run_frequency = run_frequency
        self.ecmwf_api_key = ecmwf_api_key
        self.ecmwf_api_url = ecmwf_api_url
        self.ecmwf_api_email = ecmwf_api_email

        # Pandas date_list (info best retrieved per forecast day)
        self.dates = pd.date_range(
            start=date_start, end=date_end, freq=run_interval + run_frequency
        )

    def retrieve(
        self,
        mars_dict: dict,
        n_jobs=None,
        backend="loky",
        tries=5,
        cost=False,
    ):
        """Retrieve the data from the server.

        Function will use the ecmwf api to download the data from the server.
        Note that mars has a max of two active requests per user and 20 queued
        requests.
        Data is downloaded in parallel using joblib from ECMWF MARS server using the ECMWF python API.


        Parameters:
            mars_dict (dict): Dictionary of mars parameters.
            n_jobs (int, optional): Download in parallel? by default None, i.e. no parallelization
            backend (str, optional) : Specify the parallelization backend implementation in joblib, by default "loky"
            tries (int, optional): Number of tries for each request if it fails, by default 5
            cost (bool, optional):  Pass a cost request to mars to estimate the size and efficiency of your request,
                but not actually download the data. Can be useful for defining requests,
                by default False.
        """
        chk = ["date", "target", "time", "format", "output"]
        for i in chk:
            if i in mars_dict.keys():
                raise ValueError(f"don't include {i} in the mars_dict")

        parallel = Parallel(n_jobs=n_jobs, backend=backend)

        def _retrieve_datetime(i, j, cost=cost):
            i_dict = {"date": i, "time": j}

            if cost:
                filename = f"{i}_{j}.txt"  # NOSONAR
            else:
                filename = f"{i}_{j}.nc"
                i_dict["format"] = "netcdf"  # NOSONAR

            target = os.path.join(self.save_path, filename)
            msg = f"retrieving mars data --- {filename}"

            req_dict = {**i_dict, **mars_dict}
            for k, v in req_dict.items():
                if isinstance(v, (list, tuple)):
                    req_dict[k] = "/".join([str(x) for x in v])  # NOSONAR

            req_dict = ["{}={}".format(k, v) for k, v in req_dict.items()]
            if cost:
                req_dict = "list,output=cost,{}".format(",".join(req_dict))  # NOSONAR
            else:
                req_dict = "retrieve,{}".format(",".join(req_dict))  # NOSONAR

            for j in range(tries):
                try:
                    print(msg)
                    server = ECMWFService(
                        "mars",
                        url=self.ecmwf_api_url,
                        email=self.ecmwf_api_email,
                        key=self.ecmwf_api_key,
                    )
                    server.execute(req_dict, target)
                    return 1  # NOSONAR
                except:  # NOSONAR
                    if j < tries - 1:
                        continue  # NOSONAR
                    else:
                        return 0  # NOSONAR

        self.success = parallel(
            delayed(_retrieve_datetime)(str(k.date()), f"{k.hour:02}")
            for k in self.dates
        )
        self.retrieve_ran = True

        return self

    def info(self) -> pd.Series:
        """
        Return info on each ECMWF request.

        Returns:
            pd.Series: Successful request for each run == 1.
        """
        if not self.retrieve_ran:
            raise ValueError(
                "Before using self.info(), prepare the request using "
                + "self.retrieve()"
            )
        y = pd.Series(self.success, index=self.dates, name="success", dtype=bool)

        return y
