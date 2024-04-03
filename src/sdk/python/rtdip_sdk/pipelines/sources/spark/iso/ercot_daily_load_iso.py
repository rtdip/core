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
import base64
import logging
from _datetime import datetime, timedelta
from io import BytesIO
from typing import List
from zipfile import ZipFile

import pandas as pd
import requests
from bs4 import BeautifulSoup
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import pkcs12
from pyspark.sql import SparkSession
from requests import HTTPError

from . import BaseISOSource
from ...._pipeline_utils.iso import ERCOT_SCHEMA
from ...._pipeline_utils.temp_cert_files import TempCertFiles


class ERCOTDailyLoadISOSource(BaseISOSource):
    """
    The ERCOT Daily Load ISO Source is used to read daily load data from ERCOT using WebScrapping.
    It supports actual and forecast data. To read more about the reports, visit the following URLs
    (The urls are only accessible if the requester/client is in US)-

    For load type `actual`: [Actual System Load by Weather Zone](https://www.ercot.com/mp/data-products/
    data-product-details?id=NP6-345-CD)
    <br>
    For load type `forecast`: [Seven-Day Load Forecast by Weather Zone](https://www.ercot.com/mp/data-products/
    data-product-details?id=NP3-561-CD)


    Parameters:
        spark (SparkSession): Spark Session instance
        options (dict): A dictionary of ISO Source specific configurations (See Attributes table below)

    Attributes:
        load_type (list): Must be one of `actual` or `forecast`.
        date (str): Must be in `YYYY-MM-DD` format.
        certificate_pfx_key (str): The certificate key data or password received from ERCOT.
        certificate_pfx_key_contents (str): The certificate data received from ERCOT, it could be base64 encoded.

    Please check the BaseISOSource for available methods.

    BaseISOSource:
        ::: src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso.base_iso
    """

    spark: SparkSession
    options: dict
    url_forecast: str = "https://mis.ercot.com/misapp/GetReports.do?reportTypeId=12312"
    url_actual: str = "https://mis.ercot.com/misapp/GetReports.do?reportTypeId=13101"
    url_prefix: str = "https://mis.ercot.com"
    query_datetime_format: str = "%Y-%m-%d"
    required_options = [
        "load_type",
        "date",
        "certificate_pfx_key",
        "certificate_pfx_key_contents",
    ]
    spark_schema = ERCOT_SCHEMA
    default_query_timezone = "UTC"

    def __init__(self, spark: SparkSession, options: dict) -> None:
        super().__init__(spark, options)
        self.spark = spark
        self.options = options
        self.load_type = self.options.get("load_type", "actual")
        self.date = self.options.get("date", "").strip()
        self.certificate_pfx_key = self.options.get("certificate_pfx_key", "").strip()
        self.certificate_pfx_key_contents = self.options.get(
            "certificate_pfx_key_contents", ""
        ).strip()

    def generate_temp_client_cert_files_from_pfx(self):
        password = self.certificate_pfx_key.encode()
        pfx: bytes = base64.b64decode(self.certificate_pfx_key_contents)

        if base64.b64encode(pfx) != self.certificate_pfx_key_contents.encode():
            pfx = self.certificate_pfx_key_contents

        key, cert, _ = pkcs12.load_key_and_certificates(data=pfx, password=password)
        key_bytes = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )

        cert_bytes = cert.public_bytes(encoding=serialization.Encoding.PEM)
        return TempCertFiles(cert_bytes, key_bytes)

    def _pull_data(self) -> pd.DataFrame:
        """
        Pulls data from the ERCOT API and parses the zip files for CSV data.

        Returns:
            Raw form of data.
        """

        logging.info(f"Getting {self.load_type} data for date {self.date}")
        url = self.url_forecast
        req_date = datetime.strptime(self.date, self.query_datetime_format)

        if self.load_type == "actual":
            req_date = req_date + timedelta(days=1)
            url = self.url_actual

        url_lists, files = self.generate_urls_for_zip(url, req_date)
        dfs = []
        logging.info(f"Generated {len(url_lists)} URLs - {url_lists}")
        logging.info(f"Requesting files - {files}")

        for url in url_lists:
            df = self.download_zip(url)
            dfs.append(df)
        final_df = pd.concat(dfs)
        return final_df

    def download_zip(self, url) -> pd.DataFrame:
        logging.info(f"Downloading zip using {url}")
        with self.generate_temp_client_cert_files_from_pfx() as cert:
            response = requests.get(url, cert=cert)

        if not response.content:
            raise HTTPError("Empty Response was returned")

        logging.info("Unzipping the file")
        zf = ZipFile(BytesIO(response.content))
        csvs = [s for s in zf.namelist() if ".csv" in s]

        if len(csvs) == 0:
            raise ValueError("No data was found in the specified interval")

        df = pd.read_csv(zf.open(csvs[0]))
        return df

    def generate_urls_for_zip(self, url: str, date: datetime) -> (List[str], List[str]):
        logging.info(f"Finding urls list for date {date}")
        with self.generate_temp_client_cert_files_from_pfx() as cert:
            page_response = requests.get(url, timeout=5, cert=cert)

        page_content = BeautifulSoup(page_response.content, "html.parser")
        zip_info = []
        length = len(page_content.find_all("td", {"class": "labelOptional_ind"}))

        for i in range(0, length):
            zip_name = page_content.find_all("td", {"class": "labelOptional_ind"})[
                i
            ].text
            zip_link = page_content.find_all("a")[i].get("href")
            zip_info.append((zip_name, zip_link))

        date_str = date.strftime("%Y%m%d")
        zip_info = list(
            filter(
                lambda f_info: f_info[0].endswith("csv.zip") and date_str in f_info[0],
                zip_info,
            )
        )

        urls = []
        files = []

        if len(zip_info) == 0:
            raise ValueError(f"No file was found for date - {date_str}")

        # As Forecast is generated every hour, pick the latest one.
        zip_info = sorted(zip_info, key=lambda item: item[0], reverse=True)
        zip_info_item = zip_info[0]

        file_name, file_url = zip_info_item
        urls.append(self.url_prefix + file_url)
        files.append(file_name)

        return urls, files

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.load_type == "actual":
            df["Date"] = pd.to_datetime(df["OperDay"], format="%m/%d/%Y")

            df = df.rename(
                columns={
                    "COAST": "Coast",
                    "EAST": "East",
                    "FAR_WEST": "FarWest",
                    "NORTH": "North",
                    "NORTH_C": "NorthCentral",
                    "SOUTH_C": "SouthCentral",
                    "SOUTHERN": "Southern",
                    "WEST": "West",
                    "TOTAL": "SystemTotal",
                    "DSTFlag": "DstFlag",
                }
            )

        else:
            df = df.rename(columns={"DSTFlag": "DstFlag"})

            df["Date"] = pd.to_datetime(df["DeliveryDate"], format="%m/%d/%Y")

        return df

    def _validate_options(self) -> bool:
        try:
            datetime.strptime(self.date, self.query_datetime_format)
        except ValueError:
            raise ValueError(
                f"Unable to parse date. Please specify in {self.query_datetime_format} format."
            )
        return True
