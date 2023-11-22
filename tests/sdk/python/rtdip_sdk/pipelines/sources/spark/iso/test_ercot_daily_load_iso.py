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
import os
import sys

sys.path.insert(0, ".")
import pandas as pd
from requests import HTTPError

import pytest
from pyspark.sql import DataFrame, SparkSession
from pytest_mock import MockerFixture

from src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso import ERCOTDailyLoadISOSource
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.iso import ERCOT_SCHEMA
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries

dir_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")

api_urls_response_file = (
    f"{dir_path}/ercot_daily_load_raw_api_urls_response_actual.html"
)

iso_configuration = {
    "load_type": "actual",
    "date": "2023-11-17",
    "certificate_pfx_key": "SOME_KEY",
    "certificate_pfx_key_contents": "SOME_DATA",
}

patch_module_name = "requests.get"

url_actual = "https://mis.ercot.com/misapp/GetReports.do?reportTypeId=13101"


def test_ercot_daily_load_iso_read_setup(spark_session: SparkSession):
    iso_source = ERCOTDailyLoadISOSource(spark_session, iso_configuration)

    assert iso_source.system_type().value == 2
    assert iso_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )

    assert isinstance(iso_source.settings(), dict)
    expected_values = [
        "certificate_pfx_key",
        "certificate_pfx_key_contents",
        "date",
        "load_type",
    ]
    assert sorted(iso_source.required_options) == expected_values
    assert iso_source.pre_read_validation()


def mock_certificates(mocker):
    class Key:
        @staticmethod
        def private_bytes(*args, **kwargs) -> bytes:
            return b""

    class Cert:
        @staticmethod
        def public_bytes(*args, **kwargs) -> bytes:
            return b""

    mocker.patch(
        "cryptography.hazmat.primitives.serialization.pkcs12.load_key_and_certificates",
        side_effect=lambda *args, **kwargs: (Key(), Cert(), ""),
    )


def ercot_daily_load_iso_read_batch_test(
    spark_session: SparkSession,
    mocker: MockerFixture,
    load_type: str,
    url_to_match: str,
    file_download_url_to_match: str,
    urls_file: str,
    zip_file: str,
    date: str,
    expected_data_file: str,
    expected_rows_count: int = 24,
):
    iso_source = ERCOTDailyLoadISOSource(
        spark_session,
        {**iso_configuration, "load_type": load_type, "date": date},
    )

    with open(f"{dir_path}/{urls_file}", "rb") as file:
        urls_bytes = file.read()

    with open(f"{dir_path}/{zip_file}", "rb") as file:
        zip_download_bytes = file.read()

    class URLsResponse:
        content = urls_bytes
        status_code = 200

    class ZipDownloadResponse(URLsResponse):
        content = zip_download_bytes

    def get_response(url: str, *args, **kwargs):
        if url == url_to_match:
            return URLsResponse()
        else:
            assert url == file_download_url_to_match
            return ZipDownloadResponse()

    mocker.patch(patch_module_name, side_effect=get_response)
    mock_certificates(mocker)

    df = iso_source.read_batch()

    assert df.count() == expected_rows_count
    assert isinstance(df, DataFrame)
    assert str(df.schema) == str(ERCOT_SCHEMA)

    expected_df_spark = spark_session.createDataFrame(
        pd.read_csv(f"{dir_path}/{expected_data_file}", parse_dates=["Date"]),
        schema=ERCOT_SCHEMA,
    )

    cols = df.columns
    assert df.orderBy(cols).collect() == expected_df_spark.orderBy(cols).collect()


def test_ercot_daily_load_iso_read_batch_actual(
    spark_session: SparkSession, mocker: MockerFixture
):
    ercot_daily_load_iso_read_batch_test(
        spark_session,
        mocker,
        load_type="actual",
        url_to_match=url_actual,
        file_download_url_to_match=(
            "https://mis.ercot.com/misdownload/servlets/mirDownload?mimic_duns=1118490502000"
            "&doclookupId=959096531"
        ),
        urls_file="ercot_daily_load_raw_api_urls_response_actual.html",
        zip_file="ercot_daily_load_actual_sample1.zip",
        expected_data_file="ercot_daily_load_actual_expected.csv",
        date="2023-11-17",
    )


def test_ercot_daily_load_iso_read_batch_forecast(
    spark_session: SparkSession, mocker: MockerFixture
):
    ercot_daily_load_iso_read_batch_test(
        spark_session,
        mocker,
        load_type="forecast",
        url_to_match="https://mis.ercot.com/misapp/GetReports.do?reportTypeId=12312",
        file_download_url_to_match=(
            "https://mis.ercot.com/misdownload/servlets/mirDownload?mimic_duns=1118490502000"
            "&doclookupId=959500285"
        ),
        urls_file="ercot_daily_load_raw_api_urls_response_forecast.html",
        zip_file="ercot_daily_load_forecast_sample1.zip",
        date="2023-11-19",
        expected_data_file="ercot_daily_load_forecast_expected.csv",
        expected_rows_count=192,
    )


def test_ercot_daily_load_iso_empty_response(
    spark_session: SparkSession, mocker: MockerFixture
):
    iso_source = ERCOTDailyLoadISOSource(
        spark_session,
        {**iso_configuration, "load_type": "actual", "date": "2023-11-16"},
    )

    with open(api_urls_response_file, "rb") as file:
        empty_urls_bytes = file.read()

    class EmptyURLsResponse:
        content = empty_urls_bytes
        status_code = 200

    class EmptyZipDownloadResponse(EmptyURLsResponse):
        content = b""

    def get_response(url: str, *args, **kwargs):
        if url == url_actual:
            return EmptyURLsResponse()
        else:
            return EmptyZipDownloadResponse()

    mocker.patch(patch_module_name, side_effect=get_response)
    mock_certificates(mocker)

    with pytest.raises(HTTPError) as exc_info:
        iso_source.read_batch()

    assert str(exc_info.value) == "Empty Response was returned"


def test_ercot_daily_load_iso_no_file(
    spark_session: SparkSession, mocker: MockerFixture
):
    iso_source = ERCOTDailyLoadISOSource(
        spark_session,
        {**iso_configuration, "date": "2023-09-16"},
    )

    with open(api_urls_response_file, "rb") as file:
        no_file_urls_bytes = file.read()

    with open(f"{dir_path}/ercot_daily_load_actual_sample1.zip", "rb") as file:
        no_file_zip_download_bytes = file.read()

    class NoFileURLsResponse:
        content = no_file_urls_bytes
        status_code = 200

    class NoFileZipDownloadResponse(NoFileURLsResponse):
        content = no_file_zip_download_bytes

    def get_response(url: str, *args, **kwargs):
        if url == url_actual:
            return NoFileURLsResponse()
        else:
            return NoFileZipDownloadResponse()

    mocker.patch(patch_module_name, side_effect=get_response)
    mock_certificates(mocker)

    with pytest.raises(ValueError) as exc_info:
        iso_source.read_batch()

    assert str(exc_info.value) == "No file was found for date - 20230917"


def test_ercot_daily_load_iso_no_data(
    spark_session: SparkSession, mocker: MockerFixture
):
    iso_source = ERCOTDailyLoadISOSource(
        spark_session,
        {**iso_configuration},
    )

    with open(api_urls_response_file, "rb") as file:
        no_data_urls_bytes = file.read()

    with open(f"{dir_path}/ercot_daily_load_actual_sample2.zip", "rb") as file:
        no_data_zip_download_bytes = file.read()

    class NoDataURLsResponse:
        content = no_data_urls_bytes
        status_code = 200

    class NoDataZipDownloadResponse(NoDataURLsResponse):
        content = no_data_zip_download_bytes

    def get_response(url: str, *args, **kwargs):
        if url == url_actual:
            return NoDataURLsResponse()
        else:
            return NoDataZipDownloadResponse()

    mocker.patch(patch_module_name, side_effect=get_response)
    mock_certificates(mocker)

    with pytest.raises(ValueError) as exc_info:
        iso_source.read_batch()

    assert str(exc_info.value) == "No data was found in the specified interval"


def test_ercot_daily_load_iso_iso_invalid_date(spark_session: SparkSession):
    with pytest.raises(ValueError) as exc_info:
        iso_source = ERCOTDailyLoadISOSource(
            spark_session, {**iso_configuration, "date": "2023/11/01"}
        )
        iso_source.pre_read_validation()

    expected = "Unable to parse date. Please specify in %Y-%m-%d format."
    assert str(exc_info.value) == expected
