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
import io
import json
import sys
from io import StringIO

import numpy as np
import pandas as pd
from requests import HTTPError

from rtdip_sdk.pipelines._pipeline_utils.iso import PJM_PRICING_SCHEMA

sys.path.insert(0, ".")
import pytest
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso import (
    PJMDailyPricingISOSource,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from pyspark.sql import SparkSession, DataFrame
from pytest_mock import MockerFixture

iso_configuration = {"load_type": "real_time", "api_key": "SAMPLE"}

raw_api_rt_response = """
{
    "links": [
        {
            "rel": "self",
            "href": "https://api.pjm.com/api/v1/rt_hrl_lmps?RowCount=5&Order=Asc&StartRow=1&IsActiveMetadata=True&Fields=congestion_price_rt%2Cdatetime_beginning_ept%2Cdatetime_beginning_utc%2Cequipment%2Cmarginal_loss_price_rt%2Cpnode_id%2Cpnode_name%2Crow_is_current%2Csystem_energy_price_rt%2Ctotal_lmp_rt%2Ctype%2Cversion_nbr%2Cvoltage%2Czone&datetime_beginning_ept=2024-02-20%2000%3A00%3A00to2024-02-20%2023%3A00%3A00"
        },
        
        {
            "rel": "metadata",
            "href": "https://api.pjm.com/api/v1/rt_hrl_lmps/metadata"
        }
    ],
    "items": [
        {
            "datetime_beginning_utc": "2024-02-20T05:00:00",
            "datetime_beginning_ept": "2024-02-20T00:00:00",
            "pnode_id": 1,
            "pnode_name": "PJM-RTO",
            "voltage": null,
            "equipment": null,
            "type": "ZONE",
            "zone": null,
            "system_energy_price_rt": 17.940000,
            "total_lmp_rt": 17.956076,
            "congestion_price_rt": 0.005357,
            "marginal_loss_price_rt": 0.011552,
            "row_is_current": true,
            "version_nbr": 1
        },
        {
            "datetime_beginning_utc": "2024-02-20T05:00:00",
            "datetime_beginning_ept": "2024-02-20T00:00:00",
            "pnode_id": 3,
            "pnode_name": "MID-ATL/APS",
            "voltage": null,
            "equipment": null,
            "type": "ZONE",
            "zone": null,
            "system_energy_price_rt": 17.940000,
            "total_lmp_rt": 18.101796,
            "congestion_price_rt": -0.133939,
            "marginal_loss_price_rt": 0.296568,
            "row_is_current": true,
            "version_nbr": 1
        }
    ],
    "searchSpecification": {
        "rowCount": 5,
        "order": "Asc",
        "startRow": 1,
        "isActiveMetadata": true,
        "fields": [
            "congestion_price_rt",
            "datetime_beginning_ept",
            "datetime_beginning_utc",
            "equipment",
            "marginal_loss_price_rt",
            "pnode_id",
            "pnode_name",
            "row_is_current",
            "system_energy_price_rt",
            "total_lmp_rt",
            "type",
            "version_nbr",
            "voltage",
            "zone"
        ],
        "filters": [
            {
                "datetime_beginning_ept": "2024-02-20 00:00:00to2024-02-20 23:00:00"
            }
        ]
    },
    "totalRows": 325176
}
"""

raw_api_dh_response = """
{
    "links": [
        {
            "rel": "self",
            "href": "https://api.pjm.com/api/v1/da_hrl_lmps?RowCount=5&Order=Asc&StartRow=1&IsActiveMetadata=True&Fields=congestion_price_da%2Cdatetime_beginning_ept%2Cdatetime_beginning_utc%2Cequipment%2Cmarginal_loss_price_da%2Cpnode_id%2Cpnode_name%2Crow_is_current%2Csystem_energy_price_da%2Ctotal_lmp_da%2Ctype%2Cversion_nbr%2Cvoltage%2Czone&datetime_beginning_ept=2024-02-20%2000%3A00%3A00to2024-02-20%2023%3A00%3A00"
        },
        {
            "rel": "metadata",
            "href": "https://api.pjm.com/api/v1/da_hrl_lmps/metadata"
        }
    ],
    "items": [
        {
            "datetime_beginning_utc": "2024-02-20T05:00:00",
            "datetime_beginning_ept": "2024-02-20T00:00:00",
            "pnode_id": 1,
            "pnode_name": "PJM-RTO",
            "voltage": null,
            "equipment": null,
            "type": "ZONE",
            "zone": null,
            "system_energy_price_da": 23.49,
            "total_lmp_da": 23.632196,
            "congestion_price_da": -0.045139,
            "marginal_loss_price_da": 0.187335,
            "row_is_current": true,
            "version_nbr": 1
        },
        {
            "datetime_beginning_utc": "2024-02-20T05:00:00",
            "datetime_beginning_ept": "2024-02-20T00:00:00",
            "pnode_id": 3,
            "pnode_name": "MID-ATL/APS",
            "voltage": null,
            "equipment": null,
            "type": "ZONE",
            "zone": null,
            "system_energy_price_da": 23.49,
            "total_lmp_da": 23.679954,
            "congestion_price_da": -0.277542,
            "marginal_loss_price_da": 0.467496,
            "row_is_current": true,
            "version_nbr": 1
        }
    ],
    "searchSpecification": {
        "rowCount": 5,
        "order": "Asc",
        "startRow": 1,
        "isActiveMetadata": true,
        "fields": [
            "congestion_price_da",
            "datetime_beginning_ept",
            "datetime_beginning_utc",
            "equipment",
            "marginal_loss_price_da",
            "pnode_id",
            "pnode_name",
            "row_is_current",
            "system_energy_price_da",
            "total_lmp_da",
            "type",
            "version_nbr",
            "voltage",
            "zone"
        ],
        "filters": [
            {
                "datetime_beginning_ept": "2024-02-20 00:00:00to2024-02-20 23:00:00"
            }
        ]
    },
    "totalRows": 325176
}
"""


expected_rt_data = (
    "StartTime,PnodeId,PnodeName,Voltage,Equipment,Type,Zone,"
    "SystemEnergyPrice,TotalLmp,CongestionPrice,MarginalLossPrice,VersionNbr\n"
    "2024-02-20 05:00:00,1,PJM-RTO,,,ZONE,,17.94,17.956076,0.005357,0.011552,1\n"
    "2024-02-20 05:00:00,3,MID-ATL/APS,,,ZONE,,17.94,18.101796,-0.133939,0.296568,1"
)
expected_dh_data = (
    "StartTime,PnodeId,PnodeName,Voltage,Equipment,Type,Zone,SystemEnergyPrice,"
    "TotalLmp,CongestionPrice,MarginalLossPrice,VersionNbr\n"
    "2024-02-20 05:00:00,1,PJM-RTO,,,ZONE,,23.49,23.632196,-0.045139,0.187335,1\n"
    "2024-02-20 05:00:00,3,MID-ATL/APS,,,ZONE,,23.49,23.679954,-0.277542,0.467496,1\n"
)


patch_module_name = "requests.get"


def test_pjm_daily_pricing_iso_read_setup(spark_session: SparkSession):
    iso_source = PJMDailyPricingISOSource(spark_session, iso_configuration)

    assert iso_source.system_type().value == 2
    assert iso_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )

    assert isinstance(iso_source.settings(), dict)

    assert sorted(iso_source.required_options) == ["api_key", "load_type"]
    assert iso_source.pre_read_validation()


def test_pjm_daily_pricing_iso_read_real_time(
    spark_session: SparkSession, mocker: MockerFixture
):
    iso_source = PJMDailyPricingISOSource(
        spark_session, {**iso_configuration, "load_type": "real_time"}
    )

    sample_bytes = bytes(raw_api_rt_response.encode("utf-8"))

    class MyResponse:
        content = sample_bytes
        status_code = 200

        def json(self) -> dict:
            return json.load(io.BytesIO(self.content))

    def get_response(url: str, headers: dict):
        assert url.startswith("https://api.pjm.com/api/v1/")
        assert headers == {"Ocp-Apim-Subscription-Key": "SAMPLE"}
        return MyResponse()

    mocker.patch(patch_module_name, side_effect=get_response)

    df = iso_source.read_batch()

    assert df.count() == 2
    assert isinstance(df, DataFrame)
    assert str(df.schema) == str(PJM_PRICING_SCHEMA)

    expected_df_spark = spark_session.createDataFrame(
        pd.read_csv(
            StringIO(expected_rt_data),
            parse_dates=["StartTime"],
        ).replace({np.NAN: None}),
        schema=PJM_PRICING_SCHEMA,
    )

    cols = df.columns
    assert df.orderBy(cols).collect() == expected_df_spark.orderBy(cols).collect()


def test_pjm_daily_pricing_iso_read_day_ahead(
    spark_session: SparkSession, mocker: MockerFixture
):
    iso_source = PJMDailyPricingISOSource(
        spark_session, {**iso_configuration, "load_type": "day_ahead"}
    )

    sample_bytes = bytes(raw_api_dh_response.encode("utf-8"))

    class MyResponse:
        content = sample_bytes
        status_code = 200

        def json(self) -> dict:
            return json.load(io.BytesIO(self.content))

    def get_response(url: str, headers: dict):
        assert url.startswith("https://api.pjm.com/api/v1/")
        assert headers == {"Ocp-Apim-Subscription-Key": "SAMPLE"}
        return MyResponse()

    mocker.patch(patch_module_name, side_effect=get_response)

    df = iso_source.read_batch()

    assert df.count() == 2
    assert isinstance(df, DataFrame)
    assert str(df.schema) == str(PJM_PRICING_SCHEMA)

    expected_df_spark = spark_session.createDataFrame(
        pd.read_csv(
            StringIO(expected_dh_data),
            parse_dates=["StartTime"],
        ).replace({np.NAN: None}),
        schema=PJM_PRICING_SCHEMA,
    )

    cols = df.columns
    assert df.orderBy(cols).collect() == expected_df_spark.orderBy(cols).collect()


def test_pjm_daily_load_iso_iso_fetch_url_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    base_iso_source = PJMDailyPricingISOSource(spark_session, iso_configuration)
    sample_bytes = bytes("Unknown Error".encode("utf-8"))

    class MyResponse:
        content = sample_bytes
        status_code = 401

    mock_res = MyResponse()
    mocker.patch(patch_module_name, side_effect=lambda url, headers: mock_res)

    with pytest.raises(HTTPError) as exc_info:
        base_iso_source.read_batch()

    expected = (
        "Unable to access URL `https://api.pjm.com/api/v1/rt_hrl_lmps?startRow=1&rowCount=5"
        "&datetime_beginning_ept=2024-03-29 00:00to2024-04-01 23:00`."
        " Received status code 401 with message b'Unknown Error'"
    )
    assert str(exc_info.value) == expected


def test_pjm_daily_load_iso_invalid_load_type(spark_session: SparkSession):
    with pytest.raises(ValueError) as exc_info:
        iso_source = PJMDailyPricingISOSource(
            spark_session, {**iso_configuration, "load_type": "both"}
        )
        iso_source.pre_read_validation()

    assert (
        str(exc_info.value)
        == "Invalid load_type `both` given. Supported values are ['real_time', 'day_ahead']."
    )
