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

import sys

sys.path.insert(0, ".")
from unittest.mock import patch
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.pi_omf import (
    SparkPIOMFDestination,
)
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.rest_api import (
    SparkRestAPIDestination,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    PyPiLibrary,
)
from pyspark.sql import SparkSession
from pytest_mock import MockerFixture
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
from pyspark.sql.functions import to_json, col
from requests.exceptions import HTTPError
import pytest
import os

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


class TestStreamingQueryClass:
    isActive: bool = False  # NOSONAR


tag_name = "Sample.Script.RTDIP7"
test_url = "https://test.com/api"
source_schema = StructType(
    [
        StructField("TagName", StringType(), True),
        StructField("EventTime", StringType(), True),
        StructField("Status", StringType(), False),
        StructField("Value", FloatType(), True),
        StructField("ValueType", StringType(), True),
        StructField("ChangeType", StringType(), False),
    ]
)

source_data = [
    {
        "TagName": tag_name,
        "EventTime": "2023-10-01T15:44:56Z",
        "Status": "Good",
        "Value": 7.3,
        "ValueType": "float",
        "ChangeType": "insert",
    },
    {
        "TagName": tag_name,
        "EventTime": "2023-09-30T06:56:00+00:00",
        "Status": "Good",
        "Value": 6.2,
        "ValueType": "float",
        "ChangeType": "insert",
    },
    {
        "TagName": "RTDIP8",
        "EventTime": "2023-09-30T07:58:01+00:00",
        "Status": "Good",
        "Value": 5.0,
        "ValueType": "float",
        "ChangeType": "insert",
    },
]


def test_spark_pi_omf_write_setup():
    delta_merge_destination = SparkPIOMFDestination(
        None, {}, test_url, "username", "password", 1, 1, create_type_message=False
    )
    assert delta_merge_destination.system_type().value == 2
    assert delta_merge_destination.libraries() == Libraries(
        pypi_libraries=[PyPiLibrary(name="requests", version="2.30.0", repo=None)],
        maven_libraries=[],
        pythonwheel_libraries=[],
    )
    assert isinstance(delta_merge_destination.settings(), dict)
    assert delta_merge_destination.pre_write_validation()
    assert delta_merge_destination.post_write_validation()


def test_spark_pi_omf_write_batch(spark_session: SparkSession, mocker: MockerFixture):
    mocked_response = mocker.Mock()
    mocked_response.status_code = 200
    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.destinations.spark.pi_omf.requests.post",
        return_value=mocked_response,
    )
    mocker.patch.object(SparkRestAPIDestination, "_api_micro_batch", return_value=True)
    mocked_container_execute = mocker.spy(SparkPIOMFDestination, "_setup_omf_execute")
    mocked_data_execute = mocker.spy(SparkRestAPIDestination, "_api_micro_batch")

    source_df = spark_session.createDataFrame(schema=source_schema, data=source_data)
    expected_schema = StructType(
        [
            StructField(
                "payload",
                ArrayType(
                    StructType(
                        [
                            StructField("containerid", StringType(), False),
                            StructField(
                                "values",
                                ArrayType(
                                    StructType(
                                        [
                                            StructField("time", StringType(), False),
                                            StructField("Value", FloatType(), False),
                                        ]
                                    ),
                                    False,
                                ),
                                False,
                            ),
                        ]
                    ),
                    False,
                ),
                True,
            )
        ]
    )

    expected_data = [
        {
            "payload": [
                {
                    "containerid": "RTDIP8",
                    "values": [{"time": "2023-09-30T07:58:01+00:00", "Value": 5.0}],
                }
            ]
        },
        {
            "payload": [
                {
                    "containerid": tag_name,
                    "values": [
                        {"time": "2023-10-01T15:44:56Z", "Value": 7.3},
                        {"time": "2023-09-30T06:56:00+00:00", "Value": 6.2},
                    ],
                }
            ]
        },
    ]
    expected_df = spark_session.createDataFrame(
        schema=expected_schema, data=expected_data
    )
    expected_df = expected_df.withColumn("payload", to_json(col("payload")))
    SparkPIOMFDestination(
        source_df, {}, test_url, "username", "password", 3, 1, create_type_message=False
    ).write_batch()
    mocked_container_execute.assert_called_once_with(
        mocker.ANY,
        data='[{"id": "Sample.Script.RTDIP7", "typeid": "RTDIPnumber"}, {"id": "RTDIP8", "typeid": "RTDIPnumber"}]',
        message_type="container",
    )
    mocked_data_execute.assert_called_once()
    _, kwargs = mocked_data_execute.call_args
    assert kwargs["micro_batch_df"].select("payload").collect() == expected_df.collect()


def test_spark_pi_omf_write_stream(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch(
        "pyspark.sql.DataFrame.writeStream",
        new_callable=mocker.Mock(
            return_value=mocker.Mock(
                trigger=mocker.Mock(
                    return_value=mocker.Mock(
                        foreachBatch=mocker.Mock(
                            return_value=mocker.Mock(
                                queryName=mocker.Mock(
                                    return_value=mocker.Mock(
                                        outputMode=mocker.Mock(
                                            return_value=mocker.Mock(
                                                options=mocker.Mock(
                                                    return_value=mocker.Mock(
                                                        start=mocker.Mock(
                                                            return_value=TestStreamingQueryClass()
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        ),
    )
    expected_df = spark_session.createDataFrame([{"id": "1"}])
    restapi_destination = SparkPIOMFDestination(
        expected_df,
        {},
        test_url,
        "username",
        "password",
        3,
        1,
        create_type_message=False,
    )
    actual = restapi_destination.write_stream()
    assert actual is None


def test_spark_rest_api_write_batch_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.destinations.spark.pi_omf.SparkPIOMFDestination._send_container_message",
        side_effect=Exception,
    )

    expected_df = spark_session.createDataFrame([{"id": "1"}])
    restapi_destination = SparkPIOMFDestination(
        expected_df,
        {},
        test_url,
        "username",
        "password",
        1,
        1,
        create_type_message=False,
    )
    with pytest.raises(Exception):
        restapi_destination.write_batch()


def test_spark_rest_api_write_stream_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    mocker.patch(
        "pyspark.sql.DataFrame.writeStream",
        new_callable=mocker.Mock(
            return_value=mocker.Mock(
                trigger=mocker.Mock(
                    return_value=mocker.Mock(
                        foreachBatch=mocker.Mock(
                            return_value=mocker.Mock(
                                queryName=mocker.Mock(
                                    return_value=mocker.Mock(
                                        outputMode=mocker.Mock(
                                            return_value=mocker.Mock(
                                                options=mocker.Mock(
                                                    return_value=mocker.Mock(
                                                        start=mocker.Mock(
                                                            side_effect=Exception
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        ),
    )
    expected_df = spark_session.createDataFrame([{"id": "1"}])
    restapi_destination = SparkPIOMFDestination(
        expected_df,
        {},
        test_url,
        "username",
        "password",
        2,
        2,
        create_type_message=False,
    )
    with pytest.raises(Exception):
        restapi_destination.write_stream()


def test_send_type_message_fails(spark_session: SparkSession, mocker: MockerFixture):
    mocked_response = mocker.Mock()
    mocked_response.status_code = 404
    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.destinations.spark.pi_omf.requests.post",
        return_value=mocked_response,
    )
    sample_df = spark_session.createDataFrame([{"id": "1"}])
    with pytest.raises(HTTPError):
        SparkPIOMFDestination(
            sample_df,
            {},
            test_url,
            "username",
            "password",
            2,
            2,
            compression=True,
            create_type_message=True,
        )
