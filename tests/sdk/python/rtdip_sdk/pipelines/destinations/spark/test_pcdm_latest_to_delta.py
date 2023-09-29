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
import pytest
from src.sdk.python.rtdip_sdk._sdk_utils.compare_versions import _get_package_version
from src.sdk.python.rtdip_sdk.pipelines.destinations import (
    SparkPCDMLatestToDeltaDestination,
)
from src.sdk.python.rtdip_sdk.pipelines.utilities.spark.delta_table_create import (
    DeltaTableCreateUtility,
    DeltaTableColumn,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    MavenLibrary,
)
from pyspark.sql import SparkSession
from pytest_mock import MockerFixture
from datetime import datetime


class MockDeltaDestination:
    def write_stream(self):
        return None


class TestStreamingQueryClass:
    isActive: bool = False  # NOSONAR


def create_delta_table(spark_session, name):
    table_create_utility = DeltaTableCreateUtility(
        spark=spark_session,
        table_name=name,
        columns=[
            DeltaTableColumn(name="TagName", type="string", nullable=True),
            DeltaTableColumn(name="EventTime", type="timestamp", nullable=True),
            DeltaTableColumn(name="Status", type="string", nullable=True),
            DeltaTableColumn(name="Value", type="string", nullable=True),
            DeltaTableColumn(name="ValueType", type="string", nullable=True),
            DeltaTableColumn(name="GoodEventTime", type="timestamp", nullable=True),
            DeltaTableColumn(name="GoodValue", type="string", nullable=True),
            DeltaTableColumn(name="GoodValueType", type="string", nullable=True),
        ],
        properties={
            "delta.logRetentionDuration": "7 days",
            "delta.enableChangeDataFeed": "true",
        },
    )

    table_create_utility.execute()


def test_spark_pcdm_latest_to_delta_write_setup(spark_session: SparkSession):
    pcdm_latest_o_delta_destination = SparkPCDMLatestToDeltaDestination(
        spark_session,
        None,
        {},
        "test_delta_latest_destination_setup",
    )
    assert pcdm_latest_o_delta_destination.system_type().value == 2
    assert pcdm_latest_o_delta_destination.libraries() == Libraries(
        maven_libraries=[
            MavenLibrary(
                group_id="io.delta",
                artifact_id="delta-core_2.12",
                version=_get_package_version("delta-spark"),
            )
        ],
        pypi_libraries=[],
        pythonwheel_libraries=[],
    )
    assert isinstance(pcdm_latest_o_delta_destination.settings(), dict)
    assert pcdm_latest_o_delta_destination.pre_write_validation()
    assert pcdm_latest_o_delta_destination.post_write_validation()


def test_spark_pcdm_latest_to_delta_write_batch(spark_session: SparkSession):
    create_delta_table(spark_session, "test_spark_pcdm_latest_to_delta_write_batch")

    test_df = spark_session.createDataFrame(
        [
            {
                "EventDate": datetime(2023, 1, 20).date(),
                "TagName": "Tag1",
                "EventTime": datetime(2023, 1, 20, 1, 0),
                "Status": "Good",
                "Value": "1.01",
                "ValueType": "float",
                "ChangeType": "insert",
            },
            {
                "EventDate": datetime(2023, 1, 20).date(),
                "TagName": "Tag1",
                "EventTime": datetime(2023, 1, 20, 1, 1),
                "Status": "Good",
                "Value": "1.02",
                "ValueType": "float",
                "ChangeType": "insert",
            },
        ]
    )

    expected_df = spark_session.createDataFrame(
        [
            {
                "TagName": "Tag1",
                "EventTime": datetime(2023, 1, 20, 1, 1),
                "Status": "Good",
                "Value": "1.02",
                "ValueType": "float",
                "GoodEventTime": datetime(2023, 1, 20, 1, 1),
                "GoodValue": "1.02",
                "GoodValueType": "float",
            }
        ]
    ).select(
        "TagName",
        "EventTime",
        "Status",
        "Value",
        "ValueType",
        "GoodEventTime",
        "GoodValue",
        "GoodValueType",
    )

    pcdm_latest_to_delta_destination = SparkPCDMLatestToDeltaDestination(
        spark_session,
        test_df,
        {},
        "test_spark_pcdm_latest_to_delta_write_batch",
    )
    pcdm_latest_to_delta_destination.write_batch()

    actual_df = spark_session.table("test_spark_pcdm_latest_to_delta_write_batch")
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()


def test_spark_pcdm_to_delta_write_stream_merge(
    spark_session: SparkSession, mocker: MockerFixture
):
    mocker.patch(
        "pyspark.sql.DataFrame.writeStream",
        new_callable=mocker.Mock(
            return_value=mocker.Mock(
                trigger=mocker.Mock(
                    return_value=mocker.Mock(
                        format=mocker.Mock(
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
                )
            )
        ),
    )
    expected_df = spark_session.createDataFrame(
        [
            {
                "EventDate": datetime(2023, 1, 20).date(),
                "TagName": "Tag1",
                "EventTime": datetime(2023, 1, 20, 1, 0),
                "Status": "Good",
                "Value": "1.02",
                "ValueType": "float",
                "ChangeType": "insert",
            },
        ]
    )
    pcdm_latest_to_delta_destination = SparkPCDMLatestToDeltaDestination(
        spark_session,
        expected_df,
        {},
        "test_spark_pcdm_to_delta_write_stream",
    )
    actual = pcdm_latest_to_delta_destination.write_stream()
    assert actual is None


def test_spark_pcdm_latest_to_delta_write_batch_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.destinations.spark.pcdm_latest_to_delta",
        new_callable=mocker.Mock(
            return_value=mocker.Mock(
                SparkDeltaMergeDestination=mocker.Mock(
                    return_value=mocker.Mock(
                        _write_data_by_type=mocker.Mock(side_effect=Exception)
                    )
                )
            )
        ),
    )
    expected_df = spark_session.createDataFrame([{"id": "1"}])
    pcdm_to_delta_destination = SparkPCDMLatestToDeltaDestination(
        spark_session,
        expected_df,
        {},
        "test_spark_pcdm_latest_to_delta_write_batch",
    )
    with pytest.raises(Exception):
        pcdm_to_delta_destination.write_batch()


def test_spark_pcdm_to_delta_write_stream_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    mocker.patch(
        "pyspark.sql.DataFrame.writeStream",
        new_callable=mocker.Mock(
            return_value=mocker.Mock(
                trigger=mocker.Mock(
                    return_value=mocker.Mock(
                        format=mocker.Mock(
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
                )
            )
        ),
    )
    expected_df = spark_session.createDataFrame([{"id": "1"}])
    pcdm_to_delta_destination = SparkPCDMLatestToDeltaDestination(
        spark_session,
        expected_df,
        {},
        "test_spark_pcdm_latest_to_delta_write_stream",
    )
    with pytest.raises(Exception):
        pcdm_to_delta_destination.write_stream()
