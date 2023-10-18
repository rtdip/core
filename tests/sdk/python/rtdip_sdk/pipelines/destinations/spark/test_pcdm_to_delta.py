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
from semver.version import Version
import pytest
from src.sdk.python.rtdip_sdk._sdk_utils.compare_versions import (
    _get_package_version,
    _get_python_package_version,
)
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.delta import (
    SparkDeltaDestination,
)
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.pcdm_to_delta import (
    SparkPCDMToDeltaDestination,
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
from pyspark.sql.functions import col
from pytest_mock import MockerFixture
from datetime import datetime


class MockDeltaDestination:
    def write_stream(self):
        return None


class TestStreamingQueryClass:
    isActive: bool = False  # NOSONAR


def create_delta_table(spark_session, name, value_type):
    table_create_utility = DeltaTableCreateUtility(
        spark=spark_session,
        table_name=name,
        columns=[
            DeltaTableColumn(
                name="EventDate",
                type="date",
                nullable=True,
                metadata={"delta.generationExpression": "CAST(EventTime AS DATE)"},
            ),
            DeltaTableColumn(name="TagName", type="string", nullable=True),
            DeltaTableColumn(name="EventTime", type="timestamp", nullable=True),
            DeltaTableColumn(name="Status", type="string", nullable=True),
            DeltaTableColumn(name="Value", type=value_type, nullable=True),
        ],
        partitioned_by=["EventDate"],
        properties={
            "delta.logRetentionDuration": "7 days",
            "delta.enableChangeDataFeed": "true",
        },
    )
    table_create_utility.execute()


def test_spark_pcdm_to_delta_write_setup(spark_session: SparkSession):
    pcdm_to_delta_destination = SparkPCDMToDeltaDestination(
        spark_session,
        None,
        "test_delta_merge_destination_setup_float",
        "test_delta_merge_destination_setup_string",
        {},
        "append",
    )
    assert pcdm_to_delta_destination.system_type().value == 2
    delta_spark_artifact_id = "delta-core_2.12"
    if (
        Version.compare(
            _get_python_package_version("delta-spark"), Version.parse("3.0.0")
        )
        >= 0
    ):
        delta_spark_artifact_id = "delta-spark_2.12"
    assert pcdm_to_delta_destination.libraries() == Libraries(
        maven_libraries=[
            MavenLibrary(
                group_id="io.delta",
                artifact_id=delta_spark_artifact_id,
                version=_get_package_version("delta-spark"),
            )
        ],
        pypi_libraries=[],
        pythonwheel_libraries=[],
    )
    assert isinstance(pcdm_to_delta_destination.settings(), dict)
    assert pcdm_to_delta_destination.pre_write_validation()
    assert pcdm_to_delta_destination.post_write_validation()


def test_spark_pcdm_to_delta_write_batch_append(spark_session: SparkSession):
    create_delta_table(
        spark_session, "test_spark_pcdm_to_delta_write_batch_append_float", "float"
    )
    create_delta_table(
        spark_session, "test_spark_pcdm_to_delta_write_batch_append_string", "string"
    )
    test_df = spark_session.createDataFrame(
        [
            {
                "TagName": "Tag1",
                "EventTime": datetime(2023, 1, 20, 1, 0),
                "Status": "Good",
                "Value": "1.01",
                "ValueType": "float",
                "ChangeType": "insert",
            },
            {
                "TagName": "Tag2",
                "EventTime": datetime(2023, 1, 20, 1, 0),
                "Status": "Good",
                "Value": "test",
                "ValueType": "string",
                "ChangeType": "insert",
            },
        ]
    )
    expected_float_df = (
        spark_session.createDataFrame(
            [
                {
                    "EventDate": datetime(2023, 1, 20).date(),
                    "TagName": "Tag1",
                    "EventTime": datetime(2023, 1, 20, 1, 0),
                    "Status": "Good",
                    "Value": float(1.01),
                }
            ]
        )
        .withColumn("Value", col("Value").cast("float"))
        .select("EventDate", "TagName", "EventTime", "Status", "Value")
    )
    expected_string_df = spark_session.createDataFrame(
        [
            {
                "EventDate": datetime(2023, 1, 20).date(),
                "TagName": "Tag2",
                "EventTime": datetime(2023, 1, 20, 1, 0),
                "Status": "Good",
                "Value": "test",
            }
        ]
    ).select("EventDate", "TagName", "EventTime", "Status", "Value")
    pcdm_to_delta_destination = SparkPCDMToDeltaDestination(
        spark_session,
        test_df,
        {},
        "test_spark_pcdm_to_delta_write_batch_append_float",
        "test_spark_pcdm_to_delta_write_batch_append_string",
        mode="overwrite",
        merge=False,
    )
    pcdm_to_delta_destination.write_batch()

    actual_float_df = spark_session.table(
        "test_spark_pcdm_to_delta_write_batch_append_float"
    )
    assert expected_float_df.schema == actual_float_df.schema
    assert expected_float_df.collect() == actual_float_df.collect()

    actual_string_df = spark_session.table(
        "test_spark_pcdm_to_delta_write_batch_append_string"
    )
    assert expected_string_df.schema == actual_string_df.schema
    assert expected_string_df.collect() == actual_string_df.collect()


def test_spark_pcdm_to_delta_write_batch_merge(spark_session: SparkSession):
    create_delta_table(
        spark_session, "test_spark_pcdm_to_delta_write_batch_merge_float", "float"
    )
    create_data_float_df = spark_session.createDataFrame(
        [
            {
                "EventDate": datetime(2023, 1, 20).date(),
                "TagName": "Tag1",
                "EventTime": datetime(2023, 1, 20, 1, 0),
                "Status": "Good",
                "Value": float(1.01),
            },
            {
                "EventDate": datetime(2023, 1, 20).date(),
                "TagName": "Tag3",
                "EventTime": datetime(2023, 1, 20, 2, 0),
                "Status": "Good",
                "Value": float(1.05),
            },
        ]
    ).withColumn("Value", col("Value").cast("float"))
    delta_destination = SparkDeltaDestination(
        create_data_float_df,
        {},
        "test_spark_pcdm_to_delta_write_batch_merge_float",
        "overwrite",
    )
    delta_destination.write_batch()

    create_delta_table(
        spark_session, "test_spark_pcdm_to_delta_write_batch_merge_string", "string"
    )
    create_data_string_df = spark_session.createDataFrame(
        [
            {
                "EventDate": datetime(2023, 1, 20).date(),
                "TagName": "Tag2",
                "EventTime": datetime(2023, 1, 20, 1, 0),
                "Status": "Good",
                "Value": "test1",
            },
            {
                "EventDate": datetime(2023, 1, 20).date(),
                "TagName": "Tag4",
                "EventTime": datetime(2023, 1, 20, 4, 0),
                "Status": "Good",
                "Value": "test2",
            },
        ]
    )
    delta_destination = SparkDeltaDestination(
        create_data_string_df,
        {},
        "test_spark_pcdm_to_delta_write_batch_merge_string",
        "overwrite",
    )
    delta_destination.write_batch()

    test_df = spark_session.createDataFrame(
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
            {
                "EventDate": datetime(2023, 1, 20).date(),
                "TagName": "Tag3",
                "EventTime": datetime(2023, 1, 20, 2, 0),
                "Status": "Good",
                "Value": "1.05",
                "ValueType": "float",
                "ChangeType": "delete",
            },
            {
                "EventDate": datetime(2023, 1, 20).date(),
                "TagName": "Tag2",
                "EventTime": datetime(2023, 1, 20, 1, 0),
                "Status": "Good",
                "Value": "test",
                "ValueType": "string",
                "ChangeType": "insert",
            },
            {
                "EventDate": datetime(2023, 1, 20).date(),
                "TagName": "Tag4",
                "EventTime": datetime(2023, 1, 20, 4, 0),
                "Status": "Good",
                "Value": "test",
                "ValueType": "string",
                "ChangeType": "delete",
            },
        ]
    )
    expected_float_df = (
        spark_session.createDataFrame(
            [
                {
                    "EventDate": datetime(2023, 1, 20).date(),
                    "TagName": "Tag1",
                    "EventTime": datetime(2023, 1, 20, 1, 0),
                    "Status": "Good",
                    "Value": float(1.02),
                }
            ]
        )
        .withColumn("Value", col("Value").cast("float"))
        .select("EventDate", "TagName", "EventTime", "Status", "Value")
    )
    expected_string_df = spark_session.createDataFrame(
        [
            {
                "EventDate": datetime(2023, 1, 20).date(),
                "TagName": "Tag2",
                "EventTime": datetime(2023, 1, 20, 1, 0),
                "Status": "Good",
                "Value": "test",
            }
        ]
    ).select("EventDate", "TagName", "EventTime", "Status", "Value")
    pcdm_to_delta_destination = SparkPCDMToDeltaDestination(
        spark_session,
        test_df,
        {},
        "test_spark_pcdm_to_delta_write_batch_merge_float",
        "test_spark_pcdm_to_delta_write_batch_merge_string",
        mode="update",
        merge=True,
    )
    pcdm_to_delta_destination.write_batch()
    actual_float_df = spark_session.table(
        "test_spark_pcdm_to_delta_write_batch_merge_float"
    )
    assert expected_float_df.schema == actual_float_df.schema
    assert expected_float_df.collect() == actual_float_df.collect()

    actual_string_df = spark_session.table(
        "test_spark_pcdm_to_delta_write_batch_merge_string"
    )
    assert expected_string_df.schema == actual_string_df.schema
    assert expected_string_df.collect() == actual_string_df.collect()


def test_spark_pcdm_to_delta_write_stream_append(
    spark_session: SparkSession, mocker: MockerFixture
):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.destinations.spark.pcdm_to_delta.SparkDeltaDestination",
        return_value=MockDeltaDestination(),
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
    pcdm_to_delta_destination = SparkPCDMToDeltaDestination(
        spark_session,
        expected_df,
        {},
        "test_spark_pcdm_to_delta_write_batch_merge_float",
        "test_spark_pcdm_to_delta_write_batch_merge_string",
        mode="append",
        merge=False,
    )
    actual = pcdm_to_delta_destination.write_stream()
    assert actual is None


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
    pcdm_to_delta_destination = SparkPCDMToDeltaDestination(
        spark_session,
        expected_df,
        {},
        "test_spark_pcdm_to_delta_write_batch_merge_float",
        "test_spark_pcdm_to_delta_write_batch_merge_string",
        mode="update",
        merge=True,
    )
    actual = pcdm_to_delta_destination.write_stream()
    assert actual is None


def test_spark_pcdm_to_delta_write_batch_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.destinations.spark.pcdm_to_delta",
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
    pcdm_to_delta_destination = SparkPCDMToDeltaDestination(
        spark_session,
        expected_df,
        {},
        "test_spark_pcdm_to_delta_write_batch_merge_float",
        "test_spark_pcdm_to_delta_write_batch_merge_string",
        mode="update",
        merge=True,
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
    pcdm_to_delta_destination = SparkPCDMToDeltaDestination(
        spark_session,
        expected_df,
        {},
        "test_spark_pcdm_to_delta_write_batch_merge_float",
        "test_spark_pcdm_to_delta_write_batch_merge_string",
        mode="update",
        merge=True,
    )
    with pytest.raises(Exception):
        pcdm_to_delta_destination.write_stream()
