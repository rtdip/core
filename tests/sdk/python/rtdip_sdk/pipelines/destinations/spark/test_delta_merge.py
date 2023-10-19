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
from importlib_metadata import version
from src.sdk.python.rtdip_sdk._sdk_utils.compare_versions import (
    _get_python_package_version,
    _package_version_meets_minimum,
    _get_package_version,
)
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.delta_merge import (
    SparkDeltaMergeDestination,
    DeltaMergeCondition,
    DeltaMergeConditionValues,
)
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.delta import (
    SparkDeltaDestination,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    MavenLibrary,
)
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
from pytest_mock import MockerFixture


class TestStreamingQueryClass:
    isActive: bool = False


MERGE_CONDITION = "source.id = target.id"
MOCKER_WRITESTREAM = "pyspark.sql.DataFrame.writeStream"


def test_spark_delta_merge_write_setup(spark_session: SparkSession):
    delta_merge_destination = SparkDeltaMergeDestination(
        spark_session, None, "test_delta_merge_destination_setup", {}, "1=2"
    )
    assert delta_merge_destination.system_type().value == 2
    delta_spark_artifact_id = "delta-core_2.12"
    if (
        Version.compare(
            _get_python_package_version("delta-spark"), Version.parse("3.0.0")
        )
        >= 0
    ):
        delta_spark_artifact_id = "delta-spark_2.12"
    assert delta_merge_destination.libraries() == Libraries(
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
    assert isinstance(delta_merge_destination.settings(), dict)
    assert delta_merge_destination.pre_write_validation()
    assert delta_merge_destination.post_write_validation()


def test_spark_delta_merge_update_write_batch(spark_session: SparkSession):
    create_table_df = spark_session.createDataFrame([{"id": "1", "value": "1"}])
    expected_df = spark_session.createDataFrame([{"id": "1", "value": "2"}])
    delta_destination = SparkDeltaDestination(
        create_table_df, {}, "test_spark_delta_merge_update_write_batch", "overwrite"
    )
    delta_destination.write_batch()
    delta_destination_merge = SparkDeltaMergeDestination(
        spark_session,
        expected_df,
        "test_spark_delta_merge_update_write_batch",
        {},
        MERGE_CONDITION,
        when_matched_update_list=[DeltaMergeConditionValues(values={"value": "2"})],
    )
    delta_destination_merge.write_batch()
    actual_df = spark_session.table("test_spark_delta_merge_update_write_batch")
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()


def test_spark_delta_merge_update_all_write_batch(spark_session: SparkSession):
    create_table_df = spark_session.createDataFrame([{"id": "1", "value": "1"}])
    expected_df = spark_session.createDataFrame([{"id": "1", "value": "2"}])
    delta_destination = SparkDeltaDestination(
        create_table_df,
        {},
        "test_spark_delta_merge_update_all_write_batch",
        "overwrite",
    )
    delta_destination.write_batch()
    delta_destination_merge = SparkDeltaMergeDestination(
        spark_session,
        expected_df,
        "test_spark_delta_merge_update_all_write_batch",
        {},
        MERGE_CONDITION,
        when_matched_update_list=[DeltaMergeConditionValues(values="*")],
    )
    delta_destination_merge.write_batch()
    actual_df = spark_session.table("test_spark_delta_merge_update_all_write_batch")
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()


def test_spark_delta_merge_insert_write_batch(spark_session: SparkSession):
    create_table_df = spark_session.createDataFrame([{"id": "1", "value": "1"}])
    merge_table_df = spark_session.createDataFrame([{"id": "2", "value": "2"}])
    expected_df = spark_session.createDataFrame(
        [{"id": "1", "value": "1"}, {"id": "2", "value": "2"}]
    )
    delta_destination = SparkDeltaDestination(
        create_table_df, {}, "test_spark_delta_merge_insert_write_batch", "overwrite"
    )
    delta_destination.write_batch()
    delta_destination_merge = SparkDeltaMergeDestination(
        spark_session,
        merge_table_df,
        "test_spark_delta_merge_insert_write_batch",
        {},
        MERGE_CONDITION,
        when_not_matched_insert_list=[
            DeltaMergeConditionValues(
                values={"id": "source.id", "value": "source.value"}
            )
        ],
    )
    delta_destination_merge.write_batch()
    actual_df = spark_session.table(
        "test_spark_delta_merge_insert_write_batch"
    ).orderBy("id")
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()


def test_spark_delta_merge_insert_all_write_batch(spark_session: SparkSession):
    create_table_df = spark_session.createDataFrame([{"id": "1", "value": "1"}])
    merge_table_df = spark_session.createDataFrame([{"id": "2", "value": "2"}])
    expected_df = spark_session.createDataFrame(
        [{"id": "1", "value": "1"}, {"id": "2", "value": "2"}]
    )
    delta_destination = SparkDeltaDestination(
        create_table_df,
        {},
        "test_spark_delta_merge_insert_all_write_batch",
        "overwrite",
    )
    delta_destination.write_batch()
    delta_destination_merge = SparkDeltaMergeDestination(
        spark_session,
        merge_table_df,
        "test_spark_delta_merge_insert_all_write_batch",
        {},
        MERGE_CONDITION,
        when_not_matched_insert_list=[DeltaMergeConditionValues(values="*")],
    )
    delta_destination_merge.write_batch()
    actual_df = spark_session.table(
        "test_spark_delta_merge_insert_all_write_batch"
    ).orderBy("id")
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()


def test_spark_delta_merge_delete_write_batch(spark_session: SparkSession):
    create_table_df = spark_session.createDataFrame(
        [{"id": "1", "value": "1"}, {"id": "2", "value": "2"}]
    )
    merge_table_df = spark_session.createDataFrame([{"id": "2", "value": "2"}])
    expected_df = spark_session.createDataFrame([{"id": "1", "value": "1"}])
    delta_destination = SparkDeltaDestination(
        create_table_df, {}, "test_spark_delta_merge_delete_write_batch", "overwrite"
    )
    delta_destination.write_batch()
    delta_destination_merge = SparkDeltaMergeDestination(
        spark_session,
        merge_table_df,
        "test_spark_delta_merge_delete_write_batch",
        {},
        MERGE_CONDITION,
        when_matched_delete_list=[DeltaMergeCondition()],
    )
    delta_destination_merge.write_batch()
    actual_df = spark_session.table(
        "test_spark_delta_merge_delete_write_batch"
    ).orderBy("id")
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()


def test_spark_delta_merge_update_by_source_write_batch(spark_session: SparkSession):
    create_table_df = spark_session.createDataFrame(
        [{"id": "1", "value": "1"}, {"id": "2", "value": "2"}]
    )
    merge_table_df = spark_session.createDataFrame([{"id": "2", "value": "2"}])
    expected_df = spark_session.createDataFrame(
        [{"id": "1", "value": "0"}, {"id": "2", "value": "2"}]
    )
    delta_destination = SparkDeltaDestination(
        create_table_df,
        {},
        "test_spark_delta_merge_update_by_source_write_batch",
        "overwrite",
    )
    delta_destination.write_batch()
    try:
        _package_version_meets_minimum("delta_spark", "2.3.0")
        delta_destination_merge = SparkDeltaMergeDestination(
            spark_session,
            merge_table_df,
            "test_spark_delta_merge_update_by_source_write_batch",
            {},
            MERGE_CONDITION,
            when_not_matched_by_source_update_list=[
                DeltaMergeConditionValues(values={"value": "0"})
            ],
        )
        delta_destination_merge.write_batch()
        actual_df = spark_session.table(
            "test_spark_delta_merge_update_by_source_write_batch"
        ).orderBy("id")
        assert expected_df.schema == actual_df.schema
        assert expected_df.collect() == actual_df.collect()
    except AssertionError:
        with pytest.raises(AssertionError):
            delta_destination_merge = SparkDeltaMergeDestination(
                spark_session,
                merge_table_df,
                "test_spark_delta_merge_update_by_source_write_batch",
                {},
                MERGE_CONDITION,
                when_not_matched_by_source_update_list=[
                    DeltaMergeConditionValues(values={"value": "0"})
                ],
            )
            delta_destination_merge.write_batch()


def test_spark_delta_merge_delete_by_source_write_batch(spark_session: SparkSession):
    create_table_df = spark_session.createDataFrame(
        [{"id": "1", "value": "1"}, {"id": "2", "value": "2"}]
    )
    merge_table_df = spark_session.createDataFrame([{"id": "2", "value": "2"}])
    expected_df = spark_session.createDataFrame([{"id": "2", "value": "2"}])
    delta_destination = SparkDeltaDestination(
        create_table_df,
        {},
        "test_spark_delta_merge_delete_by_source_write_batch",
        "overwrite",
    )
    delta_destination.write_batch()
    try:
        _package_version_meets_minimum("delta_spark", "2.3.0")
        delta_destination_merge = SparkDeltaMergeDestination(
            spark_session,
            merge_table_df,
            "test_spark_delta_merge_delete_by_source_write_batch",
            {},
            MERGE_CONDITION,
            when_not_matched_by_source_delete_list=[DeltaMergeCondition()],
        )
        delta_destination_merge.write_batch()
        actual_df = spark_session.table(
            "test_spark_delta_merge_delete_by_source_write_batch"
        ).orderBy("id")
        assert expected_df.schema == actual_df.schema
        assert expected_df.collect() == actual_df.collect()
    except AssertionError:
        with pytest.raises(AssertionError):
            delta_destination_merge = SparkDeltaMergeDestination(
                spark_session,
                merge_table_df,
                "test_spark_delta_merge_delete_by_source_write_batch",
                {},
                MERGE_CONDITION,
                when_not_matched_by_source_delete_list=[DeltaMergeCondition()],
            )
            delta_destination_merge.write_batch()


def test_spark_delta_merge_update_write_stream_micro_batch(spark_session: SparkSession):
    create_table_df = spark_session.createDataFrame([{"id": "1", "value": "1"}])
    expected_df = spark_session.createDataFrame([{"id": "1", "value": "2"}])
    delta_destination = SparkDeltaDestination(
        create_table_df,
        {},
        "test_spark_delta_merge_update_write_stream_micro_batch",
        "overwrite",
    )
    delta_destination.write_batch()
    delta_destination_merge = SparkDeltaMergeDestination(
        spark_session,
        expected_df,
        "test_spark_delta_merge_update_write_stream_micro_batch",
        {},
        MERGE_CONDITION,
        when_matched_update_list=[DeltaMergeConditionValues(values={"value": "2"})],
    )
    delta_destination_merge._stream_merge_micro_batch(expected_df)
    actual_df = spark_session.table(
        "test_spark_delta_merge_update_write_stream_micro_batch"
    )
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()


def test_spark_delta_merge_write_stream(
    spark_session: SparkSession, mocker: MockerFixture
):
    mocker.patch(
        MOCKER_WRITESTREAM,
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
    expected_df = spark_session.createDataFrame([{"id": "1"}])
    delta_merge_destination = SparkDeltaMergeDestination(
        spark_session,
        expected_df,
        "test_spark_delta_merge_write_stream",
        {},
        MERGE_CONDITION,
    )
    actual = delta_merge_destination.write_stream()
    assert actual is None


def test_spark_delta_merge_write_batch_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    mocker.patch(
        "pyspark.sql.DataFrame.write",
        new_callable=mocker.Mock(
            return_value=mocker.Mock(
                format=mocker.Mock(
                    return_value=mocker.Mock(
                        mode=mocker.Mock(
                            return_value=mocker.Mock(
                                options=mocker.Mock(
                                    return_value=mocker.Mock(
                                        saveAsTable=mocker.Mock(side_effect=Exception)
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
    delta_merge_destination = SparkDeltaMergeDestination(
        spark_session,
        expected_df,
        "test_spark_delta_merge_write_batch",
        {},
        MERGE_CONDITION,
    )
    with pytest.raises(Exception):
        delta_merge_destination.write_batch()


def test_spark_delta_merge_write_stream_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    mocker.patch(
        MOCKER_WRITESTREAM,
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
    eventhub_destination = SparkDeltaMergeDestination(
        spark_session,
        expected_df,
        "test_spark_delta_merge_write_stream",
        {},
        "overwrite",
    )
    with pytest.raises(Exception):
        eventhub_destination.write_stream()


def test_spark_delta_merge_write_stream_path(
    spark_session: SparkSession, mocker: MockerFixture
):
    mocker.patch(
        MOCKER_WRITESTREAM,
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
    expected_df = spark_session.createDataFrame([{"id": "1"}])
    delta_merge_destination = SparkDeltaMergeDestination(
        spark_session, expected_df, "/path/to/table", {}, MERGE_CONDITION
    )
    actual = delta_merge_destination.write_stream()
    assert actual is None
