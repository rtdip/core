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
from importlib_metadata import version
import pytest
from src.sdk.python.rtdip_sdk._sdk_utils.compare_versions import _get_package_version
from src.sdk.python.rtdip_sdk.pipelines.destinations import (
    SparkKafkaEventhubDestination,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    MavenLibrary,
)
from pyspark.sql import SparkSession
from pytest_mock import MockerFixture
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
    BinaryType,
    ArrayType,
)


kafka_configuration_dict = {"failOnDataLoss": "true", "startingOffsets": "earliest"}

eventhub_connection_string = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test_key;EntityPath=test_eventhub"


class TestStreamingQueryClass:
    isActive: bool = False  # NOSONAR


def test_spark_kafka_write_setup(spark_session: SparkSession):
    kafka_configuration = kafka_configuration_dict
    kafka_destination = SparkKafkaEventhubDestination(
        spark=spark_session,
        data=spark_session.createDataFrame([{"value": 1}]),
        options=kafka_configuration,
        connection_string=eventhub_connection_string,
        consumer_group="test_consumer_group",
    )
    assert kafka_destination.system_type().value == 2
    assert kafka_destination.libraries() == Libraries(
        maven_libraries=[
            MavenLibrary(
                group_id="org.apache.spark",
                artifact_id="spark-sql-kafka-0-10_2.12",
                version=_get_package_version("pyspark"),
            )
        ],
        pypi_libraries=[],
        pythonwheel_libraries=[],
    )
    assert isinstance(kafka_destination.settings(), dict)
    assert kafka_destination.pre_write_validation()
    assert kafka_destination.post_write_validation()


def test_spark_kafka_write_batch(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch(
        "pyspark.sql.DataFrame.write",
        new_callable=mocker.Mock(
            return_value=mocker.Mock(
                format=mocker.Mock(
                    return_value=mocker.Mock(
                        options=mocker.Mock(
                            return_value=mocker.Mock(
                                save=mocker.Mock(return_value=None)
                            )
                        )
                    )
                )
            )
        ),
    )
    kafka_configuration = kafka_configuration_dict
    kafka_destination = SparkKafkaEventhubDestination(
        spark=spark_session,
        data=spark_session.createDataFrame(
            [
                {"value": 1},
                {"key": 2},
                {"topic": 3},
                {"partition": "1"},
            ]
        ),
        options=kafka_configuration,
        connection_string=eventhub_connection_string,
        consumer_group="test_consumer_group",
    )
    actual = kafka_destination.write_batch()
    assert actual is None


def test_spark_kafka_write_stream(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch(
        "pyspark.sql.DataFrame.writeStream",
        new_callable=mocker.Mock(
            return_value=mocker.Mock(
                trigger=mocker.Mock(
                    return_value=mocker.Mock(
                        format=mocker.Mock(
                            return_value=mocker.Mock(
                                options=mocker.Mock(
                                    return_value=mocker.Mock(
                                        queryName=mocker.Mock(
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
        ),
    )
    kafka_configuration = kafka_configuration_dict
    kafka_destination = SparkKafkaEventhubDestination(
        spark=spark_session,
        data=spark_session.createDataFrame([{"id": "1"}]),
        options=kafka_configuration,
        connection_string=eventhub_connection_string,
        consumer_group="test_consumer_group",
    )
    actual = kafka_destination.write_stream()
    assert actual is None


def test_spark_kafka_write_batch_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    kafka_configuration = kafka_configuration_dict
    kafka_destination = SparkKafkaEventhubDestination(
        spark=spark_session,
        data=spark_session.createDataFrame([{"value": 1}]),
        options=kafka_configuration,
        connection_string=eventhub_connection_string,
        consumer_group="test_consumer_group",
    )
    mocker.patch.object(
        kafka_destination,
        "spark",
        new_callable=mocker.PropertyMock(
            return_value=mocker.Mock(
                write=mocker.Mock(
                    format=mocker.Mock(
                        return_value=mocker.Mock(
                            options=mocker.Mock(
                                return_value=mocker.Mock(
                                    load=mocker.Mock(side_effect=Exception)
                                )
                            )
                        )
                    )
                )
            )
        ),
    )
    assert kafka_destination.pre_write_validation()
    with pytest.raises(Exception):
        kafka_destination.write_batch()


def test_spark_kafka_fails_on_converting_column_type(
    spark_session: SparkSession,
):
    kafka_configuration = kafka_configuration_dict
    schema = StructType(
        [
            StructField("value", IntegerType(), True),
            StructField("key", IntegerType(), True),
            StructField(
                "headers",
                ArrayType(
                    StructType(
                        [
                            StructField("key", StringType(), True),
                            StructField("value", StringType(), False),
                        ]
                    ),
                    False,
                ),
                True,
            ),
            StructField("topic", IntegerType(), True),
            StructField("partition", StringType(), True),
        ]
    )
    df = spark_session.createDataFrame(
        [
            {
                "value": 1,
                "key": 2,
                "headers": [{"key": "testKey", "value": "strValue"}],
                "topic": 3,
                "partition": "nonInt",
            }
        ],
        schema=schema,
    )
    destination = SparkKafkaEventhubDestination(
        spark=spark_session,
        data=df,
        options=kafka_configuration,
        connection_string=eventhub_connection_string,
        consumer_group="test_consumer_group",
    )
    with pytest.raises(ValueError) as error:
        destination._transform_to_eventhub_schema(df)
    assert str(error.value) == "key and value in the headers column cannot be nullable"


def test_spark_kafka_fails_on_invalid_connection_string_malformed(
    spark_session: SparkSession,
):
    kafka_configuration = kafka_configuration_dict
    with pytest.raises(ValueError) as error:
        SparkKafkaEventhubDestination(
            spark=spark_session,
            data=spark_session.createDataFrame([{"value": 1}]),
            options=kafka_configuration,
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test_key;EntityPath",
            consumer_group="test_consumer_group",
        )
    assert str(error.value) == "Connection string is either blank or malformed."


def test_spark_kafka_fails_on_invalid_connection_string_sharedaccesssignature(
    spark_session: SparkSession,
):
    kafka_configuration = kafka_configuration_dict
    with pytest.raises(ValueError) as error:
        SparkKafkaEventhubDestination(
            spark=spark_session,
            data=spark_session.createDataFrame([{"value": 1}]),
            options=kafka_configuration,
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test_key;EntityPath=test_eventhub;SharedAccessSignature=test",
            consumer_group="test_consumer_group",
        )
    assert (
        str(error.value)
        == "Only one of the SharedAccessKey or SharedAccessSignature must be present."
    )


def test_spark_kafka_fails_on_invalid_connection_string_missing_sharedaccesskey(
    spark_session: SparkSession,
):
    kafka_configuration = kafka_configuration_dict
    with pytest.raises(ValueError) as error:
        SparkKafkaEventhubDestination(
            spark=spark_session,
            data=spark_session.createDataFrame([{"value": 1}]),
            options=kafka_configuration,
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;EntityPath=test_eventhub",
            consumer_group="test_consumer_group",
        )
    assert (
        str(error.value)
        == "Connection string must have both SharedAccessKeyName and SharedAccessKey."
    )


def test_spark_kafka_fails_on_invalid_connection_string_missing_endpoint(
    spark_session: SparkSession,
):
    kafka_configuration = kafka_configuration_dict
    with pytest.raises(ValueError) as error:
        SparkKafkaEventhubDestination(
            spark=spark_session,
            data=spark_session.createDataFrame([{"value": 1}]),
            options=kafka_configuration,
            connection_string="TestNoEndpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test_key;EntityPath=test_eventhub",
            consumer_group="test_consumer_group",
        )
    assert str(error.value) == "Connection string is either blank or malformed."


def test_spark_kafka_write_stream_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    kafka_configuration = kafka_configuration_dict
    kafka_destination = SparkKafkaEventhubDestination(
        spark=spark_session,
        data=spark_session.createDataFrame([{"value": 1}]),
        options=kafka_configuration,
        connection_string=eventhub_connection_string,
        consumer_group="test_consumer_group",
    )
    mocker.patch.object(
        kafka_destination,
        "spark",
        new_callable=mocker.PropertyMock(
            return_value=mocker.Mock(
                writeStream=mocker.Mock(
                    format=mocker.Mock(
                        return_value=mocker.Mock(
                            options=mocker.Mock(
                                return_value=mocker.Mock(
                                    load=mocker.Mock(side_effect=Exception)
                                )
                            )
                        )
                    )
                )
            )
        ),
    )
    assert kafka_destination.pre_write_validation()
    with pytest.raises(Exception):
        kafka_destination.write_stream()
