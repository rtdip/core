# Copyright 2025 RTDIP
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

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    StringType,
    BinaryType,
    BooleanType,
    LongType,
    MapType,
    IntegerType,
    ArrayType,
    DoubleType,
    FloatType,
)
from pyspark.sql.functions import col

from .models import Libraries
from ..._sdk_utils.compare_versions import _package_version_meets_minimum


class SparkClient:
    spark_configuration: dict
    spark_libraries: Libraries
    spark_session: SparkSession
    spark_remote: str

    def __init__(
        self,
        spark_configuration: dict,
        spark_libraries: Libraries,
        spark_remote: str = None,
    ):
        self.spark_configuration = spark_configuration
        self.spark_libraries = spark_libraries
        if spark_remote != None:
            _package_version_meets_minimum("pyspark", "3.4.0")
        self.spark_remote = spark_remote
        self.spark_session = self.get_spark_session()

    def get_spark_session(self) -> SparkSession:
        try:
            temp_spark_configuration = self.spark_configuration.copy()

            spark = SparkSession.builder
            spark_app_name = "spark.app.name"
            spark_master = "spark.master"

            if spark_app_name in temp_spark_configuration:
                spark = spark.appName(temp_spark_configuration[spark_app_name])
                temp_spark_configuration.pop(spark_app_name)

            if spark_master in temp_spark_configuration:
                spark = spark.master(temp_spark_configuration[spark_master])
                temp_spark_configuration.pop(spark_master)

            if self.spark_remote:
                spark = spark.remote(self.spark_remote)

            if len(self.spark_libraries.maven_libraries) > 0:
                temp_spark_configuration["spark.jars.packages"] = ",".join(
                    maven_package.to_string()
                    for maven_package in self.spark_libraries.maven_libraries
                )

            for configuration in temp_spark_configuration.items():
                spark = spark.config(configuration[0], configuration[1])

            spark_session = spark.getOrCreate()
            # TODO: Implemented in DBR 11 but not yet available in pyspark
            # spark_session.streams.addListener(SparkStreamingListener())
            return spark_session

        except Exception as e:
            logging.exception(str(e))
            raise e


def get_dbutils(
    spark: SparkSession,
):  # please note that this function is used in mocking by its name
    try:
        from pyspark.dbutils import DBUtils  # noqa

        if "dbutils" not in locals():
            utils = DBUtils(spark)
            return utils
        else:
            return locals().get("dbutils")
    except ImportError:
        return None


# # TODO: Implemented in DBR 11 but not yet available in open source pyspark
# from pyspark.sql.streaming import StreamingQueryListener
# class SparkStreamingListener(StreamingQueryListener):
#     def onQueryStarted(self, event):
#         logging.info("Query started: {} {}".format(event.id, event.name))

#     def onQueryProgress(self, event):
#         logging.info("Query Progress: {}".format(event))

#     def onQueryTerminated(self, event):
#         logging.info("Query terminated: {} {}".format(event.id, event.name))


def is_dataframe_partially_conformed_in_schema(
    dataframe: DataFrame, schema: StructType, throw_error: bool = True
) -> bool:
    """
    Checks if all columns in the dataframe are contained in the schema with appropriate types.

    Parameters:
    dataframe (DataFrame): The dataframe to check.
    schema (StructType): The schema to conform to.
    throw_error (bool): If True, raises an error on non-conformance. Defaults to True.

    Returns:
    bool: True if the dataframe conforms to the schema, False otherwise.
    """
    for column in dataframe.schema:
        if column.name in schema.names:
            schema_field = schema[column.name]
            if not isinstance(column.dataType, type(schema_field.dataType)):
                if throw_error:
                    raise ValueError(
                        "Column {0} is of Type {1}, expected Type {2}".format(
                            column, column.dataType, schema_field.dataType
                        )
                    )
                return False
        else:
            # dataframe contains column not expected ins schema
            if not throw_error:
                return False
            else:
                raise ValueError(
                    "Column {0} is not expected in dataframe".format(column)
                )
    return True


def conform_dataframe_to_schema(
    dataframe: DataFrame, schema: StructType, throw_error: bool = True
) -> DataFrame:
    """
    Tries to convert all columns to the given schema.

    Parameters:
    dataframe (DataFrame): The dataframe to conform.
    schema (StructType): The schema to conform to.
    throw_error (bool): If True, raises an error on non-conformance. Defaults to True.

    Returns:
    DataFrame: The conformed dataframe.
    """
    for column in dataframe.schema:
        c_name = column.name
        if c_name in schema.names:
            schema_field = schema[c_name]
            if not isinstance(column.dataType, type(schema_field.dataType)):
                dataframe = dataframe.withColumn(
                    c_name, dataframe[c_name].cast(schema_field.dataType)
                )
        else:
            if throw_error:
                raise ValueError(f"Column '{c_name}' is not expected in the dataframe")
            else:
                dataframe = dataframe.drop(c_name)
    return dataframe


def split_by_source(df: DataFrame, split_by_col: str, timestamp_col: str) -> dict:
    """

    Helper method to separate individual time series based on their source.

    Parameters:
    df (DataFrame): The input DataFrame.
    split_by_col (str): The column name to split the DataFrame by.
    timestamp_col (str): The column name to order the DataFrame by.

    Returns:
    dict: A dictionary where keys are distinct values from split_by_col and values are DataFrames filtered and ordered by timestamp_col.
    """
    tag_names = df.select(split_by_col).distinct().collect()
    tag_names = [row[split_by_col] for row in tag_names]
    source_dict = {
        tag: df.filter(col(split_by_col) == tag).orderBy(timestamp_col)
        for tag in tag_names
    }

    return source_dict


EVENTHUB_SCHEMA = StructType(
    [
        StructField("body", BinaryType(), True),
        StructField("partition", StringType(), True),
        StructField("offset", StringType(), True),
        StructField("sequenceNumber", LongType(), True),
        StructField("enqueuedTime", TimestampType(), True),
        StructField("publisher", StringType(), True),
        StructField("partitionKey", StringType(), True),
        StructField("properties", MapType(StringType(), StringType(), True), True),
        StructField(
            "systemProperties", MapType(StringType(), StringType(), True), True
        ),
    ]
)

OPC_PUBLISHER_SCHEMA = StructType(
    [
        StructField("ApplicationUri", StringType(), True),
        StructField("DisplayName", StringType(), True),
        StructField("NodeId", StringType(), True),
        StructField(
            "Value",
            StructType(
                [
                    StructField("SourceTimestamp", StringType(), True),
                    StructField(
                        "StatusCode",
                        StructType(
                            [
                                StructField("Code", LongType(), True),
                                StructField("Symbol", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField("Value", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

OPC_PUBLISHER_AE_SCHEMA = StructType(
    [
        StructField("NodeId", StringType(), True),
        StructField("EndpointUrl", StringType(), True),
        StructField("DisplayName", StringType(), True),
        StructField(
            "Value",
            StructType(
                [
                    StructField(
                        "ConditionId",
                        StructType(
                            [
                                StructField("Value", StringType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "AckedState",
                        StructType(
                            [
                                StructField("Value", StringType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "AckedState/FalseState",
                        StructType(
                            [
                                StructField("Value", StringType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "AckedState/Id",
                        StructType(
                            [
                                StructField("Value", BooleanType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "AckedState/TrueState",
                        StructType(
                            [
                                StructField("Value", StringType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "ActiveState",
                        StructType(
                            [
                                StructField("Value", StringType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "ActiveState/FalseState",
                        StructType(
                            [
                                StructField("Value", StringType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "ActiveState/Id",
                        StructType(
                            [
                                StructField("Value", BooleanType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "ActiveState/TrueState",
                        StructType(
                            [
                                StructField("Value", StringType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "EnabledState",
                        StructType(
                            [
                                StructField("Value", StringType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "EnabledState/FalseState",
                        StructType(
                            [
                                StructField("Value", StringType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "EnabledState/Id",
                        StructType(
                            [
                                StructField("Value", BooleanType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "EnabledState/TrueState",
                        StructType(
                            [
                                StructField("Value", StringType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "EventId",
                        StructType(
                            [
                                StructField("Value", StringType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "EventType",
                        StructType(
                            [
                                StructField("Value", StringType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "HighHighLimit",
                        StructType(
                            [
                                StructField("Value", DoubleType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "HighLimit",
                        StructType(
                            [
                                StructField("Value", DoubleType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "InputNode",
                        StructType(
                            [
                                StructField("Value", StringType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "LowLimit",
                        StructType(
                            [
                                StructField("Value", DoubleType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "LowLowLimit",
                        StructType(
                            [
                                StructField("Value", DoubleType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "Message",
                        StructType(
                            [
                                StructField("Value", StringType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "Quality",
                        StructType(
                            [
                                StructField("Value", StringType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "ReceiveTime",
                        StructType(
                            [
                                StructField("Value", TimestampType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "Retain",
                        StructType(
                            [
                                StructField("Value", BooleanType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "Severity",
                        StructType(
                            [
                                StructField("Value", DoubleType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "SourceName",
                        StructType(
                            [
                                StructField("Value", StringType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "SourceNode",
                        StructType(
                            [
                                StructField("Value", StringType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "Time",
                        StructType(
                            [
                                StructField("Value", TimestampType(), True),
                                StructField("SourceTimestamp", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
    ]
)


PROCESS_DATA_MODEL_SCHEMA = StructType(
    [
        StructField("TagName", StringType(), True),
        StructField("EventTime", TimestampType(), True),
        StructField("Status", StringType(), True),
        StructField("Value", StringType(), True),
        StructField("ValueType", StringType(), True),
        StructField("ChangeType", StringType(), True),
    ]
)

PROCESS_DATA_MODEL_EVENT_SCHEMA = StructType(
    [
        StructField("TagName", StringType(), True),
        StructField("EventTime", TimestampType(), True),
        StructField("Status", StringType(), True),
        StructField("Value", StringType(), True),
    ]
)

KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("timestampType", IntegerType(), True),
    ]
)

KAFKA_EVENTHUB_SCHEMA = StructType(
    [
        StructField("body", BinaryType(), True),
        StructField("partition", StringType(), True),
        StructField("offset", StringType(), True),
        StructField("sequenceNumber", LongType(), True),
        StructField("enqueuedTime", TimestampType(), True),
        StructField("publisher", StringType(), True),
        StructField("partitionKey", StringType(), True),
        StructField("properties", MapType(StringType(), StringType(), True), True),
        StructField(
            "systemProperties", MapType(StringType(), StringType(), True), True
        ),
    ],
)

KINESIS_SCHEMA = StructType(
    [
        StructField("partitionKey", StringType(), True),
        StructField("data", BinaryType(), True),
        StructField("stream", StringType(), True),
        StructField("shardId", StringType(), True),
        StructField("sequenceNumber", StringType(), True),
        StructField("approximateArrivalTimestamp", TimestampType(), True),
    ]
)

FLEDGE_SCHEMA = ArrayType(
    StructType(
        [
            StructField("asset", StringType(), True),
            StructField("readings", MapType(StringType(), StringType(), True), True),
            StructField("timestamp", TimestampType(), True),
        ]
    )
)

EDGEX_SCHEMA = StructType(
    [
        StructField("apiVersion", StringType(), True),
        StructField("id", StringType(), True),
        StructField("deviceName", StringType(), True),
        StructField("profileName", StringType(), True),
        StructField("sourceName", StringType(), True),
        StructField("origin", LongType(), True),
        StructField(
            "readings",
            ArrayType(
                StructType(
                    [
                        StructField("id", StringType(), True),
                        StructField("origin", LongType(), True),
                        StructField("deviceName", StringType(), True),
                        StructField("resourceName", StringType(), True),
                        StructField("profileName", StringType(), True),
                        StructField("valueType", StringType(), True),
                        StructField("value", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

APM_SCHEMA = StructType(
    [
        StructField(
            "SystemTimeSeries",
            StructType(
                [
                    StructField("Id", StringType(), True),
                    StructField("TenantId", StringType(), True),
                    StructField("IdType", StringType(), True),
                    StructField(
                        "Samples",
                        ArrayType(
                            StructType(
                                [
                                    StructField("ItemName", StringType(), True),
                                    StructField("Time", StringType(), True),
                                    StructField("Value", StringType(), True),
                                    StructField("Unit", StringType(), True),
                                    StructField(
                                        "NormalizedQuality", StringType(), True
                                    ),
                                    StructField("HighValue", DoubleType(), True),
                                    StructField("LowValue", DoubleType(), True),
                                    StructField("TargetValue", DoubleType(), True),
                                ]
                            )
                        ),
                        True,
                    ),
                ]
            ),
            True,
        )
    ]
)

SEM_SCHEMA = StructType(
    [
        StructField("apiVersion", StringType(), True),
        StructField("deviceName", StringType(), True),
        StructField("id", StringType(), True),
        StructField("origin", LongType(), True),
        StructField("profileName", StringType(), True),
        StructField(
            "readings",
            ArrayType(
                StructType(
                    [
                        StructField("deviceName", StringType(), True),
                        StructField("id", StringType(), True),
                        StructField("origin", LongType(), True),
                        StructField("profileName", StringType(), True),
                        StructField("resourceName", StringType(), True),
                        StructField("value", StringType(), True),
                        StructField("valueType", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("sourceName", StringType(), True),
    ]
)

AIO_SCHEMA = MapType(
    StringType(),
    StructType(
        [
            StructField("SourceTimestamp", TimestampType(), True),
            StructField("Value", StringType(), True),
        ]
    ),
)

OPCUA_SCHEMA = ArrayType(
    StructType(
        [
            StructField("DataSetWriterId", LongType(), True),
            StructField("Timestamp", TimestampType(), True),
            StructField(
                "Payload",
                MapType(
                    StringType(),
                    StructType(
                        [
                            StructField("Value", StringType(), True),
                        ]
                    ),
                ),
            ),
        ]
    )
)

MIRICO_METADATA_SCHEMA = StructType(
    [
        StructField("retroName", StringType(), True),
        StructField("siteName", StringType(), True),
        StructField("retroAltitude", FloatType(), True),
        StructField("sensorAltitude", FloatType(), True),
        StructField("retroLongitude", FloatType(), True),
        StructField("gasType", StringType(), True),
        StructField("sensorLatitude", FloatType(), True),
        StructField("retroLatitude", FloatType(), True),
        StructField("sensorLongitude", FloatType(), True),
    ]
)
