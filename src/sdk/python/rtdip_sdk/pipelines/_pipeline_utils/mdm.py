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

from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType, StringType, MapType, IntegerType

MDM_USAGE_SCHEMA = StructType([
    StructField("uid", StringType(), True),
    StructField("series_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("interval_timestamp", TimestampType(), True),
    StructField("value", DoubleType(), True)
])

MDM_META_SCHEMA = StructType([
    StructField("uid", StringType(), True),
    StructField("series_id", StringType(), True),
    StructField("series_parent_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("uom", StringType(), True),
    StructField("description", StringType(), True),
    StructField("timestamp_start", TimestampType(), True),
    StructField("timestamp_end", TimestampType(), True),
    StructField("time_zone", StringType(), True),
    StructField("version", StringType(), True),
    StructField("series_type", IntegerType(), True),
    StructField("model_type", IntegerType(), True),
    StructField("value_type", IntegerType(), True),
    StructField("properties", MapType(keyType=StringType(), valueType=StringType()), True)
])
