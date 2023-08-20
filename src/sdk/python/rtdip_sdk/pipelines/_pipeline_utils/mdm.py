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

from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    DoubleType,
    StringType,
    MapType,
    IntegerType,
)

MDM_USAGE_SCHEMA = StructType(
    [
        StructField("Uid", StringType(), True),
        StructField("SeriesId", StringType(), True),
        StructField("Timestamp", TimestampType(), True),
        StructField("IntervalTimestamp", TimestampType(), True),
        StructField("Value", DoubleType(), True),
    ]
)

MDM_META_SCHEMA = StructType(
    [
        StructField("Uid", StringType(), True),
        StructField("SeriesId", StringType(), True),
        StructField("SeriesParentId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("Uom", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("TimestampStart", TimestampType(), True),
        StructField("TimestampEnd", TimestampType(), True),
        StructField("Timezone", StringType(), True),
        StructField("Version", StringType(), True),
        StructField("SeriesType", IntegerType(), True),
        StructField("ModelType", IntegerType(), True),
        StructField("ValueType", IntegerType(), True),
        StructField(
            "Properties", MapType(keyType=StringType(), valueType=StringType()), True
        ),
    ]
)
