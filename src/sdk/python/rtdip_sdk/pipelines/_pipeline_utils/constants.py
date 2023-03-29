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

from .models import MavenLibrary, PyPiLibrary
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, BinaryType, LongType, MapType

DEFAULT_PACKAGES = {
    "spark_delta_core": MavenLibrary(
                group_id="io.delta",
                artifact_id="delta-core_2.12",
                version="2.2.0"
            ),
    "spark_delta_sharing": MavenLibrary(
                group_id="io.delta",
                artifact_id="delta-sharing-spark_2.12",
                version="0.6.2"
            ),
    "spark_azure_eventhub": MavenLibrary(
                group_id="com.microsoft.azure", 
                artifact_id="azure-eventhubs-spark_2.12",
                version="2.3.22"
            ),
    "rtdip_sdk": PyPiLibrary(
                name="rtdip_sdk",
                version="0.1.7"
            )
}

EVENTHUB_SCHEMA = StructType(
            [StructField('body', BinaryType(), True), 
             StructField('partition', StringType(), True), 
             StructField('offset', StringType(), True), 
             StructField('sequenceNumber', LongType(), True), 
             StructField('enqueuedTime', TimestampType(), True), 
             StructField('publisher', StringType(), True), 
             StructField('partitionKey', StringType(), True), 
             StructField('properties', MapType(StringType(), StringType(), True), True), 
             StructField('systemProperties', MapType(StringType(), StringType(), True), True)]
        )