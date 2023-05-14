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

from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType

MISO_SCHEMA = StructType([
            StructField("DATE_TIME", TimestampType(), True),
            StructField("LRZ1", DoubleType(), True),
            StructField("LRZ2_7", DoubleType(), True),
            StructField("LRZ3_5", DoubleType(), True),
            StructField("LRZ4", DoubleType(), True),
            StructField("LRZ6", DoubleType(), True),
            StructField("LRZ8_9_10", DoubleType(), True),
            StructField("MISO", DoubleType(), True),
            ])
