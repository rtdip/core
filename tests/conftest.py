# Copyright 2022 RTDIP
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
import pytest
import os
import shutil

from src.sdk.python.rtdip_sdk.pipelines.destinations import * # NOSONAR
from src.sdk.python.rtdip_sdk.pipelines.sources import * # NOSONAR
from src.sdk.python.rtdip_sdk.pipelines.utilities.spark.session import SparkSessionUtility

SPARK_TESTING_CONFIGURATION = {
    "spark.executor.cores": "4",
    "spark.executor.instances": "4",
    "spark.sql.shuffle.partitions": "4",
    "spark.app.name": "test_app", 
    "spark.master": "local[*]"
}

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSessionUtility(SPARK_TESTING_CONFIGURATION.copy()).execute()
    path = spark.conf.get("spark.sql.warehouse.dir")
    prefix = "file:"
    if path.startswith(prefix):
        path = path[len(prefix):]    
    if os.path.isdir(path):
        shutil.rmtree(path)    
    yield spark
    spark.stop()
    if os.path.isdir(path):
        shutil.rmtree(path)

