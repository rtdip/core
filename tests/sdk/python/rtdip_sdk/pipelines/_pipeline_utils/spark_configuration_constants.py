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

import sys
sys.path.insert(0, '.')
import pytest
import os
import shutil
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.delta import SparkDeltaDestination
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.delta import SparkDeltaSource
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.delta_sharing import SparkDeltaSharingSource
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.eventhub import SparkEventhubSource
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark import SparkClient
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.constants import DEFAULT_PACKAGES

SPARK_TESTING_CONFIGURATION = {
    # "spark.executor.cores": "2",
    # "spark.executor.instances": "1",
    # "spark.sql.shuffle.partitions": "1",
    "spark.app.name": "test_app", 
    # "spark.master": "local[*]"
}

@pytest.fixture(scope="session")
def spark_session():
    component_list = [SparkDeltaSource(None, {}, "test_table"), SparkDeltaSharingSource(None, {}, "test_table"), SparkDeltaDestination("test_table", {}), SparkEventhubSource(None, {})]
    task_libraries = Libraries()
    task_libraries.get_libraries_from_components(component_list)
    spark_configuration = SPARK_TESTING_CONFIGURATION.copy()
    for component in component_list:
        spark_configuration = {**spark_configuration, **component.settings()}
    spark_client = SparkClient(spark_configuration, task_libraries)
    spark = spark_client.spark_session
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