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

from src.sdk.python.rtdip_sdk.pipelines.secrets.models import PipelineSecret
from src.sdk.python.rtdip_sdk.pipelines.secrets.databricks import DatabricksSecrets
from src.sdk.python.rtdip_sdk.pipelines.converters.pipeline_job_json import (
    PipelineJobFromJsonConverter,
    PipelineJobToJsonConverter,
)

from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.pipeline_job_templates import (
    get_spark_pipeline_job,
)


def test_pipeline_job_convert_from_json():
    pipeline_json = '{"name": "test_job", "description": "test_job", "version": "0.0.1", "task_list": [{"name": "test_task", "description": "test_task", "step_list": [{"name": "test_step1", "description": "test_step1", "component": "SparkEventhubSource", "component_parameters": {"options": {"eventhubs.connectionString": {"pipeline_secret": {"type": "DatabricksSecrets", "vault": "test_vault", "key": "test_key"}}, "eventhubs.consumerGroup": "$Default", "eventhubs.startingPosition": {"offset": "0", "seqNo": -1, "enqueuedTime": null, "isInclusive": true}}}, "provide_output_to_step": ["test_step2"]}, {"name": "test_step2", "description": "test_step2", "depends_on_step": ["test_step1"], "component": "BinaryToStringTransformer", "component_parameters": {"source_column_name": "body", "target_column_name": "body"}, "provide_output_to_step": ["test_step3"]}, {"name": "test_step3", "description": "test_step3", "depends_on_step": ["test_step2"], "component": "SparkDeltaDestination", "component_parameters": {"destination": "test_table", "options": {}, "mode": "overwrite"}}], "batch_task": true}]}'

    pipeline_job_expected = get_spark_pipeline_job()
    pipeline_job_expected.task_list[0].step_list[0].component_parameters["options"][
        "eventhubs.connectionString"
    ] = PipelineSecret(type=DatabricksSecrets, vault="test_vault", key="test_key")

    pipeline_job_actual = PipelineJobFromJsonConverter(pipeline_json).convert()

    assert pipeline_job_expected.__dict__ == pipeline_job_actual.__dict__


def test_pipeline_job_convert_to_json():
    pipeline_job = get_spark_pipeline_job()
    pipeline_job.task_list[0].step_list[0].component_parameters["options"][
        "eventhubs.connectionString"
    ] = PipelineSecret(type=DatabricksSecrets, vault="test_vault", key="test_key")

    pipeline_job_json_actual = PipelineJobToJsonConverter(pipeline_job).convert()

    pipeline_job_json_expected = '{"name": "test_job", "description": "test_job", "version": "0.0.1", "task_list": [{"name": "test_task", "description": "test_task", "step_list": [{"name": "test_step1", "description": "test_step1", "component": "SparkEventhubSource", "component_parameters": {"options": {"eventhubs.connectionString": {"pipeline_secret": {"type": "DatabricksSecrets", "vault": "test_vault", "key": "test_key"}}, "eventhubs.consumerGroup": "$Default", "eventhubs.startingPosition": {"offset": "0", "seqNo": -1, "enqueuedTime": null, "isInclusive": true}}}, "provide_output_to_step": ["test_step2"]}, {"name": "test_step2", "description": "test_step2", "depends_on_step": ["test_step1"], "component": "BinaryToStringTransformer", "component_parameters": {"source_column_name": "body", "target_column_name": "body"}, "provide_output_to_step": ["test_step3"]}, {"name": "test_step3", "description": "test_step3", "depends_on_step": ["test_step2"], "component": "SparkDeltaDestination", "component_parameters": {"destination": "test_table", "options": {}, "mode": "overwrite"}}], "batch_task": true}]}'

    assert pipeline_job_json_actual == pipeline_job_json_expected
