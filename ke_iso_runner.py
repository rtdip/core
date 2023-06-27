import sys

sys.path.append('/home/keithe/checkout2/ellisk737/core/src/sdk/python')

from rtdip_sdk.pipelines.execute import PipelineJob, PipelineStep, PipelineTask, PipelineJobExecute
from rtdip_sdk.pipelines.sources import *
from rtdip_sdk.pipelines.destinations import SparkDeltaDestination
from rtdip_sdk.pipelines.transformers import *

import logging

logging.getLogger().setLevel("INFO")

step_list = []
# pip install pyspark==3.2.2
step_list.append(PipelineStep(
    name="test_step1",
    description="test_step1",
    component=MISODailyLoadISOSource,
    # component=MISODailyLoadISOSource,
    component_parameters={"options": {
        # "start_date": "20230510",
        # "end_date": "20230520",
        "load_type": "actual",
        "date": "20230520",
        # "table_name": "miso_iso_data"
    },
        # "table_name":"miso_iso_data"
    },
    #provide_output_to_step=["test_step2", "test_step4"]
    provide_output_to_step=["test_step3", "test_step5"]
))

# transform step
# step_list.append(PipelineStep(
#     name="test_step2",
#     description="test_step2",
#     component=MISOToMDMTransformer,
#     component_parameters={
#         "output_type": "usage",
#     },
#     depends_on_step=["test_step1"],
#     provide_output_to_step=["test_step3"]
# ))

# write step
step_list.append(PipelineStep(
    name="test_step3",
    description="test_step3",
    component=SparkDeltaDestination,
    component_parameters={
        "table_name": "miso_iso_usage_data",
        "options": {
            "partitionBy":"timestamp"

        },
        "mode": "overwrite"
    },
   #depends_on_step=["test_step2"]
    depends_on_step=["test_step1"]
))

# step_list.append(PipelineStep(
#     name="test_step4",
#     description="test_step4",
#     component=MISOToMDMTransformer,
#     component_parameters={
#         "output_type": "meta",
#     },
#     depends_on_step=["test_step1"],
#     provide_output_to_step=["test_step5"]
# ))


step_list.append(PipelineStep(
    name="test_step5",
    description="test_step5",
    component=SparkDeltaDestination,
    component_parameters={
        "table_name": "miso_iso_meta_data",
        "options": {},
        "mode": "overwrite"
    },
    #depends_on_step=["test_step4"]
    depends_on_step=["test_step3"]
))

task = PipelineTask(
    name="test_task",
    description="test_task",
    step_list=step_list,
    batch_task=True
)

pipeline_job = PipelineJob(
    name="test_job",
    description="test_job",
    version="0.0.1",
    task_list=[task]
)

pipeline = PipelineJobExecute(pipeline_job)

result = pipeline.run()
