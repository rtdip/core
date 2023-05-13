from rtdip_sdk.pipelines.execute import PipelineJob, PipelineStep, PipelineTask, PipelineJobExecute
from rtdip_sdk.pipelines.sources import *
from rtdip_sdk.pipelines.destinations import SparkDeltaDestination

step_list = []

step_list.append(PipelineStep(
    name="test_step1",
    description="test_step1",
    component=MISODailyLoadISOSource,
    component_parameters={"options": {
        # "start_date": "20230501",
        # "end_date": "20230502",
        "load_type": "actual",
        "date": "20230510"
    },
    },
    provide_output_to_step=["test_step3"]
))

# transform step
# step_list.append(PipelineStep(
#     name="test_step2",
#     description="test_step2",
#     component=BinaryToStringTransformer,
#     component_parameters={
#         "source_column_name": "body",
#         "target_column_name": "body"
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
        "table_name": "miso_iso_data",
        "options": {},
        "mode": "overwrite"
    },
    depends_on_step=["test_step1"]
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
