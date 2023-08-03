# Jobs

In a production environment, pipelines will be run as jobs that are either batch jobs executed on a schedule or a streaming job executed to be run continuously. 

## Build a Pipeline

### Prerequisites

Ensure that you have followed the installation instructions as specified in the [Getting Started](../../getting-started/installation.md) section and follow the steps which highlight the installation requirements for Pipelines. In particular:

1. [RTDIP SDK Installation](../../getting-started/installation.md#installing-the-rtdip-sdk)
1. [Java](../../getting-started/installation.md#java) - If your pipeline steps utilize pyspark then Java must be installed.

!!! note "RTDIP SDK installation"
    Ensure you have installed the RTDIP SDK, as a minimum, as follows:
    ```
    pip install "rtdip-sdk[pipelines]"
    ```

    For all installation options please see the RTDIP SDK installation [instructions.](../../getting-started/installation.md#installing-the-rtdip-sdk)

### Import

Import the required components of a Pipeline Job.

```python 
from rtdip_sdk.pipelines.execute import PipelineJob, PipelineStep, PipelineTask
from rtdip_sdk.pipelines.sources import SparkEventhubSource
from rtdip_sdk.pipelines.transformers import BinaryToStringTransformer
from rtdip_sdk.pipelines.destinations import SparkDeltaDestination
from rtdip_sdk.pipelines.secrets import PipelineSecret, DatabricksSecrets
import json
```

### Steps

Pipeline steps are constructed from [components](components.md) and added to a Pipeline task as a list. Each component is created as a `PipelineStep` and populated with the following information.

| Parameter | Description | Requirements |
|-----------|-------------|----------|
| Name | Each component requires a unique name that also facilitates dependencies between each component | Contains only letters, numbers and underscores |
 Description | A brief description of each component | Will populate certain components of a runtime such as Delta Live Tables |
| Component | The component Class | Populate with the Class Name |
| Component Parameters | Configures the component with specific information, such as connection information and component specific settings | Use Pipeline Secrets for sensitive Information |
| Depends On Step | Specifies any component names that must be executed prior to this component | A python list of component names |
| Provides Output To Step | Specifies any component names that require this component's output as an input | A python list of component names |

```python

step_list = []

# read step
eventhub_configuration = {
    "eventhubs.connectionString": PipelineSecret(type=DatabricksSecrets, vault="test_vault", key="test_key"),
    "eventhubs.consumerGroup": "$Default",
    "eventhubs.startingPosition": json.dumps({"offset": "0", "seqNo": -1, "enqueuedTime": None, "isInclusive": True})
}    
step_list.append(PipelineStep(
    name="test_step1",
    description="test_step1",
    component=SparkEventhubSource,
    component_parameters={"options": eventhub_configuration},
    provide_output_to_step=["test_step2"]
))

# transform step
step_list.append(PipelineStep(
    name="test_step2",
    description="test_step2",
    component=BinaryToStringTransformer,
    component_parameters={
        "source_column_name": "body",
        "target_column_name": "body"
    },
    depends_on_step=["test_step1"],
    provide_output_to_step=["test_step3"]
))

# write step
step_list.append(PipelineStep(
    name="test_step3",
    description="test_step3",
    component=SparkDeltaDestination,
    component_parameters={
        "destination": "test_table",
        "options": {},
        "mode": "overwrite"    
    },
    depends_on_step=["test_step2"]
))
```

### Tasks

Tasks contain a list of steps. Each task is created as a `PipelineTask` and populated with the following information.

| Parameter | Description | Requirements |
|-----------|-------------|----------|
| Name | Each task requires a unique name | Contains only letters, numbers and underscores |
| Description | A brief description of the task | Will populate certain components of a runtime such as Delta Live Tables |
| Step List | A python list of [steps](#steps) that are to be executed by the task | A list of step names that contain only letters, numbers and underscores |
| Batch Task | The task should be executed as a batch task | Optional, defaults to False |

```python
task = PipelineTask(
    name="test_task",
    description="test_task",
    step_list=step_list,
    batch_task=True
)
```

### Jobs

Jobs contain a list of tasks. A job is created as a `PipelineJob` and populated with the following information.

| Parameter | Description | Requirements |
|-----------|-------------|----------|
| Name | The Job requires a unique name | Contains only letters, numbers and underscores |
| Description | A brief description of the job | Will populate certain components of a runtime such as Delta Live Tables |
| Version | Enables version control of the task for certain environments | Follow semantic versioning |
| Task List | A python list of [tasks](#tasks) that are to be executed by the  job | A list of task names that contain only letters, numbers and underscores |

```python
pipeline_job = PipelineJob(
    name="test_job",
    description="test_job", 
    version="0.0.1",
    task_list=[task]
)
```

## Execute

Pipeline Jobs can be executed directly if the run environment where the code has been written facilitates it. To do so, the above Pipeline Job can be executed as follows:

!!! note "Pyspark Installation"
    Ensure you have [Java](../../getting-started/installation.md#java) installed in your environment and you have installed pyspark using the below command:
    ```
    pip install "rtdip-sdk[pipelines,pyspark]"
    ```

```python
from rtdip_sdk.pipelines.execute import PipelineJobExecute

pipeline = PipelineJobExecute(pipeline_job)

result = pipeline.run()
```

## Conclusion

The above sets out how a Pipeline Job can be constructed and executed. Most pipelines, however, will be exevcuted by orchestration engines. See the **Deploy** section for more information above how Pipeline Jobs can be deployed and executed in this way.

