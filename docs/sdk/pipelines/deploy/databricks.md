# Databricks Workflows

## Import 

```python
from rtdip_sdk.pipelines.deploy import DatabricksDBXDeploy, DatabricksCluster, DatabricksJobCluster, DatabricksJobForPipelineJob, DatabricksTaskForPipelineTask
```

## Authentication

=== "Azure Active Directory"

    Refer to the [Azure Active Directory](../../authentication/azure.md) documentation for further options to perform Azure AD authentication, such as Service Principal authentication using certificates or secrets. Below is an example of performing default authentication that retrieives a token for Azure Databricks. 

    Also refer to the [Code Reference](../../code-reference/authentication/azure.md) for further technical information.

    ```python
    from rtdip_sdk.authentication import authenticate as auth

    authentication = auth.DefaultAuth().authenticate()
    access_token = authentication.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
    ```

    !!! note "Note"
        </b>If you are experiencing any trouble authenticating please see [Troubleshooting - Authentication](../../queries/databricks/troubleshooting.md)<br />

=== "Databricks"

    Refer to the [Databricks](../../authentication/databricks.md) documentation for further information about generating a Databricks PAT Token. Below is an example of performing default authentication that retrieives a token for a Databricks Workspace. 

    Provide your `dbapi.....` token to the `access_token` in the examples below.

    ```python
    access_token = "dbapi.........."
    ```

## Deploy

Deployments to Databricks are done using [DBX](https://dbx.readthedocs.io/en/latest/). DBX enables users to control exactly how they deploy their RTDIP Pipelines to Databricks.

Any of the Classes below can be imported from the following location:

```python
from rtdip_sdk.pipelines.deploy import *
```

Parameters for a Databricks Job can be managed using the following Classes:

| Class | Description |
|-------|-------------|
|DatabricksCluster| Provides Parameters for setting up a Databricks Cluster|
|DatabricksJobCluster| Sets up a Jobs Cluster as defined by the provided `DatabricksCluster`|
|DatabricksTask| Defines the setup of the Task at the Databricks Task level including Task specific Clusters, Libraries, Schedules, Notifications and Timeouts |
|DatabricksJob| Defines the setup at the Job level including Clusters, Libraries, Schedules, Notifications, Access Controls, Timeouts and Tags |
|DatabricksTaskForPipelineTask| Provides Databricks Task information to be used for the equivalent named task defined in your RTDIP Pipeline task `PipelineTask`|
|DatabricksJobForPipelineJob|Provides Databricks Job information to be used for the equivalent named Job defined in your RTDIP Pipeline Job `PipelineJob`|

A simple example of deploying an RTDIP Pipeline Job to an Azure Databricks Job is below.

```python

databricks_host_name = "{databricks-host-url}" #Replace with your databricks workspace url

# Setup a Cluster for the Databricks Job
databricks_job_cluster = DatabricksJobCluster(
    job_cluster_key="test_job_cluster", 
    new_cluster=DatabricksCluster(
        spark_version = "11.3.x-scala2.12",
        node_type_id = "Standard_D3_v2",
        num_workers = 2
    )
)

# Define a Databricks Task for the Pipeline Task
databricks_task = DatabricksTaskForPipelineTask(name="test_task", job_cluster_key="test_job_cluster")

# Create a Databricks Job for the Pipeline Job
databricks_job = DatabricksJobForPipelineJob(
    job_clusters=[databricks_job_cluster],
    databricks_task_for_pipeline_task_list=[databricks_task]
)

# Deploy to Databricks
databricks_job = DatabricksDBXDeploy(pipeline_job=pipeline_job, databricks_job_for_pipeline_job=databricks_job, host=databricks_host_name, token=access_token)

deploy_result = databricks_job.deploy()
```

## Launch

Once a job is deployed to Databricks, it can be executed immediately using the following code.

```python
# Run/Launch the Job in Databricks
launch_result = databricks_job.launch()
```