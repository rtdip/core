# Databricks Workflows

Deploying to Databricks is simplified using the RTDIP SDK as this method of deployment will handle the setup of the libraries and spark configuration directly from the components being used in your pipeline.

## Prerequisites 

- This deployment method expects to deploy a local file to Databricks Workflows

## Import 

```python
from rtdip_sdk.pipelines.deploy import DatabricksSDKDeploy, CreateJob, JobCluster, ClusterSpec, Task, NotebookTask, ComputeSpecKind, AutoScale, RuntimeEngine, DataSecurityMode
```

## Authentication

=== "Azure Active Directory"

    Refer to the [Azure Active Directory](../../authentication/azure.md) documentation for further options to perform Azure AD authentication, such as Service Principal authentication using certificates or secrets. Below is an example of performing default authentication that retrieves a token for Azure Databricks. 

    Also refer to the [Code Reference](../../code-reference/authentication/azure.md) for further technical information.

    ```python
    from rtdip_sdk.authentication import authenticate as auth

    authentication = auth.DefaultAuth().authenticate()
    access_token = authentication.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
    ```

    !!! note "Note"
        </b>If you are experiencing any trouble authenticating please see [Troubleshooting - Authentication](../../queries/databricks/troubleshooting.md)<br />

=== "Databricks"

    Refer to the [Databricks](../../authentication/databricks.md) documentation for further information about generating a Databricks PAT Token. Below is an example of performing default authentication that retrieves a token for a Databricks Workspace. 

    Provide your `dbapi.....` token to the `access_token` in the examples below.

    ```python
    access_token = "dbapi.........."
    ```

## Deploy

Deployments to Databricks are done using the Databricks [SDK](https://docs.databricks.com/dev-tools/sdk-python.html). The Databricks SDK enables users to control exactly how they deploy their RTDIP Pipelines to Databricks.

Any of the Classes below can be imported from the following location:

```python
from rtdip_sdk.pipelines.deploy import *
```

Parameters for a Databricks Job can be managed using the following Classes:

| Class | Description |
|-------|-------------|
|ClusterSpec| Provides Parameters for setting up a Databricks Cluster|
|JobCluster| Sets up a Jobs Cluster as defined by the provided `DatabricksCluster`|
|Task| Defines the setup of the Task at the Databricks Task level including Task specific Clusters, Libraries, Schedules, Notifications and Timeouts |
|CreateJob| Defines the setup at the Job level including Clusters, Libraries, Schedules, Notifications, Access Controls, Timeouts and Tags |
|NotebookTask| Provides the Notebook information to the `Task`|
|DatabricksSDKDeploy|Leverages the Databricks SDK to deploy the job to Databricks Workflows|

!!! note "Note"
    All classes for deployment are available from the Databricks SDK and can be accessed using `from rtdip_sdk.pipelines.deploy import ` and choosing the classes you need for your Databricks deployment

## Example 

A simple example of deploying an RTDIP Pipeline Job to an Azure Databricks Job is below.

```python
databricks_host_name = "{databricks-host-url}" #Replace with your databricks workspace url

# Setup a Cluster for the Databricks Job
cluster_list = []
cluster_list.append(JobCluster(
    job_cluster_key="test_cluster",
    new_cluster=ClusterSpec(
        node_type_id="Standard_E4ds_v5",
        autoscale=AutoScale(min_workers=1, max_workers=3),
        spark_version="13.2.x-scala2.12",
        data_security_mode=DataSecurityMode.SINGLE_USER,
        runtime_engine=RuntimeEngine.PHOTON
    )
))

# Define a Notebook Task for the Databricks Job
task_list = []
task_list.append(Task(
    task_key="test_task",
    job_cluster_key="test_cluster",
    notebook_task=NotebookTask(
        notebook_path="/directory/to/pipeline.py"
    )
))

# Create a Databricks Job for the Task
job = CreateJob(
    name="test_job_rtdip",
    job_clusters=cluster_list,
    tasks=task_list
)

# Deploy to Databricks
databricks_job = DatabricksSDKDeploy(databricks_job=job, host=databricks_host_name, token=access_token)

deploy_result = databricks_job.deploy()
```

## Launch

Once a job is deployed to Databricks, it can be executed immediately using the following code.

```python
# Run/Launch the Job in Databricks
launch_result = databricks_job.launch()
```

## Stop

A job that is running and is deployed to Databricks, can be cancelled using the following code.

```python
# Run/Launch the Job in Databricks
stop_result = databricks_job.stop()