# Apache Airflow

## Databricks Provider

Apache Airflow can orchestrate an RTDIP Pipeline that has been deployed as a Databricks Job. For further information on how to deploy an RTDIP Pipeline as a Databricks Job, please see [here.](databricks.md) 

Databricks has also provided more information about running Databricks jobs from Apache Airflow [here.](https://docs.databricks.com/workflows/jobs/how-to/use-airflow-with-jobs.html)

### Prerequisites

1. An Apache Airflow instance must be running.
1. Authentication between Apache Airflow and Databricks must be [configured.](https://docs.databricks.com/workflows/jobs/how-to/use-airflow-with-jobs.html#create-a-databricks-personal-access-token-for-airflow)
1. The python packages `apache-airflow` and `apache-airflow-providers-databricks` must be installed.
1. You have created an [RTDIP Pipeline and deployed it to Databricks.](databricks.md)


### Example

The `JOB ID` in the example below can be obtained from the Databricks Job.

```python
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago

default_args = {
  'owner': 'airflow'
}

with DAG('databricks_dag',
  start_date = days_ago(2),
  schedule_interval = None,
  default_args = default_args
  ) as dag:

  opr_run_now = DatabricksRunNowOperator(
    task_id = 'run_now',
    databricks_conn_id = 'databricks_default',
    job_id = JOB_ID
  )
```

