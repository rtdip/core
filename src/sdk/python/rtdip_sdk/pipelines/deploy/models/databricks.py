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

from enum import Enum
from typing import Dict, Optional, List
from pydantic import BaseModel

class DatabricksClusterClients(BaseModel):
    jobs: bool
    notebooks: bool

class DatabricksClusterWorkloadType(BaseModel):
    workload_type: DatabricksClusterClients

class DatabricksClusterAutoscale(BaseModel):
    min_workers: int
    max_workers: int

class DatabricksClusterRuntimeEngine(Enum):
    NULL = "NULL"
    STANDARD = "STANDARD"
    PHOTON = "PHOTON"

class DatabricksClusterAWSAvailability(Enum):
    ON_DEMAND = "ON_DEMAND"
    SPOT_WITH_FALLBACK = "SPOT_WITH_FALLBACK"
    SPOT_WITHOUT_FALLBACK = "SPOT_WITHOUT_FALLBACK"

class DatabricksClusterAWSEBSVolumeType(Enum):
    GENERAL_PURPOSE_SSD = "GENERAL_PURPOSE_SSD"
    THROUGHPUT_OPTIMIZED_HDD = "THROUGHPUT_OPTIMIZED_HDD"

class DatabricksClusterAWSAttributes(BaseModel):
    ebs_volume_count: Optional[str]
    availability: Optional[DatabricksClusterAWSAvailability]
    instance_profile_arn: Optional[str]
    first_on_demand: Optional[int]
    ebs_volume_type: Optional[DatabricksClusterAWSEBSVolumeType]
    spot_bid_price_percent: Optional[int]
    ebs_volume_throughput: Optional[int]
    zone_id: Optional[str]
    ebs_volume_size: Optional[int]
    ebs_volume_iops: Optional[int]

class DatabricksClusterLogConfig(BaseModel):
    dbfs: Optional[object]
    s3: Optional[object]

class DatabricksCluster(BaseModel):
    spark_version: str
    node_type_id: Optional[str]
    cluster_name: Optional[str]
    num_workers: Optional[int]
    runtime_engine: Optional[DatabricksClusterRuntimeEngine]
    workload_type: Optional[DatabricksClusterWorkloadType]
    ssh_public_keys: Optional[List[str]]
    policy_id: Optional[str]
    driver_instance_pool_id: Optional[str]
    enable_local_disk_encryption: Optional[bool]
    custom_tags: Optional[Dict]
    autoscale: Optional[DatabricksClusterAutoscale]
    spark_conf: Optional[Dict]
    driver_node_type_id: Optional[str]
    instance_pool_id: Optional[str]
    cluster_source: Optional[str]
    spark_env_vars: Optional[Dict]
    autotermination_minutes: Optional[int]
    cluster_log_conf: Optional[DatabricksClusterLogConfig]
    enable_elastic_disk: Optional[bool]
    virtual_cluster_size: Optional[str]
    enable_serverless_compute: Optional[bool]

class DatabricksJobPauseStatus(Enum):
    UNPAUSED = "UNPAUSED"
    PAUSED = "PAUSED"

class DatabricksJobSchedule(BaseModel):
    schedule_cron_expression: str
    timezone_id: str
    pause_status: Optional[DatabricksJobPauseStatus]

class DatabricksJobWebhookNotifications(BaseModel):
    on_failure: List[str]
    on_start: List[str]
    on_success: List[str]

class DatabricksJobEmailNotifications(BaseModel):
    no_alert_for_skipped_runs: bool
    on_failure: List[str]
    on_start: List[str]
    on_success: List[str]

class DatabricksJobCluster(BaseModel):
    job_cluster_key: str
    new_cluster: DatabricksCluster

class DatabricksJobFormat(Enum):
    SINGLE_TASK = "SINGLE_TASK"
    MULTI_TASK = "MULTI_TASK"

class DatabricksGitProvider(Enum):
    GITHUB = "gitHub"
    GITHUBENTERPRISE = "gitHubEnterprise"
    BITBUCKETCLOUD = "bitbucketCloud"
    BITBUCKETSERVER = "bitbucketServer"
    GITLAB = "gitLab"
    GITLABENTERPRISE = "gitLabEnterpriseEdition"
    AZUREDEVOPS = "azureDevOpsServices"
    AWSCODECOMMIT = "awsCodeCommit"

class DatabricksJobGitSource(BaseModel):
    git_tag: str
    git_provider: DatabricksGitProvider
    git_url: str
    git_branch: str
    git_commit: str

class DatabricksPermissionLevel(Enum):
    CAN_MANAGE = "CAN_MANAGE"
    CAN_RESTART = "CAN_RESTART"
    CAN_ATTACH_TO = "CAN_ATTACH_TO"
    IS_OWNER = "IS_OWNER"
    CAN_MANAGE_RUN = "CAN_MANAGE_RUN"
    CAN_VIEW = "CAN_VIEW"
    CAN_READ = "CAN_READ"
    CAN_EDIT = "CAN_EDIT"
    CAN_RUN = "CAN_RUN"
    CAN_USE = "CAN_USE"
    CAN_MANAGE_STAGING_VERSIONS = "CAN_MANAGE_STAGING_VERSIONS"
    CAN_MANAGE_PRODUCTION_VERSIONS = "CAN_MANAGE_PRODUCTION_VERSIONS"
    CAN_EDIT_METADATA = "CAN_EDIT_METADATA"
    CAN_VIEW_METADATA = "CAN_VIEW_METADATA"
    CAN_BIND = "CAN_BIND"

class DatabricksJobAccessControlList(BaseModel):
    group_name: str
    permission_level: DatabricksPermissionLevel
    service_principal_name: str
    user_name: str

class DatabricksSparkPythonTask(BaseModel):
    python_file: str
    parameters: Optional[List[str]]

class DatbricksLibrariesPypi(BaseModel):
    package: str
    repo: Optional[str]

class DatabricksLibrariesCran(BaseModel):
    package: str
    repo: Optional[str]

class DatabricksLibrariesMaven(BaseModel):
    coordinates: str
    exclusions: Optional[List[str]]
    repo: Optional[str]

class DatabricksLibraries(BaseModel):
    pypi: Optional[DatbricksLibrariesPypi]
    maven: Optional[DatabricksLibrariesMaven]
    cran: Optional[DatabricksLibrariesCran]
    egg: Optional[str]
    jar: Optional[str]
    whl: Optional[str]

class DatabricksTask(BaseModel):
    task_key: str
    name: Optional[str]
    description: Optional[str]
    libraries: Optional[List[DatabricksLibraries]]
    depends_on: Optional[List[str]]
    spark_python_task: Optional[DatabricksSparkPythonTask]
    email_notifications: Optional[DatabricksJobEmailNotifications]
    webhook_notifications: Optional[DatabricksJobWebhookNotifications]
    job_cluster_key: Optional[str]
    new_cluster: Optional[DatabricksJobCluster]
    existing_cluster_id: Optional[str]
    timeout_seconds: Optional[int]
    retry_on_timeout: Optional[bool]
    min_retry_interval_millis: Optional[int]

class DatabricksJob(BaseModel):
    name: str
    tasks: List[DatabricksTask]
    tags: Optional[Dict]
    job_clusters: Optional[List[DatabricksJobCluster]]
    email_notifications: Optional[DatabricksJobEmailNotifications]
    webhook_notifications: Optional[DatabricksJobWebhookNotifications]
    timeout_seconds: Optional[int]
    schedule: Optional[DatabricksJobSchedule]
    max_concurrent_runs: Optional[int]
    git_source: Optional[DatabricksJobGitSource]
    format: Optional[DatabricksJobFormat]
    access_control_list: Optional[DatabricksJobAccessControlList]

class DatabricksTaskForPipelineTask(BaseModel):
    name: str
    email_notifications: Optional[DatabricksJobEmailNotifications]
    webhook_notifications: Optional[DatabricksJobWebhookNotifications]
    job_cluster_key: Optional[str]
    task_cluster: Optional[DatabricksJobCluster]
    existing_cluster_id: Optional[str]
    timeout_seconds: Optional[int]
    retry_on_timeout: Optional[bool]
    min_retry_interval_millis: Optional[int]

class DatabricksJobForPipelineJob(BaseModel):
    databricks_task_for_pipeline_task_list: Optional[List[DatabricksTaskForPipelineTask]]
    tags: Optional[Dict]
    job_clusters: Optional[List[DatabricksJobCluster]]
    email_notifications: Optional[DatabricksJobEmailNotifications]
    webhook_notifications: Optional[DatabricksJobWebhookNotifications]
    timeout_seconds: Optional[int]
    schedule: Optional[DatabricksJobSchedule]
    max_concurrent_runs: Optional[int]
    git_source: Optional[DatabricksJobGitSource]
    format: Optional[DatabricksJobFormat]
    access_control_list: Optional[DatabricksJobAccessControlList]

class DatabricksDBXProject(BaseModel):
    environments: dict
    inplace_jinja_support: bool
    failsafe_cluster_reuse_with_assets: bool
    context_based_upload_for_execute: bool

