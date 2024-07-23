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
from semver.version import Version
from .models import MavenLibrary, PyPiLibrary
from ..._sdk_utils.compare_versions import (
    _get_python_package_version,
    _get_package_version,
)


def get_default_package(package_name):
    delta_spark_artifact_id = "delta-core_2.12"
    if (
        Version.compare(
            _get_python_package_version("delta-spark"), Version.parse("3.0.0")
        )
        >= 0
    ):
        delta_spark_artifact_id = "delta-spark_2.12"
    DEFAULT_PACKAGES = {
        "spark_delta_core": MavenLibrary(
            group_id="io.delta",
            artifact_id=delta_spark_artifact_id,
            version=_get_package_version("delta-spark"),
        ),
        "spark_delta_sharing": MavenLibrary(
            group_id="io.delta", artifact_id="delta-sharing-spark_2.12", version="1.0.0"
        ),
        "spark_azure_eventhub": MavenLibrary(
            group_id="com.microsoft.azure",
            artifact_id="azure-eventhubs-spark_2.12",
            version="2.3.22",
        ),
        "spark_sql_kafka": MavenLibrary(
            group_id="org.apache.spark",
            artifact_id="spark-sql-kafka-0-10_2.12",
            version=_get_package_version("pyspark"),
        ),
        "spark_remote": MavenLibrary(
            group_id="org.apache.spark",
            artifact_id="spark-connect_2.12",
            version=_get_package_version("pyspark"),
        ),
        "azure_adls_gen_2": PyPiLibrary(
            name="azure-storage-file-datalake", version="12.12.0"
        ),
        "azure_key_vault_secret": PyPiLibrary(
            name="azure-keyvault-secrets", version="4.7.0"
        ),
        "azure_storage_mgmt": PyPiLibrary(name="azure-mgmt-storage", version="21.0.0"),
        "azure_eventgrid_mgmt": PyPiLibrary(
            name="azure-mgmt-eventgrid", version="10.2.0"
        ),
        "aws_boto3": PyPiLibrary(name="boto3", version="1.28.2"),
        "hashicorp_vault": PyPiLibrary(name="hvac", version="1.1.0"),
        "api_requests": PyPiLibrary(name="requests", version="2.30.0"),
        "pyarrow": PyPiLibrary(name="pyarrow", version="14.0.2"),
        "pandas": PyPiLibrary(name="pandas", version="2.0.1"),
    }
    return DEFAULT_PACKAGES[package_name]
