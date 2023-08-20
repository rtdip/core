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
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from src.sdk.python.rtdip_sdk.pipelines.utilities.spark.adls_gen2_spn_connect import (
    SparkADLSGen2SPNConnectUtility,
)
from pyspark.sql import SparkSession


def test_adls_gen2_spn_connect_setup(spark_session: SparkSession):
    adls_gen2_spn_connect_utility = SparkADLSGen2SPNConnectUtility(
        spark=spark_session,
        storage_account="test_storage_account",
        tenant_id="test_tenant_id",
        client_id="test_client_id",
        client_secret="test_client_secret",
    )

    assert adls_gen2_spn_connect_utility.system_type().value == 2
    assert adls_gen2_spn_connect_utility.libraries() == Libraries()
    assert isinstance(adls_gen2_spn_connect_utility.settings(), dict)


def test_adls_gen2_spn_connect_utility(spark_session: SparkSession):
    adls_gen2_spn_connect_utility = SparkADLSGen2SPNConnectUtility(
        spark=spark_session,
        storage_account="test_storage_account",
        tenant_id="test_tenant_id",
        client_id="test_client_id",
        client_secret="test_client_secret",
    )

    result = adls_gen2_spn_connect_utility.execute()
    assert result
    assert "OAuth" == spark_session.conf.get(
        "fs.azure.account.auth.type.test_storage_account.dfs.core.windows.net"
    )
    assert (
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        == spark_session.conf.get(
            "fs.azure.account.oauth.provider.type.test_storage_account.dfs.core.windows.net"
        )
    )
    assert "test_client_id" == spark_session.conf.get(
        "fs.azure.account.oauth2.client.id.test_storage_account.dfs.core.windows.net"
    )
    assert "test_client_secret" == spark_session.conf.get(
        "fs.azure.account.oauth2.client.secret.test_storage_account.dfs.core.windows.net"
    )
    assert (
        "https://login.microsoftonline.com/test_tenant_id/oauth2/token"
        == spark_session.conf.get(
            "fs.azure.account.oauth2.client.endpoint.test_storage_account.dfs.core.windows.net"
        )
    )
