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
from pytest_mock import MockerFixture

from src.sdk.python.rtdip_sdk.pipelines.utilities import (
    AzureAutoloaderResourcesUtility,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.constants import (
    get_default_package,
)


class MockResult:
    def __init__(self) -> None:  # NOSONAR
        pass

    def result(self, **kwargs):  # NOSONAR
        pass


class MockEventSubscription:
    def __init__(self) -> None:  # NOSONAR
        pass

    def get(self, **kwargs):
        return None

    def list_by_system_topic(self, **kwargs):
        return []

    def begin_create_or_update(self, **kwargs):
        return MockResult()


class MockSystemTopics:
    def __init__(self) -> None:  # NOSONAR
        pass

    def list_by_resource_group(self, **kwargs):
        return []

    def begin_create_or_update(self, **kwargs):
        return MockResult()


class MockQueue:
    def __init__(self) -> None:  # NOSONAR
        pass

    def create(self, **kwargs):  # NOSONAR
        pass

    def get(self, **kwargs):
        return None

    def list(self, **kwargs):
        return []


class MockStorageAccounts:
    location = "test_location"

    def get_properties(self, **kwargs):
        return self


class MockStorageManagementClient:
    storage_accounts = MockStorageAccounts()
    queue = MockQueue()

    def __init__(self) -> None:  # NOSONAR
        pass


class MockEventGridClient:
    system_topics = MockSystemTopics()
    system_topic_event_subscriptions = MockEventSubscription()
    event_subscriptions = MockEventSubscription()

    def __init__(self) -> None:  # NOSONAR
        pass


def test_azure_autoloader_resource_utility_setup():
    azure_autoloader_resource_utility = AzureAutoloaderResourcesUtility(
        subscription_id="test_subscription_id",
        resource_group_name="test_resource_group",
        storage_account="test_storage_account",
        container="test_container",
        credential="test_credential",
        directory="/test/directory",
        event_subscription_name="test_event_subscription_name",
        queue_name="test_queue_name",
    )

    assert azure_autoloader_resource_utility.system_type().value == 1
    assert azure_autoloader_resource_utility.libraries() == Libraries(
        maven_libraries=[],
        pypi_libraries=[
            get_default_package("azure_eventgrid_mgmt"),
            get_default_package("azure_storage_mgmt"),
        ],
        pythonwheel_libraries=[],
    )
    assert isinstance(azure_autoloader_resource_utility.settings(), dict)


def test_azure_autoloader_resources_utility(mocker: MockerFixture):
    azure_autoloader_resource_utility = AzureAutoloaderResourcesUtility(
        subscription_id="test_subscription_id",
        resource_group_name="test_resource_group",
        storage_account="test_storage_account",
        container="test_container",
        credential="test_credential",
        directory="/test/directory",
        event_subscription_name="test_event_subscription_name",
        queue_name="test_queue_name",
    )

    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.utilities.azure.autoloader_resources.EventGridManagementClient",
        return_value=MockEventGridClient(),
    )

    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.utilities.azure.autoloader_resources.StorageManagementClient",
        return_value=MockStorageManagementClient(),
    )
    result = azure_autoloader_resource_utility.execute()
    assert result
