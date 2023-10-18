# Copyright 2023 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from ..interfaces import UtilitiesInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package

from azure.core.credentials import TokenCredential
from azure.mgmt.eventgrid import EventGridManagementClient
from azure.mgmt.eventgrid.models import (
    EventSubscription,
    EventSubscriptionFilter,
    StorageQueueEventSubscriptionDestination,
    EventDeliverySchema,
    StringContainsAdvancedFilter,
    RetryPolicy,
    SystemTopic,
)
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.storage.models import StorageQueue


class AzureAutoloaderResourcesUtility(UtilitiesInterface):
    """
    Creates the required Azure Resources for the Databricks Autoloader Notification Mode

    Args:
        subscription_id (str): Azure Subscription ID
        resource_group_name (str): Resource Group Name of Subscription
        storage_account (str): Storage Account Name
        container (str): Container Name
        directory (str): Directory to be used for filtering messages in the Event Subscription. This will be equivalent to the Databricks Autoloader Path
        credential (TokenCredential): Credentials to authenticate with Storage Account
        event_subscription_name (str): Name of the Event Subscription
        queue_name (str): Name of the queue that will be used for the Endpoint of the Messages
        system_topic_name (optional, str): The system topic name. Defaults to the storage account name if not provided.
    """

    subscription_id: str
    resource_group_name: str
    storage_account: str
    container: str
    directory: str
    credential: TokenCredential
    event_subscription_name: str
    queue_name: str
    system_topic_name: str

    def __init__(
        self,
        subscription_id: str,
        resource_group_name: str,
        storage_account: str,
        container: str,
        directory: str,
        credential: TokenCredential,
        event_subscription_name: str,
        queue_name: str,
        system_topic_name: str = None,
    ) -> None:
        self.subscription_id = subscription_id
        self.resource_group_name = resource_group_name
        self.storage_account = storage_account
        self.container = container
        self.directory = directory
        self.credential = credential
        self.event_subscription_name = event_subscription_name
        self.queue_name = queue_name
        self.system_topic_name = (
            storage_account if system_topic_name is None else system_topic_name
        )

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYTHON
        """
        return SystemType.PYTHON

    @staticmethod
    def libraries():
        libraries = Libraries()
        libraries.add_pypi_library(get_default_package("azure_eventgrid_mgmt"))
        libraries.add_pypi_library(get_default_package("azure_storage_mgmt"))
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def execute(self) -> bool:
        storage_mgmt_client = StorageManagementClient(
            credential=self.credential, subscription_id=self.subscription_id
        )

        account_properties = storage_mgmt_client.storage_accounts.get_properties(
            resource_group_name=self.resource_group_name,
            account_name=self.storage_account,
        )

        queue_response = storage_mgmt_client.queue.list(
            resource_group_name=self.resource_group_name,
            account_name=self.storage_account,
        )

        queue_list = [
            queue for queue in queue_response if queue.name == self.queue_name
        ]

        if queue_list == []:
            storage_mgmt_client.queue.create(
                resource_group_name=self.resource_group_name,
                account_name=self.storage_account,
                queue_name=self.queue_name,
                queue=StorageQueue(),
            )

        eventgrid_client = EventGridManagementClient(
            credential=self.credential, subscription_id=self.subscription_id
        )

        system_topic_response = eventgrid_client.system_topics.list_by_resource_group(
            resource_group_name=self.resource_group_name,
            filter="name eq '{}'".format(self.system_topic_name),
        )

        source = "/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Storage/StorageAccounts/{}".format(
            self.subscription_id, self.resource_group_name, self.storage_account
        )

        system_topic_list = [
            system_topic
            for system_topic in system_topic_response
            if system_topic.source == source
        ]

        if system_topic_list == []:
            eventgrid_client.system_topics.begin_create_or_update(
                resource_group_name=self.resource_group_name,
                system_topic_name=self.system_topic_name,
                system_topic_info=SystemTopic(
                    location=account_properties.location,
                    source=source,
                    topic_type="Microsoft.Storage.StorageAccounts",
                ),
            ).result()

        system_topic_event_subscription_response = (
            eventgrid_client.system_topic_event_subscriptions.list_by_system_topic(
                resource_group_name=self.resource_group_name,
                system_topic_name=self.system_topic_name,
                filter="name eq '{}'".format(self.event_subscription_name),
            )
        )

        system_topic_event_subscription_list = [
            system_topic_event_subscription
            for system_topic_event_subscription in system_topic_event_subscription_response
            if system_topic_event_subscription.source == source
        ]

        if system_topic_event_subscription_list == []:
            event_subscription_destination = StorageQueueEventSubscriptionDestination(
                resource_id=source,
                queue_name=self.queue_name,
                queue_message_time_to_live_in_seconds=None,
            )

            event_subscription_filter = EventSubscriptionFilter(
                subject_begins_with="/blobServices/default/containers/{}/blobs/{}".format(
                    self.container, self.directory
                ),
                included_event_types=[
                    "Microsoft.Storage.BlobCreated",
                    "Microsoft.Storage.BlobRenamed",
                    "Microsoft.Storage.DirectoryRenamed",
                ],
                advanced_filters=[
                    StringContainsAdvancedFilter(
                        key="data.api",
                        values=[
                            "CopyBlob",
                            "PutBlob",
                            "PutBlockList",
                            "FlushWithClose",
                            "RenameFile",
                            "RenameDirectory",
                        ],
                    )
                ],
            )

            retry_policy = RetryPolicy()

            event_subscription_info = EventSubscription(
                destination=event_subscription_destination,
                filter=event_subscription_filter,
                event_delivery_schema=EventDeliverySchema.EVENT_GRID_SCHEMA,
                retry_policy=retry_policy,
            )

            eventgrid_client.system_topic_event_subscriptions.begin_create_or_update(
                resource_group_name=self.resource_group_name,
                system_topic_name=self.system_topic_name,
                event_subscription_name=self.event_subscription_name,
                event_subscription_info=event_subscription_info,
            ).result()
            return True
