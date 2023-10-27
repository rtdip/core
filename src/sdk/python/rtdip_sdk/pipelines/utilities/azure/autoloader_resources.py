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
from azure.core.exceptions import ResourceNotFoundError


class AzureAutoloaderResourcesUtility(UtilitiesInterface):
    """
    Creates the required Azure Resources for the Databricks Autoloader Notification Mode.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.utilities import AzureAutoloaderResourcesUtility

    azure_autoloader_resources_utility = AzureAutoloaderResourcesUtility(
        subscription_id="YOUR-SUBSCRIPTION-ID",
        resource_group_name="YOUR-RESOURCE-GROUP",
        storage_account="YOUR-STORAGE-ACCOUNT-NAME",
        container="YOUR-CONTAINER-NAME",
        directory="DIRECTORY",
        credential="YOUR-CLIENT-ID",
        event_subscription_name="YOUR-EVENT-SUBSCRIPTION",
        queue_name="YOUR-QUEUE-NAME",
        system_topic_name=None
    )

    result = azure_autoloader_resources_utility.execute()
    ```

    Parameters:
        subscription_id (str): Azure Subscription ID
        resource_group_name (str): Resource Group Name of Subscription
        storage_account (str): Storage Account Name
        container (str): Container Name
        directory (str): Directory to be used for filtering messages in the Event Subscription. This will be equivalent to the Databricks Autoloader Path
        credential (TokenCredential): Credentials to authenticate with Storage Account
        event_subscription_name (str): Name of the Event Subscription
        queue_name (str): Name of the queue that will be used for the Endpoint of the Messages
    """

    subscription_id: str
    resource_group_name: str
    storage_account: str
    container: str
    directory: str
    credential: TokenCredential
    event_subscription_name: str
    queue_name: str

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
    ) -> None:
        self.subscription_id = subscription_id
        self.resource_group_name = resource_group_name
        self.storage_account = storage_account
        self.container = container
        self.directory = directory
        self.credential = credential
        self.event_subscription_name = event_subscription_name
        self.queue_name = queue_name

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

        try:
            queue_response = storage_mgmt_client.queue.get(
                resource_group_name=self.resource_group_name,
                account_name=self.storage_account,
                queue_name=self.queue_name,
            )
        except ResourceNotFoundError:
            queue_response = None

        if queue_response == None:
            storage_mgmt_client.queue.create(
                resource_group_name=self.resource_group_name,
                account_name=self.storage_account,
                queue_name=self.queue_name,
                queue=StorageQueue(),
            )

        eventgrid_client = EventGridManagementClient(
            credential=self.credential, subscription_id=self.subscription_id
        )

        source = "/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Storage/StorageAccounts/{}".format(
            self.subscription_id, self.resource_group_name, self.storage_account
        )

        try:
            event_subscription_response = eventgrid_client.event_subscriptions.get(
                scope=source, event_subscription_name=self.event_subscription_name
            )
        except ResourceNotFoundError:
            event_subscription_response = None

        if event_subscription_response == None:
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

            eventgrid_client.event_subscriptions.begin_create_or_update(
                scope=source,
                event_subscription_name=self.event_subscription_name,
                event_subscription_info=event_subscription_info,
            ).result()

            return True
