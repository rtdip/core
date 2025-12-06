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
from src.sdk.python.rtdip_sdk.pipelines.sources.python.azure_blob import (
    PythonAzureBlobSource,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from pytest_mock import MockerFixture
import pytest
import polars as pl
from io import BytesIO

account_url = "https://testaccount.blob.core.windows.net"
container_name = "test-container"
credential = "test-sas-token"


def test_python_azure_blob_setup():
    azure_blob_source = PythonAzureBlobSource(
        account_url=account_url,
        container_name=container_name,
        credential=credential,
        file_pattern="*.parquet",
    )
    assert azure_blob_source.system_type().value == 1
    assert azure_blob_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )
    assert isinstance(azure_blob_source.settings(), dict)
    assert azure_blob_source.pre_read_validation()
    assert azure_blob_source.post_read_validation()


def test_python_azure_blob_read_batch_combine(mocker: MockerFixture):
    azure_blob_source = PythonAzureBlobSource(
        account_url=account_url,
        container_name=container_name,
        credential=credential,
        file_pattern="*.parquet",
        combine_blobs=True,
    )

    # Mock blob service client
    mock_blob_service = mocker.MagicMock()
    mock_container_client = mocker.MagicMock()
    mock_blob = mocker.MagicMock()
    mock_blob.name = "test.parquet"

    mock_container_client.list_blobs.return_value = [mock_blob]

    mock_blob_client = mocker.MagicMock()
    mock_stream = mocker.MagicMock()
    mock_stream.readall.return_value = b"test_data"
    mock_blob_client.download_blob.return_value = mock_stream

    mock_container_client.get_blob_client.return_value = mock_blob_client
    mock_blob_service.get_container_client.return_value = mock_container_client

    # Mock BlobServiceClient constructor
    mocker.patch(
        "azure.storage.blob.BlobServiceClient",
        return_value=mock_blob_service,
    )

    # Mock Polars read_parquet
    test_df = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    mocker.patch.object(pl, "read_parquet", return_value=test_df)

    lf = azure_blob_source.read_batch()
    assert isinstance(lf, pl.LazyFrame)


def test_python_azure_blob_read_batch_eager(mocker: MockerFixture):
    azure_blob_source = PythonAzureBlobSource(
        account_url=account_url,
        container_name=container_name,
        credential=credential,
        file_pattern="*.parquet",
        combine_blobs=True,
        eager=True,
    )

    # Mock blob service client
    mock_blob_service = mocker.MagicMock()
    mock_container_client = mocker.MagicMock()
    mock_blob = mocker.MagicMock()
    mock_blob.name = "test.parquet"

    mock_container_client.list_blobs.return_value = [mock_blob]

    mock_blob_client = mocker.MagicMock()
    mock_stream = mocker.MagicMock()
    mock_stream.readall.return_value = b"test_data"
    mock_blob_client.download_blob.return_value = mock_stream

    mock_container_client.get_blob_client.return_value = mock_blob_client
    mock_blob_service.get_container_client.return_value = mock_container_client

    # Mock BlobServiceClient constructor
    mocker.patch(
        "azure.storage.blob.BlobServiceClient",
        return_value=mock_blob_service,
    )

    # Mock Polars read_parquet
    test_df = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    mocker.patch.object(pl, "read_parquet", return_value=test_df)

    df = azure_blob_source.read_batch()
    assert isinstance(df, pl.DataFrame)


def test_python_azure_blob_read_batch_no_combine(mocker: MockerFixture):
    azure_blob_source = PythonAzureBlobSource(
        account_url=account_url,
        container_name=container_name,
        credential=credential,
        file_pattern="*.parquet",
        combine_blobs=False,
    )

    # Mock blob service client
    mock_blob_service = mocker.MagicMock()
    mock_container_client = mocker.MagicMock()
    mock_blob1 = mocker.MagicMock()
    mock_blob1.name = "test1.parquet"
    mock_blob2 = mocker.MagicMock()
    mock_blob2.name = "test2.parquet"

    mock_container_client.list_blobs.return_value = [mock_blob1, mock_blob2]

    mock_blob_client = mocker.MagicMock()
    mock_stream = mocker.MagicMock()
    mock_stream.readall.return_value = b"test_data"
    mock_blob_client.download_blob.return_value = mock_stream

    mock_container_client.get_blob_client.return_value = mock_blob_client
    mock_blob_service.get_container_client.return_value = mock_container_client

    # Mock BlobServiceClient constructor
    mocker.patch(
        "azure.storage.blob.BlobServiceClient",
        return_value=mock_blob_service,
    )

    # Mock Polars read_parquet
    test_df = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    mocker.patch.object(pl, "read_parquet", return_value=test_df)

    result = azure_blob_source.read_batch()
    assert isinstance(result, list)
    assert len(result) == 2
    assert all(isinstance(lf, pl.LazyFrame) for lf in result)


def test_python_azure_blob_blob_names(mocker: MockerFixture):
    azure_blob_source = PythonAzureBlobSource(
        account_url=account_url,
        container_name=container_name,
        credential=credential,
        blob_names=["specific_file.parquet"],
        combine_blobs=True,
    )

    # Mock blob service client
    mock_blob_service = mocker.MagicMock()
    mock_container_client = mocker.MagicMock()

    mock_blob_client = mocker.MagicMock()
    mock_stream = mocker.MagicMock()
    mock_stream.readall.return_value = b"test_data"
    mock_blob_client.download_blob.return_value = mock_stream

    mock_container_client.get_blob_client.return_value = mock_blob_client
    mock_blob_service.get_container_client.return_value = mock_container_client

    # Mock BlobServiceClient constructor
    mocker.patch(
        "azure.storage.blob.BlobServiceClient",
        return_value=mock_blob_service,
    )

    # Mock Polars read_parquet
    test_df = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    mocker.patch.object(pl, "read_parquet", return_value=test_df)

    lf = azure_blob_source.read_batch()
    assert isinstance(lf, pl.LazyFrame)


def test_python_azure_blob_pattern_matching(mocker: MockerFixture):
    azure_blob_source = PythonAzureBlobSource(
        account_url=account_url,
        container_name=container_name,
        credential=credential,
        file_pattern="*.parquet",
    )

    # Mock blob service client
    mock_blob_service = mocker.MagicMock()
    mock_container_client = mocker.MagicMock()

    # Create mock blobs with different naming patterns
    mock_blob1 = mocker.MagicMock()
    mock_blob1.name = "data.parquet"
    mock_blob2 = mocker.MagicMock()
    mock_blob2.name = "Data/2024/file.parquet_DataFrame_1"  # Shell-style naming
    mock_blob3 = mocker.MagicMock()
    mock_blob3.name = "test.csv"

    mock_container_client.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]

    # Get the actual blob list using the real method
    blob_list = azure_blob_source._get_blob_list(mock_container_client)

    # Should match both parquet files (standard and Shell-style)
    assert len(blob_list) == 2
    assert "data.parquet" in blob_list
    assert "Data/2024/file.parquet_DataFrame_1" in blob_list
    assert "test.csv" not in blob_list


def test_python_azure_blob_no_blobs_found(mocker: MockerFixture):
    azure_blob_source = PythonAzureBlobSource(
        account_url=account_url,
        container_name=container_name,
        credential=credential,
        file_pattern="*.parquet",
    )

    # Mock blob service client
    mock_blob_service = mocker.MagicMock()
    mock_container_client = mocker.MagicMock()
    mock_container_client.list_blobs.return_value = []

    mock_blob_service.get_container_client.return_value = mock_container_client

    # Mock BlobServiceClient constructor
    mocker.patch(
        "azure.storage.blob.BlobServiceClient",
        return_value=mock_blob_service,
    )

    with pytest.raises(ValueError, match="No blobs found matching pattern"):
        azure_blob_source.read_batch()


def test_python_azure_blob_read_stream():
    azure_blob_source = PythonAzureBlobSource(
        account_url=account_url,
        container_name=container_name,
        credential=credential,
        file_pattern="*.parquet",
    )
    with pytest.raises(NotImplementedError):
        azure_blob_source.read_stream()
