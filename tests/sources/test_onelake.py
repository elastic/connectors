from contextlib import asynccontextmanager
from datetime import datetime
from functools import partial
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from connectors.sources.onelake import OneLakeDataSource
from tests.sources.support import create_source


@asynccontextmanager
async def create_onelake_source(
    use_text_extraction_service=False,
):
    async with create_source(
        OneLakeDataSource,
        tenant_id="fake-tenant",
        client_id="fake-client",
        client_secret="fake-client-secret",
        workspace_name="FakeWorkspace",
        data_path="FakeDatalake.Lakehouse/Files/Data",
        account_name="onelake",
        use_text_extraction_service=use_text_extraction_service,
    ) as source:
        yield source


@pytest.mark.asyncio
async def test_init():
    """Test OneLakeDataSource initialization"""

    async with create_onelake_source() as source:
        # Check that all configuration values are set correctly
        assert source.tenant_id == source.configuration["tenant_id"]
        assert source.client_id == source.configuration["client_id"]
        assert source.client_secret == source.configuration["client_secret"]
        assert source.workspace_name == source.configuration["workspace_name"]
        assert source.data_path == source.configuration["data_path"]
        assert (
            source.account_url
            == f"https://{source.configuration['account_name']}.dfs.fabric.microsoft.com"
        )

        # Check that clients are initially None
        assert source.service_client is None
        assert source.file_system_client is None
        assert source.directory_client is None


def test_get_default_configuration():
    """Test get_default_configuration class method"""

    config = OneLakeDataSource.get_default_configuration()

    # Check that all required configuration fields are present
    required_fields = [
        "tenant_id",
        "client_id",
        "client_secret",
        "workspace_name",
        "data_path",
        "account_name",
    ]

    for field in required_fields:
        assert field in config
        assert "label" in config[field]
        assert "order" in config[field]
        assert "type" in config[field]

    # Check specific configurations
    assert config["account_name"]["default_value"] == "onelake"
    assert config["client_secret"]["sensitive"] is True
    assert config["account_name"]["required"] is False


@pytest.mark.asyncio
async def test_initialize():
    """Test initialize method"""

    async with create_onelake_source() as source:
        mock_service_client = Mock()
        mock_file_system_client = Mock()
        mock_directory_client = Mock()

        with patch.object(
            source, "_get_service_client", new_callable=AsyncMock
        ) as mock_get_service:
            with patch.object(
                source, "_get_file_system_client", new_callable=AsyncMock
            ) as mock_get_fs:
                with patch.object(
                    source, "_get_directory_client", new_callable=AsyncMock
                ) as mock_get_dir:
                    mock_get_service.return_value = mock_service_client
                    mock_get_fs.return_value = mock_file_system_client
                    mock_get_dir.return_value = mock_directory_client

                    # Test first initialization
                    await source.initialize()

                    assert source.service_client == mock_service_client
                    assert source.file_system_client == mock_file_system_client
                    assert source.directory_client == mock_directory_client

                    mock_get_service.assert_called_once()
                    mock_get_fs.assert_called_once()
                    mock_get_dir.assert_called_once()

                    # Test that it doesn't re-initialize if service_client already exists
                    mock_get_service.reset_mock()
                    mock_get_fs.reset_mock()
                    mock_get_dir.reset_mock()

                    await source.initialize()

                    # Should not be called again
                    mock_get_service.assert_not_called()
                    mock_get_fs.assert_not_called()
                    mock_get_dir.assert_not_called()


@pytest.mark.asyncio
async def test_ping_for_successful_connection():
    """Test ping method of OneLakeDataSource class"""

    # Setup
    async with create_onelake_source() as source:
        with patch.object(
            source, "_get_directory_paths", new_callable=AsyncMock
        ) as mock_get_paths:
            mock_get_paths.return_value = []

            await source.ping()

            mock_get_paths.assert_called_once_with(source.configuration["data_path"])


@pytest.mark.asyncio
async def test_ping_for_failed_connection():
    """Test ping method of OneLakeDataSource class with negative case"""

    # Setup
    async with create_onelake_source() as source:
        with patch.object(
            source, "_get_directory_paths", new_callable=AsyncMock
        ) as mock_get_paths:
            mock_get_paths.side_effect = Exception("Something went wrong")

            # Run & Check
            with pytest.raises(Exception, match="Something went wrong"):
                await source.ping()

            mock_get_paths.assert_called_once_with(source.configuration["data_path"])


@pytest.mark.asyncio
async def test_get_token_credentials():
    """Test _get_token_credentials method of OneLakeDataSource class"""

    # Setup
    async with create_onelake_source() as source:
        tenant_id = source.configuration["tenant_id"]
        client_id = source.configuration["client_id"]
        client_secret = source.configuration["client_secret"]

        with patch(
            "connectors.sources.onelake.ClientSecretCredential", autospec=True
        ) as mock_credential:
            mock_instance = mock_credential.return_value

            # Run
            credentials = source._get_token_credentials()

            # Check
            mock_credential.assert_called_once_with(tenant_id, client_id, client_secret)
            assert credentials is mock_instance


@pytest.mark.asyncio
async def test_get_token_credentials_error():
    """Test _get_token_credentials method when credential creation fails"""

    async with create_onelake_source() as source:
        with patch(
            "connectors.sources.onelake.ClientSecretCredential", autospec=True
        ) as mock_credential:
            mock_credential.side_effect = Exception("Credential error")

            with pytest.raises(Exception, match="Credential error"):
                source._get_token_credentials()


@pytest.mark.asyncio
async def test_get_service_client():
    """Test _get_service_client method of OneLakeDataSource class"""

    # Setup
    async with create_onelake_source() as source:
        mock_service_client = Mock()
        mock_credentials = Mock()

        with patch(
            "connectors.sources.onelake.DataLakeServiceClient",
            autospec=True,
        ) as mock_client, patch.object(
            source, "_get_token_credentials", return_value=mock_credentials
        ):
            mock_client.return_value = mock_service_client

            # Run
            service_client = await source._get_service_client()

            # Check
            mock_client.assert_called_once_with(
                account_url=source.account_url,
                credential=mock_credentials,
            )
            assert service_client is mock_service_client


@pytest.mark.asyncio
async def test_get_service_client_error():
    """Test _get_service_client method when client creation fails"""

    async with create_onelake_source() as source:
        with patch(
            "connectors.sources.onelake.DataLakeServiceClient",
            side_effect=Exception("Service client error"),
        ):
            with pytest.raises(Exception, match="Service client error"):
                await source._get_service_client()


@pytest.mark.asyncio
async def test_get_file_system_client():
    """Test _get_file_system_client method of OneLakeDataSource class"""

    # Setup
    async with create_onelake_source() as source:
        mock_file_system_client = Mock()
        workspace_name = source.configuration["workspace_name"]

        # Set up the service_client that _get_file_system_client depends on
        mock_service_client = Mock()
        mock_service_client.get_file_system_client.return_value = (
            mock_file_system_client
        )
        source.service_client = mock_service_client

        # Run
        file_system_client = await source._get_file_system_client()

        # Check
        mock_service_client.get_file_system_client.assert_called_once_with(
            workspace_name
        )
        assert file_system_client == mock_file_system_client


@pytest.mark.asyncio
async def test_get_file_system_client_error():
    """Test _get_file_system_client method when client creation fails"""

    async with create_onelake_source() as source:
        mock_service_client = Mock()
        mock_service_client.get_file_system_client.side_effect = Exception("Test error")
        source.service_client = mock_service_client

        with pytest.raises(Exception, match="Test error"):
            await source._get_file_system_client()


@pytest.mark.asyncio
async def test_get_directory_client():
    """Test _get_directory_client method of OneLakeDataSource class"""

    # Setup
    async with create_onelake_source() as source:
        mock_directory_client = Mock()
        data_path = source.configuration["data_path"]

        # Set up the file_system_client that _get_directory_client depends on
        mock_file_system_client = Mock()
        mock_file_system_client.get_directory_client.return_value = (
            mock_directory_client
        )
        source.file_system_client = mock_file_system_client

        # Run
        directory_client = await source._get_directory_client()

        # Check
        mock_file_system_client.get_directory_client.assert_called_once_with(data_path)
        assert directory_client == mock_directory_client


@pytest.mark.asyncio
async def test_get_directory_client_error():
    """Test _get_directory_client method when client creation fails"""

    async with create_onelake_source() as source:
        mock_file_system_client = Mock()
        mock_file_system_client.get_directory_client.side_effect = Exception(
            "Test error"
        )
        source.file_system_client = mock_file_system_client

        with pytest.raises(Exception, match="Test error"):
            await source._get_directory_client()


@pytest.mark.asyncio
async def test_get_file_client_success():
    """Test successful file client retrieval"""

    mock_file_client = Mock()
    mock_directory_client = Mock()
    mock_directory_client.get_file_client.return_value = mock_file_client

    async with create_onelake_source() as source:
        # Mock the directory_client directly since that's what _get_file_client uses
        source.directory_client = mock_directory_client

        result = await source._get_file_client("test.txt")

        assert result == mock_file_client
        mock_directory_client.get_file_client.assert_called_once_with("test.txt")


@pytest.mark.asyncio
async def test_get_file_client_error():
    """Test file client retrieval with error"""

    async with create_onelake_source() as source:
        mock_directory_client = Mock()
        mock_directory_client.get_file_client.side_effect = Exception(
            "Error while getting file client"
        )
        source.directory_client = mock_directory_client

        with pytest.raises(Exception, match="Error while getting file client"):
            await source._get_file_client("test.txt")


@pytest.mark.asyncio
async def test_get_directory_paths():
    """Test _get_directory_paths method of OneLakeDataSource class"""

    # Setup
    async with create_onelake_source() as source:
        mock_paths = ["path1", "path2"]
        directory_path = "mock_directory_path"

        # Set up the file_system_client so initialize() is not called
        mock_file_system_client = Mock()
        source.file_system_client = mock_file_system_client

        # Mock the run_in_executor call
        with patch("asyncio.get_running_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(return_value=mock_paths)

            # Run
            paths = await source._get_directory_paths(directory_path)

            # Check
            assert paths == mock_paths
            mock_loop.return_value.run_in_executor.assert_called_once_with(
                None, mock_file_system_client.get_paths, directory_path
            )


@pytest.mark.asyncio
async def test_get_directory_paths_with_initialize():
    """Test _get_directory_paths method when file_system_client is None"""

    async with create_onelake_source() as source:
        mock_paths = ["path1", "path2"]
        directory_path = "mock_directory_path"

        # Ensure file_system_client is None to trigger initialize()
        source.file_system_client = None

        # Mock initialize to set up file_system_client
        async def mock_initialize():
            mock_file_system_client = Mock()
            source.file_system_client = mock_file_system_client

        with patch.object(
            source, "initialize", side_effect=mock_initialize
        ) as mock_init:
            with patch("asyncio.get_running_loop") as mock_loop:
                mock_loop.return_value.run_in_executor = AsyncMock(
                    return_value=mock_paths
                )

                # Run
                paths = await source._get_directory_paths(directory_path)

                # Check
                mock_init.assert_called_once()
                assert paths == mock_paths


@pytest.mark.asyncio
async def test_get_directory_paths_error():
    """Test _get_directory_paths method when getting paths fails"""

    async with create_onelake_source() as source:
        directory_path = "mock_directory_path"

        # Set up the file_system_client so initialize() is not called
        mock_file_system_client = Mock()
        source.file_system_client = mock_file_system_client

        with patch("asyncio.get_running_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                side_effect=Exception("Error while getting directory paths")
            )

            with pytest.raises(Exception, match="Error while getting directory paths"):
                await source._get_directory_paths(directory_path)


@pytest.mark.asyncio
async def test_format_file():
    """Test format_file method of OneLakeDataSource class"""

    # Setup
    async with create_onelake_source() as source:
        mock_file_client = MagicMock()
        mock_file_properties = MagicMock(
            creation_time=datetime(2022, 4, 21, 12, 12, 30),
            last_modified=datetime(2022, 4, 22, 15, 45, 10),
            size=2048,
            name="path/to/file.txt",
        )

        mock_file_properties.name.split.return_value = ["path", "to", "file.txt"]
        mock_file_client.file_system_name = "my_file_system"

        expected_output = {
            "_id": "my_file_system_file.txt",
            "name": "file.txt",
            "created_at": "2022-04-21T12:12:30",
            "_timestamp": "2022-04-22T15:45:10",
            "size": 2048,
        }

        # Mock the run_in_executor call since format_file is now async
        with patch("asyncio.get_running_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value=mock_file_properties
            )

            # Execute
            actual_output = await source.format_file(mock_file_client)

            # Assert
            assert actual_output == expected_output


@pytest.mark.asyncio
async def test_format_file_error():
    """Test format_file method when getting properties fails"""

    async with create_onelake_source() as source:
        mock_file_client = MagicMock()
        mock_file_client.file_system_name = "my_file_system"

        with patch("asyncio.get_running_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                side_effect=Exception("Test error")
            )

            with pytest.raises(Exception, match="Test error"):
                await source.format_file(mock_file_client)


@pytest.mark.asyncio
async def test_format_file_empty_name():
    """Test format_file method with empty file name"""

    async with create_onelake_source() as source:
        mock_file_client = MagicMock()
        mock_file_properties = MagicMock(
            creation_time=datetime(2022, 4, 21, 12, 12, 30),
            last_modified=datetime(2022, 4, 22, 15, 45, 10),
            size=2048,
            name="",
        )
        mock_file_properties.name.split.return_value = [""]
        mock_file_client.file_system_name = "my_file_system"

        with patch("asyncio.get_running_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value=mock_file_properties
            )

            result = await source.format_file(mock_file_client)
            assert result["name"] == ""
            assert result["_id"] == "my_file_system_"


@pytest.mark.asyncio
async def test_download_file():
    """Test download_file method of OneLakeDataSource class"""

    # Setup
    mock_file_client = Mock()
    mock_download = Mock()
    mock_chunks = ["chunk1", "chunk2", "chunk3"]

    async with create_onelake_source() as source:
        with patch("asyncio.get_running_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value=mock_download
            )
            mock_download.chunks.return_value = iter(mock_chunks)

            # Run
            chunks = []
            async for chunk in source.download_file(mock_file_client):
                chunks.append(chunk)

            # Check
            assert chunks == mock_chunks
            mock_loop.return_value.run_in_executor.assert_called_once()


@pytest.mark.asyncio
async def test_download_file_with_error():
    """Test download_file method of OneLakeDataSource class with exception handling"""

    # Setup
    mock_file_client = Mock()

    async with create_onelake_source() as source:
        with patch("asyncio.get_running_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                side_effect=Exception("Test error")
            )

            # Run & Check
            with pytest.raises(Exception, match="Test error"):
                async for _ in source.download_file(mock_file_client):
                    pass


@pytest.mark.asyncio
async def test_get_content_with_download():
    """Test get_content method when doit=True"""

    async with create_onelake_source() as source:

        class FileClientMock:
            file_system_name = "mockfilesystem"

            class FileProperties:
                def __init__(self, name, size):
                    self.name = name
                    self.size = size

            def get_file_properties(self):
                return self.FileProperties(name="file1.txt", size=2000)

        with patch.object(
            source,
            "_get_file_client",
            new_callable=AsyncMock,
            return_value=FileClientMock(),
        ), patch.object(
            source, "can_file_be_downloaded", return_value=True
        ), patch.object(
            source, "get_file_extension", return_value="txt"
        ), patch.object(
            source,
            "download_and_extract_file",
            new_callable=AsyncMock,
            return_value={
                "_id": "mockfilesystem_file1.txt",
                "_attachment": "TW9jayBjb250ZW50",
            },
        ):
            actual_response = await source.get_content("file1.txt", doit=True)
            assert actual_response == {
                "_id": "mockfilesystem_file1.txt",
                "_attachment": "TW9jayBjb250ZW50",
            }


@pytest.mark.asyncio
async def test_get_content_without_download():
    """Test get_content method when doit=False"""

    async with create_onelake_source() as source:
        actual_response = await source.get_content("file1.txt", doit=False)
        assert actual_response is None


@pytest.mark.asyncio
async def test_prepare_files():
    """Test prepare_files method of OneLakeDataSource class"""

    # Setup
    doc_paths = [
        Mock(name="doc1.txt"),
        Mock(name="doc2.txt"),
    ]

    async with create_onelake_source() as source:
        mock_file_results = [
            {"name": "doc1.txt", "id": "1"},
            {"name": "doc2.txt", "id": "2"},
        ]

        with patch.object(
            source, "_process_items_concurrently", new_callable=AsyncMock
        ) as mock_process:
            mock_process.return_value = mock_file_results

            result = []
            async for item in source.prepare_files(doc_paths):
                result.append(item)

            # Check results
            assert result == mock_file_results
            # Check that _process_items_concurrently was called with the paths
            mock_process.assert_called_once()
            call_args = mock_process.call_args[0]
            assert call_args[0] == doc_paths  # First argument should be the paths


@pytest.mark.asyncio
async def test_get_docs():
    """Test get_docs method of OneLakeDataSource class"""

    mock_paths = [
        Mock(name="doc1", path="folder/doc1"),
        Mock(name="doc2", path="folder/doc2"),
    ]

    mock_file_docs = [{"name": "doc1", "id": "1"}, {"name": "doc2", "id": "2"}]

    async def mock_prepare_files_impl(paths):
        for doc in mock_file_docs:
            yield doc

    async with create_onelake_source() as source:
        with patch.object(
            source, "_get_directory_paths", new_callable=AsyncMock
        ) as mock_get_paths:
            mock_get_paths.return_value = mock_paths

            with patch.object(
                source, "prepare_files", side_effect=mock_prepare_files_impl
            ):
                result = []
                async for doc, get_content in source.get_docs():
                    result.append((doc, get_content))

                mock_get_paths.assert_called_once_with(
                    source.configuration["data_path"]
                )
                assert len(result) == 2
                for (doc, get_content), expected_doc in zip(result, mock_file_docs):
                    assert doc == expected_doc
                    assert isinstance(get_content, partial)
                    assert get_content.func == source.get_content
                    assert get_content.args == (doc["name"],)


@pytest.mark.asyncio
async def test_process_items_concurrently():
    """Test _process_items_concurrently method"""

    async with create_onelake_source() as source:
        items = ["item1", "item2", "item3"]

        async def mock_process_item(item):
            return f"processed_{item}"

        result = await source._process_items_concurrently(items, mock_process_item)

        expected = ["processed_item1", "processed_item2", "processed_item3"]
        assert result == expected


@pytest.mark.asyncio
async def test_process_items_concurrently_with_custom_concurrency():
    """Test _process_items_concurrently method with custom max_concurrency"""

    async with create_onelake_source() as source:
        items = ["item1", "item2"]

        async def mock_process_item(item):
            return f"processed_{item}"

        result = await source._process_items_concurrently(
            items, mock_process_item, max_concurrency=1
        )

        expected = ["processed_item1", "processed_item2"]
        assert result == expected


@pytest.mark.asyncio
async def test_get_files_properties():
    """Test get_files_properties method"""

    async with create_onelake_source() as source:
        mock_file_client1 = Mock()
        mock_file_client2 = Mock()
        mock_properties1 = Mock()
        mock_properties2 = Mock()

        mock_file_client1.get_file_properties.return_value = mock_properties1
        mock_file_client2.get_file_properties.return_value = mock_properties2

        file_clients = [mock_file_client1, mock_file_client2]

        with patch.object(
            source, "_process_items_concurrently", new_callable=AsyncMock
        ) as mock_process:
            mock_process.return_value = [mock_properties1, mock_properties2]

            result = await source.get_files_properties(file_clients)

            assert result == [mock_properties1, mock_properties2]
            mock_process.assert_called_once()
            # Check that the first argument is the file_clients list
            call_args = mock_process.call_args[0]
            assert call_args[0] == file_clients


@pytest.mark.asyncio
async def test_get_content_file_cannot_be_downloaded():
    """Test get_content method when file cannot be downloaded"""

    async with create_onelake_source() as source:

        class FileClientMock:
            file_system_name = "mockfilesystem"

            class FileProperties:
                def __init__(self, name, size):
                    self.name = name
                    self.size = size

            def get_file_properties(self):
                return self.FileProperties(
                    name="large_file.exe", size=200000000
                )  # Very large file

        with patch.object(
            source,
            "_get_file_client",
            new_callable=AsyncMock,
            return_value=FileClientMock(),
        ), patch.object(
            source, "can_file_be_downloaded", return_value=False  # Cannot download
        ), patch.object(
            source, "get_file_extension", return_value="exe"
        ):
            result = await source.get_content("large_file.exe", doit=True)

            # Should return only the basic doc without content
            expected = {
                "_id": "mockfilesystem_large_file.exe",
            }
            assert result == expected


@pytest.mark.asyncio
async def test_get_content_extracted_doc_is_none():
    """Test get_content method when download_and_extract_file returns None"""

    async with create_onelake_source() as source:

        class FileClientMock:
            file_system_name = "mockfilesystem"

            class FileProperties:
                def __init__(self, name, size):
                    self.name = name
                    self.size = size

            def get_file_properties(self):
                return self.FileProperties(name="file.txt", size=1000)

        with patch.object(
            source,
            "_get_file_client",
            new_callable=AsyncMock,
            return_value=FileClientMock(),
        ), patch.object(
            source, "can_file_be_downloaded", return_value=True
        ), patch.object(
            source, "get_file_extension", return_value="txt"
        ), patch.object(
            source,
            "download_and_extract_file",
            new_callable=AsyncMock,
            return_value=None,
        ):
            result = await source.get_content("file.txt", doit=True)

            # Should return the basic doc when extracted_doc is None
            expected = {
                "_id": "mockfilesystem_file.txt",
            }
            assert result == expected
