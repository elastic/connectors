from contextlib import asynccontextmanager
from datetime import datetime
from functools import partial
from unittest.mock import AsyncMock, MagicMock, Mock, call, patch

import pytest

from connectors.sources.onelake import OneLakeDataSource
from tests.sources.support import create_source


@asynccontextmanager
async def create_abs_source(
    use_text_extraction_service=False,
):
    async with create_source(
        OneLakeDataSource,
        tenant_id="fake-tenant",
        client_id="-fake-client",
        client_secret="fake-client",
        workspace_name="FakeWorkspace",
        data_path="FakeDatalake.Lakehouse/Files/Data",
        use_text_extraction_service=use_text_extraction_service,
    ) as source:
        yield source


@pytest.mark.asyncio
async def test_ping_for_successful_connection():
    """Test ping method of OneLakeDataSource class"""

    # Setup
    async with create_abs_source() as source:
        with patch.object(
            source, "_get_directory_paths", new_callable=AsyncMock
        ) as mock_get_paths:
            mock_get_paths.return_value = []

            # Run
            await source.ping()

            # Check
            mock_get_paths.assert_called_once_with(source.configuration["data_path"])


@pytest.mark.asyncio
async def test_ping_for_failed_connection():
    """Test ping method of OneLakeDataSource class with negative case"""

    # Setup
    async with create_abs_source() as source:
        with patch.object(
            source, "_get_directory_paths", new_callable=AsyncMock
        ) as mock_get_paths:
            mock_get_paths.side_effect = Exception("Something went wrong")

            # Run & Check
            with pytest.raises(Exception, match="Something went wrong"):
                await source.ping()

            mock_get_paths.assert_called_once_with(source.configuration["data_path"])

        # Cleanup
        mock_get_paths.reset_mock


@pytest.mark.asyncio
async def test_get_account_url():
    """Test _get_account_url method of OneLakeDataSource class"""

    # Setup
    async with create_abs_source() as source:
        account_name = source.configuration["account_name"]
        expected_url = f"https://{account_name}.dfs.fabric.microsoft.com"

        # Run
        actual_url = source._get_account_url()

        # Check
        assert actual_url == expected_url


@pytest.mark.asyncio
async def test_get_token_credentials():
    """Test _get_token_credentials method of OneLakeDataSource class"""

    # Setup
    async with create_abs_source() as source:
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

    async with create_abs_source() as source:
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
    async with create_abs_source() as source:
        mock_service_client = Mock()
        mock_account_url = "https://mockaccount.dfs.fabric.microsoft.com"
        mock_credentials = Mock()

        with patch(
            "connectors.sources.onelake.DataLakeServiceClient",
            autospec=True,
        ) as mock_client, patch.object(
            source,
            "_get_account_url",
            return_value=mock_account_url,
        ), patch.object(
            source, "_get_token_credentials", return_value=mock_credentials
        ):
            mock_client.return_value = mock_service_client

            # Run
            service_client = await source._get_service_client()

            # Check
            mock_client.assert_called_once_with(
                account_url=mock_account_url,
                credential=mock_credentials,
            )
            assert service_client is mock_service_client


@pytest.mark.asyncio
async def test_get_service_client_error():
    """Test _get_service_client method when client creation fails"""

    async with create_abs_source() as source:
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
    async with create_abs_source() as source:
        mock_file_system_client = Mock()
        workspace_name = source.configuration["workspace_name"]

        with patch.object(
            source, "_get_service_client", new_callable=AsyncMock
        ) as mock_get_service_client:
            mock_service_client = Mock()
            mock_service_client.get_file_system_client.return_value = (
                mock_file_system_client
            )
            mock_get_service_client.return_value = mock_service_client

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

    async with create_abs_source() as source:
        mock_service_client = Mock()
        mock_service_client.get_file_system_client.side_effect = Exception(
            "File system error"
        )

        with patch.object(
            source, "_get_service_client", new_callable=AsyncMock
        ) as mock_get_service_client:
            mock_get_service_client.return_value = mock_service_client

            with pytest.raises(Exception, match="File system error"):
                await source._get_file_system_client()


@pytest.mark.asyncio
async def test_get_directory_client():
    """Test _get_directory_client method of OneLakeDataSource class"""

    # Setup
    async with create_abs_source() as source:
        mock_directory_client = Mock()
        data_path = source.configuration["data_path"]

        with patch.object(
            source, "_get_file_system_client", new_callable=AsyncMock
        ) as mock_get_file_system_client:
            mock_file_system_client = Mock()
            mock_file_system_client.get_directory_client.return_value = (
                mock_directory_client
            )
            mock_get_file_system_client.return_value = mock_file_system_client

            # Run
            directory_client = await source._get_directory_client()

            # Check
            mock_file_system_client.get_directory_client.assert_called_once_with(
                data_path
            )
            assert directory_client == mock_directory_client


@pytest.mark.asyncio
async def test_get_directory_client_error():
    """Test _get_directory_client method when client creation fails"""

    async with create_abs_source() as source:
        mock_file_system_client = Mock()
        mock_file_system_client.get_directory_client.side_effect = Exception(
            "Directory error"
        )

        with patch.object(
            source, "_get_file_system_client", new_callable=AsyncMock
        ) as mock_get_file_system_client:
            mock_get_file_system_client.return_value = mock_file_system_client

            with pytest.raises(Exception, match="Directory error"):
                await source._get_directory_client()


@pytest.mark.asyncio
async def test_get_file_client_success():
    """Test successful file client retrieval"""

    mock_file_client = Mock()
    mock_directory_client = Mock()
    mock_directory_client.get_file_client.return_value = mock_file_client

    async with create_abs_source() as source:
        with patch.object(
            source, "_get_directory_client", new_callable=AsyncMock
        ) as mock_get_directory:
            mock_get_directory.return_value = mock_directory_client

            result = await source._get_file_client("test.txt")

            assert result == mock_file_client
            mock_directory_client.get_file_client.assert_called_once_with("test.txt")


@pytest.mark.asyncio
async def test_get_file_client_error():
    """Test file client retrieval with error"""

    async with create_abs_source() as source:
        with patch.object(
            source, "_get_directory_client", new_callable=AsyncMock
        ) as mock_get_directory:
            mock_get_directory.side_effect = Exception("Test error")

            with pytest.raises(Exception, match="Test error"):
                await source._get_file_client("test.txt")


@pytest.mark.asyncio
async def test_get_directory_paths():
    """Test _get_directory_paths method of OneLakeDataSource class"""

    # Setup
    async with create_abs_source() as source:
        mock_paths = ["path1", "path2"]
        directory_path = "mock_directory_path"

        with patch.object(
            source, "_get_file_system_client", new_callable=AsyncMock
        ) as mock_get_file_system_client:
            mock_get_paths = Mock(return_value=mock_paths)
            mock_file_system_client = mock_get_file_system_client.return_value
            mock_file_system_client.get_paths = mock_get_paths

            # Run
            paths = await source._get_directory_paths(directory_path)

            # Check
            mock_file_system_client.get_paths.assert_called_once_with(
                path=directory_path
            )
            assert paths == mock_paths


@pytest.mark.asyncio
async def test_get_directory_paths_error():
    """Test _get_directory_paths method when getting paths fails"""

    async with create_abs_source() as source:
        directory_path = "mock_directory_path"
        with patch.object(
            source, "_get_file_system_client", new_callable=AsyncMock
        ) as mock_get_file_system_client:
            mock_file_system_client = mock_get_file_system_client.return_value
            mock_file_system_client.get_paths = AsyncMock(
                side_effect=Exception("Path error")
            )

            with pytest.raises(Exception, match="Path error"):
                await source._get_directory_paths(directory_path)


@pytest.mark.asyncio
async def test_format_file():
    """Test format_file method of OneLakeDataSource class"""

    # Setup
    async with create_abs_source() as source:
        mock_file_client = MagicMock()
        mock_file_properties = MagicMock(
            creation_time=datetime(2022, 4, 21, 12, 12, 30),
            last_modified=datetime(2022, 4, 22, 15, 45, 10),
            size=2048,
            name="path/to/file.txt",
        )

        mock_file_properties.name.split.return_value = ["path", "to", "file.txt"]
        mock_file_client.get_file_properties.return_value = mock_file_properties
        mock_file_client.file_system_name = "my_file_system"

        expected_output = {
            "_id": "my_file_system_file.txt",
            "name": "file.txt",
            "created_at": "2022-04-21T12:12:30",
            "_timestamp": "2022-04-22T15:45:10",
            "size": 2048,
        }

        # Execute
        actual_output = source.format_file(mock_file_client)

        # Assert
        assert actual_output == expected_output
        mock_file_client.get_file_properties.assert_called_once()


@pytest.mark.asyncio
async def test_format_file_error():
    """Test format_file method when getting properties fails"""

    async with create_abs_source() as source:
        mock_file_client = MagicMock()
        mock_file_client.get_file_properties.side_effect = Exception("Properties error")
        mock_file_client.file_system_name = "my_file_system"

        with pytest.raises(Exception, match="Properties error"):
            source.format_file(mock_file_client)


@pytest.mark.asyncio
async def test_format_file_empty_name():
    """Test format_file method with empty file name"""

    async with create_abs_source() as source:
        mock_file_client = MagicMock()
        mock_file_properties = MagicMock(
            creation_time=datetime(2022, 4, 21, 12, 12, 30),
            last_modified=datetime(2022, 4, 22, 15, 45, 10),
            size=2048,
            name="",
        )
        mock_file_properties.name.split.return_value = [""]
        mock_file_client.get_file_properties.return_value = mock_file_properties
        mock_file_client.file_system_name = "my_file_system"

        result = source.format_file(mock_file_client)
        assert result["name"] == ""
        assert result["_id"] == "my_file_system_"


@pytest.mark.asyncio
async def test_download_file():
    """Test download_file method of OneLakeDataSource class"""

    # Setup
    mock_file_client = Mock()
    mock_download = Mock()
    mock_file_client.download_file.return_value = mock_download

    mock_chunks = ["chunk1", "chunk2", "chunk3"]

    mock_download.chunks.return_value = iter(mock_chunks)

    async with create_abs_source() as source:
        # Run
        chunks = []
        async for chunk in source.download_file(mock_file_client):
            chunks.append(chunk)

    # Check
    assert chunks == mock_chunks
    mock_file_client.download_file.assert_called_once()
    mock_download.chunks.assert_called_once()


@pytest.mark.asyncio
async def test_download_file_with_error():
    """Test download_file method of OneLakeDataSource class with exception handling"""

    # Setup
    mock_file_client = Mock()
    mock_download = Mock()
    mock_file_client.download_file.return_value = mock_download
    mock_download.chunks.side_effect = Exception("Download error")

    async with create_abs_source() as source:
        # Run & Check
        with pytest.raises(Exception, match="Download error"):
            async for _ in source.download_file(mock_file_client):
                pass

        mock_file_client.download_file.assert_called_once()
        mock_download.chunks.assert_called_once()


@pytest.mark.asyncio
async def test_get_content_with_download():
    """Test get_content method when doit=True"""

    mock_configuration = {
        "account_name": "mockaccount",
        "tenant_id": "mocktenant",
        "client_id": "mockclient",
        "client_secret": "mocksecret",
        "workspace_name": "mockworkspace",
        "data_path": "mockpath",
    }

    async with create_abs_source() as source:
        source.configuration = mock_configuration

        class FileClientMock:
            file_system_name = "mockfilesystem"

            class FileProperties:
                def __init__(self, name, size):
                    self.name = name
                    self.size = size

            def get_file_properties(self):
                return self.FileProperties(name="file1.txt", size=2000)

        with patch.object(
            source, "_get_file_client", return_value=FileClientMock()
        ), patch.object(
            source, "can_file_be_downloaded", return_value=True
        ), patch.object(
            source,
            "download_and_extract_file",
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

    async with create_abs_source() as source:
        source.configuration = {
            "account_name": "mockaccount",
            "tenant_id": "mocktenant",
            "client_id": "mockclient",
            "client_secret": "mocksecret",
            "workspace_name": "mockworkspace",
            "data_path": "mockpath",
        }

        class FileClientMock:
            file_system_name = "mockfilesystem"

            class FileProperties:
                def __init__(self, name, size):
                    self.name = name
                    self.size = size

            def get_file_properties(self):
                return self.FileProperties(name="file1.txt", size=2000)

        with patch.object(source, "_get_file_client", return_value=FileClientMock()):
            actual_response = await source.get_content("file1.txt", doit=False)
            assert actual_response is None


@pytest.mark.asyncio
async def test_prepare_files():
    """Test prepare_files method of OneLakeDataSource class"""

    # Setup
    doc_paths = [
        Mock(
            name="doc1",
            **{"name.split.return_value": ["folder", "doc1"], "path": "folder/doc1"},
        ),
        Mock(
            name="doc2",
            **{"name.split.return_value": ["folder", "doc2"], "path": "folder/doc2"},
        ),
    ]
    mock_field_client = Mock()

    async def mock_format_file(*args, **kwargs):
        """Mock for the format_file method"""

        return "file_document", "partial_function"

    async with create_abs_source() as source:
        with patch.object(
            source, "_get_file_client", new_callable=AsyncMock
        ) as mock_get_file_client:
            mock_get_file_client.return_value = mock_field_client

            with patch.object(source, "format_file", side_effect=mock_format_file):
                result = []
                # Run
                async for item in source.prepare_files(doc_paths):
                    result.append(await item)

                # Check results
                assert result == [
                    ("file_document", "partial_function"),
                    ("file_document", "partial_function"),
                ]

                mock_get_file_client.assert_has_calls([call("doc1"), call("doc2")])


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

    async with create_abs_source() as source:
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
