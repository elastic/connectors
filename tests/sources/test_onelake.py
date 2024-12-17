from contextlib import asynccontextmanager
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from connectors.sources.onelake import OneLakeDataSource
from tests.sources.support import create_source


@asynccontextmanager
async def create_abs_source(
    use_text_extraction_service=False,
):
    async with create_source(
        OneLakeDataSource,
        tenant_id="",
        client_id="",
        client_secret="",
        workspace_name="",
        data_path="",
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
async def test_get_file_client():
    """Test _get_file_client method of OneLakeDataSource class"""

    # Setup
    mock_client = Mock()
    async with create_abs_source() as source:
        with patch.object(
            source, "_get_file_client", new_callable=AsyncMock
        ) as mock_method:
            mock_method.return_value = mock_client

            # Run
            file_client = await source._get_file_client()

            # Check
            assert file_client is mock_client


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
