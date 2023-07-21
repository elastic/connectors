#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from datetime import datetime
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import aioboto3
import aiofiles
import pytest
from botocore.exceptions import ClientError, HTTPClientError

from connectors.source import ConfigurableFieldValueError
from connectors.sources.s3 import S3DataSource
from tests.sources.support import assert_basics, create_source


@pytest.mark.asyncio
async def test_basics():
    """Test get_default_configuration method of S3DataSource"""
    with mock.patch(
        "aioboto3.resources.collection.AIOResourceCollection", AIOResourceCollection
    ), mock.patch("aiobotocore.client.AioBaseClient", S3Object):
        await assert_basics(S3DataSource, "buckets", ["ent-search-ingest-dev"])


class Summary:
    """This class is used to initialize file summary"""

    def __init__(self, key):
        """Setup key of file object

        Args:
            key (String): Key of file
        """
        self.key = key

    @property
    async def size(self):
        """Set size of file

        Returns:
            int: Size of file
        """
        return 12

    @property
    async def last_modified(self):
        """Set last_modified time

        Returns:
            datetime: Current time
        """
        return datetime.now()

    @property
    async def storage_class(self):
        """Set storage_class of file

        Returns:
            string: Storage class of file
        """
        return "STANDARD"

    @property
    async def owner(self):
        """Set owner of file

        Returns:
            dict: Dictionary of owner and id
        """
        return {"DisplayName": "User1", "ID": "abcd1234"}


class AIOResourceCollection:
    """Class for mock AIOResourceCollection"""

    def __init__(self, *args, **kw):
        """Setup AIOResourceCollection"""
        pass

    async def __anext__(self):
        """This method is used to return file object

        Yields:
            Summary: Summary class object
        """
        yield Summary("1.txt")
        yield Summary("2.md")
        yield Summary("3/")
        yield Summary("4.java")

    def __aiter__(self):
        """Returns next object

        Returns:
            Summary: Summary class object
        """
        return self.__anext__()


class S3Object(dict):
    """Class for mock S3 object"""

    def __init__(self, *args, **kw):
        """Setup document of Mock object"""
        self.meta = mock.MagicMock()
        self["Body"] = self
        self["LocationConstraint"] = None
        self["Buckets"] = [
            {"Name": "bucket1", "CreationDate": "2022-09-01T12:29:29"},
            {"Name": "bucket2", "CreationDate": "2022-09-01T12:29:29"},
        ]
        self.called = False

    async def read(self, *args):
        """Method returns object content

        Returns:
            string: Content of the object
        """
        if self.called:
            return b""
        self.called = True
        return b"xxxxx"

    async def __aenter__(self):
        """Make a dummy connection and return it"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Make sure the dummy connection gets closed"""
        pass

    async def _make_api_call(self, *args, **kw):
        """Make dummy API call"""
        return self


async def create_fake_coroutine(data):
    """create a method for returning fake coroutine value"""
    return data


@pytest.mark.asyncio
async def test_ping():
    """Test ping method of S3DataSource class"""
    # Setup
    source = create_source(S3DataSource)

    # Execute
    with mock.patch(
        "aioboto3.resources.collection.AIOResourceCollection", AIOResourceCollection
    ), mock.patch("aiobotocore.client.AioBaseClient", S3Object):
        await source.ping()


@pytest.mark.asyncio
async def test_ping_negative():
    """Test ping method of S3DataSource class with negative case"""
    # Setup
    source = create_source(S3DataSource)

    # Execute
    with mock.patch.object(
        aioboto3.Session, "client", side_effect=Exception("Something went wrong")
    ):
        with pytest.raises(Exception):
            await source.ping()


@pytest.mark.asyncio
async def test_get_bucket_region():
    """Test get_bucket_region method of S3DataSource"""
    # Setup
    source = create_source(S3DataSource)

    # Execute
    with mock.patch("aiobotocore.client.AioBaseClient", S3Object):
        response = await source.s3_client.get_bucket_region("dummy")

        # Assert
        assert response is None


@pytest.mark.asyncio
async def test_get_bucket_region_negative():
    """Test get_bucket_region method of S3DataSource for negative case"""
    # Setup
    source = create_source(S3DataSource)
    with mock.patch.object(
        aioboto3.Session, "client", side_effect=Exception("Something went wrong")
    ):
        # Execute
        with pytest.raises(Exception):
            await source.s3_client.get_bucket_region(bucket_name="dummy")


class ReadAsyncMock(AsyncMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def read():
        return b"test content"


@mock.patch("aiobotocore.client.AioBaseClient")
@pytest.mark.asyncio
async def test_get_content(s3_client):
    """Test get_content method of S3Client"""

    # Setup
    source = create_source(S3DataSource)
    document = {
        "id": "123",
        "filename": "test.pdf",
        "bucket": "test-bucket",
        "_timestamp": "2022-01-01T00:00:00.000Z",
        "size_in_bytes": 1024,
    }
    s3_client = MagicMock()
    s3_client.download_fileobj = AsyncMock()
    async_response = AsyncMock()
    async_response.__aenter__ = AsyncMock(return_value=ReadAsyncMock)

    with patch("aiofiles.os.remove"):
        with patch("connectors.utils.convert_to_b64"):
            with patch.object(aiofiles, "open", return_value=async_response):
                # Execute
                result = await source.s3_client.get_content(
                    document, s3_client, timestamp=None, doit=True
                )

                # Assert
                assert result == {
                    "_id": "123",
                    "_timestamp": "2022-01-01T00:00:00.000Z",
                    "_attachment": b"test content",
                }


@mock.patch("aiobotocore.client.AioBaseClient")
@pytest.mark.asyncio
async def test_get_content_with_upper_extension(s3_client):
    """Test get_content method of S3Client"""

    # Setup
    source = create_source(S3DataSource)
    document = {
        "id": "123",
        "filename": "test.TXT",
        "bucket": "test-bucket",
        "_timestamp": "2022-01-01T00:00:00.000Z",
        "size_in_bytes": 1024,
    }
    s3_client = MagicMock()
    s3_client.download_fileobj = AsyncMock()
    async_response = AsyncMock()
    async_response.__aenter__ = AsyncMock(return_value=ReadAsyncMock)

    with patch("aiofiles.os.remove"):
        with patch("connectors.utils.convert_to_b64"):
            with patch.object(aiofiles, "open", return_value=async_response):
                # Execute
                result = await source.s3_client.get_content(
                    document, s3_client, timestamp=None, doit=True
                )

                # Assert
                assert result == {
                    "_id": "123",
                    "_timestamp": "2022-01-01T00:00:00.000Z",
                    "_attachment": b"test content",
                }


@pytest.mark.asyncio
async def test_get_content_with_unsupported_file(mock_aws):
    """Test get_content method of S3Client for unsupported file"""
    # Setup
    source = create_source(S3DataSource)
    with mock.patch("aiobotocore.client.AioBaseClient", S3Object):
        response = await source.s3_client.get_content(
            {"id": 1, "filename": "a.png", "bucket": "dummy"}, "client", doit=1
        )
        assert response is None


@pytest.mark.asyncio
async def test_get_content_when_not_doit(mock_aws):
    """Test get_content method of S3Client when doit is none"""
    # Setup
    source = create_source(S3DataSource)
    with mock.patch("aiobotocore.client.AioBaseClient", S3Object):
        response = await source.s3_client.get_content(
            {"id": 1, "filename": "a.txt", "bucket": "dummy"}, "client"
        )
        assert response is None


@pytest.mark.asyncio
async def test_get_content_when_size_is_large(mock_aws):
    """Test get_content method of S3Client when size is greater than max size"""
    # Setup
    source = create_source(S3DataSource)
    with mock.patch("aiobotocore.client.AioBaseClient", S3Object):
        response = await source.s3_client.get_content(
            {
                "id": 1,
                "filename": "a.txt",
                "bucket": "dummy",
                "size_in_bytes": 20000000000,
            },
            "client",
            doit=1,
        )
        assert response is None


async def get_roles(*args):
    return {}


@pytest.mark.asyncio
async def test_get_docs(mock_aws):
    """Test get_docs method of S3DataSource"""
    # Setup
    source = create_source(S3DataSource)
    source.s3_client.get_bucket_location = mock.Mock(
        return_value=await create_fake_coroutine("ap-south-1")
    )
    with mock.patch(
        "aioboto3.resources.collection.AIOResourceCollection", AIOResourceCollection
    ), mock.patch("aiobotocore.client.AioBaseClient", S3Object), mock.patch(
        "aiobotocore.utils.AioInstanceMetadataFetcher.retrieve_iam_role_credentials",
        get_roles,
    ):
        num = 0
        # Execute
        async for (doc, _) in source.get_docs():
            # Assert
            assert doc["_id"] in (
                "d0295955cdb6d488a4a1d3f10dbf141b",
                "94fce1b79d35d3ff4f96a678ebaed3b5",
                "4c54b941aa8015c287f97c88a9aec30d",
                "a7455a44f81ea06fdca84490a297ae11",
                "f1e7a00575c76e9cc96a6989bc4b9c25",
            )
            num += 1


@pytest.mark.asyncio
async def test_get_bucket_list():
    """Test get_bucket_list method of S3Client"""
    # Setup
    source = create_source(S3DataSource)
    source.s3_client.bucket_list = []
    source.s3_client.bucket_list = {
        "Buckets": [
            {"Name": "bucket1", "CreationDate": "2022-09-01T12:29:29"},
            {"Name": "bucket2", "CreationDate": "2022-09-01T12:29:29"},
        ]
    }
    expected_response = ["ent-search-ingest-dev"]

    # Execute
    actual_response = await source.s3_client.get_bucket_list()

    # Assert
    assert expected_response == actual_response


@pytest.mark.asyncio
async def test_get_bucket_list_for_wildcard():
    # Setup
    source = create_source(S3DataSource)
    source.configuration.set_field(name="buckets", type="list", value=["*"])

    # Execute
    with mock.patch(
        "aioboto3.resources.collection.AIOResourceCollection", AIOResourceCollection
    ), mock.patch("aiobotocore.client.AioBaseClient", S3Object):
        actual_response = await source.s3_client.get_bucket_list()

    # Assert
    assert ["bucket1", "bucket2"] == actual_response


@pytest.mark.asyncio
async def test_validate_config_for_empty_bucket_string():
    """This function test validate_configwhen buckets string is empty"""
    # Setup
    source = create_source(S3DataSource)
    source.configuration.set_field(name="buckets", type="list", value=[""])
    # Execute
    with pytest.raises(ConfigurableFieldValueError) as e:
        await source.validate_config()

    assert e.match("buckets")


@pytest.mark.asyncio
async def test_get_content_with_clienterror():
    """Test get_content method of S3Client for client error"""
    # Setup
    source = create_source(S3DataSource)
    document = {
        "id": "abc",
        "filename": "file.pdf",
        "bucket": "test-bucket",
        "size_in_bytes": 1024,
        "_timestamp": "2022-03-08T12:00:00.000Z",
    }
    s3_client = mock.MagicMock()
    s3_client.download_fileobj.side_effect = ClientError(
        {"Error": {"Code": "MockException"}}, "operation_name"
    )
    with pytest.raises(ClientError):
        # Execute
        await source.s3_client.get_content(
            doc=document, s3_client=s3_client, timestamp=None, doit=True
        )


@pytest.mark.asyncio
async def test_close_with_client_session():
    """Test close method of S3DataSource with client session"""

    # Setup
    source = create_source(S3DataSource)
    source.session = aioboto3.Session()
    await source.s3_client.client()

    # Execute
    await source.close()
    with pytest.raises(HTTPClientError):
        await source.ping()
