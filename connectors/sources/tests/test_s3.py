#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
from datetime import datetime
from unittest import mock

import aioboto3
import pytest

from connectors.sources.s3 import S3DataSource
from connectors.sources.tests.support import assert_basics, create_source


@pytest.fixture(scope="session", autouse=True)
def execute_before_all_tests():
    """This method execute at the start, once"""
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        os.environ["AWS_ACCESS_KEY_ID"] = "access_key"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "secret_key"


@pytest.mark.asyncio
async def test_basics(patch_logger):
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
async def test_ping(patch_logger):
    """Test ping method of S3DataSource class"""
    # Setup
    source = create_source(S3DataSource)

    # Execute
    with mock.patch(
        "aioboto3.resources.collection.AIOResourceCollection", AIOResourceCollection
    ), mock.patch("aiobotocore.client.AioBaseClient", S3Object):
        await source.ping()


@pytest.mark.asyncio
async def test_ping_negative(patch_logger):
    """Test ping method of S3DataSource class with negative case"""
    # Setup
    source = create_source(S3DataSource)

    # Execute
    source.session = aioboto3.Session(
        aws_access_key_id="dummy123", aws_secret_access_key="dummy123"
    )
    with pytest.raises(Exception):
        await source.ping()


@pytest.mark.asyncio
async def test_get_bucket_region():
    """Test get_bucket_region method of S3DataSource"""
    # Setup
    source = create_source(S3DataSource)

    # Execute
    with mock.patch("aiobotocore.client.AioBaseClient", S3Object):
        response = await source.get_bucket_region("dummy")

        # Assert
        assert response is None


@pytest.mark.asyncio
async def test_get_bucket_region_negative(caplog, patch_logger):
    """Test get_bucket_region method of S3DataSource for negative case"""
    # Setup
    source = create_source(S3DataSource)
    caplog.set_level("WARN")

    # Execute
    with pytest.raises(Exception):
        await source.get_bucket_region("dummy")

        # Assert
        assert "Unable to fetch the region" in caplog.text


@pytest.mark.asyncio
async def test_get_content(patch_logger, mock_aws):
    """Test get_content method of S3DataSource"""
    # Setup
    source = create_source(S3DataSource)
    with mock.patch("aiobotocore.client.AioBaseClient", S3Object):
        response = await source._get_content(
            {"id": 1, "filename": "a.txt", "bucket": "dummy", "size": 1000000},
            "region",
            doit=1,
        )
        assert response == {"_timestamp": None, "_attachment": "eHh4eHg=", "_id": 1}


@pytest.mark.asyncio
async def test_get_content_with_unsupported_file(patch_logger, mock_aws):
    """Test get_content method of S3DataSource for unsupported file"""
    # Setup
    source = create_source(S3DataSource)
    with mock.patch("aiobotocore.client.AioBaseClient", S3Object):
        response = await source._get_content(
            {"id": 1, "filename": "a.png", "bucket": "dummy"}, "region", doit=1
        )
        assert response is None


@pytest.mark.asyncio
async def test_get_content_when_not_doit(patch_logger, mock_aws):
    """Test get_content method of S3DataSource when doit is none"""
    # Setup
    source = create_source(S3DataSource)
    with mock.patch("aiobotocore.client.AioBaseClient", S3Object):
        response = await source._get_content(
            {"id": 1, "filename": "a.txt", "bucket": "dummy"}, "region"
        )
        assert response is None


@pytest.mark.asyncio
async def test_get_content_when_size_is_large(patch_logger, mock_aws):
    """Test get_content method of S3DataSource when size is greater than max size"""
    # Setup
    source = create_source(S3DataSource)
    with mock.patch("aiobotocore.client.AioBaseClient", S3Object):
        response = await source._get_content(
            {"id": 1, "filename": "a.txt", "bucket": "dummy", "size": 20000000000},
            "region",
            doit=1,
        )
        assert response is None


@pytest.mark.asyncio
async def test_pdf_file(patch_logger, mock_aws):
    """Test get_content method of S3DataSource for pdf file"""
    # Setup
    source = create_source(S3DataSource)
    with mock.patch("aiobotocore.client.AioBaseClient", S3Object):
        response = await source._get_content(
            {"id": 1, "filename": "dummy.pdf", "bucket": "dummy", "size": 1000000},
            "region",
            doit=1,
        )
        assert response == {"_timestamp": None, "_attachment": "eHh4eHg=", "_id": 1}


async def get_roles(*args):
    return {}


@pytest.mark.asyncio
async def test_get_docs(patch_logger, mock_aws):
    """Test get_docs method of S3DataSource"""
    # Setup
    source = create_source(S3DataSource)
    source.get_bucket_location = mock.Mock(
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
        async for (doc, dl) in source.get_docs():
            assert doc["_id"] in (
                "d0295955cdb6d488a4a1d3f10dbf141b",
                "94fce1b79d35d3ff4f96a678ebaed3b5",
                "4c54b941aa8015c287f97c88a9aec30d",
                "a7455a44f81ea06fdca84490a297ae11",
                "f1e7a00575c76e9cc96a6989bc4b9c25",
            )
            num += 1


def test_get_bucket_list():
    """Test get_bucket_list method of S3dataSource"""
    # Setup
    source = create_source(S3DataSource)
    source.bucket_list = {
        "Buckets": [
            {"Name": "bucket1", "CreationDate": "2022-09-01T12:29:29"},
            {"Name": "bucket2", "CreationDate": "2022-09-01T12:29:29"},
        ]
    }
    expected_response = ["bucket1", "bucket2"]

    # Execute
    actual_response = source.get_bucket_list()

    # Assert
    assert expected_response == actual_response


def test_validate_configuration_for_empty_bucket_string():
    """This function test _validate_configuration  when buckets string is empty"""
    # Setup
    source = create_source(S3DataSource)
    source.configuration.set_field(name="buckets", value=[""])
    # Execute
    with pytest.raises(Exception):
        source._validate_configuration()
