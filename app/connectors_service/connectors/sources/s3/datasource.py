#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from functools import partial

from connectors_sdk.source import BaseDataSource
from connectors_sdk.utils import hash_id

from connectors.sources.s3.client import S3Client
from connectors.sources.s3.validator import S3AdvancedRulesValidator

DEFAULT_PAGE_SIZE = 100
DEFAULT_MAX_RETRY_ATTEMPTS = 5
DEFAULT_CONNECTION_TIMEOUT = 90
DEFAULT_READ_TIMEOUT = 90


class S3DataSource(BaseDataSource):
    """Amazon S3"""

    name = "Amazon S3"
    service_type = "s3"
    advanced_rules_enabled = True

    def __init__(self, configuration):
        """Set up the connection to the Amazon S3.

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.s3_client = S3Client(configuration=configuration)

    def _set_internal_logger(self):
        self.s3_client.set_logger(self._logger)

    def advanced_rules_validators(self):
        return [S3AdvancedRulesValidator(self)]

    async def ping(self):
        """Verify the connection with AWS"""
        try:
            await self.s3_client.fetch_buckets()
            self._logger.info("Successfully connected to AWS.")
        except Exception:
            self._logger.exception("Error while connecting to AWS.")
            raise

    async def format_document(self, bucket_name, bucket_object):
        """Prepare document for bucket object.

        Args:
            bucket_name: Name of bucket
            bucket_object: Response of bucket objects
        Returns:
            document: Modified document.
        """

        doc_id = hash_id(f"{bucket_name}/{bucket_object.key}")
        owner = await bucket_object.owner
        document = {
            "_id": doc_id,
            "filename": bucket_object.key,
            "size_in_bytes": await bucket_object.size,
            "bucket": bucket_name,
            "owner": owner.get("DisplayName") if owner else "",
            "storage_class": await bucket_object.storage_class,
            "_timestamp": (await bucket_object.last_modified).isoformat(),
        }
        return document

    async def advanced_sync(self, rule):
        async def process_object(obj_summary, s3_client):
            document = await self.format_document(
                bucket_name=bucket, bucket_object=obj_summary
            )
            return document, partial(
                self.get_content, doc=document, s3_client=s3_client
            )

        bucket = rule["bucket"]
        prefix = rule.get("prefix", "")
        async for obj_summary, s3_client in self.s3_client.get_bucket_objects(
            bucket=bucket, prefix=prefix
        ):
            if not rule.get("extension"):
                yield await process_object(obj_summary, s3_client)

            elif self.get_file_extension(obj_summary.key) in rule.get("extension", []):
                yield await process_object(obj_summary, s3_client)

    async def get_docs(self, filtering=None):
        """Get documents from Amazon S3

        Returns:
            dictionary: Document of file content

        Yields:
            dictionary: Document from Amazon S3.
        """
        if filtering and filtering.has_advanced_rules():
            for rule in filtering.get_advanced_rules():
                async for document, attachment in self.advanced_sync(rule=rule):
                    yield document, attachment

        else:
            bucket_list = await self.s3_client.get_bucket_list()
            for bucket in bucket_list:
                async for obj_summary, s3_client in self.s3_client.get_bucket_objects(
                    bucket=bucket
                ):
                    document = await self.format_document(
                        bucket_name=bucket, bucket_object=obj_summary
                    )
                    yield (
                        document,
                        partial(
                            self.get_content,
                            doc=document,
                            s3_client=s3_client,
                        ),
                    )

    async def get_content(self, doc, s3_client, timestamp=None, doit=None):
        if not (doit):
            return

        filename = doc["filename"]
        file_size = doc["size_in_bytes"]
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        bucket = doc["bucket"]
        document = {
            "_id": doc["id"],
            "_timestamp": doc["_timestamp"],
        }

        # s3 has a unique download method so we can't utilize
        # the generic download_and_extract_file func
        async with self.create_temp_file(file_extension) as async_buffer:
            await s3_client.download_fileobj(
                Bucket=bucket, Key=filename, Fileobj=async_buffer
            )
            await async_buffer.close()

            document = await self.handle_file_content_extraction(
                document, filename, async_buffer.name
            )

        return document

    async def close(self):
        """Closes unclosed client session"""
        await self.s3_client.close_client()

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Amazon S3.

        Returns:
            dictionary: Default configuration.
        """
        return {
            "buckets": {
                "display": "textarea",
                "label": "AWS Buckets",
                "order": 1,
                "tooltip": "AWS Buckets are ignored when Advanced Sync Rules are used.",
                "type": "list",
            },
            "aws_access_key_id": {
                "label": "AWS Access Key Id",
                "order": 2,
                "type": "str",
            },
            "aws_secret_access_key": {
                "label": "AWS Secret Key",
                "order": 3,
                "sensitive": True,
                "type": "str",
            },
            "read_timeout": {
                "default_value": DEFAULT_READ_TIMEOUT,
                "display": "numeric",
                "label": "Read timeout",
                "order": 4,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "connect_timeout": {
                "default_value": DEFAULT_CONNECTION_TIMEOUT,
                "display": "numeric",
                "label": "Connection timeout",
                "order": 5,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "max_attempts": {
                "default_value": DEFAULT_MAX_RETRY_ATTEMPTS,
                "display": "numeric",
                "label": "Maximum retry attempts",
                "order": 6,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "page_size": {
                "default_value": DEFAULT_PAGE_SIZE,
                "display": "numeric",
                "label": "Maximum size of page",
                "order": 7,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 8,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
        }
