import base64
import logging
from typing import AsyncIterator, Dict

from tika import parser

from connectors.es.settings import TIMESTAMP_FIELD


def extract_content_with_tika(b64_content: str) -> str:
    """
    Extracts text content from a base64-encoded binary content using Tika.

    Args:
        b64_content (str): Base64 encoded content.

    Returns:
        str: Extracted text content.
    """
    binary_data = base64.b64decode(b64_content)

    # Parse the binary data using Tika
    parsed = parser.from_buffer(binary_data)

    # Extract text and metadata
    text = parsed.get("content", "")

    return text


class ConnectorBase:
    def __init__(self, data_provider, logger=None, download_content=True):
        """
        Initializes the ConnectorBase instance.

        Args:
            data_provider: An instance of the data provider.
            logger (logging.Logger, optional): Logger instance. Defaults to None.
            download_content (bool, optional): Flag to determine if content should be downloaded. Defaults to True.
        """
        self.data_provider = data_provider
        self.download_content = download_content

        if logger is None:
            logger = logging.getLogger("elastic-connectors")
        self.logger = logger
        self.data_provider.set_logger(logger)

    def get_configuration(self):
        """
        Gets the configuration from the data provider.

        Returns:
            The configuration of the data provider.
        """
        return self.data_provider.configuration

    async def validate(self):
        """
        Validates the data provider configuration and pings the data provider.

        Raises:
            Exception: If validation or ping fails.
        """
        try:
            await self.data_provider.validate_config()
            await self.ping()
        except Exception as e:
            self.logger.error("Validation failed", exc_info=True)
            raise e

    async def ping(self):
        """
        Pings the data provider.

        Raises:
            Exception: If ping fails.
        """
        try:
            return await self.data_provider.ping()
        except Exception as e:
            self.logger.error("Ping failed", exc_info=True)
            raise e

    async def async_get_docs(self) -> AsyncIterator[Dict]:
        """
        Asynchronously retrieves documents from the data provider.

        Yields:
            dict: A document from the data provider.

        Raises:
            Exception: If an error occurs while extracting content.
        """
        async for doc, lazy_download in self.data_provider.get_docs(filtering=None):

            doc["id"] = doc.pop("_id")

            if lazy_download is not None and self.download_content:
                # content downloaded and represented in a binary format {'_attachment': <binary data>}
                try:
                    data = await lazy_download(
                        doit=True, timestamp=doc[TIMESTAMP_FIELD]
                    )
                    # binary to string conversion
                    binary_data = data.get("_attachment", None)

                    text = extract_content_with_tika(binary_data)

                    doc.update({"body": text})
                except Exception as e:
                    print(f"Error extracting content: {e}")

            yield doc

    async def close(self):
        """
        Closes the data provider connection.
        """
        await self.data_provider.close()

    async def __aenter__(self):
        """
        Asynchronous context manager entry. Validates the configuration.

        Returns:
            ConnectorBase: The instance itself.
        """
        await self.validate()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """
        Asynchronous context manager exit. Closes the data provider connection.
        """
        await self.close()
