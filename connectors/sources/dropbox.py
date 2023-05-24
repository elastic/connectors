#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Dropbox source module responsible to fetch documents from Dropbox online.
"""
from functools import cached_property

import dropbox
import requests
from dropbox.exceptions import ApiError, AuthError, BadInputError

from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import CancellableSleeps, iso_utc

RETRY_COUNT = 3


class DropboxClient:
    """Dropbox client to handle API calls made to Dropbox"""

    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self.path = (
            ""
            if self.configuration["path"] in ["/", None]
            else self.configuration["path"]
        )
        self.retry_count = self.configuration["retry_count"]
        self._session = None

    @cached_property
    def _connection(self):
        self._session = requests.Session()
        return dropbox.Dropbox(  # pyright: ignore
            app_key=self.configuration["app_key"],
            app_secret=self.configuration["app_secret"],
            oauth2_refresh_token=self.configuration["refresh_token"],
            max_retries_on_error=self.retry_count,
            max_retries_on_rate_limit=self.retry_count,
            session=self._session,
        )

    def ping(self):
        self._connection.users_get_current_account()

    def check_path(self):
        return self._connection.files_get_metadata(path=self.path)

    def close(self):
        self._sleeps.cancel()
        if self._session is not None:
            self._connection.close()
            del self._session


class DropboxDataSource(BaseDataSource):
    """Dropbox"""

    name = "Dropbox"
    service_type = "dropbox"

    def __init__(self, configuration):
        """Setup the connection to the Dropbox

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.dropbox_client = DropboxClient(configuration=configuration)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Dropbox

        Returns:
            dictionary: Default configuration.
        """
        return {
            "path": {
                "label": "Path to fetch files/folders",
                "order": 1,
                "required": False,
                "type": "str",
                "value": "/",
                "default_value": "/",
            },
            "app_key": {
                "label": "Dropbox App Key",
                "sensitive": True,
                "order": 2,
                "type": "str",
                "value": "abc#123",
            },
            "app_secret": {
                "label": "Dropbox App Secret",
                "sensitive": True,
                "order": 3,
                "type": "str",
                "value": "abc#123",
            },
            "refresh_token": {
                "label": "Dropbox Refresh Token",
                "sensitive": True,
                "order": 4,
                "type": "str",
                "value": "abc#123",
            },
            "retry_count": {
                "default_value": RETRY_COUNT,
                "display": "numeric",
                "label": "Maximum retries for failed requests",
                "order": 5,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "value": RETRY_COUNT,
                "validations": [{"type": "less_than", "constraint": 10}],
            },
        }

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields
        Also validate, if user configured path is available in Dropbox."""

        self.configuration.check_valid()
        await self._remote_validation()

    async def _remote_validation(self):
        try:
            if self.dropbox_client.path not in ["", None]:
                self.dropbox_client.check_path()
        except BadInputError:
            raise ConfigurableFieldValueError(
                "Configured App Key or App Secret is invalid"
            )
        except AuthError:
            raise ConfigurableFieldValueError("Configured Refresh Token is invalid")
        except ApiError as err:
            if err.error.is_path() and err.error.get_path().is_not_found():
                raise ConfigurableFieldValueError(
                    f"Configured Path: {self.dropbox_client.path} is invalid"
                )
            else:
                raise Exception(
                    f"Error while validating the configured path. Error: {err}"
                )
        except Exception as exception:
            raise Exception(f"Something went wrong. Error: {exception}")

    async def close(self):
        self.dropbox_client.close()

    async def ping(self):
        try:
            self.dropbox_client.ping()
            logger.debug("Successfully connected to Dropbox")
        except BadInputError:
            raise ConfigurableFieldValueError(
                "Configured App Key or App Secret is invalid"
            )
        except AuthError:
            raise ConfigurableFieldValueError("Configured Refresh Token is invalid")
        except Exception as exception:
            raise Exception(f"Something went wrong. Error: {exception}")

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch dropbox objects in async manner

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: dictionary containing meta-data of the files.
        """
        yield {"_id": "123", "_timestamp": iso_utc()}, None
