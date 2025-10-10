#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import asyncio
import os
from functools import cached_property

import aiofiles
import aiohttp
import requests.adapters
from aiofiles.os import remove
from connectors_sdk.logger import logger
from exchangelib import (
    IMPERSONATION,
    OAUTH2,
    Account,
    Configuration,
    Credentials,
    FaultTolerance,
    Identity,
    OAuth2Credentials,
)
from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter
from ldap3 import SAFE_SYNC, Connection, Server

from connectors.sources.outlook.constants import (
    API_SCOPE,
    CALENDAR_FIELDS,
    CERT_FILE,
    CONTACT_FIELDS,
    EWS_ENDPOINT,
    MAIL_FIELDS,
    MAIL_TYPES,
    OUTLOOK_CLOUD,
    RETRIES,
    RETRY_INTERVAL,
    SEARCH_FILTER_FOR_ADMIN,
    SEARCH_FILTER_FOR_NORMAL_USERS,
    TASK_FIELDS,
    TOP,
)
from connectors.utils import (
    CancellableSleeps,
    RetryStrategy,
    get_pem_format,
    retryable,
    url_encode,
)


class TokenFetchFailed(Exception):
    """Exception class to notify that connector was unable to fetch authentication token from Graph API"""

    pass


class UsersFetchFailed(Exception):
    """Exception class to notify that connector was unable to fetch users from Active Directory"""

    pass


class UnauthorizedException(Exception):
    """Exception class unauthorized calls"""

    pass


class Forbidden(Exception):
    pass


class NotFound(Exception):
    pass


class SSLFailed(Exception):
    pass


class ManageCertificate:
    async def store_certificate(self, certificate):
        async with aiofiles.open(CERT_FILE, "w") as file:
            await file.write(certificate)

    def get_certificate_path(self):
        return os.path.join(os.getcwd(), CERT_FILE)

    async def remove_certificate_file(self):
        if os.path.exists(CERT_FILE):
            await remove(CERT_FILE)


class RootCAAdapter(requests.adapters.HTTPAdapter):
    """Class to verify SSL Certificate for Exchange Servers"""

    def cert_verify(self, conn, url, verify, cert):
        try:
            super().cert_verify(
                conn=conn,
                url=url,
                verify=ManageCertificate().get_certificate_path(),
                cert=cert,
            )
        except Exception as exception:
            msg = f"Something went wrong while verifying SSL certificate. Error: {exception}"
            raise SSLFailed(msg) from exception


class ExchangeUsers:
    """Fetch users from Exchange Active Directory"""

    def __init__(
        self, ad_server, domain, exchange_server, user, password, ssl_enabled, ssl_ca
    ):
        self.ad_server = Server(host=ad_server)
        self.domain = domain
        self.exchange_server = exchange_server
        self.user = user
        self.password = password
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca

    @cached_property
    def _create_connection(self):
        return Connection(
            server=self.ad_server,
            user=self.user,
            password=self.password,
            client_strategy=SAFE_SYNC,
            auto_bind=True,  # pyright: ignore
        )

    async def close(self):
        await ManageCertificate().remove_certificate_file()

    def _fetch_normal_users(self, search_query):
        try:
            has_value_for_normal_users, _, response, _ = self._create_connection.search(
                search_query,
                SEARCH_FILTER_FOR_NORMAL_USERS,
                attributes=["mail"],
            )

            if not has_value_for_normal_users:
                msg = "Error while fetching users from Exchange Active Directory."
                raise UsersFetchFailed(msg)

            for user in response:
                yield user

        except Exception as e:
            msg = f"Something went wrong while fetching users. Error: {e}"
            raise UsersFetchFailed(msg) from e

    def _fetch_admin_users(self, search_query):
        try:
            (
                has_value_for_admin_users,
                _,
                response_for_admin,
                _,
            ) = self._create_connection.search(
                search_query,
                SEARCH_FILTER_FOR_ADMIN,
                attributes=["mail"],
            )

            if not has_value_for_admin_users:
                msg = "Error while fetching users from Exchange Active Directory."
                raise UsersFetchFailed(msg)

            for user in response_for_admin:
                yield user
        except Exception as e:
            msg = f"Something went wrong while fetching users. Error: {e}"
            raise UsersFetchFailed(msg) from e

    async def get_users(self):
        ldap_domain_name_list = ["DC=" + domain for domain in self.domain.split(".")]
        search_query = ",".join(ldap_domain_name_list)

        for user in self._fetch_normal_users(search_query=search_query):
            yield user

        for user in self._fetch_admin_users(search_query=search_query):
            yield user

    async def get_user_accounts(self):
        await ManageCertificate().store_certificate(certificate=self.ssl_ca)
        BaseProtocol.HTTP_ADAPTER_CLS = (
            RootCAAdapter if self.ssl_enabled else NoVerifyHTTPAdapter
        )

        credentials = Credentials(
            username=self.user,
            password=self.password,
        )
        configuration = Configuration(
            credentials=credentials,
            server=self.exchange_server,
            retry_policy=FaultTolerance(max_wait=120),
        )

        async for user in self.get_users():
            if "searchResRef" in user["type"]:
                continue

            user_account = Account(
                primary_smtp_address=user.get("attributes", {}).get("mail"),
                config=configuration,
                access_type=IMPERSONATION,
            )
            yield user_account


class Office365Users:
    """Fetch users from Office365 Active Directory"""

    def __init__(self, client_id, client_secret, tenant_id):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

    @cached_property
    def _get_session(self):
        return aiohttp.ClientSession(raise_for_status=True)

    async def close(self):
        await self._get_session.close()
        del self._get_session

    def _check_errors(self, response):
        match response.status:
            case 400:
                msg = "Found invalid tenant id or client id value"
                raise UnauthorizedException(msg)
            case 401:
                msg = "Found invalid client secret value"
                raise UnauthorizedException(msg)
            case 403:
                msg = f"Missing permission or something went wrong. Error: {response}"
                raise Forbidden(msg)
            case 404:
                msg = f"Resource Not Found. Error: {response}"
                raise NotFound(msg)
            case _:
                raise

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=UnauthorizedException,
    )
    async def _fetch_token(self):
        try:
            async with self._get_session.post(
                url=f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "scope": API_SCOPE,
                },
            ) as response:
                token_response = await response.json()
                return token_response["access_token"]
        except Exception as exception:
            self._check_errors(response=exception)

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_users(self):
        access_token = await self._fetch_token()
        filter_ = url_encode("accountEnabled eq true")
        url = f"https://graph.microsoft.com/v1.0/users?$top={TOP}&$filter={filter_}"
        while True:
            try:
                async with self._get_session.get(
                    url=url,
                    headers={
                        "Authorization": f"Bearer {access_token}",
                        "Content-Type": "application/json",
                    },
                ) as response:
                    json_response = await response.json()
                    yield json_response
                    url = json_response.get("@odata.nextLink")
                    if url is None:
                        break
            except Exception:
                raise

    async def get_user_accounts(self):
        async for users in self.get_users():
            for user in users.get("value", []):
                mail = user.get("mail")
                if mail is None:
                    continue

                credentials = OAuth2Credentials(
                    client_id=self.client_id,
                    tenant_id=self.tenant_id,
                    client_secret=self.client_secret,
                    identity=Identity(primary_smtp_address=mail),
                )
                configuration = Configuration(
                    credentials=credentials,
                    auth_type=OAUTH2,
                    service_endpoint=EWS_ENDPOINT,
                    retry_policy=FaultTolerance(max_wait=120),
                )
                user_account = Account(
                    primary_smtp_address=mail,
                    config=configuration,
                    autodiscover=False,
                    access_type=IMPERSONATION,
                )
                yield user_account


class OutlookClient:
    """Outlook client to handle API calls made to Outlook"""

    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self._logger = logger
        self.is_cloud = self.configuration["data_source"] == OUTLOOK_CLOUD
        self.ssl_enabled = self.configuration.get("ssl_enabled", False)
        self.certificate = self.configuration.get("ssl_ca", None)

        if self.ssl_enabled and self.certificate:
            self.ssl_ca = get_pem_format(self.certificate)
        else:
            self.ssl_ca = ""

    def set_logger(self, logger_):
        self._logger = logger_

    @cached_property
    def _get_user_instance(self):
        if self.is_cloud:
            return Office365Users(
                client_id=self.configuration["client_id"],
                client_secret=self.configuration["client_secret"],
                tenant_id=self.configuration["tenant_id"],
            )

        return ExchangeUsers(
            ad_server=self.configuration["active_directory_server"],
            domain=self.configuration["domain"],
            exchange_server=self.configuration["exchange_server"],
            user=self.configuration["username"],
            password=self.configuration["password"],
            ssl_enabled=self.ssl_enabled,
            ssl_ca=self.ssl_ca,
        )

    async def _fetch_all_users(self):
        self._logger.debug("Fetching all users.")
        async for user in self._get_user_instance.get_users():
            yield user

    async def ping(self):
        await anext(self._get_user_instance.get_users())

    async def get_mails(self, account):
        for mail_type in MAIL_TYPES:
            self._logger.debug(
                f"Fetching {mail_type['folder']} mails for {account.primary_smtp_address}"
            )
            if mail_type["folder"] == "archive":
                # If 'Archive' folder is not present, skipping the iteration
                try:
                    folder_object = (
                        account.root / "Top of Information Store" / "Archive"
                    )
                except Exception:  # noqa S112
                    continue
            else:
                folder_object = getattr(account, mail_type["folder"])

            for mail in await asyncio.to_thread(folder_object.all().only, *MAIL_FIELDS):
                yield mail, mail_type

    async def get_calendars(self, account):
        for calendar in await asyncio.to_thread(
            account.calendar.all().only, *CALENDAR_FIELDS
        ):
            yield calendar

    async def get_child_calendars(self, account):
        for child_calendar in account.calendar.children:
            for calendar in await asyncio.to_thread(
                child_calendar.all().only, *CALENDAR_FIELDS
            ):
                yield calendar, child_calendar

    async def get_tasks(self, account):
        for task in await asyncio.to_thread(account.tasks.all().only, *TASK_FIELDS):
            yield task

    async def get_contacts(self, account):
        folder = account.root / "Top of Information Store" / "Contacts"
        for contact in await asyncio.to_thread(folder.all().only, *CONTACT_FIELDS):
            yield contact
