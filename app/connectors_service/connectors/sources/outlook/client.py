#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import asyncio
import ssl
from functools import cached_property

import aiohttp
import requests.adapters
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
from exchangelib.errors import ErrorFolderNotFound, ErrorManagedFolderNotFound
from exchangelib.folders import Calendar, Messages
from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter
from ldap3 import SAFE_SYNC, Connection, Server

from connectors.sources.outlook.constants import (
    API_SCOPE,
    CALENDAR_FIELDS,
    CONTACT_FOLDER_FIELDS,
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

# Per-folder faults meaning a folder is absent: skip the folder, keep syncing.
# Account-wide and connection-wide errors are handled elsewhere.
FOLDER_SKIP_ERRORS = (ErrorFolderNotFound, ErrorManagedFolderNotFound)


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


class SSLCertificateError(Exception):
    """Raised when SSL is enabled but the CA certificate is missing or unusable."""

    pass


def _extract_ldap_mail(attributes):
    mail = attributes.get("mail")
    if isinstance(mail, list):
        mail = mail[0] if mail else None
    if not mail:
        return None
    return mail


class InMemoryCAAdapter(requests.adapters.HTTPAdapter):
    """HTTP adapter that verifies Exchange server TLS using an in-memory CA."""

    ssl_context: ssl.SSLContext | None = None

    def init_poolmanager(self, *args, **kwargs):
        ssl_context = type(self).ssl_context
        if ssl_context is not None:
            kwargs["ssl_context"] = ssl_context
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        ssl_context = type(self).ssl_context
        if ssl_context is not None:
            kwargs["ssl_context"] = ssl_context
        return super().proxy_manager_for(*args, **kwargs)


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
        pass

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
        # exchangelib applies HTTP_ADAPTER_CLS (and our CA context) process-wide;
        # safe because each connector uses a single CA.
        if self.ssl_enabled:
            # Fail loudly on a missing/unusable CA instead of silently using an
            # unverified or system-CA connection.
            if not self.ssl_ca:
                msg = (
                    "SSL is enabled for the Exchange server but no CA "
                    "certificate was provided. Provide a valid PEM-encoded "
                    "certificate."
                )
                raise SSLCertificateError(msg)
            try:
                InMemoryCAAdapter.ssl_context = ssl.create_default_context(
                    cadata=self.ssl_ca
                )
            except (ssl.SSLError, ValueError) as exception:
                msg = (
                    "SSL is enabled for the Exchange server but the configured "
                    "CA certificate could not be loaded. Provide a valid "
                    "PEM-encoded certificate."
                )
                raise SSLCertificateError(msg) from exception
            BaseProtocol.HTTP_ADAPTER_CLS = InMemoryCAAdapter
        else:
            BaseProtocol.HTTP_ADAPTER_CLS = NoVerifyHTTPAdapter

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
            if "searchResRef" in user.get("type", ""):
                continue

            mail = _extract_ldap_mail(user.get("attributes", {}))
            if mail is None:
                logger.warning(
                    "Skipping Active Directory user without a valid mail attribute: "
                    f"{user.get('dn', 'unknown')}"
                )
                continue

            user_account = Account(
                primary_smtp_address=mail,
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
            try:
                # Resolve folders off the event loop (blocking exchangelib call).
                if mail_type["folder"] == "archive":
                    # msg_folder_root is locale-agnostic; the "Archive" leaf has no
                    # distinguished ID, so resolve it by name and skip if absent.
                    folder_object = await asyncio.to_thread(
                        lambda: account.msg_folder_root / "Archive"
                    )
                    # A folder literally named "Archive" that isn't a mail folder
                    # can't be projected with MAIL_FIELDS; only iterate real mail
                    # folders so the item stream stays a closed list.
                    if not isinstance(folder_object, Messages):
                        self._logger.debug(
                            f"Skipping 'Archive' folder for {account.primary_smtp_address}: "
                            f"not a mail folder ({type(folder_object).__name__})"
                        )
                        continue
                else:
                    folder_object = await asyncio.to_thread(
                        getattr, account, mail_type["folder"]
                    )
            except FOLDER_SKIP_ERRORS:
                self._logger.warning(
                    f"Could not resolve {mail_type['folder']} folder for "
                    f"{account.primary_smtp_address}, skipping."
                )
                continue

            for mail in await asyncio.to_thread(folder_object.all().only, *MAIL_FIELDS):
                yield mail, mail_type

    async def get_calendars(self, account):
        # Resolve the folder off the event loop (blocking call); skip if absent.
        try:
            folder = await asyncio.to_thread(getattr, account, "calendar")
        except FOLDER_SKIP_ERRORS:
            self._logger.warning(
                f"Could not resolve Calendar folder for {account.primary_smtp_address}, skipping."
            )
            return
        for calendar in await asyncio.to_thread(folder.all().only, *CALENDAR_FIELDS):
            yield calendar

    async def get_child_calendars(self, account):
        # Resolve folder and children off the event loop; skip if absent.
        try:
            child_calendars = await asyncio.to_thread(
                lambda: list(account.calendar.children)
            )
        except FOLDER_SKIP_ERRORS:
            self._logger.warning(
                f"Could not resolve Calendar folder for {account.primary_smtp_address}, "
                "skipping child calendars."
            )
            return
        for child_calendar in child_calendars:
            # Only descend into folders that are actually calendars. A non-calendar
            # child can't be projected with CALENDAR_FIELDS and its items aren't
            # CalendarItems, so identifying the folder type up front keeps the item
            # stream a closed list instead of relying on catching a ValueError.
            if not isinstance(child_calendar, Calendar):
                self._logger.debug(
                    f"Skipping non-calendar child folder "
                    f"{getattr(child_calendar, 'name', 'unknown')} "
                    f"({type(child_calendar).__name__}) for {account.primary_smtp_address}"
                )
                continue
            calendars = await asyncio.to_thread(
                child_calendar.all().only, *CALENDAR_FIELDS
            )
            for calendar in calendars:
                yield calendar, child_calendar

    async def get_tasks(self, account):
        # Resolve the folder off the event loop (blocking call); skip if absent.
        try:
            folder = await asyncio.to_thread(getattr, account, "tasks")
        except FOLDER_SKIP_ERRORS:
            self._logger.warning(
                f"Could not resolve Tasks folder for {account.primary_smtp_address}, skipping."
            )
            return
        for task in await asyncio.to_thread(folder.all().only, *TASK_FIELDS):
            yield task

    async def get_contacts(self, account):
        # account.contacts uses a locale-agnostic distinguished folder ID; resolve
        # it off the event loop (blocking call); skip if absent.
        try:
            folder = await asyncio.to_thread(getattr, account, "contacts")
        except FOLDER_SKIP_ERRORS:
            self._logger.warning(
                f"Could not resolve Contacts folder for {account.primary_smtp_address}, skipping."
            )
            return
        for contact in await asyncio.to_thread(
            folder.all().only, *CONTACT_FOLDER_FIELDS
        ):
            yield contact
