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
from exchangelib.errors import (
    ErrorFolderNotFound,
    ErrorManagedFolderNotFound,
    TransportError,
)
from exchangelib.folders import (
    AllCategorizedItems,
    AllContacts,
    AllItems,
    AllPersonMetadata,
    BaseFolder,
    Calendar,
    CommonViews,
    Conflicts,
    Contacts,
    ConversationHistory,
    ConversationSettings,
    DeletedItems,
    Directory,
    Drafts,
    Favorites,
    Files,
    FromFavoriteSenders,
    IMContactList,
    Inbox,
    Journal,
    JunkEmail,
    LocalFailures,
    Messages,
    MsgFolderRoot,
    MyContacts,
    Notes,
    Outbox,
    QuarantinedEmail,
    QuickContacts,
    RecipientCache,
    RecoverableItemsDeletions,
    RecoverableItemsPurges,
    RecoverableItemsRoot,
    RecoverableItemsVersions,
    RSSFeeds,
    SearchFolders,
    SentItems,
    ServerFailures,
    Sharing,
    Shortcuts,
    Signal,
    SmsAndChatsSync,
    SpoolerQueue,
    SyncIssues,
    System,
    Tasks,
    Views,
    WorkingSet,
)
from exchangelib.items import Item, Message
from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter
from ldap3 import SAFE_SYNC, Connection, Server

from connectors.sources.outlook.constants import (
    API_SCOPE,
    CALENDAR_FIELDS,
    CONTACT_FOLDER_FIELDS,
    EWS_ENDPOINT,
    MAIL_FIELDS,
    MAIL_OBJECT,
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

# Folder-absent faults: skip the folder, keep syncing. Access denied is left to
# propagate, so get_docs skips the whole mailbox instead of indexing it partly.
FOLDER_SKIP_ERRORS = (ErrorFolderNotFound, ErrorManagedFolderNotFound)
# Extra folders are opt-in and best effort, so any per-folder EWS fault skips
# them. TransportError is the base class of those faults.
EXTRA_MAIL_FOLDER_ERRORS = (TransportError,)

# Already synced with their own document type. Matched by class, not id: a
# folder the mailbox denies GetFolder on resolves with id=None, and re-indexing
# it here would overwrite those documents with the generic `Mail` type.
DEFAULT_MAIL_FOLDERS = (Inbox, JunkEmail, SentItems)

# Mail-capable but not user mail. Search folders alias items already indexed
# under their real folder, so they would overwrite those documents.
NON_USER_MAIL_FOLDERS = (
    AllCategorizedItems,
    AllContacts,
    AllItems,
    AllPersonMetadata,
    CommonViews,
    Conflicts,
    ConversationHistory,
    ConversationSettings,
    DeletedItems,
    Directory,
    Drafts,
    Favorites,
    Files,
    FromFavoriteSenders,
    IMContactList,
    Journal,
    LocalFailures,
    MyContacts,
    Notes,
    Outbox,
    QuarantinedEmail,
    QuickContacts,
    RecipientCache,
    RecoverableItemsDeletions,
    RecoverableItemsPurges,
    RecoverableItemsRoot,
    RecoverableItemsVersions,
    RSSFeeds,
    SearchFolders,
    ServerFailures,
    Sharing,
    Shortcuts,
    Signal,
    SmsAndChatsSync,
    SpoolerQueue,
    SyncIssues,
    System,
    Views,
    WorkingSet,
)


def _folder_sync_id(folder):
    folder_id = getattr(folder, "id", None)
    if folder_id is not None:
        return folder_id
    folder_id_obj = getattr(folder, "folder_id", None)
    if folder_id_obj is None:
        return None
    return getattr(folder_id_obj, "id", folder_id_obj)


def _parent_folder_sync_id(folder):
    parent = getattr(folder, "parent_folder_id", None)
    if parent is None:
        return None
    return getattr(parent, "id", parent)


def _is_non_user_mail_folder(folder):
    return isinstance(folder, NON_USER_MAIL_FOLDERS)


def _is_mail_folder(folder):
    if isinstance(folder, (Calendar, Contacts, Tasks, MsgFolderRoot)):
        return False
    if _is_non_user_mail_folder(folder):
        return False
    supported = getattr(folder, "supported_item_models", None)
    if not supported:
        return False
    return Message in supported


def _discover_additional_mail_folders(msg_folder_root, synced_folder_ids):
    additional = []
    # walk() is pre-order, so pruning by parent id drops whole subtrees.
    # Default folders are skipped unpruned: their subfolders are user mail.
    pruned_folder_ids = set()
    for folder in msg_folder_root.walk():
        sync_id = _folder_sync_id(folder)
        parent_id = _parent_folder_sync_id(folder)
        if _is_non_user_mail_folder(folder) or (
            parent_id is not None and parent_id in pruned_folder_ids
        ):
            if sync_id is not None:
                pruned_folder_ids.add(sync_id)
            continue
        # Skipped, not pruned: subfolders of a default folder are user mail.
        if isinstance(folder, DEFAULT_MAIL_FOLDERS):
            continue
        if not _is_mail_folder(folder):
            continue
        if sync_id is not None and sync_id in synced_folder_ids:
            continue
        additional.append(folder)
    return additional


# exchangelib raises ValueError on unrecognised item tags (e.g. a stray
# EndTimeZone). Degrade to Item so the sync continues; folder allowlists skip it.
_reported_unexpected_item_tags = set()


@classmethod
def _tolerant_item_model_from_tag(cls, tag):
    try:
        return cls.ITEM_MODEL_MAP[tag]
    except KeyError:
        if tag not in _reported_unexpected_item_tags:
            _reported_unexpected_item_tags.add(tag)
            logger.warning(f"Unexpected EWS item tag {tag}; skipping")
        return Item


BaseFolder.item_model_from_tag = _tolerant_item_model_from_tag


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

    def _create_ldap_connection(self):
        return Connection(
            server=self.ad_server,
            user=self.user,
            password=self.password,
            client_strategy=SAFE_SYNC,
            auto_bind=True,  # pyright: ignore
        )

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[UsersFetchFailed],
    )
    def _ldap_search(self, search_query, search_filter):
        connection = self._create_ldap_connection()
        try:
            has_value, _, response, _ = connection.search(
                search_query,
                search_filter,
                attributes=["mail"],
            )

            if not has_value:
                msg = "Error while fetching users from Exchange Active Directory."
                raise UsersFetchFailed(msg)

            return response
        finally:
            try:
                connection.unbind()
            except Exception as exc:
                logger.debug("Failed to unbind LDAP connection: %s", exc)

    async def close(self):
        pass

    def _fetch_normal_users(self, search_query):
        try:
            for user in self._ldap_search(search_query, SEARCH_FILTER_FOR_NORMAL_USERS):
                yield user
        except UsersFetchFailed:
            raise
        except Exception as e:
            msg = f"Something went wrong while fetching users. Error: {e}"
            raise UsersFetchFailed(msg) from e

    def _fetch_admin_users(self, search_query):
        try:
            for user in self._ldap_search(search_query, SEARCH_FILTER_FOR_ADMIN):
                yield user
        except UsersFetchFailed:
            raise
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
        self.sync_all_mail_folders = self.configuration.get(
            "sync_all_mail_folders", False
        )
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

    async def _resolve_default_mail_folder(self, account, mail_type):
        if mail_type["folder"] == "archive":
            # "Archive" has no distinguished ID; resolve by name, skip if absent.
            folder_object = await asyncio.to_thread(
                lambda: account.msg_folder_root / "Archive"
            )
            if not isinstance(folder_object, Messages):
                self._logger.debug(
                    f"Skipping 'Archive' folder for {account.primary_smtp_address}: "
                    f"not a mail folder ({type(folder_object).__name__})"
                )
                return None
            return folder_object

        return await asyncio.to_thread(getattr, account, mail_type["folder"])

    async def _fetch_folder_mails(self, folder_object):
        # Materialize in the thread; lazy iteration would block the event loop.
        return await asyncio.to_thread(
            lambda folder=folder_object: list(folder.all().only(*MAIL_FIELDS))
        )

    async def get_mails(self, account):
        synced_folder_ids = set()
        for mail_type in MAIL_TYPES:
            self._logger.debug(
                f"Fetching {mail_type['folder']} mails for {account.primary_smtp_address}"
            )
            try:
                folder_object = await self._resolve_default_mail_folder(
                    account, mail_type
                )
            except FOLDER_SKIP_ERRORS:
                self._logger.warning(
                    f"Could not resolve {mail_type['folder']} folder for "
                    f"{account.primary_smtp_address}, skipping."
                )
                continue

            if folder_object is None:
                continue

            sync_id = _folder_sync_id(folder_object)
            if sync_id is not None:
                synced_folder_ids.add(sync_id)

            for mail in await self._fetch_folder_mails(folder_object):
                yield mail, mail_type

        if not self.sync_all_mail_folders:
            return

        try:
            extra_folders = await asyncio.to_thread(
                _discover_additional_mail_folders,
                account.msg_folder_root,
                synced_folder_ids,
            )
        except EXTRA_MAIL_FOLDER_ERRORS:
            self._logger.warning(
                f"Could not walk mail folders for {account.primary_smtp_address}, "
                "skipping additional folders."
            )
            return

        for folder_object in extra_folders:
            folder_name = getattr(folder_object, "name", None) or "unknown"
            mail_type = {"constant": MAIL_OBJECT, "folder_name": folder_name}
            self._logger.debug(
                f"Fetching additional mail folder {folder_name!r} for "
                f"{account.primary_smtp_address}"
            )
            # Guard the fetch, not the yields, which would also swallow errors
            # raised by the consumer.
            try:
                mails = await self._fetch_folder_mails(folder_object)
            except EXTRA_MAIL_FOLDER_ERRORS as error:
                self._logger.warning(
                    f"Could not fetch mail from folder {folder_name!r} for "
                    f"{account.primary_smtp_address}, skipping: "
                    f"{error.__class__.__name__}."
                )
                continue

            for mail in mails:
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
        # Materialize the queryset in the thread; lazy iteration would run the
        # blocking EWS fetch back on the event loop.
        calendars = await asyncio.to_thread(
            lambda: list(folder.all().only(*CALENDAR_FIELDS))
        )
        for calendar in calendars:
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
            # A non-calendar child can't take CALENDAR_FIELDS; skip it up front.
            if not isinstance(child_calendar, Calendar):
                self._logger.debug(
                    f"Skipping non-calendar child folder "
                    f"{getattr(child_calendar, 'name', 'unknown')} "
                    f"({type(child_calendar).__name__}) for {account.primary_smtp_address}"
                )
                continue
            # Materialize the queryset in the thread; lazy iteration would run the
            # blocking EWS fetch back on the event loop.
            calendars = await asyncio.to_thread(
                lambda child=child_calendar: list(child.all().only(*CALENDAR_FIELDS))
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
        # Materialize the queryset in the thread; lazy iteration would run the
        # blocking EWS fetch back on the event loop.
        tasks = await asyncio.to_thread(lambda: list(folder.all().only(*TASK_FIELDS)))
        for task in tasks:
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
        # Materialize the queryset in the thread; lazy iteration would run the
        # blocking EWS fetch back on the event loop.
        contacts = await asyncio.to_thread(
            lambda: list(folder.all().only(*CONTACT_FOLDER_FIELDS))
        )
        for contact in contacts:
            yield contact
