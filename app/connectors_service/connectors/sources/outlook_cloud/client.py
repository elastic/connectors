#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import asyncio
from collections import Counter
from functools import cached_property

import aiohttp
from aiohttp.client_exceptions import (
    ClientConnectionError,
    ClientPayloadError,
    ClientResponseError,
)
from connectors_sdk.logger import logger

from connectors.sources.outlook_cloud.constants import (
    DEFAULT_MAIL_FOLDERS,
    DEFAULT_RETRY_SECONDS,
    EXCLUDED_WELL_KNOWN_FOLDERS,
    FOLDER_FIELDS,
    GRAPH_BASE_URL,
    GRAPH_HOST_URL,
    GRAPH_REQUEST_HEADERS,
    MAIL_OBJECT,
    MAIL_SEARCH_FOLDER_ODATA_TYPE,
    MAILBOX_ACCESSIBLE,
    MAILBOX_MISSING,
    MAILBOX_OUT_OF_SCOPE,
    MAX_CONCURRENT_MAILBOXES,
    MESSAGE_FIELDS,
    MESSAGES_PAGE_SIZE,
    NETWORK_RETRY_SECONDS,
    REQUEST_TIMEOUT,
    RETRIES,
    RETRYABLE_STATUSES,
    USER_FIELDS,
    USERS_PAGE_SIZE,
)
from connectors.sources.shared.microsoft.graph import GraphAPIToken, PermissionsMissing
from connectors.utils import CancellableSleeps, url_encode

# Network faults that are worth another attempt, like the throttling statuses.
TRANSIENT_ERRORS = (ClientConnectionError, ClientPayloadError, asyncio.TimeoutError)


class NoAccessibleMailboxes(Exception):
    """Users were found, but none of them has a mailbox the application can read."""

    pass


class OutlookCloudClient:
    """Client for the Microsoft Graph calls made by the Outlook Cloud connector."""

    def __init__(self, configuration):
        self.configuration = configuration
        self._sleeps = CancellableSleeps()
        self._logger = logger

    def set_logger(self, logger_):
        self._logger = logger_

    @cached_property
    def session(self):
        return aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            raise_for_status=True,
        )

    @cached_property
    def _token(self):
        return GraphAPIToken(
            http_session=self.session,
            tenant_id=self.configuration["tenant_id"],
            tenant_name=None,
            client_id=self.configuration["client_id"],
            client_secret=self.configuration["client_secret"],
        )

    async def close(self):
        self._sleeps.cancel()
        if "session" in self.__dict__:
            await self.session.close()
            del self.session

    @staticmethod
    def _retry_delay(error, attempt):
        """Seconds to wait before retrying.

        Retry-After if Graph sent one. Otherwise a doubling default: 30 s for a Graph
        answer, 2 s for a network fault (nothing to wait for, the server did not answer).
        """
        try:
            return int(error.headers["Retry-After"])
        except (AttributeError, KeyError, TypeError, ValueError):
            pass
        first = (
            DEFAULT_RETRY_SECONDS
            if isinstance(error, ClientResponseError)
            else NETWORK_RETRY_SECONDS
        )
        return first * 2 ** (attempt - 1)

    async def _get(self, url, *, as_json=True):
        """GET a Graph URL with retries. Returns JSON or raw bytes."""
        if not url.startswith(GRAPH_HOST_URL):
            msg = f"Refusing to send the Microsoft Graph access token to {url.split('?')[0]}"
            raise ValueError(msg)
        for attempt in range(1, RETRIES + 1):
            token = await self._token.get()
            headers = {"Authorization": f"Bearer {token}"} | GRAPH_REQUEST_HEADERS
            try:
                async with self.session.get(url, headers=headers) as response:
                    if as_json:
                        return await response.json()
                    return await response.read()
            except ClientResponseError as error:
                if error.status == 401:
                    msg = "Microsoft Graph rejected the credentials (401). Check the tenant ID, client ID and client secret, and that admin consent is given."
                    raise PermissionsMissing(msg) from error
                if error.status not in RETRYABLE_STATUSES or attempt == RETRIES:
                    raise
                reason, delay = error.status, self._retry_delay(error, attempt)
            except TRANSIENT_ERRORS as error:
                if attempt == RETRIES:
                    raise
                reason, delay = type(error).__name__, self._retry_delay(error, attempt)

            self._logger.warning(
                f"Request to Microsoft Graph failed ({reason}). Retrying in {delay} seconds (attempt {attempt} of {RETRIES})."
            )
            await self._sleeps.sleep(delay)

    async def _get_json(self, url):
        """GET a Graph URL. Throttling, temporary server errors and network faults are retried, then raised.

        Raises:
            ValueError: the URL is not on Microsoft Graph. The access token is never sent elsewhere,
                e.g. to a paging link (`@odata.nextLink`) that points to another host.
        """
        return await self._get(url, as_json=True)

    async def _get_bytes(self, url):
        return await self._get(url, as_json=False)

    async def _get_users_page(self, url):
        try:
            return await self._get_json(url)
        except ClientResponseError as error:
            if error.status == 403:
                msg = "Microsoft Graph refused to list users. Grant the application permission User.Read.All and give admin consent."
                raise PermissionsMissing(msg) from error
            raise

    async def ping(self):
        await self._get_users_page(f"{GRAPH_BASE_URL}/users?$top=1&$select=id")

    async def _get_user_pages(self):
        """Yield one list of enabled users per Graph page."""
        filter_ = url_encode("accountEnabled eq true")
        url = f"{GRAPH_BASE_URL}/users?$filter={filter_}&$select={USER_FIELDS}&$top={USERS_PAGE_SIZE}"
        while url:
            page = await self._get_users_page(url)
            yield page.get("value", [])
            url = page.get("@odata.nextLink")

    async def get_users(self):
        """Yield every enabled user (id, displayName, mail, jobTitle)."""
        async for users in self._get_user_pages():
            for user in users:
                yield user

    async def check_mailbox_access(self, user_id):
        """Probe a mailbox. Return MAILBOX_ACCESSIBLE, MAILBOX_OUT_OF_SCOPE (403) or MAILBOX_MISSING (404).

        Any other error is raised, never treated as a skip.
        """
        try:
            await self._get_json(
                f"{GRAPH_BASE_URL}/users/{user_id}/mailFolders/inbox?$select=id"
            )
        except ClientResponseError as error:
            if error.status == 403:
                return MAILBOX_OUT_OF_SCOPE
            if error.status == 404:
                return MAILBOX_MISSING
            raise
        return MAILBOX_ACCESSIBLE

    async def get_accessible_mailboxes(self):
        """Yield the enabled users whose mailbox the application can read.

        Users without a mail address, without a mailbox, or outside the scope the
        admin granted are skipped. One summary is logged at the end.

        Raises:
            NoAccessibleMailboxes: no mailbox is readable, or no users were found. Without
                this a misconfigured scope would look like an empty, successful sync.
        """
        results = Counter()
        slots = asyncio.Semaphore(MAX_CONCURRENT_MAILBOXES)

        async def probe(user):
            if not user.get("mail"):
                return MAILBOX_MISSING
            async with slots:
                return await self.check_mailbox_access(user["id"])

        async for users in self._get_user_pages():
            # Let every probe finish, then raise the first failure: none is left running.
            statuses = await asyncio.gather(
                *(probe(user) for user in users), return_exceptions=True
            )
            for status in statuses:
                if isinstance(status, BaseException):
                    raise status
            for user, status in zip(users, statuses, strict=True):
                results[status] += 1
                if status == MAILBOX_ACCESSIBLE:
                    yield user

        self._logger.info(
            f"Mailboxes: {results[MAILBOX_ACCESSIBLE]} readable, "
            f"{results[MAILBOX_OUT_OF_SCOPE]} outside the granted scope, "
            f"{results[MAILBOX_MISSING]} without a mailbox."
        )

        if not results[MAILBOX_ACCESSIBLE]:
            raise NoAccessibleMailboxes(self._no_mailbox_message(results))

    @staticmethod
    def _no_mailbox_message(results):
        """Explain why no mailbox is readable. Failing here keeps an empty result from wiping the index."""
        if not results.total():
            return (
                "Microsoft Graph returned no enabled users. Check that User.Read.All has admin consent "
                "and that the tenant has enabled users."
            )
        message = (
            f"Microsoft Graph returned {results.total()} users, but none has a mailbox this application can read "
            f"({results[MAILBOX_OUT_OF_SCOPE]} outside the granted scope, {results[MAILBOX_MISSING]} without a mailbox). "
        )
        if results[MAILBOX_OUT_OF_SCOPE]:
            return (
                message
                + "Check that Mail.Read is granted and that the Exchange scope includes the mailboxes. "
                "Changes to Exchange role assignments can take 30 minutes to 2 hours to apply."
            )
        return (
            message
            + "Check that the users have an email address and an Exchange Online mailbox."
        )

    async def _resolve_well_known_folder_id(self, user_id, well_known_name):
        url = (
            f"{GRAPH_BASE_URL}/users/{user_id}/mailFolders/{well_known_name}"
            f"?$select=id"
        )
        try:
            payload = await self._get_json(url)
        except ClientResponseError as error:
            if error.status in (403, 404):
                return None
            raise
        folder_id = payload.get("id")
        return folder_id if isinstance(folder_id, str) and folder_id else None

    async def _folder_exclusion_and_default_ids(self, user_id):
        excluded_ids = set()
        for well_known in EXCLUDED_WELL_KNOWN_FOLDERS:
            folder_id = await self._resolve_well_known_folder_id(user_id, well_known)
            if folder_id:
                excluded_ids.add(folder_id)

        default_ids = set()
        for entry in DEFAULT_MAIL_FOLDERS:
            folder_id = await self._resolve_well_known_folder_id(
                user_id, entry["well_known"]
            )
            if folder_id:
                default_ids.add(folder_id)
        return excluded_ids, default_ids

    def _folder_skip_warning(self, user_id, folder_label, error):
        self._logger.warning(
            f"Could not read mail folder {folder_label!r} for user {user_id}, skipping: "
            f"{error.__class__.__name__}."
        )

    async def get_messages(self, user_id, folder_segment, mail_type):
        """Yield Graph message objects from a well-known folder name or folder id."""
        segment = url_encode(folder_segment)
        url = (
            f"{GRAPH_BASE_URL}/users/{user_id}/mailFolders/{segment}/messages"
            f"?$select={MESSAGE_FIELDS}&$top={MESSAGES_PAGE_SIZE}"
        )
        folder_label = mail_type.get("folder_name") or mail_type.get(
            "constant", folder_segment
        )
        try:
            page = await self._get_json(url)
        except ClientResponseError as error:
            if error.status in (403, 404):
                self._folder_skip_warning(user_id, folder_label, error)
                return
            raise

        while True:
            for message in page.get("value") or []:
                yield message
            url = page.get("@odata.nextLink")
            if not url:
                break
            page = await self._get_json(url)

    async def get_raw_message(self, user_id, message_id):
        url = (
            f"{GRAPH_BASE_URL}/users/{user_id}/messages/{url_encode(message_id)}/$value"
        )
        return await self._get_bytes(url)

    async def _list_folders(self, url):
        folders = []
        while url:
            page = await self._get_json(url)
            folders.extend(page.get("value") or [])
            url = page.get("@odata.nextLink")
        return folders

    def _should_prune_folder(self, folder, excluded_ids, pruned_ids):
        folder_id = folder.get("id")
        parent_id = folder.get("parentFolderId")
        odata_type = folder.get("@odata.type") or ""
        if parent_id and parent_id in pruned_ids:
            return True
        if MAIL_SEARCH_FOLDER_ODATA_TYPE in odata_type or odata_type.endswith(
            ".mailSearchFolder"
        ):
            return True
        if folder_id and folder_id in excluded_ids:
            return True
        return False

    async def _walk_additional_folders(
        self,
        user_id,
        folder,
        excluded_ids,
        default_ids,
        synced_ids,
        pruned_ids,
    ):
        folder_id = folder.get("id")
        if self._should_prune_folder(folder, excluded_ids, pruned_ids):
            if folder_id:
                pruned_ids.add(folder_id)
            return

        if folder_id and folder_id in default_ids:
            async for child in self._enumerate_child_folders(
                user_id,
                folder_id,
                excluded_ids,
                default_ids,
                synced_ids,
                pruned_ids,
            ):
                yield child
            return

        if folder_id and folder_id in synced_ids:
            async for child in self._enumerate_child_folders(
                user_id,
                folder_id,
                excluded_ids,
                default_ids,
                synced_ids,
                pruned_ids,
            ):
                yield child
            return

        if folder_id:
            synced_ids.add(folder_id)
            display_name = folder.get("displayName") or "unknown"
            yield {
                "folder_id": folder_id,
                "mail_type": {
                    "constant": MAIL_OBJECT,
                    "folder_name": display_name,
                },
            }

        if folder_id:
            async for child in self._enumerate_child_folders(
                user_id,
                folder_id,
                excluded_ids,
                default_ids,
                synced_ids,
                pruned_ids,
            ):
                yield child

    async def _enumerate_child_folders(
        self,
        user_id,
        folder_id,
        excluded_ids,
        default_ids,
        synced_ids,
        pruned_ids,
    ):
        url = (
            f"{GRAPH_BASE_URL}/users/{user_id}/mailFolders/{url_encode(folder_id)}"
            f"/childFolders?$select={FOLDER_FIELDS}&includeHiddenFolders=true"
        )
        try:
            children = await self._list_folders(url)
        except ClientResponseError as error:
            if error.status in (403, 404):
                self._folder_skip_warning(user_id, folder_id, error)
                return
            raise

        for child in children:
            async for folder in self._walk_additional_folders(
                user_id,
                child,
                excluded_ids,
                default_ids,
                synced_ids,
                pruned_ids,
            ):
                yield folder

    async def get_additional_mail_folders(self, user_id):
        """Yield extra mail folders when sync_all_mail_folders is enabled."""
        excluded_ids, default_ids = await self._folder_exclusion_and_default_ids(user_id)
        synced_ids = set(default_ids)
        pruned_ids = set()

        url = (
            f"{GRAPH_BASE_URL}/users/{user_id}/mailFolders"
            f"?$select={FOLDER_FIELDS}&includeHiddenFolders=true"
        )
        try:
            root_folders = await self._list_folders(url)
        except ClientResponseError as error:
            if error.status in (403, 404):
                self._folder_skip_warning(user_id, "mailFolders", error)
                return
            raise

        for folder in root_folders:
            async for entry in self._walk_additional_folders(
                user_id,
                folder,
                excluded_ids,
                default_ids,
                synced_ids,
                pruned_ids,
            ):
                yield entry
