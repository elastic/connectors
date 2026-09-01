#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import base64
import json
import math
import time
import uuid
from urllib.parse import urlencode, urlparse

try:
    import jwt as pyjwt  # PyJWT; optional — only needed for jwt_bearer grant
    _PYJWT_AVAILABLE = True
except ImportError:
    pyjwt = None  # type: ignore[assignment]
    _PYJWT_AVAILABLE = False

import aiohttp
from connectors_sdk.logger import logger

from connectors.utils import CancellableSleeps, RetryStrategy, retryable

MAX_CONCURRENT_CLIENT_SUPPORT = 10
TABLE_FETCH_SIZE = 50
TABLE_BATCH_SIZE = 5
RETRIES = 3
RETRY_INTERVAL = 2
ORDER_BY_CREATION_DATE_QUERY = "ORDERBYsys_created_on^"
ENDPOINTS = {
    "TABLE": "/api/now/table/{table}",
    "ATTACHMENT": "/api/now/attachment",
    "DOWNLOAD": "/api/now/attachment/{sys_id}/file",
    "BATCH": "/api/now/v1/batch",
}


class InvalidResponse(Exception):
    pass


class ServiceNowClient:
    """ServiceNow Client"""

    def __init__(self, configuration):
        """Setup the ServiceNow client.

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """

        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self.services = self.configuration["services"]
        self.retry_count = self.configuration["retry_count"]
        self._logger = logger
        self._session = None  # initialised lazily on first async call
        self._token_expiry = 0.0  # epoch seconds via monotonic clock; 0 = not yet fetched

    def set_logger(self, logger_):
        self._logger = logger_

    def _build_jwt_assertion(self, token_url):
        """Build a signed JWT assertion for the jwt_bearer grant type.

        The JWT is signed with the configured RSA/EC private key using the
        algorithm specified by 'jwt_algorithm' (default RS256).  The 'iss'
        and 'sub' claims are both set to the client_id unless a separate
        'jwt_subject' is provided.

        Args:
            token_url (str): The token endpoint URL — used as the 'aud' claim.

        Returns:
            str: Compact serialised, signed JWT.

        Raises:
            RuntimeError: If PyJWT is not installed.
            InvalidResponse: If the private key is not configured.
        """
        if not _PYJWT_AVAILABLE:
            raise RuntimeError(
                "PyJWT is required for the jwt_bearer grant type. "
                "Install it with: pip install PyJWT cryptography"
            )
        private_key = self.configuration.get("jwt_private_key", "")
        if not private_key:
            raise InvalidResponse(
                "jwt_private_key must be set when OAuth Grant Type is 'JWT Bearer'."
            )
        client_id = self.configuration["client_id"]
        subject = self.configuration.get("jwt_subject", "") or client_id
        algorithm = self.configuration.get("jwt_algorithm", "RS256")
        now = int(time.time())
        payload = {
            "iss": client_id,
            "sub": subject,
            "aud": token_url,
            "iat": now,
            "exp": now + 3600,
        }
        key_id = self.configuration.get("jwt_key_id", "")
        headers = {"kid": key_id} if key_id else {}
        return pyjwt.encode(payload, private_key, algorithm=algorithm, headers=headers)

    async def _fetch_access_token(self):
        """Fetch an OAuth 2.0 Bearer token from ServiceNow.

        Supports five grant types selected by the 'oauth_grant_type' config field:

        - 'password' (default): Resource Owner Password Credentials; requires
          client_id, client_secret, username, and oauth_password.  Works on all
          instances including ServiceNow PDIs.

        - 'client_credentials': Machine-to-machine; requires only client_id and
          client_secret.  Not available on ServiceNow developer (PDI) instances.

        - 'refresh_token': Refresh Token grant; requires client_id, client_secret,
          username, and refresh_token.

        - 'authorization_code': Authorization Code grant (+ optional PKCE); the
          caller is expected to have completed the browser-based auth flow and
          stored the resulting authorization code in 'oauth_authorization_code'.
          A redirect_uri must also be supplied.  When PKCE is used the
          'pkce_code_verifier' field must carry the original code verifier.
          After the initial exchange the response's refresh_token is used for
          all subsequent refreshes.

        - 'jwt_bearer': JWT Bearer assertion grant
          (urn:ietf:params:oauth:grant-type:jwt-bearer); requires client_id,
          jwt_private_key (PEM), and optionally jwt_subject, jwt_key_id, and
          jwt_algorithm.

        The token URL is derived from scheme + host only to avoid double-path bugs.

        Returns:
            str: Access token string.
        """
        parsed = urlparse(self.configuration["url"])
        token_url = f"{parsed.scheme}://{parsed.netloc}/oauth_token.do"
        grant_type = self.configuration.get("oauth_grant_type", "password")

        if grant_type == "jwt_bearer":
            data = {
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "client_id": self.configuration["client_id"],
                "assertion": self._build_jwt_assertion(token_url),
            }
            client_secret = self.configuration.get("client_secret", "")
            if client_secret:
                data["client_secret"] = client_secret
        elif grant_type == "authorization_code":
            # If we already have a cached refresh token from a previous code
            # exchange, use it to obtain a fresh access token instead of
            # re-exchanging the one-time authorization code.
            cached_refresh = getattr(self, "_cached_refresh_token", None)
            if cached_refresh:
                data = {
                    "grant_type": "refresh_token",
                    "client_id": self.configuration["client_id"],
                    "client_secret": self.configuration["client_secret"],
                    "refresh_token": cached_refresh,
                }
            else:
                data = {
                    "grant_type": "authorization_code",
                    "client_id": self.configuration["client_id"],
                    "client_secret": self.configuration["client_secret"],
                    "code": self.configuration["oauth_authorization_code"],
                    "redirect_uri": self.configuration["oauth_redirect_uri"],
                }
                pkce_verifier = self.configuration.get("pkce_code_verifier", "")
                if pkce_verifier:
                    data["code_verifier"] = pkce_verifier
        else:
            data = {
                "grant_type": grant_type,
                "client_id": self.configuration["client_id"],
                "client_secret": self.configuration["client_secret"],
            }
            if grant_type == "password":
                data["username"] = self.configuration["oauth_username"]
                data["password"] = self.configuration["oauth_password"]
            elif grant_type == "refresh_token":
                data["username"] = self.configuration["oauth_username_refresh"]
                data["refresh_token"] = self.configuration["refresh_token"]

        self._logger.debug(f"Fetching OAuth token using grant_type={grant_type}")
        self._logger.debug(
            f"Token request -> url={token_url} "
            f"client_id={'set' if data.get('client_id') else 'MISSING'} "
            f"client_secret={'set' if data.get('client_secret') else 'MISSING'} "
            f"username={'set' if data.get('username') else 'n/a'} "
            f"refresh_token={'set' if data.get('refresh_token') else 'n/a'} "
            f"code={'set' if data.get('code') else 'n/a'} "
            f"assertion={'set' if data.get('assertion') else 'n/a'}"
        )
        timeout = aiohttp.ClientTimeout(total=None)  # pyright: ignore
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                token_url,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            ) as response:
                if response.status == 401:
                    body = await response.text()
                    self._logger.error(
                        f"OAuth token request returned 401. Response body: {body}"
                    )
                response.raise_for_status()
                token_response = await response.json(content_type=None)

        # Cache the refresh token returned by authorization_code exchanges so
        # that subsequent calls use the refresh_token grant instead of the
        # one-time code.
        new_refresh_token = token_response.get("refresh_token")
        if grant_type == "authorization_code" and new_refresh_token:
            self._cached_refresh_token = new_refresh_token

        access_token = token_response.get("access_token")
        if not access_token:
            msg = "OAuth token response did not contain an access_token."
            raise InvalidResponse(msg)
        expires_in = int(token_response.get("expires_in", 1800))  # ServiceNow default is 1800s
        # Subtract 60s so we refresh before the token actually expires
        self._token_expiry = time.monotonic() + expires_in - 60
        self._logger.debug(
            f"OAuth access token fetched successfully (expires in {expires_in}s, "
            f"will refresh after {expires_in - 60}s)."
        )
        return access_token

    async def _create_session(self):
        """Async helper: build and return a configured ClientSession.

        Uses Basic Auth when auth_method is 'basic', otherwise fetches an
        OAuth 2.0 Bearer token and injects it as an Authorization header.
        """
        auth_method = self.configuration.get("auth_method", "oauth")
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_CLIENT_SUPPORT)
        timeout = aiohttp.ClientTimeout(total=None)  # pyright: ignore
        if auth_method == "basic":
            self._logger.debug("Generating aiohttp client session (Basic Auth)")
            basic_auth = aiohttp.BasicAuth(
                login=self.configuration["username"],
                password=self.configuration["basic_auth_password"],
            )
            return aiohttp.ClientSession(
                connector=connector,
                base_url=self.configuration["url"],
                auth=basic_auth,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                timeout=timeout,
                raise_for_status=True,
            )
        # OAuth (default)
        self._logger.debug("Generating aiohttp client session (OAuth)")
        access_token = await self._fetch_access_token()
        return aiohttp.ClientSession(
            connector=connector,
            base_url=self.configuration["url"],
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {access_token}",
            },
            timeout=timeout,
            raise_for_status=True,
        )

    async def _get_session(self):
        """Return a lazily-created, cached aiohttp ClientSession.

        For OAuth: proactively refreshes the session when the access token is
        within 60s of expiry so long-running syncs never hit a mid-batch 401.
        For Basic Auth: session is created once and reused indefinitely.

        Returns:
            aiohttp.ClientSession: An instance of Client Session
        """
        auth_method = self.configuration.get("auth_method", "oauth")
        if (
            auth_method != "basic"
            and self._session is not None
            and time.monotonic() >= self._token_expiry
        ):
            self._logger.debug(
                "OAuth access token near expiry. Proactively refreshing session."
            )
            await self.close_session()  # sets self._session = None
        if self._session is None:
            self._session = await self._create_session()
        return self._session

    async def _read_response(self, response):
        fetched_response = await response.read()
        if fetched_response == b"":
            msg = "Request to ServiceNow server returned an empty response."
            raise InvalidResponse(msg)
        elif not response.headers["Content-Type"].startswith("application/json"):
            if response.headers.get("Connection") == "close":
                msg = "Couldn't connect to ServiceNow instance"
                raise Exception(msg)
            msg = f"Cannot proceed due to unexpected response type '{response.headers['Content-Type']}'; response type must begin with 'application/json'."
            raise InvalidResponse(msg)
        return fetched_response

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_table_length(self, table_name):
        try:
            url = ENDPOINTS["TABLE"].format(table=table_name)
            params = {"sysparm_limit": 1}
            response = await self._api_call(
                url=url, params=params, actions={}, method="get"
            )
            await self._read_response(response=response)
            return int(response.headers.get("x-total-count", 0))
        except Exception as exception:
            self._logger.warning(
                f"Error while fetching {table_name} length. Exception: {exception}."
            )
            raise

    def _prepare_url(self, url, params, offset):
        if not url.endswith("/file"):
            query = ORDER_BY_CREATION_DATE_QUERY
            if "sysparm_query" in params.keys():
                query += params["sysparm_query"]
            params.update(
                {
                    "sysparm_query": query,
                    "sysparm_limit": TABLE_FETCH_SIZE,
                    "sysparm_offset": offset,
                }
            )
        full_url = url
        if params:
            params_string = urlencode(params)
            full_url = f"{url}?{params_string}"
        return full_url

    async def get_filter_apis(self, rules, mapping):
        apis = []
        for rule in rules:
            params = {"sysparm_query": rule["query"]}
            table_name = mapping[rule["service"]]
            total_count = await self.get_table_length(table_name)
            paginated_apis = self.get_record_apis(
                url=ENDPOINTS["TABLE"].format(table=table_name),
                params=params,
                total_count=total_count,
            )
            apis.extend(paginated_apis)
        return apis

    def get_record_apis(self, url, params, total_count):
        headers = [
            {"name": "Content-Type", "value": "application/json"},
            {"name": "Accept", "value": "application/json"},
        ]
        apis = []
        for page in range(math.ceil(total_count / TABLE_FETCH_SIZE)):
            apis.append(
                {
                    "id": str(uuid.uuid4()),
                    "headers": headers,
                    "method": "GET",
                    "url": self._prepare_url(
                        url=url,
                        params=params.copy(),
                        offset=page * TABLE_FETCH_SIZE,
                    ),
                }
            )
        return apis

    def get_attachment_apis(self, url, ids):
        headers = [
            {"name": "Content-Type", "value": "application/json"},
            {"name": "Accept", "value": "application/json"},
        ]
        apis = []
        for id_ in ids:
            params = {"table_sys_id": id_}
            apis.append(
                {
                    "id": str(uuid.uuid4()),
                    "headers": headers,
                    "method": "GET",
                    "url": self._prepare_url(url=url, params=params.copy(), offset=0),
                }
            )
        return apis

    async def get_data(self, batched_apis):
        try:
            batch_data = self._prepare_batch(requests=batched_apis)
            async for response in self._batch_api_call(batch_data=batch_data):
                yield response
        except Exception as exception:
            self._logger.debug(
                f"Error while fetching batch: {batched_apis} data. Exception: {exception}."
            )
            raise

    def _prepare_batch(self, requests):
        return {"batch_request_id": str(uuid.uuid4()), "rest_requests": requests}

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _batch_api_call(self, batch_data):
        """Execute a ServiceNow batch API request and yield each serviced result.

        On a 401 response inside the batch envelope:
        - OAuth: closes the session so the @retryable decorator gets a fresh token
          on the next attempt.
        - Basic Auth: credentials are baked into the session and cannot be refreshed
          at runtime, so close_session() is skipped. The error is logged at ERROR
          level because it indicates a configuration problem rather than a transient
          token expiry.

        Args:
            batch_data (dict): Batch request payload as produced by _prepare_batch().

        Yields:
            list: Decoded 'result' list from each successfully serviced sub-request.

        Raises:
            InvalidResponse: If any sub-request returns a non-200 status code.
        """
        response = await self._api_call(
            url=ENDPOINTS["BATCH"], params={}, actions=batch_data, method="post"
        )
        json_response = json.loads(await self._read_response(response=response))

        auth_method = self.configuration.get("auth_method", "oauth")
        for response in json_response["serviced_requests"]:
            if response["status_code"] == 401:
                if auth_method != "basic":
                    # Session close lets @retryable rebuild it with a fresh token.
                    self._logger.warning(
                        "OAuth token expired (401 inside batch response). Refreshing session."
                    )
                    await self.close_session()
                else:
                    # Credentials are wrong; recreating the session would not help.
                    self._logger.error(
                        "Basic Auth credentials rejected (401 inside batch response). Check username and password."
                    )
                msg = f"Cannot proceed due to invalid status code 401; Message {json.loads(base64.b64decode(response['body']))['error']}."
                raise InvalidResponse(msg)
            if response["status_code"] != 200:
                error_message = json.loads(base64.b64decode(response["body"]))["error"]
                msg = f"Cannot proceed due to invalid status code {response['status_code']}; Message {error_message}."
                raise InvalidResponse(msg)
            yield json.loads(base64.b64decode(response["body"]))["result"]

    async def _api_call(self, url, params, actions, method):
        """Dispatch a single HTTP request through the managed session.

        Awaits _get_session() on every call so that OAuth token refresh and
        lazy session creation are handled transparently.

        Args:
            url (str): Relative URL path (resolved against the base_url on the session).
            params (dict): Query-string parameters.
            actions (dict): Request body serialised as JSON.
            method (str): HTTP verb as a lowercase string (e.g. 'get', 'post').

        Returns:
            aiohttp.ClientResponse: The raw response object.
        """
        session = await self._get_session()
        return await getattr(session, method)(
            url=url, params=params, json=actions
        )

    async def download_func(self, url):
        response = await self._api_call(url, {}, {}, "get")
        yield response

    async def filter_services(self, configured_service):
        """Filter services based on service mappings.

        Args:
            configured_service (list): Services need to validate.

        Returns:
            dict, list: Servicenow mapping, Invalid services.
        """

        try:
            self._logger.debug("Filtering services")
            servicenow_mapping, invalid_services = {}, configured_service

            payload = {"sysparm_fields": "sys_id, label, name"}
            table_length = await self.get_table_length(table_name="sys_db_object")
            record_apis = self.get_record_apis(
                url=ENDPOINTS["TABLE"].format(table="sys_db_object"),
                params=payload,
                total_count=table_length,
            )

            for batched_apis_index in range(0, len(record_apis), TABLE_BATCH_SIZE):
                batched_apis = record_apis[
                    batched_apis_index : (
                        batched_apis_index + TABLE_BATCH_SIZE
                    )  # noqa
                ]
                async for table_data in self.get_data(batched_apis=batched_apis):
                    for mapping in table_data:  # pyright: ignore
                        sys_id = mapping.get("sys_id")
                        name = mapping.get("name")
                        if not name:
                            self._log_missing_sysparm_field(sys_id, "name")
                            continue

                        label = mapping.get("label")
                        if not label:
                            self._log_missing_sysparm_field(sys_id, "label")
                            continue

                        if label in invalid_services:
                            servicenow_mapping[label] = name
                            invalid_services.remove(label)

            return servicenow_mapping, invalid_services

        except Exception as exception:
            self._logger.exception(
                f"Error while filtering services. Exception: {exception}."
            )
            raise

    def _log_missing_sysparm_field(self, sys_id, field):
        msg = f"Entry in sys_db_object with sys_id '{sys_id}' is missing sysparm_field '{field}'. This is a non-issue if no invalid services are flagged."
        self._logger.debug(msg)

    async def ping(self):
        await self.get_table_length(table_name="sys_db_object")

    async def close_session(self):
        """Close the active client session and reset session state.

        Guards against double-close: a no-op when no session has been created
        yet or after the session has already been closed. Sets self._session to
        None so that the next call to _get_session() will create a fresh one —
        this is relied upon by the OAuth token-refresh path in _get_session()
        and by the 401 recovery path in _batch_api_call().
        """
        self._sleeps.cancel()
        if self._session is not None:
            await self._session.close()
            self._session = None
