#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Client for the convert-markdown service.

Uploads an already-downloaded document to the convert-markdown service, which
returns Markdown with table structure preserved. Conversion itself — the
Docling/torch model stack — lives in convert-markdown; this client owns deciding
what is worth sending, polling the async job, and per-document failure isolation.

Unlike the text extraction service, conversion is submitted as a background job
and polled: Docling is CPU-bound and a large document can run for tens of
minutes, far longer than a gateway will hold a request open.
"""

import asyncio
import hashlib
import os
from dataclasses import dataclass

import aiofiles
import aiohttp

# File extensions worth sending to convert-markdown. Checked here so an
# unsupported document costs no upload; convert-markdown re-validates and
# remains the authority on what it can parse.
CONVERTIBLE_EXTENSIONS = (".pdf", ".docx", ".xlsx", ".pptx", ".html", ".htm")

# Legacy binary Office formats that Docling cannot parse.
LEGACY_EXTENSIONS = (".doc", ".xls", ".ppt")

DEFAULT_BASE_URL = "http://convert-markdown:8000"


@dataclass(frozen=True)
class _CacheEntry:
    """A conversion already paid for, keyed by the digest of its source bytes."""

    markdown: str
    convert_ms: int | None


class ConvertMarkdownClient:
    """Convert a local document to Markdown via the convert-markdown service."""

    def __init__(
        self,
        base_url=DEFAULT_BASE_URL,
        convert_timeout=3600,
        request_timeout=30,
        submit_wait=5.0,
        poll_interval=1.0,
        cache=True,
        cache_max_chars=128_000_000,
        logger_=None,
    ):
        """
        Args:
            base_url: Root URL of the convert-markdown service.
            convert_timeout: Overall budget in seconds for one conversion, i.e.
                how long to keep polling the job before giving up. Conversion is
                CPU-bound and slow (seconds/page), so this is far more generous
                than the per-request timeout.
            request_timeout: Timeout in seconds for a single HTTP call.
            submit_wait: Seconds the submit blocks server-side for a fast
                conversion to finish inline before falling back to polling.
                Small documents usually return on submit.
            poll_interval: Seconds between job-status polls. A job finishes at a
                uniformly random point inside the interval, so this costs
                ``poll_interval / 2`` of dead time per polled document. A status
                poll is a trivial read against an in-memory job store, so polling
                often is far cheaper than waiting.
            cache: Reuse a conversion when a later document is byte-identical to
                an earlier one. SharePoint routinely holds the same file in
                several sites and libraries, and conversion is deterministic, so
                the later copies are pure waste. Keyed on the SHA-256 of the
                downloaded bytes, so a hit means identical input, never a
                heuristic match.
            cache_max_chars: Ceiling on total cached Markdown, as a character
                count. Past the ceiling new entries are not stored and conversion
                proceeds normally.
        """
        self.base_url = base_url.rstrip("/")
        self.convert_timeout = convert_timeout
        self.request_timeout = request_timeout
        self._submit_wait = submit_wait
        self._poll_interval = poll_interval
        self._session = None
        self._logger = logger_

        self._cache_enabled = cache
        self._cache_max_chars = cache_max_chars
        self._cache = {}
        self._cache_chars = 0
        self._cache_hits = 0

        # Conversion telemetry, so a systemic failure (service down, bad models)
        # is visible instead of silently indexing empty bodies for the whole
        # corpus.
        self._attempts = 0
        self._failures = 0

    def set_logger(self, logger_):
        self._logger = logger_

    @property
    def stats(self):
        """Return ``(attempts, failures, cache_hits)`` accumulated so far."""
        return self._attempts, self._failures, self._cache_hits

    def _log_debug(self, msg):
        if self._logger:
            self._logger.debug(msg)

    def _log_warning(self, msg):
        if self._logger:
            self._logger.warning(msg)

    def _log_error(self, msg):
        if self._logger:
            self._logger.error(msg)

    def _log_info(self, msg):
        if self._logger:
            self._logger.info(msg)

    @classmethod
    def is_convertible(cls, filename):
        """Whether convert-markdown can be expected to parse ``filename``."""
        if not filename or "." not in filename:
            return False
        return os.path.splitext(filename)[-1].lower() in CONVERTIBLE_EXTENSIONS

    def _begin_session(self):
        if self._session is not None:
            return self._session

        # No total timeout on the session: the submit blocks server-side for up
        # to submit_wait, and each call passes its own timeout instead.
        self._session = aiohttp.ClientSession(
            headers={"accept": "application/json"},
            raise_for_status=False,
        )
        return self._session

    async def close(self):
        if self._session is None:
            return
        await self._session.close()
        self._session = None

    async def convert_file(self, filepath, original_filename):
        """Convert the document at ``filepath`` and return its Markdown.

        A per-document failure is logged and counted (see :attr:`stats`) and
        comes back as an empty string, so one bad document never aborts a sync —
        matching how the text extraction service behaves on failure.
        """
        extension = os.path.splitext(original_filename)[-1].lower()

        if extension in LEGACY_EXTENSIONS:
            self._log_warning(
                f"Not converting {original_filename}: legacy binary Office file '{extension}' cannot be parsed"
            )
            return ""

        if extension not in CONVERTIBLE_EXTENSIONS:
            self._log_debug(
                f"Not converting {original_filename}: unsupported file extension '{extension or '(none)'}'"
            )
            return ""

        self._attempts += 1

        try:
            async with aiofiles.open(file=filepath, mode="rb") as source_file:
                data = await source_file.read()
        except OSError as e:
            self._failures += 1
            self._log_error(f"Failed to read {original_filename} for conversion: {e}")
            return ""

        digest = hashlib.sha256(data).hexdigest()
        cached = self._cache_take(digest)
        if cached is not None:
            self._log_info(
                f"Reused conversion for {original_filename} — identical to a document already "
                f"converted this sync, avoiding {(cached.convert_ms or 0) / 1000:.1f}s"
            )
            return cached.markdown

        self._log_info(
            f"Converting {original_filename} ({len(data) // 1024} KB) to Markdown"
        )

        loop = asyncio.get_event_loop()
        started = loop.time()
        try:
            markdown = await self._convert(data, original_filename)
        except Exception as e:
            self._failures += 1
            self._log_error(
                f"Failed to convert {original_filename} after {loop.time() - started:.1f}s: {e}"
            )
            return ""

        convert_ms = int((loop.time() - started) * 1000)
        if not markdown:
            # A job that succeeds but returns nothing is not a success worth
            # reporting as one — it is how a whole corpus gets indexed blank.
            self._log_warning(
                f"convert-markdown returned no text for {original_filename}"
            )
        else:
            self._log_info(
                f"Converted {original_filename} ({len(markdown)} chars in {convert_ms / 1000:.1f}s)"
            )
            # Only successful conversions are cached. A failure is usually
            # transient and caching it would turn one bad moment into every copy
            # of that document failing.
            self._cache_put(
                digest, _CacheEntry(markdown=markdown, convert_ms=convert_ms)
            )

        return markdown

    def _cache_take(self, digest):
        if not self._cache_enabled:
            return None
        entry = self._cache.get(digest)
        if entry is not None:
            self._cache_hits += 1
        return entry

    def _cache_put(self, digest, entry):
        if not self._cache_enabled or digest in self._cache:
            return
        if self._cache_chars + len(entry.markdown) > self._cache_max_chars:
            self._log_debug(
                f"Conversion cache at its {self._cache_max_chars}-character budget; "
                f"not caching a further {len(entry.markdown)} characters"
            )
            return
        self._cache[digest] = entry
        self._cache_chars += len(entry.markdown)

    async def _convert(self, data, original_filename):
        """Submit ``data`` to convert-markdown, then poll until the job finishes.

        Returns the Markdown. A submit returns it inline when the conversion
        finishes within the service's bounded wait (small documents); otherwise
        the job is polled until it is ``done``. Raises on a failed job, an
        overall timeout, or an unreachable/malformed service.
        """
        job = await self._submit(data, original_filename)
        status = job.get("status")

        if status == "done":
            return self._markdown_of(job)
        if status == "failed":
            msg = f"convert-markdown job failed: {job.get('error')}"
            raise RuntimeError(msg)

        status_url = job.get("status_url")
        job_id = job.get("job_id")
        if not status_url:
            msg = f"Malformed response from convert-markdown: no status_url in {job}"
            raise RuntimeError(msg)

        loop = asyncio.get_event_loop()
        deadline = loop.time() + self.convert_timeout
        while loop.time() < deadline:
            await asyncio.sleep(self._poll_interval)
            job = await self._get_job(status_url)
            status = job.get("status")
            if status == "done":
                return self._markdown_of(job)
            if status == "failed":
                msg = f"convert-markdown job {job_id} failed: {job.get('error')}"
                raise RuntimeError(msg)
            # pending / processing -> keep polling

        msg = f"convert-markdown job {job_id} did not finish within {self.convert_timeout}s"
        raise RuntimeError(msg)

    async def _submit(self, data, original_filename):
        """POST the bytes as a multipart upload; return the parsed job."""
        # Quotes would break out of the Content-Disposition filename parameter.
        safe_name = (
            original_filename.replace('"', "").replace("\r", "").replace("\n", "")
            or "document"
        )

        form = aiohttp.FormData()
        form.add_field(
            "file", data, filename=safe_name, content_type="application/octet-stream"
        )

        # The submit blocks server-side up to submit_wait; allow for that on top
        # of the normal request timeout so the socket does not time out under it.
        return await self._request_json(
            "POST",
            f"{self.base_url}/api/v1/convert/upload?wait={self._submit_wait}",
            timeout=self.request_timeout + self._submit_wait,
            data=form,
        )

    async def _get_job(self, status_url):
        """GET a job's status. ``status_url`` is a path returned by the submit."""
        return await self._request_json(
            "GET", f"{self.base_url}{status_url}", timeout=self.request_timeout
        )

    async def _request_json(self, method, url, timeout, data=None):
        """Send a request and parse the JSON body, mapping transport errors."""
        session = self._begin_session()
        try:
            async with session.request(
                method, url, data=data, timeout=aiohttp.ClientTimeout(total=timeout)
            ) as response:
                body = await response.text()
                if response.status >= 400:
                    # convert-markdown puts the reason in a JSON `detail` field.
                    msg = f"convert-markdown returned HTTP {response.status}: {body[:500]}"
                    raise RuntimeError(msg)
                try:
                    parsed = await response.json(content_type=None)
                except ValueError as e:
                    msg = f"Malformed response from convert-markdown: {e}"
                    raise RuntimeError(msg) from e
        except aiohttp.ClientError as e:
            msg = f"convert-markdown is unreachable at {self.base_url}: {e}"
            raise RuntimeError(msg) from e
        except asyncio.TimeoutError as e:
            msg = f"convert-markdown did not respond within {timeout}s"
            raise RuntimeError(msg) from e

        if not isinstance(parsed, dict):
            msg = f"Unexpected response from convert-markdown: {parsed!r}"
            raise RuntimeError(msg)
        return parsed

    @staticmethod
    def _markdown_of(job):
        """Pull the Markdown off a finished job."""
        markdown = job.get("markdown")
        if markdown is None:
            msg = "convert-markdown reported a done job with no markdown"
            raise RuntimeError(msg)
        return str(markdown)
