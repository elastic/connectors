#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests for the convert-markdown client used by the SharePoint Online source."""

import pytest
from aioresponses import aioresponses

from connectors.sources.sharepoint.sharepoint_online.convert_markdown_client import (
    ConvertMarkdownClient,
)

BASE_URL = "http://convert-markdown:8000"
SUBMIT_URL = f"{BASE_URL}/api/v1/convert/upload?wait=5.0"
STATUS_PATH = "/api/v1/jobs/abc123"


@pytest.fixture
def pdf_file(tmp_path):
    path = tmp_path / "report.pdf"
    path.write_bytes(b"%PDF-1.4 fake pdf bytes")
    return str(path)


@pytest.fixture
async def client():
    client = ConvertMarkdownClient(base_url=BASE_URL, poll_interval=0)
    yield client
    await client.close()


class TestIsConvertible:
    @pytest.mark.parametrize(
        "filename, expected",
        [
            ("report.pdf", True),
            ("sheet.XLSX", True),
            ("page.htm", True),
            ("notes.txt", False),
            ("legacy.doc", False),
            ("no_extension", False),
            ("", False),
            (None, False),
        ],
    )
    def test_is_convertible(self, filename, expected):
        assert ConvertMarkdownClient.is_convertible(filename) is expected


@pytest.mark.asyncio
class TestConvertFile:
    async def test_returns_markdown_when_submit_finishes_inline(self, client, pdf_file):
        with aioresponses() as mocked:
            mocked.post(
                SUBMIT_URL,
                status=200,
                payload={"job_id": "abc123", "status": "done", "markdown": "# Report"},
            )

            assert await client.convert_file(pdf_file, "report.pdf") == "# Report"

        assert client.stats == (1, 0, 0)

    async def test_polls_until_job_is_done(self, client, pdf_file):
        with aioresponses() as mocked:
            mocked.post(
                SUBMIT_URL,
                status=202,
                payload={
                    "job_id": "abc123",
                    "status": "pending",
                    "status_url": STATUS_PATH,
                },
            )
            mocked.get(f"{BASE_URL}{STATUS_PATH}", payload={"status": "processing"})
            mocked.get(
                f"{BASE_URL}{STATUS_PATH}",
                payload={"status": "done", "markdown": "# Polled"},
            )

            assert await client.convert_file(pdf_file, "report.pdf") == "# Polled"

        assert client.stats == (1, 0, 0)

    async def test_failed_job_is_isolated_and_counted(self, client, pdf_file):
        with aioresponses() as mocked:
            mocked.post(
                SUBMIT_URL,
                status=200,
                payload={"job_id": "abc123", "status": "failed", "error": "boom"},
            )

            # A failure comes back as empty Markdown rather than raising, so one
            # bad document never aborts the sync.
            assert await client.convert_file(pdf_file, "report.pdf") == ""

        assert client.stats == (1, 1, 0)

    async def test_unreachable_service_is_isolated(self, client, pdf_file):
        with aioresponses() as mocked:
            mocked.post(SUBMIT_URL, exception=OSError("connection refused"))

            assert await client.convert_file(pdf_file, "report.pdf") == ""

        assert client.stats == (1, 1, 0)

    async def test_http_error_is_isolated(self, client, pdf_file):
        with aioresponses() as mocked:
            mocked.post(SUBMIT_URL, status=500, body="internal error")

            assert await client.convert_file(pdf_file, "report.pdf") == ""

        assert client.stats == (1, 1, 0)

    async def test_done_job_without_markdown_is_a_failure(self, client, pdf_file):
        with aioresponses() as mocked:
            mocked.post(SUBMIT_URL, status=200, payload={"status": "done"})

            assert await client.convert_file(pdf_file, "report.pdf") == ""

        assert client.stats == (1, 1, 0)

    async def test_empty_markdown_is_not_cached(self, client, pdf_file):
        with aioresponses() as mocked:
            mocked.post(
                SUBMIT_URL, status=200, payload={"status": "done", "markdown": ""}
            )
            mocked.post(
                SUBMIT_URL,
                status=200,
                payload={"status": "done", "markdown": "# Later"},
            )

            assert await client.convert_file(pdf_file, "report.pdf") == ""
            # A conversion that produced nothing must not poison every later copy
            # of the same document.
            assert await client.convert_file(pdf_file, "report.pdf") == "# Later"

        assert client.stats == (2, 0, 0)

    @pytest.mark.parametrize("filename", ["legacy.doc", "notes.txt", "no_extension"])
    async def test_unconvertible_file_is_never_uploaded(
        self, client, pdf_file, filename
    ):
        with aioresponses() as mocked:
            assert await client.convert_file(pdf_file, filename) == ""
            assert not mocked.requests

        # Not an attempt: nothing was sent, so it must not count against the
        # conversion failure rate.
        assert client.stats == (0, 0, 0)


@pytest.mark.asyncio
class TestConversionCache:
    async def test_identical_bytes_are_converted_once(self, client, tmp_path):
        first = tmp_path / "a.pdf"
        second = tmp_path / "b.pdf"
        first.write_bytes(b"same bytes")
        second.write_bytes(b"same bytes")

        with aioresponses() as mocked:
            mocked.post(
                SUBMIT_URL, status=200, payload={"status": "done", "markdown": "# Once"}
            )

            assert await client.convert_file(str(first), "a.pdf") == "# Once"
            # Only one POST is registered, so a second upload would raise here.
            assert await client.convert_file(str(second), "b.pdf") == "# Once"

        attempts, failures, cache_hits = client.stats
        assert (attempts, failures, cache_hits) == (2, 0, 1)

    async def test_cache_can_be_disabled(self, tmp_path):
        client = ConvertMarkdownClient(base_url=BASE_URL, cache=False, poll_interval=0)
        path = tmp_path / "a.pdf"
        path.write_bytes(b"same bytes")

        try:
            with aioresponses() as mocked:
                mocked.post(
                    SUBMIT_URL,
                    status=200,
                    payload={"status": "done", "markdown": "# 1"},
                )
                mocked.post(
                    SUBMIT_URL,
                    status=200,
                    payload={"status": "done", "markdown": "# 2"},
                )

                assert await client.convert_file(str(path), "a.pdf") == "# 1"
                assert await client.convert_file(str(path), "a.pdf") == "# 2"

            assert client.stats == (2, 0, 0)
        finally:
            await client.close()

    async def test_cache_respects_its_character_budget(self, tmp_path):
        client = ConvertMarkdownClient(
            base_url=BASE_URL, cache_max_chars=3, poll_interval=0
        )
        path = tmp_path / "a.pdf"
        path.write_bytes(b"same bytes")

        try:
            with aioresponses() as mocked:
                mocked.post(
                    SUBMIT_URL,
                    status=200,
                    payload={"status": "done", "markdown": "too long to cache"},
                )
                mocked.post(
                    SUBMIT_URL,
                    status=200,
                    payload={"status": "done", "markdown": "second"},
                )

                assert (
                    await client.convert_file(str(path), "a.pdf") == "too long to cache"
                )
                # Over budget, so it was never stored and the document converts again.
                assert await client.convert_file(str(path), "a.pdf") == "second"

            assert client.stats == (2, 0, 0)
        finally:
            await client.close()
