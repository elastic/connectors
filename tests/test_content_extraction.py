#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest.mock import mock_open, patch

import pytest
from aiohttp.client_exceptions import ClientConnectionError, ServerTimeoutError

from connectors.content_extraction import ContentExtraction
from typing import Dict, Union


def test_set_and_get_configuration() -> None:
    config = {
        "extraction_service": {
            "host": "http://localhost:8090",
        }
    }
    ContentExtraction.set_extraction_config(config)
    assert ContentExtraction.get_extraction_config() == config


@pytest.mark.parametrize(
    "mock_config, expected_result",
    [
        (
            {
                "extraction_service": {
                    "host": "http://localhost:8090",
                }
            },
            True,
        ),
        ({"something_else": "???"}, False),
        ({"extraction_service": {"not_a_host": "!!!m"}}, False),
    ],
)
def test_check_configured(mock_config: Dict[str, Union[Dict[str, str], str]], expected_result: bool) -> None:
    with patch(
        "connectors.content_extraction.ContentExtraction.get_extraction_config",
        return_value=mock_config.get("extraction_service", None),
    ):
        extraction_service = ContentExtraction()
        assert extraction_service._check_configured() is expected_result


@pytest.mark.asyncio
async def test_extract_text(mock_responses, patch_logger):
    filepath = "tmp/notreal.txt"
    url = "http://localhost:8090/extract_text/"
    payload = {"extracted_text": "I've been extracted!"}

    with (
        patch("builtins.open", mock_open(read_data=b"data")),
        patch(
            "connectors.content_extraction.ContentExtraction.get_extraction_config",
            return_value={"host": "http://localhost:8090"},
        ),
    ):
        mock_responses.put(url, status=200, payload=payload)

        extraction_service = ContentExtraction()
        extraction_service._begin_session()

        assert extraction_service.get_volume_dir() is None

        response = await extraction_service.extract_text(filepath, "notreal.txt")
        await extraction_service._end_session()

        assert response == "I've been extracted!"
        patch_logger.assert_present("Text extraction is successful for 'notreal.txt'.")


@pytest.mark.asyncio
async def test_extract_text_with_file_pointer(mock_responses, patch_logger):
    filepath = "/tmp/notreal.txt"
    url = "http://localhost:8090/extract_text/?local_file_path=/tmp/notreal.txt"
    payload = {"extracted_text": "I've been extracted from a local file!"}

    with (
        patch("builtins.open", mock_open(read_data=b"data")),
        patch(
            "connectors.content_extraction.ContentExtraction.get_extraction_config",
            return_value={
                "host": "http://localhost:8090",
                "use_file_pointers": True,
                "shared_volume_dir": "/tmp",
            },
        ),
    ):
        mock_responses.put(url, status=200, payload=payload)

        extraction_service = ContentExtraction()
        extraction_service._begin_session()

        assert extraction_service.get_volume_dir() == "/tmp"

        response = await extraction_service.extract_text(filepath, "notreal.txt")
        await extraction_service._end_session()

        assert response == "I've been extracted from a local file!"
        patch_logger.assert_present("Text extraction is successful for 'notreal.txt'.")


@pytest.mark.asyncio
async def test_extract_text_when_host_is_none(mock_responses, patch_logger):
    filepath = "/tmp/notreal.txt"

    with (
        patch("builtins.open", mock_open(read_data=b"data")),
        patch(
            "connectors.content_extraction.ContentExtraction.get_extraction_config",
            return_value={
                "host": None,
                "use_file_pointers": True,
                "shared_volume_dir": "/tmp",
            },
        ),
    ):
        extraction_service = ContentExtraction()

        assert extraction_service.get_volume_dir() is None

        response = await extraction_service.extract_text(filepath, "notreal.txt")
        await extraction_service._end_session()

        assert response == ""
        patch_logger.assert_present(
            "Extraction service has been initialised but no extraction service configuration was found. No text will be extracted for this sync."
        )


@pytest.mark.asyncio
async def test_extract_text_when_response_isnt_200_logs_warning(
    mock_responses, patch_logger
):
    filepath = "tmp/notreal.txt"
    url = "http://localhost:8090/extract_text/"

    with (
        patch("builtins.open", mock_open(read_data=b"data")),
        patch(
            "connectors.content_extraction.ContentExtraction.get_extraction_config",
            return_value={"host": "http://localhost:8090"},
        ),
    ):
        mock_responses.put(
            url,
            status=422,
            payload={
                "error": "Unprocessable Entity",
                "message": "Could not process file.",
            },
        )

        extraction_service = ContentExtraction()
        extraction_service._begin_session()

        response = await extraction_service.extract_text(filepath, "notreal.txt")
        await extraction_service._end_session()
        assert response == ""

        patch_logger.assert_present(
            "Extraction service could not parse `notreal.txt'. Status: [422]; Unprocessable Entity: Could not process file."
        )


@pytest.mark.asyncio
async def test_extract_text_when_response_is_error(mock_responses, patch_logger):
    filepath = "tmp/notreal.txt"

    with (
        patch("builtins.open", mock_open(read_data=b"data")),
        patch(
            "connectors.content_extraction.ContentExtraction.get_extraction_config",
            return_value={"host": "http://localhost:8090"},
        ),
        patch(
            "connectors.content_extraction.ContentExtraction.send_file",
            side_effect=ClientConnectionError("oops!"),
        ),
    ):
        extraction_service = ContentExtraction()
        extraction_service._begin_session()

        response = await extraction_service.extract_text(filepath, "notreal.txt")
        await extraction_service._end_session()
        assert response == ""

        patch_logger.assert_present(
            "Connection to http://localhost:8090 failed while extracting data from notreal.txt. Error: oops!"
        )


@pytest.mark.asyncio
async def test_extract_text_when_response_is_timeout(mock_responses, patch_logger):
    filepath = "tmp/notreal.txt"

    with (
        patch("builtins.open", mock_open(read_data=b"data")),
        patch(
            "connectors.content_extraction.ContentExtraction.get_extraction_config",
            return_value={"host": "http://localhost:8090"},
        ),
        patch(
            "connectors.content_extraction.ContentExtraction.send_file",
            side_effect=ServerTimeoutError("nada"),
        ),
    ):
        extraction_service = ContentExtraction()
        extraction_service._begin_session()

        response = await extraction_service.extract_text(filepath, "notreal.txt")
        await extraction_service._end_session()
        assert response == ""

        patch_logger.assert_present(
            "Text extraction request to http://localhost:8090 timed out for notreal.txt: nada"
        )


@pytest.mark.asyncio
async def test_extract_text_when_response_is_200_with_error_logs_warning(
    mock_responses, patch_logger
):
    filepath = "tmp/notreal.txt"
    url = "http://localhost:8090/extract_text/"

    with (
        patch("builtins.open", mock_open(read_data=b"data")),
        patch(
            "connectors.content_extraction.ContentExtraction.get_extraction_config",
            return_value={"host": "http://localhost:8090"},
        ),
    ):
        mock_responses.put(
            url,
            status=200,
            payload={"error": "oh no!", "message": "I'm all messed up..."},
        )

        extraction_service = ContentExtraction()
        extraction_service._begin_session()

        response = await extraction_service.extract_text(filepath, "notreal.txt")
        await extraction_service._end_session()
        assert response == ""

        patch_logger.assert_present(
            "Extraction service could not parse `notreal.txt'. Status: [200]; oh no!: I'm all messed up..."
        )
