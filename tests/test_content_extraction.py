#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest.mock import mock_open, patch

import pytest

from connectors.content_extraction import ContentExtraction


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
def test_check_configured(mock_config, expected_result):
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

    with patch("builtins.open", mock_open(read_data=b"data")), patch(
        "connectors.content_extraction.ContentExtraction.get_extraction_config",
        return_value={"host": "http://localhost:8090"},
    ):
        mock_responses.put(url, status=200, payload=payload)

        extraction_service = ContentExtraction()
        extraction_service._begin_session()

        response = await extraction_service.extract_text(filepath, "notreal.txt")
        await extraction_service._end_session()

        assert response == "I've been extracted!"
        patch_logger.assert_present("Text extraction is successful for 'notreal.txt'.")


@pytest.mark.asyncio
async def test_extract_text_with_file_pointer(mock_responses, patch_logger):
    filepath = "/tmp/notreal.txt"
    url = "http://localhost:8090/extract_text/?local_file_path=/tmp/notreal.txt"
    payload = {"extracted_text": "I've been extracted from a local file!"}

    with patch("builtins.open", mock_open(read_data=b"data")), patch(
        "connectors.content_extraction.ContentExtraction.get_extraction_config",
        return_value={
            "host": "http://localhost:8090",
            "use_file_pointers": True,
            "shared_volume_dir": "/tmp",
        },
    ):
        mock_responses.put(url, status=200, payload=payload)

        extraction_service = ContentExtraction()
        extraction_service._begin_session()

        response = await extraction_service.extract_text(filepath, "notreal.txt")
        await extraction_service._end_session()

        assert response == "I've been extracted from a local file!"
        patch_logger.assert_present("Text extraction is successful for 'notreal.txt'.")


@pytest.mark.asyncio
async def test_extract_text_when_response_isnt_200_logs_warning(
    mock_responses, patch_logger
):
    filepath = "tmp/notreal.txt"
    url = "http://localhost:8090/extract_text/"

    with patch("builtins.open", mock_open(read_data=b"data")), patch(
        "connectors.content_extraction.ContentExtraction.get_extraction_config",
        return_value={"host": "http://localhost:8090"},
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
async def test_extract_text_when_response_is_200_with_error_logs_warning(
    mock_responses, patch_logger
):
    filepath = "tmp/notreal.txt"
    url = "http://localhost:8090/extract_text/"

    with patch("builtins.open", mock_open(read_data=b"data")), patch(
        "connectors.content_extraction.ContentExtraction.get_extraction_config",
        return_value={"host": "http://localhost:8090"},
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
