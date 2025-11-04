#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Utilities for resilient API response validation with graceful degradation.

API Resilience Pattern
======================

This module defines the core pattern used throughout the GitLab connector to handle
third-party API changes gracefully without breaking the connector.

Pattern Overview
----------------

1. **Client Layer** (gitlab/client.py):
   - All API methods use validate_with_fallback() to attempt Pydantic validation
   - Return type: Union of Pydantic model or dict (e.g., GitLabProject | dict[str, Any])
   - On validation success: Returns validated Pydantic model
   - On validation failure: Logs warning, returns raw dict from API

2. **Data Source Layer** (gitlab/datasource.py):
   - All formatters check isinstance(data, dict) to detect fallback case
   - When dict (validation failed):
     * Transparent passthrough: doc = dict(data)
     * Add only required Elasticsearch metadata (_id, _timestamp, type)
     * Preserve all API fields (even unexpected/new ones)
   - When Pydantic model (validation succeeded):
     * Explicit field mapping with full type safety
     * Controlled document structure

3. **Reasoning**:
   - Connector continues working when GitLab API changes
   - New API fields automatically passed through to Elasticsearch
   - Warnings logged for validation failures to alert operators
   - Elasticsearch dynamic mapping handles unexpected fields


Types Using This Pattern:
--------------------------
- GitLabProject | dict → _format_project_doc()
- GitLabMergeRequest | dict → _format_merge_request_doc()
- GitLabWorkItem | dict → _format_work_item_doc()
- GitLabRelease | dict → _format_release_doc()
"""

from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

T = TypeVar("T", bound=BaseModel)

# Maximum number of validation errors to display in warning messages
MAX_VALIDATION_ERRORS_TO_DISPLAY = 3


def safe_get_nested(data: dict | Any, *keys: str, default: Any = None) -> Any:
    """Safely extract a nested field following a path of keys."""
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return default

        if current is None:
            return default

    return current if current is not None else default


def validate_with_fallback(
    model_class: type[T],
    data: dict[str, Any],
    logger,
    context: str = "",
) -> T | dict[str, Any]:
    """Validate data with Pydantic model, falling back to raw dict on failure."""
    try:
        return model_class.model_validate(data)
    except ValidationError as e:
        error_details = []
        for error in e.errors():
            field_path = " -> ".join(str(loc) for loc in error["loc"])
            error_details.append(f"{field_path}: {error['msg']}")

        extra_errors = (
            f" (and {len(error_details) - MAX_VALIDATION_ERRORS_TO_DISPLAY} more)"
            if len(error_details) > MAX_VALIDATION_ERRORS_TO_DISPLAY
            else ""
        )
        logger.warning(
            f"GitLab API response format has changed unexpectedly for {model_class.__name__}"
            + (f" ({context})" if context else "")
            + f": {'; '.join(error_details[:MAX_VALIDATION_ERRORS_TO_DISPLAY])}"
            + extra_errors
            + ". Continuing with raw data passthrough."
        )
        return data
    except Exception as e:
        logger.error(
            f"Unexpected error validating GitLab API response for {model_class.__name__}"
            + (f" ({context})" if context else "")
            + f": {e}. Continuing with raw data passthrough."
        )
        return data
