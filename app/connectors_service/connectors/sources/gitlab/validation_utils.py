#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Utilities for resilient API response validation with graceful degradation.

Provides validate_with_fallback() to attempt Pydantic validation on API responses.
On validation failure, returns raw dict and logs warning, allowing the connector
to continue syncing even when the GitLab API schema changes unexpectedly.
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
