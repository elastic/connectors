#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""FIPS 140-2/140-3 compliance utilities.

This module provides utilities for running the connectors service in FIPS-compliant mode.
When FIPS mode is enabled, the application will:
1. Validate that the system's OpenSSL is in FIPS mode
2. Disable connectors that cannot be FIPS-compliant (e.g., those using NTLM)
3. Ensure all TLS connections use FIPS-approved cipher suites

Note: The hash_id function uses MD5 for document ID generation, which is not a security
function and does not require FIPS-approved algorithms
"""

import os
import ssl

from connectors_sdk.logger import logger

# Connectors that use NTLM or other non-FIPS-compliant algorithms
NON_FIPS_COMPLIANT_CONNECTORS = frozenset(
    {
        "network_drive",
        "sharepoint_server",
    }
)


class FIPSModeError(Exception):
    """Raised when FIPS mode requirements are not met."""

    pass


class FIPSConfig:
    """FIPS configuration and validation."""

    _instance = None
    _fips_mode = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def is_fips_mode_enabled(cls) -> bool:
        """Check if FIPS mode is enabled via configuration or environment."""
        if cls._fips_mode is None:
            env_fips = os.environ.get("ELASTICSEARCH_CONNECTORS_FIPS_MODE", "").lower()
            cls._fips_mode = env_fips == "true"
        return cls._fips_mode

    @classmethod
    def set_fips_mode(cls, enabled: bool):
        """Set FIPS mode programmatically (typically from config)."""
        cls._fips_mode = enabled

    @classmethod
    def reset(cls):
        """Reset FIPS mode state (for testing)."""
        cls._fips_mode = None


def is_openssl_fips_mode() -> bool:
    """Check if OpenSSL is running in FIPS mode.

    Returns:
        bool: True if OpenSSL is in FIPS mode, False otherwise.
    """
    # Check via SSL context - try to set a non-FIPS cipher
    # In FIPS mode, RC4 and other non-approved ciphers are rejected

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    try:
        ctx.set_ciphers("RC4-SHA")
    except ssl.SSLError:
        # RC4 rejected - FIPS mode is active
        return True

    return False


def is_connector_fips_compliant(connector_type: str) -> bool:
    """Check if a connector type is FIPS-compliant.

    Args:
        connector_type: The connector type identifier (e.g., 'sharepoint_online').

    Returns:
        bool: True if the connector is FIPS-compliant, False otherwise.
    """
    return connector_type not in NON_FIPS_COMPLIANT_CONNECTORS


def filter_fips_compliant_sources(sources: dict) -> dict[str, str]:
    """Filter sources dictionary to only include FIPS-compliant connectors.

    Args:
        sources: Dictionary mapping connector types to their module paths.

    Returns:
        dict: Filtered dictionary with only FIPS-compliant connectors.
    """
    if not FIPSConfig.is_fips_mode_enabled():
        return sources

    filtered = {}
    for connector_type, module_path in sources.items():
        if is_connector_fips_compliant(connector_type):
            filtered[connector_type] = module_path
        else:
            logger.warning(
                f"Connector '{connector_type}' is not FIPS-compliant and has been "
                f"disabled in FIPS mode."
            )

    return filtered


def validate_fips_mode():
    """Validate FIPS mode for the connectors service.

    This function should be called early in application startup when FIPS mode
    is enabled. It will:
    1. Validate that the system's OpenSSL is in FIPS mode
    2. Log confirmation of FIPS mode initialization

    Raises:
        FIPSModeError: If FIPS mode is enabled but system is not FIPS-compliant.
    """
    if not FIPSConfig.is_fips_mode_enabled():
        return

    logger.info("FIPS mode is enabled, validating system configuration...")

    # Validate OpenSSL is in FIPS mode
    if not is_openssl_fips_mode():
        msg = (
            "FIPS mode is enabled but OpenSSL is not in FIPS mode. "
            "Please ensure your system's OpenSSL is configured for FIPS compliance. "
            "Set OPENSSL_CONF to point to a FIPS-enabled OpenSSL configuration."
        )
        raise FIPSModeError(msg)

    logger.info(f"FIPS mode initialized. OpenSSL version: {ssl.OPENSSL_VERSION}")
