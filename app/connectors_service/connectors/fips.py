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

# Used to turn FIPS mode on when there is no config file, e.g. when running on Agent
FIPS_MODE_ENV_VAR = "ELASTICSEARCH_CONNECTORS_FIPS_MODE"

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


def fips_mode_from_env() -> bool:
    """Read the FIPS mode setting from the environment.

    The only accepted values are 'true' and 'false', case-insensitive. An unset
    variable means FIPS mode is off.

    Returns:
        bool: True if the environment asks for FIPS mode, False otherwise.

    Raises:
        FIPSModeError: If the variable is set to anything else.
    """
    raw_value = os.environ.get(FIPS_MODE_ENV_VAR, "")
    value = raw_value.strip().lower()

    if value == "true":
        return True

    # Unset means off, which is the default everywhere else too
    if value in ("", "false"):
        return False

    # A typo must not silently turn FIPS mode off on an image built for FIPS
    msg = (
        f"{FIPS_MODE_ENV_VAR} is set to '{raw_value}', which is not a valid value. "
        "Use 'true' to turn FIPS mode on, or 'false' to turn it off."
    )
    raise FIPSModeError(msg)


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
            cls._fips_mode = fips_mode_from_env()
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


def apply_fips_mode(config: dict) -> dict:
    """Turn FIPS mode on or off for this process, as the configuration asks.

    This is the single place that puts FIPS mode into effect. Both entry points use
    it: the standalone service (`connectors.service_cli`) and the service running
    under Elastic Agent (`connectors.agent.service_manager`).

    It does three things:
    1. Store the requested mode, so the rest of the process can read it
    2. Validate that the system's OpenSSL is in FIPS mode
    3. Remove the connectors that cannot run under FIPS

    Args:
        config: The service configuration.

    Returns:
        dict: The configuration to run with. The input is left untouched. When FIPS
            mode is on, the returned copy has the non-FIPS connectors removed.

    Raises:
        FIPSModeError: If FIPS mode is on but the system is not FIPS ready.
    """
    fips_enabled = config.get("service", {}).get("fips_mode", False)
    FIPSConfig.set_fips_mode(fips_enabled)

    # Always say which mode we are in. Silence here reads as "FIPS is on" to an
    # operator who set the environment variable but got the value wrong.
    logger.info(f"FIPS mode is {'enabled' if fips_enabled else 'disabled'}")

    validate_fips_mode()

    if not fips_enabled:
        return config

    return config | {
        "sources": filter_fips_compliant_sources(config.get("sources", {}))
    }
