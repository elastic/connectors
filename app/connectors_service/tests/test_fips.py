#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests for FIPS compliance utilities."""

import os
from unittest.mock import patch

import pytest

from connectors.fips import (
    FIPSConfig,
    FIPSModeError,
    enable_fips_mode,
    filter_fips_compliant_sources,
    get_non_fips_connectors,
    is_connector_fips_compliant,
    is_openssl_fips_mode,
    validate_fips_mode,
)


@pytest.fixture(autouse=True)
def reset_fips_config():
    """Reset FIPS config state before and after each test."""
    FIPSConfig.reset()
    yield
    FIPSConfig.reset()


class TestFIPSConfig:
    """Tests for FIPSConfig class."""

    def test_fips_mode_disabled_by_default(self):
        """FIPS mode should be disabled by default."""
        assert FIPSConfig.is_fips_mode_enabled() is False

    def test_set_fips_mode_enabled(self):
        """Setting FIPS mode to True should enable it."""
        FIPSConfig.set_fips_mode(True)
        assert FIPSConfig.is_fips_mode_enabled() is True

    def test_set_fips_mode_disabled(self):
        """Setting FIPS mode to False should disable it."""
        FIPSConfig.set_fips_mode(True)
        FIPSConfig.set_fips_mode(False)
        assert FIPSConfig.is_fips_mode_enabled() is False

    def test_fips_mode_from_environment_true(self):
        """FIPS mode can be enabled via environment variable."""
        FIPSConfig.reset()
        with patch.dict(os.environ, {"ELASTICSEARCH_CONNECTORS_FIPS_MODE": "true"}):
            FIPSConfig.reset()
            assert FIPSConfig.is_fips_mode_enabled() is True

    def test_fips_mode_from_environment_only_true_accepted(self):
        for value in ["true", "TRUE", "True", "tRuE"]:
            FIPSConfig.reset()
            with patch.dict(os.environ, {"ELASTICSEARCH_CONNECTORS_FIPS_MODE": value}):
                FIPSConfig.reset()
                assert FIPSConfig.is_fips_mode_enabled() is True, (
                    f"'{value}' should enable FIPS mode"
                )

    def test_singleton_behavior(self):
        """FIPSConfig should be a singleton."""
        config1 = FIPSConfig()
        config2 = FIPSConfig()
        assert config1 is config2


class TestIsOpenSSLFIPSMode:
    """Tests for is_openssl_fips_mode function."""

    def test_returns_boolean(self):
        """is_openssl_fips_mode should return a boolean."""
        result = is_openssl_fips_mode()
        assert isinstance(result, bool)

    def test_non_fips_environment(self):
        """In most test environments, OpenSSL is not in FIPS mode."""
        # This test documents expected behavior in standard environments
        # It may need adjustment if run in a FIPS-enabled environment
        result = is_openssl_fips_mode()
        # We don't assert the value since it depends on the environment
        assert isinstance(result, bool)


class TestValidateFIPSMode:
    """Tests for validate_fips_mode function."""

    def test_validate_fips_mode_disabled(self):
        """Validation should pass when FIPS mode is disabled."""
        FIPSConfig.set_fips_mode(False)
        # Should not raise any exception
        validate_fips_mode()

    def test_validate_fips_mode_enabled_without_fips_openssl(self):
        """Validation should fail when FIPS mode is enabled but OpenSSL is not FIPS."""
        FIPSConfig.set_fips_mode(True)
        with patch("connectors.fips.is_openssl_fips_mode", return_value=False):
            with pytest.raises(FIPSModeError) as exc_info:
                validate_fips_mode()
            assert "OpenSSL is not in FIPS mode" in str(exc_info.value)

    def test_validate_fips_mode_enabled_with_fips_openssl(self):
        """Validation should pass when FIPS mode is enabled and OpenSSL is FIPS."""
        FIPSConfig.set_fips_mode(True)
        with patch("connectors.fips.is_openssl_fips_mode", return_value=True):
            # Should not raise any exception
            validate_fips_mode()


class TestEnableFIPSMode:
    """Tests for enable_fips_mode function."""

    def test_enable_fips_mode_disabled(self):
        """enable_fips_mode should do nothing when FIPS mode is disabled."""
        FIPSConfig.set_fips_mode(False)
        # Should not raise any exception
        enable_fips_mode()

    def test_enable_fips_mode_enabled_without_fips_openssl(self):
        """enable_fips_mode should fail when FIPS is enabled but OpenSSL is not FIPS."""
        FIPSConfig.set_fips_mode(True)
        with patch("connectors.fips.is_openssl_fips_mode", return_value=False):
            with pytest.raises(FIPSModeError) as exc_info:
                enable_fips_mode()
            assert "OpenSSL is not in FIPS mode" in str(exc_info.value)

    def test_enable_fips_mode_enabled_with_fips_openssl(self):
        """enable_fips_mode should succeed when FIPS is enabled and OpenSSL is FIPS."""
        FIPSConfig.set_fips_mode(True)
        with patch("connectors.fips.is_openssl_fips_mode", return_value=True):
            # Should not raise any exception
            enable_fips_mode()


class TestConnectorCompliance:
    """Tests for connector FIPS compliance functions."""

    def test_non_fips_connectors_list(self):
        """Verify the list of non-FIPS-compliant connectors."""
        non_fips = get_non_fips_connectors()
        assert "network_drive" in non_fips
        assert "sharepoint_server" in non_fips
        # FIPS-compliant connectors should not be in the list
        assert "sharepoint_online" not in non_fips
        assert "github" not in non_fips

    def test_is_connector_fips_compliant_network_drive(self):
        """network_drive connector should not be FIPS-compliant."""
        assert is_connector_fips_compliant("network_drive") is False

    def test_is_connector_fips_compliant_sharepoint_server(self):
        """sharepoint_server connector should not be FIPS-compliant."""
        assert is_connector_fips_compliant("sharepoint_server") is False

    def test_is_connector_fips_compliant_sharepoint_online(self):
        """sharepoint_online connector should be FIPS-compliant."""
        assert is_connector_fips_compliant("sharepoint_online") is True

    def test_is_connector_fips_compliant_github(self):
        """github connector should be FIPS-compliant."""
        assert is_connector_fips_compliant("github") is True

    def test_is_connector_fips_compliant_all_compliant_connectors(self):
        """All connectors not in NON_FIPS_COMPLIANT_CONNECTORS should be compliant."""
        compliant_connectors = [
            "azure_blob_storage",
            "box",
            "confluence",
            "dir",
            "dropbox",
            "github",
            "gitlab",
            "gmail",
            "google_cloud_storage",
            "google_drive",
            "graphql",
            "jira",
            "microsoft_teams",
            "mongodb",
            "mssql",
            "mysql",
            "notion",
            "onedrive",
            "oracle",
            "outlook",
            "postgresql",
            "redis",
            "s3",
            "salesforce",
            "sandfly",
            "servicenow",
            "sharepoint_online",
            "slack",
            "zoom",
        ]
        for connector in compliant_connectors:
            assert is_connector_fips_compliant(connector) is True, (
                f"{connector} should be FIPS-compliant"
            )


class TestFilterFIPSCompliantSources:
    """Tests for filter_fips_compliant_sources function."""

    def test_filter_when_fips_disabled(self):
        """When FIPS is disabled, all sources should be returned."""
        FIPSConfig.set_fips_mode(False)
        sources = {
            "github": "connectors.sources.github:GitHubDataSource",
            "network_drive": "connectors.sources.network_drive:NASDataSource",
            "sharepoint_server": "connectors.sources.sharepoint.sharepoint_server:SharepointServerDataSource",
        }
        filtered = filter_fips_compliant_sources(sources)
        assert filtered == sources

    def test_filter_when_fips_enabled(self):
        """When FIPS is enabled, non-compliant sources should be filtered out."""
        FIPSConfig.set_fips_mode(True)
        sources = {
            "github": "connectors.sources.github:GitHubDataSource",
            "network_drive": "connectors.sources.network_drive:NASDataSource",
            "sharepoint_server": "connectors.sources.sharepoint.sharepoint_server:SharepointServerDataSource",
            "sharepoint_online": "connectors.sources.sharepoint.sharepoint_online:SharepointOnlineDataSource",
        }
        filtered = filter_fips_compliant_sources(sources)
        assert "github" in filtered
        assert "sharepoint_online" in filtered
        assert "network_drive" not in filtered
        assert "sharepoint_server" not in filtered

    def test_filter_empty_sources(self):
        """Empty sources dictionary should return empty."""
        FIPSConfig.set_fips_mode(True)
        filtered = filter_fips_compliant_sources({})
        assert filtered == {}


class TestFIPSIntegration:
    """Integration tests that run against the real environment (no mocking).
    These tests verify actual FIPS behavior based on the environment:
    - In a non-FIPS container: FIPS mode should be detected as False
    - In a FIPS container: FIPS mode should be detected as True
    Run with: make fips-test (FIPS container) or make test (non-FIPS)
    """

    def test_fips_detection_matches_environment(self):
        """FIPS detection should match the actual environment."""
        is_fips = is_openssl_fips_mode()
        # Try to set a non-FIPS cipher to verify detection is accurate
        import ssl

        try:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.set_ciphers("RC4-SHA")
            # If we get here, RC4 is allowed, so NOT FIPS
            assert is_fips is False, "FIPS detected as True but RC4 cipher is allowed"
        except ssl.SSLError:
            # RC4 rejected, so FIPS is active
            assert is_fips is True, "FIPS detected as False but RC4 cipher is rejected"

    def test_enable_fips_mode_in_non_fips_environment(self):
        """In a non-FIPS environment, enable_fips_mode should raise FIPSModeError.

        This test only runs meaningfully in a non-FIPS container.
        In a FIPS container, it will pass (no error raised).
        """
        FIPSConfig.set_fips_mode(True)

        if not is_openssl_fips_mode():
            # Non-FIPS environment: should raise error
            with pytest.raises(FIPSModeError) as exc_info:
                enable_fips_mode()
            assert "OpenSSL is not in FIPS mode" in str(exc_info.value)
        else:
            # FIPS environment: should succeed
            enable_fips_mode()  # Should not raise

    def test_enable_fips_mode_in_fips_environment(self):
        """In a FIPS environment, enable_fips_mode should succeed.

        This test only runs meaningfully in a FIPS container.
        In a non-FIPS container, it will be skipped.
        """
        if not is_openssl_fips_mode():
            pytest.skip("Not running in a FIPS environment")

        FIPSConfig.set_fips_mode(True)
        # Should not raise any exception
        enable_fips_mode()

    def test_fips_mode_disabled_always_succeeds(self):
        """When FIPS mode is disabled, enable_fips_mode should always succeed."""
        FIPSConfig.set_fips_mode(False)
        # Should not raise any exception regardless of environment
        enable_fips_mode()
