#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from connectors.fips import FIPSConfig, FIPSModeError

AUTH_METHOD_NTLM = "ntlm"
AUTH_METHOD_KERBEROS = "kerberos"
AUTH_METHODS = frozenset({AUTH_METHOD_NTLM, AUTH_METHOD_KERBEROS})

_FIPS_NTLM_MESSAGE = (
    "Exchange Server authentication method 'ntlm' is not FIPS-compliant. "
    "Set auth_method to 'kerberos' and provide a valid Kerberos ticket "
    "(for example via KRB5CCNAME) for the connector service account."
)


def get_auth_method(configuration) -> str:
    auth_method = configuration.get("auth_method", AUTH_METHOD_NTLM)
    if not auth_method:
        return AUTH_METHOD_NTLM
    return auth_method


def ensure_fips_compatible_auth(auth_method: str) -> None:
    if FIPSConfig.is_fips_mode_enabled() and auth_method == AUTH_METHOD_NTLM:
        raise FIPSModeError(_FIPS_NTLM_MESSAGE)


def kerberos_dependencies_missing() -> list[str]:
    missing = []
    try:
        import requests_gssapi  # noqa: F401
    except ImportError:
        missing.append("requests-gssapi")
    try:
        import gssapi  # noqa: F401
    except ImportError:
        missing.append("gssapi")
    return missing


def ensure_kerberos_dependencies_available() -> None:
    missing = kerberos_dependencies_missing()
    if not missing:
        return
    msg = (
        "Kerberos authentication requires optional Python packages "
        f"({', '.join(missing)}) and Kerberos libraries on the host. "
        "Install with: pip install 'elasticsearch-connectors[kerberos]' "
        "(the build may require krb5 development packages, for example "
        "libkrb5-dev on Debian or krb5-dev on Alpine)."
    )
    raise ValueError(msg)
