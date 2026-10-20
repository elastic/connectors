# FIPS 140-2/140-3 Compliance Guide

## Enabling FIPS Mode

Add the following to your `config.yml` to enable FIPS-aware mode in the application:

```yaml
service:
  fips_mode: true
```

When enabled, this will:
1. Validate that the system's OpenSSL is in FIPS mode
2. Log warnings for connectors that cannot be FIPS-compliant

## How Library FIPS Compliance Works

Most Python libraries used by this application (pyOpenSSL, cryptography, aiohttp, httpx, requests, elasticsearch, etc.) achieve FIPS compliance by **delegating cryptographic operations to the system's OpenSSL**.

This means:
- **No code changes are required** for these libraries
- FIPS compliance is inherited from the **base container/system image**
- When the system OpenSSL is in FIPS mode, these libraries automatically use FIPS-validated cryptographic operations

The only application-level concerns are:
1. Direct use of non-FIPS algorithms in application code (e.g., MD5)
2. Protocols that fundamentally require non-FIPS algorithms (e.g., NTLM)

## Known FIPS Compliance Issues

### NTLM Authentication (NOT FIPS-COMPLIANT - PROTOCOL LIMITATION)

**Affected Connectors:**
- **SharePoint Server** (`connectors/sources/sharepoint/sharepoint_server/`)
- **Network Drive** (`connectors/sources/network_drive/`)

**Exchange Server** (`connectors/sources/exchange_server/`) and the legacy **Outlook** connector in `outlook_server` mode use the same EWS and LDAP code path. They support Kerberos (GSSAPI) via the `auth_method` configuration field. NTLM remains the default outside FIPS mode; when FIPS mode is enabled, `auth_method: ntlm` is rejected and you must use `auth_method: kerberos` with a valid Kerberos ticket for the connector service account (for example via `KRB5CCNAME`).

#### Why NTLM Cannot Be FIPS-Compliant

NTLM (NT LAN Manager) is a Microsoft authentication protocol that **fundamentally relies on non-FIPS algorithms** at the protocol level. These algorithms are part of the NTLM specification itself and cannot be replaced:

| Algorithm | Usage in NTLM | FIPS Status |
|-----------|---------------|-------------|
| **MD4** | Password hashing (NTLM hash) | **Not Approved** |
| **MD5** | Challenge-response calculation | **Not Approved** |
| **DES** | LM hash (legacy) | **Deprecated/Not Approved** |
| **RC4** | Session encryption (NTLMv1) | **Not Approved** |

This is not a library implementation issue - **the NTLM protocol specification requires these algorithms**. There is no FIPS-compliant way to implement NTLM.

#### SharePoint Server

```python
# From connectors/sources/sharepoint/sharepoint_server/client.py
else HttpNtlmAuth(
    username=self.configuration["username"],
    password=self.configuration["password"],
)
```

The SharePoint Server connector uses `httpx-ntlm` for NTLM authentication when connecting to on-premises SharePoint servers.

#### Network Drive

```python
# From connectors/sources/network_drive/netdrive.py
return winrm.Session(
    self.server_ip,
    auth=(self.username, self.password),
    transport="ntlm",
    server_cert_validation="ignore",
)
```

The Network Drive connector uses:
- `pywinrm` with NTLM transport for WinRM connections
- `smbprotocol` which uses NTLM for SMB authentication

#### Exchange Server

With `auth_method: ntlm` (default), the connector uses username/password credentials. `exchangelib` negotiates NTLM for EWS and `ldap3` uses simple bind for Active Directory user lookup.

With `auth_method: kerberos`, EWS uses `exchangelib` GSSAPI authentication (`requests-gssapi`) and LDAP uses SASL GSSAPI. Install the optional Kerberos dependencies on the connector host: `pip install 'elasticsearch-connectors[kerberos]'` (building `gssapi` may require krb5 development packages, for example `libkrb5-dev`). The connector does not manage Kerberos tickets; the host must provide a valid ticket cache for the service account.

The legacy Outlook connector in `outlook_server` mode uses the same code path. Its `outlook_cloud` mode uses Microsoft OAuth.

#### Dependencies Using NTLM

| Dependency | Usage | FIPS Status |
|------------|-------|-------------|
| `httpx-ntlm==1.4.0` | SharePoint Server auth | **Not Compliant** |
| `pywinrm==0.4.3` | Network Drive WinRM | **Not Compliant** |
| `smbprotocol==1.10.1` | Network Drive SMB | **Not Compliant** (when using NTLM auth) |
| `exchangelib==5.4.0` (`requests_ntlm`) | Exchange Server EWS auth | **Not Compliant** (when using NTLM auth) |
| `requests-gssapi` (optional `[kerberos]` extra) | Exchange Server EWS/LDAP Kerberos | **Compliant** (when using Kerberos auth) |

#### Resolution

**SharePoint Server and Network Drive cannot be made FIPS-compliant while using NTLM authentication.** When FIPS mode is enabled, they are automatically disabled.

**Exchange Server** remains available in FIPS mode when configured with `auth_method: kerberos`. NTLM is rejected at configuration validation and at connection time when FIPS mode is enabled.

## Connector FIPS Compatibility Matrix

| Connector | FIPS Compatible | Notes |
|-----------|-----------------|-------|
| Azure Blob Storage | ✅ Yes | Uses Azure OAuth |
| Box | ✅ Yes | Uses OAuth 2.0 |
| Confluence | ✅ Yes | Uses API tokens over TLS |
| Directory | ✅ Yes | Local filesystem |
| Dropbox | ✅ Yes | Uses OAuth 2.0 |
| Exchange Server | ✅ Yes | Kerberos (GSSAPI); NTLM disabled in FIPS mode |
| GitHub | ✅ Yes | Uses OAuth/PAT over TLS |
| GitLab | ✅ Yes | Uses PAT over TLS |
| Gmail | ✅ Yes | Uses Google OAuth |
| Google Cloud Storage | ✅ Yes | Uses Google OAuth |
| Google Drive | ✅ Yes | Uses Google OAuth |
| GraphQL | ✅ Yes | Uses TLS |
| Jira | ✅ Yes | Uses API tokens over TLS |
| Microsoft Teams | ✅ Yes | Uses Microsoft OAuth |
| MongoDB | ✅ Yes | Uses TLS |
| MS SQL | ✅ Yes | Uses TLS |
| MySQL | ✅ Yes | Uses TLS |
| Network Drive | ❌ No | NTLM authentication |
| Notion | ✅ Yes | Uses API tokens |
| OneDrive | ✅ Yes | Uses Microsoft OAuth |
| Oracle | ✅ Yes | Uses TLS |
| Outlook | ✅ Yes | OAuth for `outlook_cloud`; Kerberos for `outlook_server` in FIPS mode |
| PostgreSQL | ✅ Yes | Uses TLS |
| Redis | ✅ Yes | Uses TLS |
| S3 | ✅ Yes | Uses AWS SigV4 (HMAC-SHA256) |
| Salesforce | ✅ Yes | Uses OAuth 2.0 |
| Sandfly | ✅ Yes | Uses TLS |
| ServiceNow | ✅ Yes | Uses OAuth/Basic over TLS |
| SharePoint Online | ✅ Yes | Uses Microsoft OAuth |
| SharePoint Server | ❌ No | NTLM authentication |
| Slack | ✅ Yes | Uses OAuth 2.0 |
| Zoom | ✅ Yes | Uses OAuth 2.0 |

## References

- [NIST FIPS 140-2](https://csrc.nist.gov/publications/detail/fips/140/2/final)
- [NIST FIPS 140-3](https://csrc.nist.gov/publications/detail/fips/140/3/final)
- [Python hashlib documentation](https://docs.python.org/3/library/hashlib.html)
- [OpenSSL FIPS Module](https://www.openssl.org/docs/fips.html)
