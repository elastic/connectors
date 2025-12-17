# Active Directory Connector

Elastic connector for syncing data from Microsoft Active Directory to Elasticsearch with USN-based incremental sync.

## Features

- ✅ **Full Sync**: Sync all AD objects (users, groups, computers, OUs)
- ✅ **Incremental Sync**: USN-based change tracking for efficient updates
- ✅ **Configurable Object Types**: Choose which object types to sync
- ✅ **LDAPS Support**: Secure LDAP over SSL/TLS
- ✅ **NTLM Authentication**: Support for Windows authentication
- ✅ **Paged Queries**: Efficient handling of large AD environments

## How USN-Based Sync Works

### Update Sequence Number (USN)
Active Directory assigns a unique Update Sequence Number (USN) to every change made to any object. This allows for efficient incremental synchronization:

1. **First Sync (Full)**:
   - Connector fetches all objects
   - Records the highest `highestCommittedUSN` from AD

2. **Subsequent Syncs (Incremental)**:
   - Connector queries: `(uSNChanged >= last_usn)`
   - Only changed/new objects are returned
   - Much faster than full sync

3. **Benefits**:
   - Minimal network traffic
   - Fast sync times
   - No polling all objects
   - Automatic detection of creates, updates, and changes

## Configuration

### Required Fields

| Field | Description | Example |
|-------|-------------|---------|
| **host** | AD server hostname or IP | `ad.example.com` |
| **port** | LDAP/LDAPS port | `636` (LDAPS) or `389` (LDAP) |
| **username** | AD username | `DOMAIN\user` or `user@domain.com` |
| **password** | Password | `***` |
| **base_dn** | Base Distinguished Name | `DC=example,DC=com` |

### Optional Fields

| Field | Default | Description |
|-------|---------|-------------|
| **use_ssl** | `true` | Use LDAPS |
| **auth_type** | `NTLM` | Authentication type (NTLM/SIMPLE) |
| **sync_users** | `true` | Sync user objects |
| **sync_groups** | `true` | Sync group objects |
| **sync_computers** | `false` | Sync computer objects |
| **sync_ous** | `false` | Sync organizational units |
| **page_size** | `1000` | LDAP page size |

## Synced Attributes

### Users
- Basic: name, description, distinguished name, objectGUID
- Identity: sAMAccountName, userPrincipalName, displayName
- Personal: givenName, surname, email, telephone
- Organizational: title, department, company, manager
- Groups: memberOf
- Tracking: whenCreated, whenChanged, uSNChanged

### Groups
- Basic: name, description, distinguished name
- Identity: sAMAccountName, groupType
- Membership: member, memberOf
- Tracking: whenCreated, whenChanged, uSNChanged

### Computers
- Basic: name, description, distinguished name
- Identity: sAMAccountName, dNSHostName
- System: operatingSystem, operatingSystemVersion
- Activity: lastLogonTimestamp
- Tracking: whenCreated, whenChanged, uSNChanged

## Installation

### 1. Install Dependencies

```bash
cd ~/connectors-ad
pip install ldap3
```

### 2. Register Connector

Add to `config.yml`:

```yaml
sources:
  active_directory: connectors.sources.active_directory:ActiveDirectoryDataSource
```

### 3. Configure in Kibana

1. Navigate to **Search → Connectors**
2. Create new **Active Directory** connector
3. Fill in AD server details
4. Configure which object types to sync
5. Run initial sync

## Development & Testing

### Run Unit Tests

```bash
# Run all tests
pytest app/connectors_service/connectors/sources/active_directory/test_active_directory.py

# Run with coverage
pytest --cov=connectors.sources.active_directory

# Run specific test
pytest app/connectors_service/connectors/sources/active_directory/test_active_directory.py::TestActiveDirectoryClient::test_ping_success
```

### Run Integration Tests

Integration tests require a real AD server:

```bash
# Set environment variables
export AD_HOST=ad.example.com
export AD_PORT=636
export AD_USERNAME='DOMAIN\testuser'
export AD_PASSWORD='password'
export AD_BASE_DN='DC=example,DC=com'

# Run integration tests
pytest -m integration
```

### Manual Testing with Docker

You can use a Docker-based AD server for testing:

```bash
# Start Samba AD Docker container
docker run -d \
  --name samba-ad \
  -e SAMBA_DOMAIN=EXAMPLE \
  -e SAMBA_REALM=EXAMPLE.COM \
  -e SAMBA_ADMIN_PASSWORD=Passw0rd \
  -p 389:389 \
  -p 636:636 \
  nowsci/samba-domain

# Configure connector to use:
# host: localhost
# port: 389
# username: Administrator@EXAMPLE.COM
# password: Passw0rd
# base_dn: DC=EXAMPLE,DC=COM
```

## Architecture

```
┌─────────────────────────────────────────┐
│ ActiveDirectoryDataSource               │
│ - get_docs() → full sync                │
│ - get_docs_incrementally() → USN sync   │
└─────────────┬───────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────┐
│ ActiveDirectoryClient                   │
│ - LDAP connection management            │
│ - Paged search                          │
│ - USN tracking                          │
│ - get_users/groups/computers/ous()      │
└─────────────┬───────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────┐
│ Microsoft Active Directory              │
│ - LDAP/LDAPS protocol                   │
│ - NTLM/Simple auth                      │
│ - USN change tracking                   │
└─────────────────────────────────────────┘
```

## Troubleshooting

### Connection Issues

**Error**: `Failed to connect to AD`

Solutions:
- Verify AD server hostname/IP is correct
- Check firewall allows LDAP (389) or LDAPS (636)
- Test with `telnet ad.example.com 636`
- Verify credentials are correct
- For NTLM: use `DOMAIN\user` format

### SSL/TLS Issues

**Error**: `SSL certificate verification failed`

Solutions:
- Set `use_ssl: false` for testing (not recommended)
- Ensure AD server certificate is valid
- Check clock sync between connector and AD

### Performance Issues

**Slow sync times**

Solutions:
- Reduce `page_size` (default 1000)
- Enable only needed object types
- Use incremental sync after initial full sync
- Check network latency to AD server

### Missing Objects

**Objects not appearing in Elasticsearch**

Solutions:
- Verify `base_dn` includes the objects
- Check object type sync is enabled
- Review connector logs for errors
- Verify user has read permissions in AD

## Production Recommendations

1. **Use LDAPS**: Always use SSL/TLS in production (`use_ssl: true`)
2. **Dedicated Service Account**: Create AD service account with read-only permissions
3. **Incremental Sync**: Use USN-based incremental sync for efficiency
4. **Monitor USN**: Track USN growth to detect AD issues
5. **Page Size**: Tune page_size based on network/AD performance
6. **Object Filtering**: Only sync needed object types

## Security Considerations

- ✅ Passwords are stored encrypted in Kibana
- ✅ LDAPS encrypts data in transit
- ✅ Connector only needs READ permissions in AD
- ✅ No write operations to AD
- ⚠️ Service account credentials should follow least-privilege principle

## Contributing

See main [DEVELOPING.md](../../../../docs/DEVELOPING.md) for connector development guidelines.

## License

Elastic License 2.0
