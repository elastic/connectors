# Connector Reference (Home)
ℹ️ Find documentation for the following connector clients in the Elastic Enterprise Search docs.


This table provides an overview of our available connectors, their current support status, and the features they support.

The columns provide specific information about each connector:

* **Status**: Indicates whether the connector is in General Availability (GA), Technical Preview, Beta, or is an Example connector.
* **Native (Elastic Cloud)**: Specifies the versions of Elastic Cloud in which the connector is natively supported, if applicable. **All connectors are available as self-managed connector clients**.
* **Advanced sync rules**: Specifies the versions in which advanced sync rules are supported, if applicable.
* **Local binary extraction service**: Specifies the versions in which the local binary extraction service is supported, if applicable.
* **Incremental syncs**: Specifies the version in which incremental syncs began to be supported, if applicable.
* **Document level security**: Specifies the version in which document level security is supported, if applicable.
* **Code**: Provides a link to the connector's source code in this repository.

| Connector | Status | Native (Elastic Cloud) | [Advanced sync rules](sync-rules.html#sync-rules-advanced) | [Local binary extraction service](https://www.elastic.co/guide/en/enterprise-search/current/connectors-content-extraction.html#connectors-content-extraction-local) | [Incremental syncs](https://www.elastic.co/guide/en/enterprise-search/current/connectors-sync-types.html#connectors-sync-types-incremental) | [Document level security](https://www.elastic.co/guide/en/enterprise-search/current/dls.html) | Source code |
| --- | --- | --- | --- | --- | --- | --- | --- |
| [Azure Blob](https://www.elastic.co/guide/en/enterprise-search/current/connectors-azure-blob.html) | **GA** | 8.9+ | - | 8.11+ | 8.13+ | - | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/azure_blob_storage.py) |
| [Box](https://www.elastic.co/guide/en/enterprise-search/current/connectors-box.html) | **Preview** | 8.14+ | - | - | 8.13+ | - | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/box.py) |
| [Confluence Cloud](https://www.elastic.co/guide/en/enterprise-search/current/connectors-confluence.html) | **GA** | 8.9+ | 8.9+ | 8.11+ | 8.13+ | 8.10 | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/confluence.py) |
| [Confluence Data Center](https://www.elastic.co/guide/en/enterprise-search/current/connectors-confluence.html) | **Preview** | 8.13+ | 8.13+ | 8.13+ | 8.13+ | 8.14+ | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/confluence.py) |
| [Confluence Server](https://www.elastic.co/guide/en/enterprise-search/current/connectors-confluence.html) | **GA** | 8.9+ | 8.9+ | 8.11+ | 8.13+ | - | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/confluence.py) |
| [Dropbox](https://www.elastic.co/guide/en/enterprise-search/current/connectors-dropbox.html) | **GA** | 8.10+ | - | 8.11+ | 8.13+ | 8.12+ | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/dropbox.py) |
| [GitHub Enterprise Cloud/Server](https://www.elastic.co/guide/en/enterprise-search/current/connectors-github.html) | **GA** | 8.11+ | 8.10+ | 8.11+ | 8.13+ | 8.12+ | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/github.py) |
| [Gmail](https://www.elastic.co/guide/en/enterprise-search/current/connectors-gmail.html) | **GA** | 8.13+ | - | - | 8.13+ | 8.10+ | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/gmail.py) |
| [Google Cloud Storage](https://www.elastic.co/guide/en/enterprise-search/current/connectors-google-cloud.html) | **GA** | 8.12+ | - | 8.11+ | 8.13+ | - | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/google_cloud_storage.py) |
| [Google Drive](https://www.elastic.co/guide/en/enterprise-search/current/connectors-google-drive.html) | **GA** | 8.11+ | - | 8.11+ | 8.13+ | 8.10+ | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/google_drive.py) |
| [GraphQL](https://www.elastic.co/guide/en/enterprise-search/current/connectors-graphql.html) | **Preview** | - | - | - | - | - | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/graphql.py) |
| [Jira Cloud](https://www.elastic.co/guide/en/enterprise-search/current/connectors-jira.html) | **GA** | 8.9+ | 8.9+ | 8.11+ | 8.13+ | 8.10+ | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/jira.py) |
| [Jira Data Center](https://www.elastic.co/guide/en/enterprise-search/current/connectors-jira.html) | **Preview** | 8.13+ | 8.13+ | 8.13+ | 8.13+ | 8.13+ | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/jira.py) |
| [Jira Server](https://www.elastic.co/guide/en/enterprise-search/current/connectors-jira.html) | **GA** | 8.9+ | 8.9+ | 8.11+ | 8.13+ | - | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/jira.py) |
| [MongoDB](https://www.elastic.co/guide/en/enterprise-search/current/connectors-mongodb.html) | **GA** | 8.8 | 8.8 native/8.12 self-managed | - | - | - | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/mongo.py) |
| [Microsoft SQL Server](https://www.elastic.co/guide/en/enterprise-search/current/connectors-ms-sql.html) | **GA** | 8.8+ | 8.11+ | - | - | - | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/mssql.py) |
| [MySQL](https://www.elastic.co/guide/en/enterprise-search/current/connectors-mysql.html) | **GA** | 8.5+ | 8.8+ | - | - | - | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/mysql.py) |
| [Network drive](https://www.elastic.co/guide/en/enterprise-search/current/connectors-network-drive.html) | **GA** | 8.9+ | 8.10+ | 8.14+ | 8.13+ | 8.11+ | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/network_drive.py) |
| [Notion](https://www.elastic.co/guide/en/enterprise-search/current/connectors-notion.html) | **Beta** | 8.14+ | 8.14+ | - | - | - | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/notion.py) |
| [OneDrive](https://www.elastic.co/guide/en/enterprise-search/current/connectors-onedrive.html) | **GA** | 8.11+ | 8.11+ | 8.11+ | 8.13+ | 8.11+ | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/onedrive.py) |
| [Opentext Documentum](https://www.elastic.co/guide/en/enterprise-search/current/connectors-opentext.html) | **Example** | n/a | n/a | n/a | n/a | - | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/opentext_documentum.py) |
| [Oracle](https://www.elastic.co/guide/en/enterprise-search/current/connectors-oracle.html) | **GA** | 8.12+ | - | - | - | - | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/oracle.py) |
| [Outlook](https://www.elastic.co/guide/en/enterprise-search/current/connectors-outlook.html) | **GA** | 8.13+ | - | 8.11+ | 8.13+ | 8.14+ | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/outlook.py) |
| [PostgreSQL](https://www.elastic.co/guide/en/enterprise-search/current/connectors-postgresql.html) | **GA** | 8.8+ | 8.11+ | - | - | - | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/postgresql.py) |
| [Redis](https://www.elastic.co/guide/en/enterprise-search/current/connectors-redis.html) | **Preview** | - | - | - | - | - | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/redis.py) |
| [Amazon S3](https://www.elastic.co/guide/en/enterprise-search/current/connectors-s3.html) | **GA** | 8.12+ | 8.12+ | 8.11+ | - | - | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/s3.py) |
| [Salesforce](https://www.elastic.co/guide/en/enterprise-search/current/connectors-salesforce.html) | **GA** | 8.12+ | 8.12+ | 8.11+ | 8.13+ | 8.13+ | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/salesforce.py) |
| [ServiceNow](https://www.elastic.co/guide/en/enterprise-search/current/connectors-servicenow.html) | **GA** | 8.10+ | 8.10+ | 8.11+ | 8.13+ | 8.13+ | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/servicenow.py) |
| [Sharepoint Online](https://www.elastic.co/guide/en/enterprise-search/current/connectors-sharepoint-online.html) | **GA** | 8.9+ | 8.9+ | 8.9+ | 8.9+ | 8.9+ | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/sharepoint_online.py) |
| [Sharepoint Server](https://www.elastic.co/guide/en/enterprise-search/current/connectors-sharepoint.html) | **Beta** | - | - | 8.11+ | 8.13+ | 8.14+ | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/sharepoint_server.py) |
| [Slack](https://www.elastic.co/guide/en/enterprise-search/current/connectors-slack.html) | **Preview** | 8.14+ | - | - | - | - | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/slack.py) |
| [Teams](https://www.elastic.co/guide/en/enterprise-search/current/connectors-teams.html) | **Preview** | 8.14+ | - | - | 8.13+ | - | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/teams.py) |
| [Zoom](https://www.elastic.co/guide/en/enterprise-search/current/connectors-zoom.html) | **Preview** | 8.14+ | - | 8.11+ | 8.13+ | - | [View code](https://github.com/elastic/connectors/tree/main/connectors/sources/zoom.py) |
