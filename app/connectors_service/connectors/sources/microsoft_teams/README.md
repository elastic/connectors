# Microsoft Teams connector setup

The Microsoft Teams connector authenticates with **application-only** credentials
(client secret or certificate; no user sign-in), the same pattern as SharePoint
Online and Outlook. It uses tenant-wide Microsoft Graph **application**
permissions. No Teams app package, RSC, or per-team/per-chat install is required.

Privacy for end users searching Elasticsearch is enforced with **document level
security (DLS)** from team/channel/chat membership. The connector app itself can
read content granted by the Graph permissions below (including private chats of
team members).

## 1. Register an Entra (Azure AD) application

1. In the [Microsoft Entra admin center](https://entra.microsoft.com), register a
   new application (confidential client).
2. Record the **Directory (tenant) ID** and **Application (client) ID**.
3. Create either:
   - a **client secret** (Certificates & secrets → New client secret), or
   - a **certificate** (upload a certificate and keep the matching private key).

Use these values for the connector's `Tenant ID`, `Client ID`, and
`Secret value` (or `Certificate` + `Private key`) configuration fields.

## 2. Grant application permissions

Add and grant **admin consent** for these Microsoft Graph **application**
permissions:

| Permission | Why |
| --- | --- |
| `Team.ReadBasic.All` | Enumerate teams in the tenant. |
| `TeamMember.Read.All` | Team membership (DLS) and user ids for chat discovery. |
| `Channel.ReadBasic.All` | List channels of each team. |
| `ChannelMember.Read.All` | Private/shared channel membership for correct DLS ACLs. |
| `ChannelMessage.Read.All` | Channel messages and replies. |
| `Chat.ReadBasic.All` | List chats for team members (names/members); app-only uses `GET /users/{id}/chats`. |
| `Chat.Read.All` | Chat message bodies. |
| `Files.Read.All` (optional) | Download channel/chat attachment content when "Fetch attachment content" is enabled. |

`ChatMember.Read.All` is optional if `Chat.ReadBasic.All` or `Chat.Read.All` is
already granted (those also authorize chat members).

### Protected APIs

`ChannelMessage.Read.All` and `Chat.Read.All` are [protected Teams APIs](https://learn.microsoft.com/en-us/graph/teams-protected-apis).
Admin consent is required; some tenants also need Microsoft to approve protected
API access for the app before app-only message calls return `200`. Verify with an
**app-only** (client credentials) token — not Graph Explorer's default delegated
login.

## What gets synced

- **Teams**, **channels**, channel **messages** / **replies**, and attachments
- **Team members**
- **Chats of team members** (discovered via team membership → each member's
  chats), including messages and attachments

Chat discovery does **not** use `User.Read.All`. Users who have chats but belong
to no team are out of scope. The same chat seen for multiple members is indexed
once (deduped by chat id).

Document level security restricts search results to members of the relevant
team, private/shared channel, or chat. Enable "Enable document level security"
in the connector configuration.

## Failures

Missing core application permissions fail the sync rather than producing a quiet
near-empty index. Optional missing resources (for example no channel files
folder) are skipped and summarized in a warning.
