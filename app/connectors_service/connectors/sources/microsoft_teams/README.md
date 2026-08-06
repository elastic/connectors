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
| `Chat.ReadBasic.All` | List chats for team members; app-only uses `GET /users/{id}/chats`. Chat membership for DLS uses dedicated `GET /chats/{id}/members`. |
| `Chat.Read.All` | Chat message bodies. |
| `Files.Read.All` | **Required** when "Fetch attachment content" is enabled (validated from the app token `roles` claim at config time). Download channel/chat attachment content. |

`ChatMember.Read.All` is optional if `Chat.ReadBasic.All` or `Chat.Read.All` is
already granted (those also authorize chat members).

### Protected APIs

`ChannelMessage.Read.All` and `Chat.Read.All` are [protected Teams APIs](https://learn.microsoft.com/en-us/graph/teams-protected-apis).
Admin consent is required; some tenants also need Microsoft to approve protected
API access for the app before app-only message calls return `200`. Verify with an
**app-only** (client credentials) token — not Graph Explorer's default delegated
login.

## What gets synced

- **Team**, **Channel**, channel **messages** / **replies**
- **Users** (unique Entra users who belong to at least one synced team)
- **Chats of team members** (discovered via team membership → each member's
  chats), including messages
- **Files** (when "Fetch attachment content" is on): channel Files-folder
  drive items plus chat/channel message file attachments resolved via
  ``contentUrl`` → shares API. Message docs carry ``attachments: [{id, title}]``
  linking to File ``_id`` (driveItem id). File docs set sparse
  ``channel_id``/``channel_title`` and/or ``chat_id``/``chat_title`` from
  discovery (no empty placeholders).

Team, Channel, and Chat documents include ``member_ids`` (Entra user ids).
Standard channels inherit the parent team's membership; private/shared channels
use channel-specific members.

Discovery is one pass per sync: teams → team members (deduped user ids) → each
user's chats (deduped by chat id). Chat ACL membership is always loaded with
`GET /chats/{id}/members` (full list). Channel replies are always loaded with
`GET .../messages/{id}/replies` (full threads). Incomplete `$expand` shortcuts are
not used for members or replies.

Chat discovery does **not** use `User.Read.All`. Users who have chats but belong
to no team are out of scope. The same chat seen for multiple members is indexed
once (deduped by chat id).

Document level security restricts search results to members of the relevant
team, private/shared channel, or chat. Enable "Enable document level security"
in the connector configuration.

## Failures

Missing core application permissions (`PermissionsMissing` / HTTP 401–403) fail
the content sync and the access-control sync rather than producing a quiet
near-empty or incomplete index. Resources that are genuinely absent (`NotFound` /
HTTP 404) — for example a deleted channel mid-sync or no channel files folder —
may be soft-skipped and summarized in a warning.
