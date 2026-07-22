# Microsoft Teams connector setup

The Microsoft Teams connector authenticates with **application-only** credentials
(no user sign-in) and follows the least-privilege permission model. Because
Microsoft only exposes read access to Teams messages through either broad
`*.All` "protected" permissions or the narrower **resource-specific consent
(RSC)** / **`WhereInstalled`** permissions, this connector uses the latter. That
means the connector's Entra (Azure AD) application must be packaged as a Teams
app and **installed into the teams and chats** you want to sync.

## 1. Register an Entra (Azure AD) application

1. In the [Microsoft Entra admin center](https://entra.microsoft.com), register a
   new application (a confidential client).
2. Record the **Directory (tenant) ID** and **Application (client) ID**.
3. Create either:
   - a **client secret** (Certificates & secrets -> New client secret), or
   - a **certificate** (upload a certificate and keep the matching private key).

Use these values for the connector's `Tenant ID`, `Client ID`, and
`Secret value` (or `Certificate` + `Private key`) configuration fields.

## 2. Grant tenant-wide application permissions (Entra)

Add and grant **admin consent** for these Microsoft Graph **application**
permissions. They are used to enumerate resources and to read chats where the
app is installed:

| Permission | Why |
| --- | --- |
| `Team.ReadBasic.All` | Enumerate all teams in the tenant. |
| `Channel.ReadBasic.All` | List channels of each team. |
| `Chat.ReadBasic.WhereInstalled` | List chats where the app is installed. |
| `Chat.Read.WhereInstalled` | Read messages of chats where the app is installed. |
| `ChatMember.Read.WhereInstalled` | Read members of chats where the app is installed. |
| `Files.Read.All` (optional) | Download channel/chat attachment content. Only needed when "Fetch attachment content" is enabled. |

> There is no `WhereInstalled` variant for reading channel messages or team
> members; those use the RSC permissions declared in the Teams app manifest
> (step 3) and are only granted for teams where the app is installed.

> **On the two `.All` permissions:** `Team.ReadBasic.All` and
> `Channel.ReadBasic.All` are unavoidable. Microsoft Graph offers no narrower
> (`.Selected` / `.WhereInstalled` / RSC) way to *enumerate* the teams and
> channels of a tenant, and both are metadata-only (names/descriptions, not
> message content). `Files.Read.All` is the only application permission that can
> download attachment bytes and is required only when "Fetch attachment content"
> is enabled.

## 3. Package and install the Teams app (RSC)

1. Edit [`manifest.json`](./manifest.json):
   - Set `id` to a new GUID (the Teams app id).
   - Set `webApplicationInfo.id` to your Entra **Application (client) ID**.
   - Set `webApplicationInfo.resource` to an Application ID URI on your tenant.
2. Add `color.png` (192x192) and `outline.png` (32x32) icons next to the
   manifest and zip the three files together into an app package.
3. Upload the package to your organization's Teams app catalog
   (Teams admin center -> Teams apps -> Manage apps -> Upload).
4. **Install** the app into the teams and chats you want to sync. To cover the
   whole tenant, use an
   [app setup policy](https://learn.microsoft.com/en-us/microsoftteams/teams-app-setup-policies)
   to auto-install it broadly.

The RSC permissions declared in the manifest
(`ChannelMessage.Read.Group`, `ChannelSettings.Read.Group`,
`TeamMember.Read.Group`, `ChatMessage.Read.Chat`, `ChatMember.Read.Chat`) are
granted per resource when the app is installed there.

## What gets synced

- **Teams** (from groups that are teams)
- **Channels** and their messages, message **replies**, and attachments
- **Team members**
- **Chats** and their messages and attachments

### Coverage notes and limitations

- **Chat coverage is limited to chats where the connector's Teams app is
  installed** (`Chat.*.WhereInstalled`), not "all chats of all team members."
  Full-tenant chat coverage would require `Chat.Read.All`, which is a broad,
  "protected" permission that this connector intentionally does not use in order
  to stay least-privilege. Install the app in the chats you need indexed.
- **Chats have no threaded replies in Microsoft Graph.** Reply threading exists
  only for *channel* messages; chats are linear, so every chat message is
  already captured by the flat message listing (a chat "reply" is just another
  message). Only channel messages expand `replies`.

Anything the app is not installed in is invisible to the connector and is
skipped. Skipped resources are counted and reported as an aggregate warning at
the end of each sync, so an under-installed tenant is visible in the logs rather
than producing a quiet, near-empty sync.

## Document level security (DLS)

When "Enable document level security" is on, access to each document is
restricted to the members of its team/chat. The access-control sync only creates
identities for users that participate in synced teams/chats, so `Users.Read.All`
is never required.
