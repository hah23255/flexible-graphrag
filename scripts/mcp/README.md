# MCP transport bearer-token helper

Local dev helper to mint a **bearer token for the MCP transport auth layer** — the outer
OAuth2 layer that gates *who may call* an MCP server. Both MCP servers
(`python-alfresco-mcp-server` and `flexible-graphrag-mcp`) support it via FastMCP's
`JWTVerifier`; when started with `MCP_TRANSPORT_AUTH=true` they require
`Authorization: Bearer <token>` and validate the token's signature against Keycloak's JWKS.

> Local development only. Targets the bundled Keycloak (`http://localhost:8091`, realm
> `alfresco`, client `flexible-graphrag`).

## Two OAuth2 layers — don't confuse them

| Layer | What it gates | This script? |
|---|---|---|
| **Transport** (`MCP_TRANSPORT_AUTH`) | Who may call the MCP server | **Yes** — mints this bearer |
| **Data-source** (`ALFRESCO_AUTH_METHOD` / `*_OAUTH2_*`) | How the server authenticates to Alfresco/Nuxeo | No — see `scripts/alfresco`, `scripts/nuxeo` |

This helper uses the **client_credentials** grant (machine-to-machine). A user token from
`scripts/alfresco` or `scripts/nuxeo` is also a valid transport bearer — same Keycloak signer.

## Config (env vars override defaults)

| Var | Default |
|---|---|
| `KC_BASE` | `http://localhost:8091` |
| `KC_REALM` | `alfresco` |
| `KC_CLIENT_ID` | `flexible-graphrag` |
| `KC_CLIENT_SECRET` | `flexible-graphrag-secret` (bundled local-dev default) |

Token endpoint: `${KC_BASE}/realms/${KC_REALM}/protocol/openid-connect/token`

## Run — pick the shell you have (no git bash required except for `.sh`)

```powershell
powershell -File scripts\mcp\get-transport-token.ps1
```
```bat
scripts\mcp\get-transport-token.bat
```
```bash
./scripts/mcp/get-transport-token.sh
```
```
venv-3.14/Scripts/python scripts/mcp/get-transport-token.py
```

## Use the token

1. Start the MCP server in HTTP mode with `MCP_TRANSPORT_AUTH=true` (plus
   `MCP_AUTH_JWKS_URI`, and optionally `MCP_AUTH_ISSUER` / `MCP_AUTH_AUDIENCE`). See the
   MCP server's own README for the transport-auth section.
2. In the MCP client (e.g. MCP Inspector) set **Bearer Token** = the printed `access_token`
   (Inspector: Header Name `Authorization`). No/expired token → 401.

**Audience note:** if the server sets `MCP_AUTH_AUDIENCE`, the token must carry a matching
`aud` — a client_credentials token may need a Keycloak audience mapper. Left unset, the
server validates on JWKS signature only, so any valid Keycloak token works.

**Client-credentials note:** this token authenticates as the Keycloak service account
(no user identity). That's fine for the transport layer, which only checks that the caller
holds a valid Keycloak-signed token.
