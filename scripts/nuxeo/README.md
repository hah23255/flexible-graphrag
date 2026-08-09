# Nuxeo OAuth2 helper scripts

Local dev helpers for minting a Nuxeo OAuth2 access/refresh token so you can test the
Nuxeo data source with **Authentication = OAuth2** in the UI (or via the REST API / MCP).
Both scripts print the token and then smoke-test `sources.nuxeo.NuxeoSource`.

> Local development only. They target `http://localhost:8081/nuxeo` and use the default
> `Administrator/Administrator` credentials — do not use against a real deployment.

## Prerequisite — register an OAuth2 client in Nuxeo

Admin Center → **Cloud Services → Consumers (OAuth2 clients)** → *Add*:

| Field | Value |
|---|---|
| Name / Client ID | `flexible-graphrag` |
| Client Secret | *(optional; blank = public PKCE client)* |
| Redirect URIs | `http://localhost:8888/callback` |
| Enabled | yes |

For `nuxeo_oauth2_headless.py`, also set **Auto-grant = true** so the authorize call
redirects straight to the callback with no browser/consent step.

## Scripts

- **`nuxeo_oauth2_token.py`** — interactive auth-code + PKCE flow. Prints an authorize
  URL; you log in, approve, and paste the redirect URL back. Use when auto-grant is off.
- **`nuxeo_oauth2_headless.py`** — headless variant. Uses Basic auth against
  `/oauth2/authorize` with auto-grant to obtain the code with no browser.

## Run (from repo root)

```
venv-3.14/Scripts/python scripts/nuxeo/nuxeo_oauth2_token.py
venv-3.14/Scripts/python scripts/nuxeo/nuxeo_oauth2_headless.py
```

Then paste the printed `access_token` / `refresh_token` into the Nuxeo source's OAuth2
fields (Client ID, Client Secret if set, Access Token, Refresh Token).
