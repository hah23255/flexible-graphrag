# Nuxeo OAuth2 helper scripts

Local dev helpers for minting a Nuxeo OAuth2 access/refresh token so you can test the
Nuxeo data source with **Authentication = OAuth2** in the UI (or via the REST API / MCP).
Both scripts print the token and then smoke-test `sources.nuxeo.NuxeoSource`.

> Local development only. They target `http://localhost:8081/nuxeo` and use the default
> `Administrator/Administrator` credentials — do not use against a real deployment.

## Prerequisite — register an OAuth2 client in Nuxeo

> **This is a manual, per-deployment step, and it does not survive recreating the
> Nuxeo containers.** The client lives in Nuxeo's `oauth2Clients` directory, not
> in this repo, so a fresh `docker compose up` on a new volume starts with only
> `nuxeo-drive` and `nuxeo-mobile`. Symptom when it is missing: the authorize
> call returns **HTTP 400** and `nuxeo_oauth2_headless.py` prints
> *"Could not obtain an authorization code headlessly."*

Admin Center → **Cloud Services → Consumers (OAuth2 clients)** → *Add*:

| Field | Value |
|---|---|
| Name / Client ID | `flexible-graphrag` |
| Client Secret | *(optional; blank = public PKCE client)* |
| Redirect URIs | `http://localhost:8888/callback` |
| Enabled | yes |
| Auto-grant | yes *(required by `nuxeo_oauth2_headless.py`)* |

Auto-grant makes the authorize call redirect straight to the callback with no
browser or consent step, which is what lets the headless script work.

### Or register it from the command line

Same thing without the UI — useful after recreating the deployment:

```bash
curl -u Administrator:Administrator -X POST \
  -H "Content-Type: application/json" \
  http://localhost:8081/nuxeo/api/v1/directory/oauth2Clients \
  -d '{"entity-type":"directoryEntry","directoryName":"oauth2Clients",
       "properties":{"clientId":"flexible-graphrag","name":"flexible-graphrag",
       "redirectURIs":"http://localhost:8888/callback",
       "autoGrant":true,"enabled":true}}'
```

Check what is currently registered:

```bash
curl -u Administrator:Administrator \
  http://localhost:8081/nuxeo/api/v1/directory/oauth2Clients
```

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
