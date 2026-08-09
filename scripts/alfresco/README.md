# Alfresco OAuth2 helper scripts

Local dev helper to mint an Alfresco **user** OAuth2 token via the built-in
`identity-service` (Keycloak) **password grant**, so you can test the Alfresco data
source with **Authentication = OAuth2** under a real user identity.

> Local development only. Defaults target `http://localhost:8091` (Keycloak) and the
> `alfresco` realm from `docker/keycloak/alfresco-realm.json`, with `admin/admin`.

## User token vs. client_credentials — why this script only mints a *user* token

| Alfresco OAuth2 mode | Identity you get | Need a script? |
|---|---|---|
| **client_credentials** | The **service account** (`service-account-flexible-graphrag`) — no display name, only default permissions | **No** — the Alfresco source self-fetches this from `client_id` + `client_secret` |
| **password (user)** | The **real user** (display name + real ACLs) | **Yes** — this script |

## Prerequisites

- Keycloak running (docker compose includes it on port `8091`).
- The `flexible-graphrag` client must have **Direct Access Grants** (password grant) enabled
  — Keycloak Admin → Clients → `flexible-graphrag` → Settings.
- The client is **confidential**, so you must supply its secret. The bundled compose realm
  (`docker/keycloak/alfresco-realm.json`) ships the local dev default **`flexible-graphrag-secret`**;
  for any other setup get it from Keycloak Admin → Clients → `flexible-graphrag` → **Credentials**.

## Config (env vars override defaults)

| Var | Default | Notes |
|---|---|---|
| `KC_BASE` | `http://localhost:8091` | Keycloak base URL (host-side) |
| `KC_REALM` | `alfresco` | |
| `KC_CLIENT_ID` | `flexible-graphrag` | confidential identity-service client |
| `KC_CLIENT_SECRET` | `flexible-graphrag-secret` | bundled local-dev default; override for other setups |
| `KC_USERNAME` / `KC_PASSWORD` | `admin` / `admin` | a realm user (maps to an Alfresco user) |

Token endpoint: `${KC_BASE}/realms/${KC_REALM}/protocol/openid-connect/token`

## Run — pick the shell you have (no git bash required except for `.sh`)

Against the bundled local-dev realm these work with **no arguments** (defaults cover
secret + admin/admin). For any other setup, override with the `KC_*` vars / `-ClientSecret`.

```powershell
# PowerShell (native, no curl needed) — recommended on Windows
powershell -File scripts\alfresco\get-user-token.ps1
```
```bat
REM cmd.exe (uses built-in curl.exe — no git bash)
scripts\alfresco\get-user-token.bat
```
```bash
# git bash / Linux / macOS (curl)
./scripts/alfresco/get-user-token.sh
```
```
# cross-platform (backend venv, run from repo root)
venv-3.14/Scripts/python scripts/alfresco/get-user-token.py
```

Then paste the printed `access_token` (and `refresh_token`) into the Alfresco source's
OAuth2 fields in the UI (or the REST/MCP config).

### Note on `curl` on Windows
`curl.exe` ships with Windows 10 (1803+) and 11 and runs natively in **cmd.exe** — it is
not git-bash-only. In **PowerShell**, `curl` is an alias for `Invoke-WebRequest`, so the
`.bat` calls `curl.exe` explicitly; in PowerShell prefer `get-user-token.ps1`
(`Invoke-RestMethod`, no curl at all).
