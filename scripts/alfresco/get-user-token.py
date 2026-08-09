"""Mint an Alfresco *user* OAuth2 token via Keycloak (identity-service) password grant.

Unlike client_credentials — which the Alfresco source self-fetches from client_id +
client_secret and which yields the SERVICE ACCOUNT (no display name, default perms) —
this prints a token for a real user, so you can test the Alfresco source with
Authentication = OAuth2 under a real identity (display name + real ACLs).

Config (environment variables override the defaults):
  KC_BASE           http://localhost:8091     Keycloak base URL (host-side)
  KC_REALM          alfresco
  KC_CLIENT_ID      flexible-graphrag          confidential identity-service client
  KC_CLIENT_SECRET  flexible-graphrag-secret   bundled local-dev default (override for other setups)
  KC_USERNAME       admin                      a realm user (maps to an Alfresco user)
  KC_PASSWORD       admin

Prereq: the client must have "Direct Access Grants" (password grant) enabled.

Run (from repo root):
  venv-3.14/Scripts/python scripts/alfresco/get-user-token.py
"""
import os
import httpx

KC_BASE   = os.environ.get("KC_BASE", "http://localhost:8091")
REALM     = os.environ.get("KC_REALM", "alfresco")
CLIENT_ID = os.environ.get("KC_CLIENT_ID", "flexible-graphrag")
SECRET    = os.environ.get("KC_CLIENT_SECRET", "flexible-graphrag-secret")  # bundled local-dev default
USERNAME  = os.environ.get("KC_USERNAME", "admin")
PASSWORD  = os.environ.get("KC_PASSWORD", "admin")

TOKEN_ENDPOINT = f"{KC_BASE}/realms/{REALM}/protocol/openid-connect/token"

data = {
    "grant_type": "password",
    "client_id": CLIENT_ID,
    "username": USERNAME,
    "password": PASSWORD,
}
if SECRET:
    data["client_secret"] = SECRET

print(f"POST {TOKEN_ENDPOINT} (user={USERNAME}, client={CLIENT_ID})")
r = httpx.post(TOKEN_ENDPOINT, data=data, timeout=30)
if r.status_code != 200:
    print("FAILED", r.status_code, r.text[:600])
    raise SystemExit(1)

tok = r.json()
print("\n=== TOKEN ===")
print("access_token :", tok.get("access_token"))
print("refresh_token:", tok.get("refresh_token"))
print("expires_in   :", tok.get("expires_in"))
print("\nPaste access_token (and refresh_token) into the Alfresco source's OAuth2 fields.")
