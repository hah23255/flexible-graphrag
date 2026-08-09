"""Mint a bearer token for the MCP *transport* auth layer (FastMCP JWTVerifier).

This is the OUTER OAuth2 layer — it gates WHO may call the MCP server
(Authorization: Bearer <token>), and is DISTINCT from the data-source auth the server
uses to reach Alfresco/Nuxeo. Both MCP servers (python-alfresco-mcp-server and
flexible-graphrag-mcp) validate the token's signature against Keycloak's JWKS when
started with MCP_TRANSPORT_AUTH=true.

Uses the client_credentials grant (machine-to-machine) against the same local Keycloak.
(A user token from scripts/alfresco or scripts/nuxeo is also a valid transport bearer,
since it is signed by the same Keycloak.)

Config (env vars override the defaults):
  KC_BASE           http://localhost:8091      Keycloak base URL (host-side)
  KC_REALM          alfresco
  KC_CLIENT_ID      flexible-graphrag
  KC_CLIENT_SECRET  flexible-graphrag-secret    bundled local-dev default

Paste the printed access_token into the MCP client's Bearer field (e.g. MCP Inspector's
"Bearer Token"), and start the server with MCP_TRANSPORT_AUTH=true.

Run (from repo root):
  venv-3.14/Scripts/python scripts/mcp/get-transport-token.py
"""
import os
import httpx

KC_BASE   = os.environ.get("KC_BASE", "http://localhost:8091")
REALM     = os.environ.get("KC_REALM", "alfresco")
CLIENT_ID = os.environ.get("KC_CLIENT_ID", "flexible-graphrag")
SECRET    = os.environ.get("KC_CLIENT_SECRET", "flexible-graphrag-secret")

TOKEN_ENDPOINT = f"{KC_BASE}/realms/{REALM}/protocol/openid-connect/token"
data = {"grant_type": "client_credentials", "client_id": CLIENT_ID, "client_secret": SECRET}

print(f"POST {TOKEN_ENDPOINT} (grant=client_credentials, client={CLIENT_ID})")
r = httpx.post(TOKEN_ENDPOINT, data=data, timeout=30)
if r.status_code != 200:
    print("FAILED", r.status_code, r.text[:600])
    raise SystemExit(1)

tok = r.json()
print("\n=== TOKEN ===")
print("access_token :", tok.get("access_token"))
print("expires_in   :", tok.get("expires_in"))
print("\nUse as: Authorization: Bearer <access_token>  (server needs MCP_TRANSPORT_AUTH=true).")
