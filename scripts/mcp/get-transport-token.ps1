# Mint a bearer token for the MCP *transport* auth layer (FastMCP JWTVerifier), client_credentials.
# Native PowerShell (Invoke-RestMethod) — no curl and no git bash needed.
#
# Outer OAuth2 layer: gates WHO may call the MCP server (distinct from data-source auth).
# Start the server with MCP_TRANSPORT_AUTH=true; paste the token as Authorization: Bearer <token>.
#
# Run (from repo root):
#   powershell -File scripts\mcp\get-transport-token.ps1
param(
  [string]$KcBase       = $(if ($env:KC_BASE) { $env:KC_BASE } else { "http://localhost:8091" }),
  [string]$Realm        = $(if ($env:KC_REALM) { $env:KC_REALM } else { "alfresco" }),
  [string]$ClientId     = $(if ($env:KC_CLIENT_ID) { $env:KC_CLIENT_ID } else { "flexible-graphrag" }),
  [string]$ClientSecret = $(if ($env:KC_CLIENT_SECRET) { $env:KC_CLIENT_SECRET } else { "flexible-graphrag-secret" })
)

$endpoint = "$KcBase/realms/$Realm/protocol/openid-connect/token"
$body = @{
  grant_type    = "client_credentials"
  client_id     = $ClientId
  client_secret = $ClientSecret
}

Write-Host "POST $endpoint (grant=client_credentials, client=$ClientId)"
try {
  $resp = Invoke-RestMethod -Method Post -Uri $endpoint -Body $body -ContentType "application/x-www-form-urlencoded"
} catch {
  Write-Error "Token request failed: $($_.Exception.Message)"
  exit 1
}

Write-Host "`n=== TOKEN ==="
Write-Host "access_token : $($resp.access_token)"
Write-Host "expires_in   : $($resp.expires_in)"
Write-Host "`nUse as Authorization: Bearer <access_token>  (server needs MCP_TRANSPORT_AUTH=true)."
