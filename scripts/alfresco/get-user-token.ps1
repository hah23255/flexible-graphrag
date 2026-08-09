# Mint an Alfresco *user* OAuth2 token via Keycloak (identity-service) password grant.
# Native PowerShell (Invoke-RestMethod) — no curl and no git bash needed.
#
# Yields a real user's token (display name + ACLs), unlike client_credentials which the
# Alfresco source self-fetches and which returns the service account.
#
# Prereq: the 'flexible-graphrag' client must have Direct Access Grants (password) enabled.
#
# Run (from repo root):
#   powershell -File scripts\alfresco\get-user-token.ps1 -ClientSecret <secret>
param(
  [string]$KcBase       = $(if ($env:KC_BASE) { $env:KC_BASE } else { "http://localhost:8091" }),
  [string]$Realm        = $(if ($env:KC_REALM) { $env:KC_REALM } else { "alfresco" }),
  [string]$ClientId     = $(if ($env:KC_CLIENT_ID) { $env:KC_CLIENT_ID } else { "flexible-graphrag" }),
  [string]$ClientSecret = $(if ($env:KC_CLIENT_SECRET) { $env:KC_CLIENT_SECRET } else { "flexible-graphrag-secret" }),
  [string]$Username     = $(if ($env:KC_USERNAME) { $env:KC_USERNAME } else { "admin" }),
  [string]$Password     = $(if ($env:KC_PASSWORD) { $env:KC_PASSWORD } else { "admin" })
)

$endpoint = "$KcBase/realms/$Realm/protocol/openid-connect/token"
$body = @{
  grant_type = "password"
  client_id  = $ClientId
  username   = $Username
  password   = $Password
}
if ($ClientSecret) { $body.client_secret = $ClientSecret }
else { Write-Warning "ClientSecret is empty; the confidential client will likely reject this (invalid_client)." }

Write-Host "POST $endpoint (user=$Username, client=$ClientId)"
try {
  $resp = Invoke-RestMethod -Method Post -Uri $endpoint -Body $body -ContentType "application/x-www-form-urlencoded"
} catch {
  Write-Error "Token request failed: $($_.Exception.Message)"
  exit 1
}

Write-Host "`n=== TOKEN ==="
Write-Host "access_token : $($resp.access_token)"
Write-Host "refresh_token: $($resp.refresh_token)"
Write-Host "expires_in   : $($resp.expires_in)"
Write-Host "`nPaste access_token (and refresh_token) into the Alfresco source's OAuth2 fields."
