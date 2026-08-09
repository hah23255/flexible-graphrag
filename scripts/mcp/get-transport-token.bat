@echo off
REM Mint a bearer token for the MCP *transport* auth layer (FastMCP JWTVerifier), client_credentials.
REM Runs in cmd.exe on Windows 10/11 using the built-in curl.exe -- NO git bash needed.
REM (In PowerShell, "curl" is an alias for Invoke-WebRequest; prefer get-transport-token.ps1 there.)
REM
REM Outer OAuth2 layer: gates WHO may call the MCP server. Start the server with
REM MCP_TRANSPORT_AUTH=true and send the token as Authorization: Bearer <token>.
setlocal
if not defined KC_BASE          set "KC_BASE=http://localhost:8091"
if not defined KC_REALM         set "KC_REALM=alfresco"
if not defined KC_CLIENT_ID     set "KC_CLIENT_ID=flexible-graphrag"
if not defined KC_CLIENT_SECRET set "KC_CLIENT_SECRET=flexible-graphrag-secret"
set "ENDPOINT=%KC_BASE%/realms/%KC_REALM%/protocol/openid-connect/token"

echo POST %ENDPOINT% (grant=client_credentials, client=%KC_CLIENT_ID%)
echo.
curl.exe -s -X POST "%ENDPOINT%" --data-urlencode "grant_type=client_credentials" --data-urlencode "client_id=%KC_CLIENT_ID%" --data-urlencode "client_secret=%KC_CLIENT_SECRET%"
echo.
echo.
echo Copy "access_token" and send it as Authorization: Bearer ^<token^> (server needs MCP_TRANSPORT_AUTH=true).
endlocal
