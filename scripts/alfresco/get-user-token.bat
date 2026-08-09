@echo off
REM Mint an Alfresco *user* OAuth2 token via Keycloak (identity-service) password grant.
REM Runs in cmd.exe on Windows 10/11 using the built-in curl.exe -- NO git bash needed.
REM (In PowerShell, "curl" is an alias for Invoke-WebRequest, so this uses curl.exe explicitly;
REM  prefer get-user-token.ps1 in PowerShell.)
REM
REM Defaults target the bundled local-dev Keycloak realm. Override any KC_* var first, e.g.:
REM   set KC_CLIENT_SECRET=your-secret
REM Then:  scripts\alfresco\get-user-token.bat
setlocal
if not defined KC_BASE          set "KC_BASE=http://localhost:8091"
if not defined KC_REALM         set "KC_REALM=alfresco"
if not defined KC_CLIENT_ID     set "KC_CLIENT_ID=flexible-graphrag"
if not defined KC_CLIENT_SECRET set "KC_CLIENT_SECRET=flexible-graphrag-secret"
if not defined KC_USERNAME      set "KC_USERNAME=admin"
if not defined KC_PASSWORD      set "KC_PASSWORD=admin"
set "ENDPOINT=%KC_BASE%/realms/%KC_REALM%/protocol/openid-connect/token"

echo POST %ENDPOINT% (user=%KC_USERNAME%, client=%KC_CLIENT_ID%)
echo.
curl.exe -s -X POST "%ENDPOINT%" --data-urlencode "grant_type=password" --data-urlencode "client_id=%KC_CLIENT_ID%" --data-urlencode "client_secret=%KC_CLIENT_SECRET%" --data-urlencode "username=%KC_USERNAME%" --data-urlencode "password=%KC_PASSWORD%"
echo.
echo.
echo Copy "access_token" (and "refresh_token") from the JSON above into the Alfresco OAuth2 fields.
endlocal
