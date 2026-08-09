import React, { useEffect, useMemo, useState } from 'react';
import { TextField, Box, FormControl, InputLabel, Select, MenuItem } from '@mui/material';
import { BaseSourceForm, BaseSourceFormProps } from './BaseSourceForm';

type AuthMethod = 'basic' | 'ticket' | 'oauth2';

interface AlfrescoSourceFormProps extends BaseSourceFormProps {
  // Single-object persistence (see NuxeoSourceForm): seeds on mount, emits full config on edit,
  // so state survives Sources<->Processing tab switches (the Sources TabPanel unmounts).
  value?: any;
  onChange?: (config: any) => void;
}

// Self-contained form: manages its own state and emits a complete Alfresco config object
// (matching the backend AlfrescoConfig model) with a basic / ticket / oauth2 auth switch.
export const AlfrescoSourceForm: React.FC<AlfrescoSourceFormProps> = ({
  value,
  onChange,
  onConfigurationChange,
  onValidationChange,
}) => {
  const defaultUrl = useMemo(
    () => import.meta.env.VITE_ALFRESCO_BASE_URL || 'http://localhost:8080',
    []
  );
  const defaultPath = useMemo(
    () => import.meta.env.VITE_PROCESS_FOLDER_PATH || '/Shared/GraphRAG',
    []
  );

  const [url, setUrl] = useState<string>(value?.url ?? defaultUrl);
  const [authMethod, setAuthMethod] = useState<AuthMethod>(value?.auth_method ?? 'basic');
  const [path, setPath] = useState<string>(value?.path ?? defaultPath);

  // Basic / ticket auth
  const [username, setUsername] = useState<string>(value?.username ?? 'admin');
  const [password, setPassword] = useState<string>(value?.password ?? 'admin');

  // OAuth2
  const [clientId, setClientId] = useState<string>(value?.oauth2?.client_id ?? '');
  const [clientSecret, setClientSecret] = useState<string>(value?.oauth2?.client_secret ?? '');
  const [tokenEndpoint, setTokenEndpoint] = useState<string>(value?.oauth2?.token_endpoint ?? '');
  const [scope, setScope] = useState<string>(value?.oauth2?.scope ?? '');
  const [accessToken, setAccessToken] = useState<string>(value?.oauth2?.access_token ?? '');
  const [refreshToken, setRefreshToken] = useState<string>(value?.oauth2?.refresh_token ?? '');

  const isValid = useMemo(() => {
    if (url.trim() === '' || path.trim() === '') return false;
    if (authMethod === 'oauth2') {
      return clientId.trim() !== '' && (accessToken.trim() !== '' || clientSecret.trim() !== '');
    }
    // basic or ticket
    return username.trim() !== '' && password.trim() !== '';
  }, [url, path, authMethod, username, password, clientId, clientSecret, accessToken]);

  const config = useMemo(() => {
    const base: any = { url, auth_method: authMethod, path };
    if (authMethod === 'oauth2') {
      base.oauth2 = {
        client_id: clientId,
        client_secret: clientSecret || undefined,
        token_endpoint: tokenEndpoint || undefined,
        scope: scope || undefined,
        access_token: accessToken || undefined,
        refresh_token: refreshToken || undefined,
      };
    } else {
      base.username = username;
      base.password = password;
    }
    return base;
  }, [url, authMethod, path, username, password, clientId, clientSecret, tokenEndpoint, scope, accessToken, refreshToken]);

  useEffect(() => {
    onValidationChange(isValid);
    onConfigurationChange(config);
    onChange?.(config);  // persist live config to App so it survives tab switches
  }, [isValid, config, onValidationChange, onConfigurationChange, onChange]);

  return (
    <BaseSourceForm
      title="Alfresco Repository"
      description="Connect to an Alfresco content management system"
    >
      <TextField
        fullWidth
        label="Alfresco Base URL"
        variant="outlined"
        value={url}
        onChange={(e) => setUrl(e.target.value)}
        size="small"
        sx={{ mb: 2 }}
        placeholder={`e.g., ${defaultUrl}`}
      />

      <FormControl fullWidth size="small" sx={{ mb: 2 }}>
        <InputLabel>Authentication</InputLabel>
        <Select
          value={authMethod}
          label="Authentication"
          onChange={(e) => setAuthMethod(e.target.value as AuthMethod)}
        >
          <MenuItem value="basic">Basic (username / password)</MenuItem>
          <MenuItem value="ticket">Ticket (login ticket)</MenuItem>
          <MenuItem value="oauth2">OAuth2 (Bearer, via Identity Service)</MenuItem>
        </Select>
      </FormControl>

      {(authMethod === 'basic' || authMethod === 'ticket') && (
        <Box sx={{ display: 'flex', gap: 2, mb: 2 }}>
          <TextField
            fullWidth
            label="Username"
            variant="outlined"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            size="small"
          />
          <TextField
            fullWidth
            label="Password"
            type="password"
            variant="outlined"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            size="small"
          />
        </Box>
      )}

      {authMethod === 'oauth2' && (
        <Box sx={{ mb: 2 }}>
          <Box sx={{ display: 'flex', gap: 2, mb: 2 }}>
            <TextField
              fullWidth
              label="Client ID"
              variant="outlined"
              value={clientId}
              onChange={(e) => setClientId(e.target.value)}
              size="small"
            />
            <TextField
              fullWidth
              label="Client Secret"
              type="password"
              variant="outlined"
              value={clientSecret}
              onChange={(e) => setClientSecret(e.target.value)}
              size="small"
            />
          </Box>
          <TextField
            fullWidth
            label="Token Endpoint"
            variant="outlined"
            value={tokenEndpoint}
            onChange={(e) => setTokenEndpoint(e.target.value)}
            size="small"
            sx={{ mb: 2 }}
            placeholder="e.g., https://<keycloak>/realms/alfresco/protocol/openid-connect/token"
          />
          <TextField
            fullWidth
            label="Scope (optional)"
            variant="outlined"
            value={scope}
            onChange={(e) => setScope(e.target.value)}
            size="small"
            sx={{ mb: 2 }}
          />
          <Box sx={{ display: 'flex', gap: 2 }}>
            <TextField
              fullWidth
              label="Access Token (optional)"
              type="password"
              variant="outlined"
              value={accessToken}
              onChange={(e) => setAccessToken(e.target.value)}
              size="small"
              helperText="Provide a pre-obtained token instead of client_credentials"
            />
            <TextField
              fullWidth
              label="Refresh Token (optional)"
              type="password"
              variant="outlined"
              value={refreshToken}
              onChange={(e) => setRefreshToken(e.target.value)}
              size="small"
            />
          </Box>
        </Box>
      )}

      <TextField
        fullWidth
        label="Path"
        variant="outlined"
        value={path}
        onChange={(e) => setPath(e.target.value)}
        size="small"
        sx={{ mb: 2 }}
        placeholder="e.g., /Shared/GraphRAG"
      />
    </BaseSourceForm>
  );
};
