<template>
  <BaseSourceForm
    title="Alfresco Repository"
    description="Connect to an Alfresco content management system"
  >
    <v-text-field
      :model-value="url"
      @update:model-value="(v: string) => url = v"
      label="Alfresco Base URL *"
      variant="outlined"
      class="mb-4"
      :placeholder="`e.g., ${defaultUrl}`"
      required
    />

    <v-select
      :model-value="authMethod"
      @update:model-value="(v: string) => authMethod = v"
      :items="authMethods"
      label="Authentication"
      variant="outlined"
      class="mb-4"
      density="compact"
    />

    <v-row v-if="authMethod === 'basic' || authMethod === 'ticket'" class="mb-4">
      <v-col cols="6">
        <v-text-field
          :model-value="username"
          @update:model-value="(v: string) => username = v"
          label="Username *"
          variant="outlined"
          required
        />
      </v-col>
      <v-col cols="6">
        <v-text-field
          :model-value="password"
          @update:model-value="(v: string) => password = v"
          label="Password *"
          type="password"
          variant="outlined"
          required
        />
      </v-col>
    </v-row>

    <template v-if="authMethod === 'oauth2'">
      <v-row class="mb-4">
        <v-col cols="6">
          <v-text-field
            :model-value="clientId"
            @update:model-value="(v: string) => clientId = v"
            label="Client ID *"
            variant="outlined"
            required
          />
        </v-col>
        <v-col cols="6">
          <v-text-field
            :model-value="clientSecret"
            @update:model-value="(v: string) => clientSecret = v"
            label="Client Secret"
            type="password"
            variant="outlined"
          />
        </v-col>
      </v-row>
      <v-text-field
        :model-value="tokenEndpoint"
        @update:model-value="(v: string) => tokenEndpoint = v"
        label="Token Endpoint"
        variant="outlined"
        class="mb-4"
        placeholder="e.g., https://<keycloak>/realms/alfresco/protocol/openid-connect/token"
      />
      <v-text-field
        :model-value="scope"
        @update:model-value="(v: string) => scope = v"
        label="Scope (optional)"
        variant="outlined"
        class="mb-4"
      />
      <v-row class="mb-4">
        <v-col cols="6">
          <v-text-field
            :model-value="accessToken"
            @update:model-value="(v: string) => accessToken = v"
            label="Access Token (optional)"
            type="password"
            variant="outlined"
            hint="Provide a pre-obtained token instead of client_credentials"
            persistent-hint
          />
        </v-col>
        <v-col cols="6">
          <v-text-field
            :model-value="refreshToken"
            @update:model-value="(v: string) => refreshToken = v"
            label="Refresh Token (optional)"
            type="password"
            variant="outlined"
          />
        </v-col>
      </v-row>
    </template>

    <v-text-field
      :model-value="path"
      @update:model-value="(v: string) => path = v"
      label="Path *"
      variant="outlined"
      class="mb-4"
      placeholder="e.g., /Shared/GraphRAG"
      required
    />
  </BaseSourceForm>
</template>

<script lang="ts">
import { defineComponent, ref, computed, watch } from 'vue';
import BaseSourceForm from './BaseSourceForm.vue';

export default defineComponent({
  name: 'AlfrescoSourceForm',
  components: {
    BaseSourceForm
  },
  props: {
    // Single-object persistence (see NuxeoSourceForm): seeds on mount, emits update:value on edit.
    value: { type: Object, default: null },
  },
  emits: ['configuration-change', 'validation-change', 'update:value'],
  setup(props, { emit }) {
    const v: any = props.value || {};
    const defaultUrl = import.meta.env.VITE_ALFRESCO_BASE_URL || 'http://localhost:8080';
    // VITE_ALFRESCO_PATH, not VITE_PROCESS_FOLDER_PATH: the latter is the LOCAL
    // filesystem folder for Process Folder, so reusing it put a Windows path in
    // an Alfresco repository field and this fallback never applied.
    const defaultPath = import.meta.env.VITE_ALFRESCO_PATH || '/Shared/GraphRAG';

    const url = ref(v.url ?? defaultUrl);
    const authMethod = ref(v.auth_method ?? 'basic');
    const path = ref(v.path ?? defaultPath);

    const username = ref(v.username ?? 'admin');
    const password = ref(v.password ?? 'admin');

    const clientId = ref(v.oauth2?.client_id ?? '');
    const clientSecret = ref(v.oauth2?.client_secret ?? '');
    const tokenEndpoint = ref(v.oauth2?.token_endpoint ?? '');
    const scope = ref(v.oauth2?.scope ?? '');
    const accessToken = ref(v.oauth2?.access_token ?? '');
    const refreshToken = ref(v.oauth2?.refresh_token ?? '');

    const authMethods = [
      { title: 'Basic (username / password)', value: 'basic' },
      { title: 'Ticket (login ticket)', value: 'ticket' },
      { title: 'OAuth2 (Bearer, via Identity Service)', value: 'oauth2' },
    ];

    const isValid = computed(() => {
      if (url.value.trim() === '' || path.value.trim() === '') return false;
      if (authMethod.value === 'oauth2') {
        return clientId.value.trim() !== '' && (accessToken.value.trim() !== '' || clientSecret.value.trim() !== '');
      }
      return username.value.trim() !== '' && password.value.trim() !== '';
    });

    const config = computed(() => {
      const base: any = { url: url.value, auth_method: authMethod.value, path: path.value };
      if (authMethod.value === 'oauth2') {
        base.oauth2 = {
          client_id: clientId.value,
          client_secret: clientSecret.value || undefined,
          token_endpoint: tokenEndpoint.value || undefined,
          scope: scope.value || undefined,
          access_token: accessToken.value || undefined,
          refresh_token: refreshToken.value || undefined,
        };
      } else {
        base.username = username.value;
        base.password = password.value;
      }
      return base;
    });

    watch([isValid, config], ([newIsValid, newConfig]) => {
      emit('validation-change', newIsValid);
      emit('configuration-change', newConfig);
      emit('update:value', newConfig);  // persist live config to App
    }, { immediate: true, deep: true });

    return {
      defaultUrl,
      url,
      authMethod,
      authMethods,
      path,
      username,
      password,
      clientId,
      clientSecret,
      tokenEndpoint,
      scope,
      accessToken,
      refreshToken,
    };
  },
});
</script>
