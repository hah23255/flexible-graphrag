import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Docker-specific configuration
const CMIS_BASE_URL = 'http://host.docker.internal:8080';  // Docker networking
const ALFRESCO_BASE_URL = 'http://host.docker.internal:8080';  // Docker networking
const NUXEO_BASE_URL = 'http://host.docker.internal:8081/nuxeo';  // Docker networking

export default defineConfig({
  plugins: [react()],
  base: '/ui/react/',
  server: {
    port: 3000,
    host: '0.0.0.0',
    // Add proxy for direct access (port 3000)
    proxy: {
      '/api': {
        target: 'http://flexible-graphrag-backend:8000',
        changeOrigin: true,
        secure: false,
      },
    },
  },
  define: {
    'import.meta.env.VITE_CMIS_BASE_URL': JSON.stringify(CMIS_BASE_URL),
    'import.meta.env.VITE_ALFRESCO_BASE_URL': JSON.stringify(ALFRESCO_BASE_URL),
    'import.meta.env.VITE_NUXEO_BASE_URL': JSON.stringify(NUXEO_BASE_URL),
  }
});
