import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';

// Fix for ES module __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Script to generate env-config.json from environment variables
 * This allows Docker containers to override URLs at runtime
 */

interface EnvConfig {
  PROCESS_FOLDER_PATH: string;
  CMIS_BASE_URL?: string;
  ALFRESCO_BASE_URL?: string;
  NUXEO_BASE_URL?: string;
}

// Default values (for standalone mode)
const defaultConfig: EnvConfig = {
  PROCESS_FOLDER_PATH: process.env.PROCESS_FOLDER_PATH || '/Shared/GraphRAG',
  CMIS_BASE_URL: process.env.CMIS_BASE_URL || 'http://localhost:8080',
  ALFRESCO_BASE_URL: process.env.ALFRESCO_BASE_URL || 'http://localhost:8080',
  NUXEO_BASE_URL: process.env.NUXEO_BASE_URL || 'http://localhost:8081/nuxeo'
};

// For Docker mode, override with Docker networking URLs
if (process.env.DOCKER_MODE === 'true') {
  defaultConfig.CMIS_BASE_URL = process.env.CMIS_BASE_URL || 'http://host.docker.internal:8080';
  defaultConfig.ALFRESCO_BASE_URL = process.env.ALFRESCO_BASE_URL || 'http://host.docker.internal:8080';
  defaultConfig.NUXEO_BASE_URL = process.env.NUXEO_BASE_URL || 'http://host.docker.internal:8081/nuxeo';
}

// Write the configuration to the assets directory
const outputPath = path.join(__dirname, 'src', 'assets', 'env-config.json');
fs.writeFileSync(outputPath, JSON.stringify(defaultConfig, null, 2));

console.log('Generated env-config.json:', defaultConfig);