/**
 * This script loads environment variables from the .env file
 * and makes them available to the Angular app via window.__env
 */

// Define the environment interface
interface Environment {
  DEFAULT_FOLDER_PATH: string;
  CMIS_BASE_URL?: string;
  ALFRESCO_BASE_URL?: string;
  NUXEO_BASE_URL?: string;
  NUXEO_PATH?: string;
}

// Declare the __env property on the window object
declare global {
  interface Window {
    __env: Environment;
  }
}

// Initialize the environment
window.__env = window.__env || {
  // Default values that will be overridden by the server
  DEFAULT_FOLDER_PATH: '/Shared/GraphRAG',
  CMIS_BASE_URL: 'http://localhost:8080',
  ALFRESCO_BASE_URL: 'http://localhost:8080',
  NUXEO_BASE_URL: 'http://localhost:8081/nuxeo',
  NUXEO_PATH: '/default-domain/workspaces/GraphRAG'
};

// The server will inject actual values from .env here
// DO NOT MODIFY THIS COMMENT: ENV_INJECTION_POINT

export {}; // This file needs to be a module
