export const environment = {
  production: true,
  apiUrl: '/api', // Update this with your production API URL
  defaultFolderPath: '/Shared/GraphRAG', // Default folder path for CMIS
  // Use localhost by default, can be overridden at runtime for Docker deployments
  cmisBaseUrl: 'http://localhost:8080', // Flexible for both standalone and Docker
  alfrescoBaseUrl: 'http://localhost:8080', // Flexible for both standalone and Docker
  nuxeoBaseUrl: 'http://localhost:8081/nuxeo' // Flexible for both standalone and Docker
};
