// This file can be replaced during build by using the `fileReplacements` array.
// `ng build` replaces `environment.ts` with `environment.prod.ts`.
// The list of file replacements can be found in `angular.json`.

export const environment = {
  production: false,
  apiUrl: '/api', // Base URL for API requests
  defaultFolderPath: '/Shared/GraphRAG', // Default folder path for CMIS
  cmisBaseUrl: 'http://localhost:8080', // Default for standalone development
  alfrescoBaseUrl: 'http://localhost:8080', // Default for standalone development
  nuxeoBaseUrl: 'http://localhost:8081/nuxeo', // Default for standalone development
  nuxeoPath: '/default-domain/workspaces/GraphRAG' // Default Nuxeo folder shown in the source dialog
};

/*
 * For easier debugging in development mode, you can import the following file
 * to ignore zone related error stack frames such as `zone.run`, `zoneDelegate.invokeTask`.
 *
 * This import should be commented out in production mode because it will have a negative impact
 * on performance if an error is thrown.
 */
// import 'zone.js/plugins/zone-error';  // Included with Angular CLI.
