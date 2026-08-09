import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { environment } from '../../environments/environment';
import { Observable, of } from 'rxjs';
import { catchError, map, tap } from 'rxjs/operators';
import '../../env-config'; // Import to ensure type declarations are available

/**
 * Service for accessing environment variables from .env file
 * Falls back to values in environment.ts if not found in .env
 */
@Injectable({
  providedIn: 'root'
})
export class EnvService {
  private envConfig: { [key: string]: string } = {};
  private loaded = false;

  constructor(private http: HttpClient) {}

  /**
   * Load environment variables from .env file
   * This should be called during app initialization
   */
  loadEnvConfig(): Observable<boolean> {
    // If already loaded, return immediately
    if (this.loaded) {
      return of(true);
    }

    // In a real implementation, this would make an HTTP request to a server endpoint
    // that reads the .env file and returns its contents
    // For now, we'll simulate this with a local request
    return this.http.get<{[key: string]: string}>('/assets/env-config.json').pipe(
      tap(config => {
        this.envConfig = config;
        this.loaded = true;
      }),
      map(() => true),
      catchError(error => {
        console.warn('Could not load environment config, using defaults', error);
        this.loaded = true;
        return of(true);
      })
    );
  }

  /**
   * Get an environment variable value
   * @param key The environment variable name
   * @param defaultValue Default value if not found
   * @returns The environment variable value or default
   */
  get(key: string, defaultValue: string = ''): string {
    return this.envConfig[key] || defaultValue;
  }

  /**
   * Get the default folder path from .env or fallback to environment.ts
   */
  get defaultFolderPath(): string {
    // Only use the standardized variable name
    return this.get('PROCESS_FOLDER_PATH', environment.defaultFolderPath);
  }

  /**
   * Get the CMIS base URL from runtime config or fallback to environment.ts
   */
  get cmisBaseUrl(): string {
    return window.__env?.CMIS_BASE_URL || environment.cmisBaseUrl;
  }

  /**
   * Get the Alfresco base URL from runtime config or fallback to environment.ts
   */
  get alfrescoBaseUrl(): string {
    return window.__env?.ALFRESCO_BASE_URL || environment.alfrescoBaseUrl;
  }

  /**
   * Get the Nuxeo base URL from runtime config or fallback to environment.ts
   */
  get nuxeoBaseUrl(): string {
    return window.__env?.NUXEO_BASE_URL || environment.nuxeoBaseUrl;
  }
}
