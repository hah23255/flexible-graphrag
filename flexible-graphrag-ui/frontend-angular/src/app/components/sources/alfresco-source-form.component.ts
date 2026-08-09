import { Component, Input, Output, EventEmitter, OnInit } from '@angular/core';
import { environment } from '../../../environments/environment';

export interface AlfrescoOAuth2Config {
  client_id?: string;
  client_secret?: string;
  token_endpoint?: string;
  scope?: string;
  access_token?: string;
  refresh_token?: string;
}

export interface AlfrescoSourceConfig {
  url: string;
  auth_method: string;
  username?: string;
  password?: string;
  oauth2?: AlfrescoOAuth2Config;
  path?: string;
}

@Component({
  selector: 'app-alfresco-source-form',
  template: `
    <app-base-source-form
      title="Alfresco Repository"
      description="Connect to an Alfresco content management system">

      <mat-form-field appearance="outline" class="full-width">
        <mat-label>Alfresco Base URL *</mat-label>
        <input matInput [(ngModel)]="url" (ngModelChange)="update()"
               [placeholder]="'e.g., ' + defaultUrl" required />
      </mat-form-field>

      <mat-form-field appearance="outline" class="full-width">
        <mat-label>Authentication</mat-label>
        <mat-select [(ngModel)]="authMethod" (ngModelChange)="update()">
          <mat-option value="basic">Basic (username / password)</mat-option>
          <mat-option value="ticket">Ticket (login ticket)</mat-option>
          <mat-option value="oauth2">OAuth2 (Bearer, via Identity Service)</mat-option>
        </mat-select>
      </mat-form-field>

      <div class="form-row" *ngIf="authMethod === 'basic' || authMethod === 'ticket'">
        <mat-form-field appearance="outline" class="half-width">
          <mat-label>Username *</mat-label>
          <input matInput [(ngModel)]="username" (ngModelChange)="update()" required />
        </mat-form-field>
        <mat-form-field appearance="outline" class="half-width">
          <mat-label>Password *</mat-label>
          <input matInput type="password" [(ngModel)]="password" (ngModelChange)="update()" required />
        </mat-form-field>
      </div>

      <ng-container *ngIf="authMethod === 'oauth2'">
        <div class="form-row">
          <mat-form-field appearance="outline" class="half-width">
            <mat-label>Client ID *</mat-label>
            <input matInput [(ngModel)]="clientId" (ngModelChange)="update()" required />
          </mat-form-field>
          <mat-form-field appearance="outline" class="half-width">
            <mat-label>Client Secret</mat-label>
            <input matInput type="password" [(ngModel)]="clientSecret" (ngModelChange)="update()" />
          </mat-form-field>
        </div>
        <mat-form-field appearance="outline" class="full-width">
          <mat-label>Token Endpoint</mat-label>
          <input matInput [(ngModel)]="tokenEndpoint" (ngModelChange)="update()"
                 placeholder="e.g., https://<keycloak>/realms/alfresco/protocol/openid-connect/token" />
        </mat-form-field>
        <mat-form-field appearance="outline" class="full-width">
          <mat-label>Scope (optional)</mat-label>
          <input matInput [(ngModel)]="scope" (ngModelChange)="update()" />
        </mat-form-field>
        <div class="form-row">
          <mat-form-field appearance="outline" class="half-width">
            <mat-label>Access Token (optional)</mat-label>
            <input matInput type="password" [(ngModel)]="accessToken" (ngModelChange)="update()" />
            <mat-hint>Provide a pre-obtained token instead of client_credentials</mat-hint>
          </mat-form-field>
          <mat-form-field appearance="outline" class="half-width">
            <mat-label>Refresh Token (optional)</mat-label>
            <input matInput type="password" [(ngModel)]="refreshToken" (ngModelChange)="update()" />
          </mat-form-field>
        </div>
      </ng-container>

      <mat-form-field appearance="outline" class="full-width">
        <mat-label>Path *</mat-label>
        <input matInput [(ngModel)]="path" (ngModelChange)="update()"
               placeholder="e.g., /Shared/GraphRAG" required />
      </mat-form-field>
    </app-base-source-form>
  `,
  styles: [`
    .full-width { width: 100%; margin-bottom: 16px; }
    .form-row { display: flex; gap: 16px; margin-bottom: 16px; }
    .half-width { flex: 1; }
  `],
  standalone: false
})
export class AlfrescoSourceFormComponent implements OnInit {
  defaultUrl = environment.alfrescoBaseUrl || 'http://localhost:8080';

  url: string = this.defaultUrl;
  authMethod: string = 'basic';
  path: string = '/Shared/GraphRAG';

  username: string = 'admin';
  password: string = 'admin';

  clientId: string = '';
  clientSecret: string = '';
  tokenEndpoint: string = '';
  scope: string = '';
  accessToken: string = '';
  refreshToken: string = '';

  @Input() value: any = null;
  @Output() valueChange = new EventEmitter<any>();
  @Output() configurationChange = new EventEmitter<AlfrescoSourceConfig>();
  @Output() validationChange = new EventEmitter<boolean>();

  ngOnInit() {
    const v = this.value || {};
    if (v.url !== undefined) this.url = v.url;
    if (v.auth_method) this.authMethod = v.auth_method;
    if (v.path !== undefined) this.path = v.path;
    if (v.username !== undefined) this.username = v.username;
    if (v.password !== undefined) this.password = v.password;
    const o = v.oauth2 || {};
    if (o.client_id !== undefined) this.clientId = o.client_id;
    if (o.client_secret !== undefined) this.clientSecret = o.client_secret;
    if (o.token_endpoint !== undefined) this.tokenEndpoint = o.token_endpoint;
    if (o.scope !== undefined) this.scope = o.scope;
    if (o.access_token !== undefined) this.accessToken = o.access_token;
    if (o.refresh_token !== undefined) this.refreshToken = o.refresh_token;
    this.update();
  }

  private isValid(): boolean {
    if (this.url.trim() === '' || this.path.trim() === '') return false;
    if (this.authMethod === 'oauth2') {
      return this.clientId.trim() !== '' && (this.accessToken.trim() !== '' || this.clientSecret.trim() !== '');
    }
    return this.username.trim() !== '' && this.password.trim() !== '';
  }

  private buildConfig(): AlfrescoSourceConfig {
    const config: AlfrescoSourceConfig = { url: this.url, auth_method: this.authMethod, path: this.path };
    if (this.authMethod === 'oauth2') {
      config.oauth2 = {
        client_id: this.clientId,
        client_secret: this.clientSecret || undefined,
        token_endpoint: this.tokenEndpoint || undefined,
        scope: this.scope || undefined,
        access_token: this.accessToken || undefined,
        refresh_token: this.refreshToken || undefined,
      };
    } else {
      config.username = this.username;
      config.password = this.password;
    }
    return config;
  }

  update(): void {
    const config = this.buildConfig();
    this.validationChange.emit(this.isValid());
    this.configurationChange.emit(config);
    this.valueChange.emit(config);
  }
}
