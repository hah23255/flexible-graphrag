export interface IngestRequest {
  data_source: string;
  paths?: string[];
  cmis_config?: {
    url: string;
    username: string;
    password: string;
    folder_path: string;
  };
  alfresco_config?: {
    url: string;
    auth_method?: string;
    username?: string;
    password?: string;
    oauth2?: {
      client_id?: string;
      client_secret?: string;
      token_endpoint?: string;
      scope?: string;
      access_token?: string;
      refresh_token?: string;
    };
    path: string;
  };
  nuxeo_config?: {
    url: string;
    auth_method?: string;
    username?: string;
    password?: string;
    token?: string;
    oauth2?: {
      client_id?: string;
      client_secret?: string;
      access_token?: string;
      refresh_token?: string;
      token_endpoint?: string;
    };
    path?: string;
  };
}

export interface ProcessFolderRequest {
  folder_path: string;
}

export interface QueryRequest {
  query: string;
  query_type?: string;
  top_k?: number;
}

export interface ApiResponse {
  status: string;
  message?: string;
  error?: string;
  answer?: string;
  results?: any[];
}

// New async processing response
export interface AsyncProcessingResponse {
  processing_id: string;
  status: 'started' | 'processing' | 'completed' | 'failed' | 'cancelled';
  message: string;
  progress?: number;
  estimated_time?: string;
  started_at?: string;
  updated_at?: string;
  error?: string;
}

// Processing status check response
export interface ProcessingStatusResponse {
  processing_id: string;
  status: 'started' | 'processing' | 'completed' | 'failed' | 'cancelled';
  message: string;
  progress: number;
  started_at: string;
  updated_at: string;
  error?: string;
  individual_files?: Array<{
    filename: string;
    status: string;
    progress: number;
    phase: string;
    message?: string;
    error?: string;
    started_at?: string;
    completed_at?: string;
  }>;
  current_file?: string;
  current_phase?: string;
  files_completed?: number;
  total_files?: number;
  estimated_time_remaining?: string;
}