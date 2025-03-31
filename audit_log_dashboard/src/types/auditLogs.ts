
export interface AuditLog {
  id: string;
  methodName: string;
  entityName: string;
  userId: string;
  apiEndPoint: string;
  responseStatusCode?: number;
  apiStartTime?: string;
  apiEndTime?: string;
  apiTimeTaken?: number;
  memoryUsedMb: number;
  coldStart: boolean;
  correlationId: string;
  requestPayload: string;
  response: string;
}

export interface AuditLogFilter {
  userId?: string;
  correlationId?: string;
}
