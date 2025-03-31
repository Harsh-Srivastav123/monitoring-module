
import { AuditLog, AuditLogFilter } from "@/types/auditLogs";

const API_URL = "https://jrtoxvwi90.execute-api.ap-south-1.amazonaws.com/dev/graphql/audit-logs";

export const fetchAuditLogsByUserId = async (userId: string): Promise<AuditLog[]> => {
  const query = `
    query auditLogs($userId: String!) {
      auditLogs(filter: { userId: $userId }) {
        id
        methodName
        entityName
        userId
        apiEndPoint
        responseStatusCode
        apiStartTime
        apiEndTime
        apiTimeTaken
        memoryUsedMb
        coldStart
        correlationId
        requestPayload
        response
      }
    }
  `;

  try {
    const response = await fetch(API_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        query,
        variables: { userId },
      }),
    });

    const data = await response.json();
    
    if (data.errors) {
      console.error("GraphQL errors:", data.errors);
      throw new Error(data.errors[0].message);
    }

    return data.data.auditLogs;
  } catch (error) {
    console.error("Error fetching audit logs by user ID:", error);
    throw error;
  }
};

export const fetchAuditLogsByCorrelationId = async (correlationId: string): Promise<AuditLog[]> => {
  const query = `
    query auditLogs($correlationId: String!) {
      auditLogs(filter: { correlationId: $correlationId }) {
        id
        methodName
        entityName
        userId
        apiEndPoint
        responseStatusCode
        apiStartTime
        apiEndTime
        apiTimeTaken
        memoryUsedMb
        coldStart
        correlationId
        requestPayload
        response
      }
    }
  `;

  try {
    const response = await fetch(API_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        query,
        variables: { correlationId },
      }),
    });

    const data = await response.json();
    
    if (data.errors) {
      console.error("GraphQL errors:", data.errors);
      throw new Error(data.errors[0].message);
    }

    return data.data.auditLogs;
  } catch (error) {
    console.error("Error fetching audit logs by correlation ID:", error);
    throw error;
  }
};

export const fetchAuditLogs = async (filter: AuditLogFilter): Promise<AuditLog[]> => {
  if (filter.userId) {
    return fetchAuditLogsByUserId(filter.userId);
  } else if (filter.correlationId) {
    return fetchAuditLogsByCorrelationId(filter.correlationId);
  } else {
    throw new Error("Either userId or correlationId must be provided");
  }
};
