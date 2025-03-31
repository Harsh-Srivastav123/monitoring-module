
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchAuditLogs } from "@/services/auditLogService";
import { AuditLog, AuditLogFilter } from "@/types/auditLogs";
import { Loader } from "lucide-react";
import { useToast } from "@/hooks/use-toast";

import AuditLogSearch from "@/components/AuditLogSearch";
import AuditLogTable from "@/components/AuditLogTable";
import AuditLogStats from "@/components/AuditLogStats";

const Dashboard = () => {
  const [filter, setFilter] = useState<AuditLogFilter | null>(null);
  const { toast } = useToast();
  
  const {
    data: logs,
    isLoading,
    isError,
    error,
    refetch,
  } = useQuery({
    queryKey: ["auditLogs", filter],
    queryFn: () => (filter ? fetchAuditLogs(filter) : Promise.resolve([] as AuditLog[])),
    enabled: !!filter,
    onError: (err: Error) => {
      toast({
        title: "Error",
        description: err.message || "Failed to fetch audit logs",
        variant: "destructive",
      });
    },
  });

  const handleSearch = (newFilter: AuditLogFilter) => {
    setFilter(newFilter);
  };

  return (
    <div className="container mx-auto p-4 md:p-6 bg-audit-background min-h-screen">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-audit-primary mb-2">
          Audit Vision Dashboard
        </h1>
        <p className="text-audit-secondary">
          View and analyze audit logs by searching for specific User ID or Correlation ID
        </p>
      </div>

      <AuditLogSearch onSearch={handleSearch} isLoading={isLoading} />

      {isLoading && (
        <div className="flex items-center justify-center p-12">
          <div className="flex flex-col items-center space-y-4">
            <Loader className="h-8 w-8 animate-spin text-audit-primary" />
            <p className="text-audit-primary">Loading audit logs...</p>
          </div>
        </div>
      )}

      {!isLoading && logs && logs.length > 0 && (
        <>
          <AuditLogStats logs={logs} />
          <AuditLogTable logs={logs} />
        </>
      )}

      {!isLoading && logs && logs.length === 0 && filter && (
        <div className="bg-audit-card shadow rounded-md p-6 text-center">
          <h2 className="text-xl font-semibold mb-2">No Audit Logs Found</h2>
          <p className="text-audit-secondary mb-4">
            No logs match your search criteria. Please try a different user ID or correlation ID.
          </p>
        </div>
      )}

      {isError && (
        <div className="bg-red-50 border border-red-200 text-red-700 shadow rounded-md p-6 text-center">
          <h2 className="text-xl font-semibold mb-2">Error Loading Audit Logs</h2>
          <p className="mb-4">
            {error instanceof Error ? error.message : "An unknown error occurred"}
          </p>
        </div>
      )}
    </div>
  );
};

export default Dashboard;
