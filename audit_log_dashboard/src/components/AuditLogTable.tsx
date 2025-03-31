
import { useState } from "react";
import { 
  Table, 
  TableBody, 
  TableCell, 
  TableHead, 
  TableHeader, 
  TableRow 
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogClose,
} from "@/components/ui/dialog";
import { AuditLog } from "@/types/auditLogs";
import { ScrollArea } from "@/components/ui/scroll-area";

interface AuditLogTableProps {
  logs: AuditLog[];
}

const AuditLogTable = ({ logs }: AuditLogTableProps) => {
  const [selectedLog, setSelectedLog] = useState<AuditLog | null>(null);

  // Function to format date strings
  const formatDate = (dateString?: string) => {
    if (!dateString) return "N/A";
    const date = new Date(dateString);
    return date.toLocaleString();
  };

  // Function to determine badge color based on status code
  const getStatusBadge = (statusCode?: number) => {
    if (!statusCode) return <Badge className="bg-gray-400">Unknown</Badge>;
    
    if (statusCode >= 200 && statusCode < 300) {
      return <Badge className="bg-audit-success">{statusCode}</Badge>;
    } else if (statusCode >= 400 && statusCode < 500) {
      return <Badge className="bg-audit-warning">{statusCode}</Badge>;
    } else if (statusCode >= 500) {
      return <Badge className="bg-audit-error">{statusCode}</Badge>;
    }
    
    return <Badge>{statusCode}</Badge>;
  };

  // Function to view log details
  const viewLogDetails = (log: AuditLog) => {
    setSelectedLog(log);
  };

  // Function to format JSON for display
  const formatJson = (jsonString: string) => {
    try {
      return JSON.stringify(JSON.parse(jsonString), null, 2);
    } catch (e) {
      return jsonString;
    }
  };

  return (
    <>
      <div className="rounded-md border">
        <ScrollArea className="h-[60vh]">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-[80px]">ID</TableHead>
                <TableHead>Method</TableHead>
                <TableHead>Entity</TableHead>
                <TableHead>Endpoint</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Time</TableHead>
                <TableHead>Duration (s)</TableHead>
                <TableHead>Cold Start</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {logs.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={9} className="text-center py-10">
                    No audit logs found. Please search for a specific user ID or correlation ID.
                  </TableCell>
                </TableRow>
              ) : (
                logs.map((log) => (
                  <TableRow key={log.id}>
                    <TableCell className="font-medium">{log.id}</TableCell>
                    <TableCell>{log.methodName}</TableCell>
                    <TableCell>{log.entityName}</TableCell>
                    <TableCell className="max-w-[150px] truncate" title={log.apiEndPoint}>
                      {log.apiEndPoint}
                    </TableCell>
                    <TableCell>{getStatusBadge(log.responseStatusCode)}</TableCell>
                    <TableCell>{formatDate(log.apiStartTime)}</TableCell>
                    <TableCell>{log.apiTimeTaken?.toFixed(6) || "N/A"}</TableCell>
                    <TableCell>
                      {log.coldStart ? (
                        <Badge className="bg-audit-warning">Yes</Badge>
                      ) : (
                        <Badge className="bg-audit-success">No</Badge>
                      )}
                    </TableCell>
                    <TableCell className="text-right">
                      <Button 
                        variant="outline" 
                        size="sm" 
                        onClick={() => viewLogDetails(log)}
                        className="border-audit-primary text-audit-primary hover:bg-audit-primary/10"
                      >
                        View Details
                      </Button>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </ScrollArea>
      </div>

      <Dialog open={selectedLog !== null} onOpenChange={(open) => !open && setSelectedLog(null)}>
        <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="text-audit-primary">
              Audit Log Details (ID: {selectedLog?.id})
            </DialogTitle>
          </DialogHeader>
          
          {selectedLog && (
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <p className="text-sm font-medium">Method</p>
                  <p className="text-sm">{selectedLog.methodName}</p>
                </div>
                <div className="space-y-2">
                  <p className="text-sm font-medium">Entity Name</p>
                  <p className="text-sm">{selectedLog.entityName}</p>
                </div>
                <div className="space-y-2">
                  <p className="text-sm font-medium">API Endpoint</p>
                  <p className="text-sm">{selectedLog.apiEndPoint}</p>
                </div>
                <div className="space-y-2">
                  <p className="text-sm font-medium">Status Code</p>
                  <p className="text-sm">{getStatusBadge(selectedLog.responseStatusCode)}</p>
                </div>
                <div className="space-y-2">
                  <p className="text-sm font-medium">User ID</p>
                  <p className="text-sm break-all">{selectedLog.userId}</p>
                </div>
                <div className="space-y-2">
                  <p className="text-sm font-medium">Correlation ID</p>
                  <p className="text-sm break-all">{selectedLog.correlationId}</p>
                </div>
                <div className="space-y-2">
                  <p className="text-sm font-medium">Start Time</p>
                  <p className="text-sm">{formatDate(selectedLog.apiStartTime)}</p>
                </div>
                <div className="space-y-2">
                  <p className="text-sm font-medium">End Time</p>
                  <p className="text-sm">{formatDate(selectedLog.apiEndTime)}</p>
                </div>
                <div className="space-y-2">
                  <p className="text-sm font-medium">Time Taken</p>
                  <p className="text-sm">{selectedLog.apiTimeTaken?.toFixed(6) || "N/A"} seconds</p>
                </div>
                <div className="space-y-2">
                  <p className="text-sm font-medium">Memory Used</p>
                  <p className="text-sm">{selectedLog.memoryUsedMb} MB</p>
                </div>
                <div className="space-y-2">
                  <p className="text-sm font-medium">Cold Start</p>
                  <p className="text-sm">{selectedLog.coldStart ? "Yes" : "No"}</p>
                </div>
              </div>

              <div className="space-y-2">
                <p className="text-sm font-medium">Request Payload</p>
                <pre className="bg-gray-50 p-3 rounded-md text-xs overflow-x-auto">
                  {formatJson(selectedLog.requestPayload)}
                </pre>
              </div>

              <div className="space-y-2">
                <p className="text-sm font-medium">Response</p>
                <pre className="bg-gray-50 p-3 rounded-md text-xs overflow-x-auto">
                  {formatJson(selectedLog.response)}
                </pre>
              </div>
            </div>
          )}

          <DialogClose asChild>
            <Button variant="outline">Close</Button>
          </DialogClose>
        </DialogContent>
      </Dialog>
    </>
  );
};

export default AuditLogTable;
