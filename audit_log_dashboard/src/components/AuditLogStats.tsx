
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { AuditLog } from "@/types/auditLogs";

interface AuditLogStatsProps {
  logs: AuditLog[];
}

const AuditLogStats = ({ logs }: AuditLogStatsProps) => {
  // Calculate statistics
  const totalLogs = logs.length;
  
  // Calculate success rate
  const successLogs = logs.filter(log => 
    log.responseStatusCode && log.responseStatusCode >= 200 && log.responseStatusCode < 300
  ).length;
  
  const successRate = totalLogs > 0 ? (successLogs / totalLogs) * 100 : 0;
  
  // Calculate average response time
  const totalTime = logs.reduce((acc, log) => acc + (log.apiTimeTaken || 0), 0);
  const avgResponseTime = totalLogs > 0 ? totalTime / totalLogs : 0;
  
  // Calculate cold starts
  const coldStarts = logs.filter(log => log.coldStart).length;
  const coldStartPercentage = totalLogs > 0 ? (coldStarts / totalLogs) * 100 : 0;

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Total Logs
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{totalLogs}</div>
        </CardContent>
      </Card>
      
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Success Rate
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">
            {successRate.toFixed(2)}%
          </div>
        </CardContent>
      </Card>
      
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Avg Response Time
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">
            {avgResponseTime.toFixed(6)} s
          </div>
        </CardContent>
      </Card>
      
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Cold Starts
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">
            {coldStarts} ({coldStartPercentage.toFixed(2)}%)
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default AuditLogStats;
