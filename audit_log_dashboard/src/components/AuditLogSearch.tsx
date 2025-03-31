
import { useState } from "react";
import { useToast } from "@/hooks/use-toast";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { AuditLogFilter } from "@/types/auditLogs";

interface AuditLogSearchProps {
  onSearch: (filter: AuditLogFilter) => void;
  isLoading: boolean;
}

const AuditLogSearch = ({ onSearch, isLoading }: AuditLogSearchProps) => {
  const [activeTab, setActiveTab] = useState<"userId" | "correlationId">("userId");
  const [userId, setUserId] = useState("");
  const [correlationId, setCorrelationId] = useState("");
  const { toast } = useToast();

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();

    if (activeTab === "userId" && !userId) {
      toast({
        title: "User ID Required",
        description: "Please enter a user ID to search",
        variant: "destructive",
      });
      return;
    }

    if (activeTab === "correlationId" && !correlationId) {
      toast({
        title: "Correlation ID Required",
        description: "Please enter a correlation ID to search",
        variant: "destructive",
      });
      return;
    }

    const filter: AuditLogFilter = {};
    if (activeTab === "userId") {
      filter.userId = userId;
    } else {
      filter.correlationId = correlationId;
    }

    onSearch(filter);
  };

  return (
    <Card className="mb-6">
      <CardHeader>
        <CardTitle className="text-2xl text-audit-primary">Audit Log Search</CardTitle>
      </CardHeader>
      <CardContent>
        <form onSubmit={handleSearch}>
          <Tabs
            defaultValue="userId"
            value={activeTab}
            onValueChange={(value) => setActiveTab(value as "userId" | "correlationId")}
            className="mb-4"
          >
            <TabsList className="grid w-full grid-cols-2">
              <TabsTrigger value="userId">Search by User ID</TabsTrigger>
              <TabsTrigger value="correlationId">Search by Correlation ID</TabsTrigger>
            </TabsList>
            <TabsContent value="userId" className="mt-4">
              <div className="space-y-2">
                <Input
                  placeholder="Enter User ID"
                  value={userId}
                  onChange={(e) => setUserId(e.target.value)}
                  className="mb-4"
                />
              </div>
            </TabsContent>
            <TabsContent value="correlationId" className="mt-4">
              <div className="space-y-2">
                <Input
                  placeholder="Enter Correlation ID"
                  value={correlationId}
                  onChange={(e) => setCorrelationId(e.target.value)}
                  className="mb-4"
                />
              </div>
            </TabsContent>
          </Tabs>
          <div className="flex justify-end">
            <Button 
              type="submit" 
              disabled={isLoading}
              className="bg-audit-primary hover:bg-audit-primary/90"
            >
              {isLoading ? "Searching..." : "Search"}
            </Button>
          </div>
        </form>
      </CardContent>
    </Card>
  );
};

export default AuditLogSearch;
