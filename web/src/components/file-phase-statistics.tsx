import React, { useMemo, useState } from "react";
import useSWR from "swr";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import prettyBytes from "pretty-bytes";
import { useSettings } from "@/hooks/use-settings";
import { Activity, BarChart3, LoaderPinwheel } from "lucide-react";

// Type definitions
type TimeRange = "1" | "2" | "3" | "4";

interface SpeedData {
  avgSpeed: number;
  medianSpeed: number;
  maxSpeed: number;
  minSpeed: number;
}

interface SpeedStats {
  time: string;
  data: SpeedData;
}

interface CompletedStats {
  time: string;
  total: number;
}

interface ApiResponse {
  speedStats: SpeedStats[];
  completedStats: CompletedStats[];
}

const formatDate = (dateStr: string, timeRange: TimeRange): string => {
  const date = new Date(dateStr);

  switch (timeRange) {
    case "1": // Last hour
      return date.toLocaleTimeString("en-US", {
        hour: "2-digit",
        minute: "2-digit",
      });
    case "2": // Last day
      return date.toLocaleTimeString("en-US", {
        hour: "2-digit",
        minute: "2-digit",
      });
    case "3": // Last week
    case "4": // Last month
      return date.toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
      });
  }
};

interface TelegramStatsProps {
  telegramId: string;
}

const timeRangeOptions = [
  { value: "1", label: "1 Hour" },
  { value: "2", label: "24 Hours" },
  { value: "3", label: "1 Week" },
  { value: "4", label: "30 Days" },
];

const axisStyle = {
  fontSize: 11,
  fill: "#62625b",
};

const TelegramStats: React.FC<TelegramStatsProps> = ({ telegramId }) => {
  const [timeRange, setTimeRange] = useState<TimeRange>("1");
  const { settings } = useSettings();

  const { data, error, isLoading } = useSWR<ApiResponse, Error>(
    `/telegram/${telegramId}/download-statistics?type=phase&timeRange=${timeRange}`,
  );

  const tooltipStyle = useMemo(
    () => ({
      backgroundColor: "rgba(255, 255, 255, 0.96)",
      border: "1px solid rgba(200, 200, 193, 0.8)",
      borderRadius: "18px",
      boxShadow: "none",
      fontSize: "12px",
    }),
    [],
  );

  if (error) {
    return (
      <div className="rounded-[4px] border border-border bg-card p-5 text-destructive">
        Failed to load statistics
      </div>
    );
  }

  if (isLoading || !data) {
    return (
      <div className="rounded-[4px] border border-border bg-card p-5 text-muted-foreground">
        <div className="flex items-center gap-3">
          <LoaderPinwheel className="h-5 w-5 animate-spin" />
          Loading statistics...
        </div>
      </div>
    );
  }

  // Transform speed data for the chart
  const speedChartData = data.speedStats.map((stat) => ({
    time: formatDate(stat.time, timeRange),
    "Average Speed": stat.data.avgSpeed,
    "Median Speed": stat.data.medianSpeed,
    "Max Speed": stat.data.maxSpeed,
    "Min Speed": stat.data.minSpeed,
  }));

  // Transform completion data for the chart
  const completionChartData = data.completedStats.map((stat) => ({
    time: formatDate(stat.time, timeRange),
    "Completed Downloads": stat.total,
  }));

  return (
    <div className="space-y-6">
      <div className="flex justify-end">
        <Select value={timeRange} onValueChange={(value: TimeRange) => setTimeRange(value)}>
          <SelectTrigger className="w-40 bg-card">
            <SelectValue placeholder="Select time range" />
          </SelectTrigger>
          <SelectContent>
            {timeRangeOptions.map((option) => (
              <SelectItem key={option.value} value={option.value}>
                {option.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <Card className="border-border">
        <CardHeader>
          <CardTitle className="flex items-center gap-3 px-1">
            <span className="flex h-10 w-10 items-center justify-center rounded-full bg-muted">
              <Activity className="h-4 w-4" />
            </span>
            Download speed over time
          </CardTitle>
        </CardHeader>
        <CardContent className="px-1">
          <div className="h-80">
            {!speedChartData || speedChartData.length === 0 ? (
              <div className="flex h-full items-center justify-center text-muted-foreground">
                No data available
              </div>
            ) : (
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={speedChartData}>
                  <CartesianGrid stroke="#e5e5e0" vertical={false} />
                  <XAxis
                    dataKey="time"
                    tick={axisStyle}
                    tickMargin={10}
                    interval="preserveStartEnd"
                    tickLine={false}
                    axisLine={false}
                  />
                  <YAxis
                    tickFormatter={(value: number) =>
                      prettyBytes(value, {
                        bits: settings?.speedUnits === "bits",
                      })
                    }
                    tick={axisStyle}
                    tickLine={false}
                    axisLine={false}
                    interval="preserveStartEnd"
                  />
                  <Tooltip
                    formatter={(value: number) =>
                      prettyBytes(value, {
                        bits: settings?.speedUnits === "bits",
                      })
                    }
                    contentStyle={tooltipStyle}
                  />
                  <Legend wrapperStyle={axisStyle} iconType="circle" />
                  <Area
                    type="monotone"
                    dataKey="Max Speed"
                    stroke="#103c25"
                    fill="#103c25"
                    fillOpacity={0.18}
                  />
                  <Area
                    type="monotone"
                    dataKey="Average Speed"
                    stroke="#e60023"
                    fill="#e60023"
                    fillOpacity={0.14}
                  />
                  <Area
                    type="monotone"
                    dataKey="Median Speed"
                    stroke="#6845ab"
                    fill="#6845ab"
                    fillOpacity={0.1}
                  />
                  <Area
                    type="monotone"
                    dataKey="Min Speed"
                    stroke="#9e0a0a"
                    fill="#9e0a0a"
                    fillOpacity={0.08}
                  />
                </AreaChart>
              </ResponsiveContainer>
            )}
          </div>
        </CardContent>
      </Card>

      <Card className="border-border">
        <CardHeader>
          <CardTitle className="flex items-center gap-3 px-1">
            <span className="flex h-10 w-10 items-center justify-center rounded-full bg-muted">
              <BarChart3 className="h-4 w-4" />
            </span>
            Completed downloads over time
          </CardTitle>
        </CardHeader>
        <CardContent className="px-1">
          <div className="h-80">
            {!completionChartData || completionChartData.length === 0 ? (
              <div className="flex h-full items-center justify-center text-muted-foreground">
                No data available
              </div>
            ) : (
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={completionChartData}>
                  <CartesianGrid stroke="#e5e5e0" vertical={false} />
                  <XAxis
                    dataKey="time"
                    tickLine={false}
                    tickMargin={10}
                    axisLine={false}
                    tick={axisStyle}
                  />
                  <YAxis
                    tick={axisStyle}
                    tickLine={false}
                    axisLine={false}
                    interval="preserveStartEnd"
                  />
                  <Tooltip
                    cursor={false}
                    contentStyle={tooltipStyle}
                  />
                  <Legend wrapperStyle={axisStyle} iconType="rect" />
                  <Bar
                    dataKey="Completed Downloads"
                    fill="#e60023"
                    fillOpacity={0.9}
                    maxBarSize={100}
                    radius={[10, 10, 0, 0]}
                  />
                </BarChart>
              </ResponsiveContainer>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default TelegramStats;
