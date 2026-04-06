import React from "react";
import useSWR from "swr";
import {
  AlertTriangle,
  ArrowDown,
  ArrowUp,
  CheckCircle,
  Clock,
  CloudDownload,
  Download,
  File,
  FileText,
  Image,
  LineChart,
  LoaderPinwheel,
  Music,
  Network,
  PauseCircle,
  Upload,
  Video,
} from "lucide-react";
import { telegramApi, type TelegramApiArg } from "@/lib/api";
import { formatDistanceToNow } from "date-fns";
import { Button } from "@/components/ui/button";
import useSWRMutation from "swr/mutation";
import type { TelegramApiResult } from "@/lib/types";
import prettyBytes from "pretty-bytes";
import { useSettings } from "@/hooks/use-settings";

interface StatisticsData {
  total: number;
  downloading: number;
  paused: number;
  completed: number;
  error: number;
  photo: number;
  video: number;
  audio: number;
  file: number;
  networkStatistics: {
    sinceDate: number;
    sentBytes: number;
    receivedBytes: number;
  };
  speedStats: {
    interval: number;
    avgSpeed: number;
    maxSpeed: number;
    medianSpeed: number;
    minSpeed: number;
  };
}

interface FileStatisticsProps {
  telegramId: string;
}

const FileStatistics: React.FC<FileStatisticsProps> = ({ telegramId }) => {
  const { settings } = useSettings();
  const { data, error, mutate } = useSWR<StatisticsData, Error>(
    `/telegram/${telegramId}/download-statistics`,
  );

  const { trigger: triggerReset, isMutating: isResetMutating } = useSWRMutation<
    TelegramApiResult,
    Error,
    string,
    TelegramApiArg
  >("/telegram/api", telegramApi, {
    onSuccess: () => {
      void mutate();
    },
  });

  if (error) {
    return (
      <div className="rounded-[4px] border border-border bg-card p-5 text-destructive">
        <div className="flex items-center gap-3">
          <AlertTriangle className="h-5 w-5" />
          Failed to load statistics.
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="rounded-[4px] border border-border bg-card p-5 text-muted-foreground">
        <div className="flex items-center gap-3">
          <LoaderPinwheel
            className="h-5 w-5 animate-spin"
            style={{ strokeWidth: "0.8px" }}
          />
          Loading statistics...
        </div>
      </div>
    );
  }

  const overviewStats = [
    { label: "Total files", value: data.total, icon: FileText },
    { label: "Downloading", value: data.downloading, icon: Download },
    { label: "Paused", value: data.paused, icon: PauseCircle },
    { label: "Completed", value: data.completed, icon: CheckCircle },
    { label: "Error", value: data.error, icon: AlertTriangle },
  ];

  const speedStats = [
    {
      label: "Avg",
      value:
        prettyBytes(data.speedStats.avgSpeed, {
          bits: settings?.speedUnits === "bits",
        }) + "/s",
      icon: PauseCircle,
      colorClass: "bg-[#f9d7dd] text-[#e60023]",
    },
    {
      label: "Max",
      value:
        prettyBytes(data.speedStats.maxSpeed, {
          bits: settings?.speedUnits === "bits",
        }) + "/s",
      icon: ArrowUp,
      colorClass: "bg-[#dce7dd] text-[#103c25]",
    },
    {
      label: "Median",
      value:
        prettyBytes(data.speedStats.medianSpeed, {
          bits: settings?.speedUnits === "bits",
        }) + "/s",
      icon: LineChart,
      colorClass: "bg-[#ebe2f8] text-[#6845ab]",
    },
    {
      label: "Min",
      value:
        prettyBytes(data.speedStats.minSpeed, {
          bits: settings?.speedUnits === "bits",
        }) + "/s",
      icon: ArrowDown,
      colorClass: "bg-[#f6dddd] text-[#9e0a0a]",
    },
  ];

  const completedTypes = [
    { label: "Photo", value: data.photo, icon: Image },
    { label: "Video", value: data.video, icon: Video },
    { label: "Audio", value: data.audio, icon: Music },
    { label: "File", value: data.file, icon: File },
  ];

  return (
    <div className="space-y-6">
      <SectionCard
        icon={CloudDownload}
        title="Download statistics"
        caption="Current queue and completed totals"
      >
        <div className="grid grid-cols-2 gap-3 lg:grid-cols-5">
          {overviewStats.map((stat) => (
            <MetricTile
              key={stat.label}
              label={stat.label}
              value={String(stat.value)}
              icon={stat.icon}
            />
          ))}
        </div>
      </SectionCard>

      <SectionCard
        icon={Clock}
        title="Speed statistics"
        caption={`${data.speedStats.interval / 60} minute interval`}
      >
        <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
          {speedStats.map((stat) => (
            <div
              key={stat.label}
              className="rounded-[4px] border border-border bg-muted p-4"
            >
              <div className="flex items-center gap-3">
                <div
                  className={`flex h-10 w-10 items-center justify-center rounded-full ${stat.colorClass}`}
                >
                  <stat.icon className="h-4 w-4" />
                </div>
                <span className="text-sm text-muted-foreground">{stat.label}</span>
              </div>
              <p className="mt-4 text-lg font-semibold text-foreground">
                {stat.value}
              </p>
            </div>
          ))}
        </div>
      </SectionCard>

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-[1.4fr_0.9fr]">
        <SectionCard
          icon={CheckCircle}
          title="Completed by type"
          caption="What the downloader is finishing most"
        >
          <div className="grid grid-cols-2 gap-3">
            {completedTypes.map((type) => (
              <MetricTile
                key={type.label}
                label={type.label}
                value={String(type.value)}
                icon={type.icon}
              />
            ))}
          </div>
        </SectionCard>

        <SectionCard
          icon={Network}
          title="Network statistics"
          caption="Traffic since last reset"
        >
          <div className="space-y-3">
            <MetricRow
              icon={Upload}
              label="Sent"
              value={prettyBytes(data.networkStatistics.sentBytes)}
            />
            <MetricRow
              icon={Download}
              label="Received"
              value={prettyBytes(data.networkStatistics.receivedBytes)}
            />
            <div className="flex items-center justify-between gap-3 pt-2">
              <Button
                variant="outline"
                size="sm"
                disabled={isResetMutating}
                onClick={() => {
                  void triggerReset({
                    data: {},
                    method: "ResetNetworkStatistics",
                  });
                }}
              >
                {isResetMutating ? "Resetting..." : "Reset"}
              </Button>
              <p className="text-right text-xs text-muted-foreground">
                Since{" "}
                {formatDistanceToNow(
                  new Date(data.networkStatistics.sinceDate * 1000),
                  {
                    addSuffix: true,
                  },
                )}
              </p>
            </div>
          </div>
        </SectionCard>
      </div>
    </div>
  );
};

function SectionCard({
  icon: Icon,
  title,
  caption,
  children,
}: {
  icon: typeof CloudDownload;
  title: string;
  caption: string;
  children: React.ReactNode;
}) {
  return (
    <div className="rounded-[4px] border border-border bg-card p-5 md:p-6">
      <div className="mb-5 flex items-start gap-3">
        <div className="flex h-11 w-11 items-center justify-center rounded-full bg-muted text-foreground">
          <Icon className="h-5 w-5" />
        </div>
        <div>
          <h3 className="text-xl font-semibold text-foreground">{title}</h3>
          <p className="text-sm text-muted-foreground">{caption}</p>
        </div>
      </div>
      {children}
    </div>
  );
}

function MetricTile({
  icon: Icon,
  label,
  value,
}: {
  icon: typeof FileText;
  label: string;
  value: string;
}) {
  return (
    <div className="rounded-[4px] bg-muted p-4">
      <div className="flex items-center gap-3 text-sm text-muted-foreground">
        <div className="flex h-10 w-10 items-center justify-center rounded-full bg-card">
          <Icon className="h-4 w-4" />
        </div>
        {label}
      </div>
      <p className="mt-4 text-2xl font-semibold text-foreground">{value}</p>
    </div>
  );
}

function MetricRow({
  icon: Icon,
  label,
  value,
}: {
  icon: typeof Upload;
  label: string;
  value: string;
}) {
  return (
    <div className="flex items-center justify-between rounded-[4px] bg-muted p-4">
      <div className="flex items-center gap-3 text-sm text-muted-foreground">
        <div className="flex h-10 w-10 items-center justify-center rounded-full bg-card">
          <Icon className="h-4 w-4" />
        </div>
        {label}
      </div>
      <span className="text-lg font-semibold text-foreground">{value}</span>
    </div>
  );
}

export default FileStatistics;
