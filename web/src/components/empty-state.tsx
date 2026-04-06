import {
  AlertTriangle,
  ArrowRight,
  Check,
  Download,
  HardDrive,
  LoaderPinwheel,
  MessageSquare,
  UserPlus,
} from "lucide-react";
import { AccountList } from "./account-list";
import { type TelegramAccount } from "@/lib/types";
import TelegramIcon from "@/components/telegram-icon";
import { AccountDialog } from "@/components/account-dialog";
import React from "react";
import { Button } from "@/components/ui/button";
import useSWR from "swr";
import prettyBytes from "pretty-bytes";
import { useRouter } from "next/navigation";

interface EmptyStateProps {
  isLoadingAccount?: boolean;
  hasAccounts: boolean;
  accounts?: TelegramAccount[];
  message?: string;
  onSelectAccount?: (accountId: string) => void;
}

export function EmptyState({
  isLoadingAccount,
  hasAccounts,
  accounts = [],
  message,
  onSelectAccount,
}: EmptyStateProps) {
  if (message) {
    return (
      <div className="w-full max-w-2xl border border-border rounded-[4px] bg-card">
        <div className="flex flex-col items-center gap-4 p-8 text-center">
          <MessageSquare className="h-6 w-6 text-muted-foreground" />
          <h2 className="text-xl font-bold">{message}</h2>
        </div>
      </div>
    );
  }

  return (
    <div className="app-shell px-4 py-6 md:px-6 md:py-8">
      <div className="grid gap-4 xl:grid-cols-[minmax(0,1.1fr)_380px]">
        <div className="border border-border rounded-[4px] bg-card p-6 md:p-8">
          <div className="grid gap-8 lg:grid-cols-[minmax(0,1fr)_240px] lg:items-start">
            <div className="space-y-6">
              <div className="flex h-10 w-10 items-center justify-center rounded-[4px] bg-foreground text-background">
                <TelegramIcon className="h-5 w-5" />
              </div>

              <div className="space-y-3">
                <h1 className="max-w-3xl text-2xl font-bold leading-tight sm:text-3xl md:text-4xl">
                  Telegram file downloader
                </h1>
                <p className="max-w-xl text-sm text-muted-foreground">
                  Add an account, pick a chat, download what matters.
                </p>
              </div>

              <AccountDialog isAdd={true}>
                <Button size="lg">
                  <UserPlus className="h-4 w-4" />
                  Add account
                </Button>
              </AccountDialog>
            </div>

            <div className="grid gap-2">
              <MetricTile
                icon={hasAccounts ? Check : AlertTriangle}
                label="Accounts"
                value={String(accounts.length)}
              />
              <MetricTile
                icon={Download}
                label="Downloader"
                value={isLoadingAccount ? "Syncing" : "Ready"}
              />
              <MetricTile
                icon={HardDrive}
                label="Mode"
                value="Board"
              />
            </div>
          </div>
        </div>

        <AllFiles />
      </div>

      {isLoadingAccount && (
        <div className="absolute inset-0 flex items-center justify-center">
          <LoaderPinwheel
            className="h-6 w-6 animate-spin text-muted-foreground"
            style={{ strokeWidth: "0.8px" }}
          />
        </div>
      )}

      {hasAccounts && accounts.length > 0 && onSelectAccount && (
        <div className="mt-8 space-y-4">
          <h2 className="text-lg font-bold">Accounts</h2>
          <AccountList accounts={accounts} onSelectAccount={onSelectAccount} />
        </div>
      )}
    </div>
  );
}

function MetricTile({
  icon: Icon,
  label,
  value,
}: {
  icon: typeof Check;
  label: string;
  value: string;
}) {
  return (
    <div className="border border-border rounded-[4px] bg-card p-3">
      <div className="flex items-center gap-2 text-xs text-muted-foreground">
        <Icon className="h-3.5 w-3.5" />
        {label}
      </div>
      <p className="mt-2 text-lg font-bold text-foreground">{value}</p>
    </div>
  );
}

interface FileCount {
  downloading: number;
  completed: number;
  downloadedSize: number;
}

function AllFiles() {
  const router = useRouter();
  const { data, error, isLoading } = useSWR<FileCount, Error>(`/files/count`);

  if (error) {
    return (
      <div className="border border-border rounded-[4px] bg-card">
        <div className="flex h-full min-h-[240px] items-center justify-center gap-2 p-4 text-destructive">
          <AlertTriangle className="h-4 w-4" />
          Failed to load library
        </div>
      </div>
    );
  }

  if (isLoading || !data) {
    return (
      <div className="border border-border rounded-[4px] bg-card">
        <div className="flex h-full min-h-[240px] items-center justify-center gap-2 p-4 text-muted-foreground">
          <LoaderPinwheel
            className="h-4 w-4 animate-spin"
            style={{ strokeWidth: "0.8px" }}
          />
          Loading library
        </div>
      </div>
    );
  }

  return (
    <div className="border border-border rounded-[4px] bg-card flex h-full flex-col gap-4 p-4 md:p-6">
      <h2 className="text-base font-bold">Library</h2>

      <div className="grid gap-2 sm:grid-cols-3 xl:grid-cols-1">
        <StatTile icon={Check} label="Downloaded" value={String(data.completed)} />
        <StatTile
          icon={Download}
          label="Downloading"
          value={String(data.downloading)}
        />
        <StatTile
          icon={HardDrive}
          label="Storage"
          value={prettyBytes(data.downloadedSize)}
        />
      </div>

      <Button
        variant="outline"
        className="w-full"
        onClick={() => router.push("/files")}
      >
        Open library
        <ArrowRight className="h-4 w-4" />
      </Button>
    </div>
  );
}

function StatTile({
  icon: Icon,
  label,
  value,
}: {
  icon: typeof Check;
  label: string;
  value: string;
}) {
  return (
    <div className="border border-border rounded-[4px] p-3">
      <div className="flex items-center gap-2 text-xs text-muted-foreground">
        <Icon className="h-3.5 w-3.5" />
        {label}
      </div>
      <p className="mt-2 text-lg font-bold text-foreground">{value}</p>
    </div>
  );
}
