"use client";

import {
  ChevronsLeftRightEllipsisIcon,
  Download,
  UnplugIcon,
} from "lucide-react";
import { TooltipWrapper } from "./ui/tooltip";
import { Badge } from "@/components/ui/badge";
import { useWebsocket } from "@/hooks/use-websocket";
import { useTelegramAccount } from "@/hooks/use-telegram-account";
import { SettingsDialog } from "@/components/settings-dialog";
import prettyBytes from "pretty-bytes";
import ChatSelect from "@/components/chat-select";
import Link from "next/link";
import TelegramIcon from "@/components/telegram-icon";
import AutomationDialog from "@/components/automation-dialog";
import ThemeToggleButton from "@/components/theme-toggle-button";
import AccountSelect from "@/components/account-select";
import { useSearchParams } from "next/navigation";
import { useSettings } from "@/hooks/use-settings";

export function Header() {
  const useTelegramAccountProps = useTelegramAccount();
  const { connectionStatus, accountDownloadSpeed } = useWebsocket();
  const { settings } = useSettings();
  const searchParams = useSearchParams();
  const messageThreadId = searchParams.get("messageThreadId");

  return (
    <div className="sticky top-0 z-20 mb-4 border-b border-border bg-background">
      <div className="flex flex-col gap-3 py-3">
        <div className="flex items-center justify-between gap-4">
          <Link
            href="/"
            className="inline-flex items-center gap-2"
          >
            <div className="flex h-8 w-8 items-center justify-center rounded-[4px] bg-foreground text-background">
              <TelegramIcon className="h-4 w-4" />
            </div>
            <span className="text-base font-bold text-foreground">
              TeleFiles
            </span>
          </Link>

          <div className="flex items-center gap-2">
            {accountDownloadSpeed !== 0 && (
              <TooltipWrapper content="Download speed">
                <Badge variant="outline" className="gap-1.5 text-xs">
                  <Download className="h-3 w-3" />
                  {`${prettyBytes(accountDownloadSpeed, { bits: settings?.speedUnits === "bits" })}/s`}
                </Badge>
              </TooltipWrapper>
            )}

            {connectionStatus && (
              <TooltipWrapper content="WebSocket status">
                <Badge
                  variant={
                    connectionStatus === "Open" ? "default" : "secondary"
                  }
                  className="gap-1.5 text-xs"
                >
                  {connectionStatus === "Open" ? (
                    <ChevronsLeftRightEllipsisIcon className="h-3 w-3" />
                  ) : (
                    <UnplugIcon className="h-3 w-3" />
                  )}
                  {connectionStatus}
                </Badge>
              </TooltipWrapper>
            )}

            <ThemeToggleButton />
            <SettingsDialog />
          </div>
        </div>

        <div className="grid gap-2 xl:grid-cols-[240px_minmax(320px,420px)_auto]">
          <AccountSelect {...useTelegramAccountProps} />
          <ChatSelect disabled={!useTelegramAccountProps.accountId} />
          <div className="flex items-center justify-between gap-2 border border-border rounded-[4px] px-3 py-2 text-sm text-muted-foreground">
            <span>{messageThreadId ? "Thread board" : "Browse by chat"}</span>
            {!messageThreadId && <AutomationDialog />}
          </div>
        </div>
      </div>
    </div>
  );
}
