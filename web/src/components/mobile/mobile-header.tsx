"use client";

import {
  ChevronsLeftRightEllipsisIcon,
  Download,
  Ellipsis,
  GalleryHorizontal,
  List,
  UnplugIcon,
} from "lucide-react";
import { useWebsocket } from "@/hooks/use-websocket";
import { useTelegramAccount } from "@/hooks/use-telegram-account";
import prettyBytes from "pretty-bytes";
import Link from "next/link";
import { Drawer as DrawerPrimitive } from "vaul";
import TelegramIcon from "@/components/telegram-icon";
import { Button } from "@/components/ui/button";
import {
  Drawer,
  DrawerOverlay,
  DrawerPortal,
  DrawerTitle,
  DrawerTrigger,
} from "@/components/ui/drawer";
import React, { type CSSProperties } from "react";
import AccountSelect from "@/components/account-select";
import ChatSelect from "@/components/chat-select";
import { cn } from "@/lib/utils";
import AutomationDialog from "@/components/automation-dialog";
import { Badge } from "@/components/ui/badge";
import ThemeToggleButton from "@/components/theme-toggle-button";
import { SettingsDialog } from "@/components/settings-dialog";
import { Label } from "../ui/label";
import { Toggle } from "@/components/ui/toggle";
import { useLocalStorage } from "@/hooks/use-local-storage";
import { useTelegramChat } from "@/hooks/use-telegram-chat";
import { useSettings } from "@/hooks/use-settings";

export function MobileHeader() {
  const { accountDownloadSpeed } = useWebsocket();
  const { settings } = useSettings();

  return (
    <div className="sticky top-0 z-30 mb-3 border-b border-border bg-background">
      <div className="flex w-full items-center justify-between py-3">
        <Link href="/" className="inline-flex items-center gap-2">
          <div className="flex h-7 w-7 items-center justify-center rounded-[4px] bg-foreground text-background">
            <TelegramIcon className="h-3.5 w-3.5" />
          </div>
          <span className="text-sm font-bold">TeleFiles</span>
        </Link>

        {accountDownloadSpeed !== 0 && (
          <Badge variant="outline" className="gap-1.5 text-xs">
            <Download className="h-3 w-3" />
            {`${prettyBytes(accountDownloadSpeed, { bits: settings?.speedUnits === "bits" })}/s`}
          </Badge>
        )}

        <MenuDrawer />
      </div>
    </div>
  );
}

function MenuDrawer() {
  const useTelegramAccountProps = useTelegramAccount();
  const { chat } = useTelegramChat();
  const { connectionStatus } = useWebsocket();
  const [layout, setLayout] = useLocalStorage<"detailed" | "gallery">(
    "telegramFileLayout",
    "gallery",
  );

  return (
    <Drawer
      direction="left"
      shouldScaleBackground={true}
      preventScrollRestoration={true}
    >
      <DrawerTrigger asChild>
        <Button size="xs" variant="ghost">
          <Ellipsis className="h-4 w-4" />
        </Button>
      </DrawerTrigger>
      <DrawerPortal>
        <DrawerOverlay />
        <DrawerPrimitive.Content
          className={cn(
            "fixed bottom-0 left-0 top-0 z-50 flex w-4/5 outline-none",
          )}
          style={{ "--initial-transform": "calc(100% + 8px)" } as CSSProperties}
          aria-describedby={undefined}
        >
          <div className="flex h-full w-full grow flex-col border-r border-border bg-background p-4">
            <DrawerTitle className="mb-6 text-base font-bold">
              TeleFiles
            </DrawerTitle>
            <div className="flex h-full flex-col justify-between">
              <div className="flex flex-1 flex-col gap-3">
                <AccountSelect {...useTelegramAccountProps} />
                <ChatSelect disabled={!useTelegramAccountProps.accountId} />
              </div>
              <div className="flex flex-col gap-3">
                <div className="flex flex-col gap-1">
                  <Label className="text-xs font-bold text-muted-foreground">
                    Auto Download
                  </Label>
                  {chat ? (
                    <AutomationDialog />
                  ) : (
                    <Button
                      variant="outline"
                      className="w-full"
                      disabled={true}
                    >
                      No chat selected
                    </Button>
                  )}
                </div>
                <div className="flex flex-col gap-1">
                  <Label className="text-xs font-bold text-muted-foreground">
                    Layout
                  </Label>
                  <Toggle
                    className="w-full border border-input rounded-[4px]"
                    pressed={layout === "gallery"}
                    onPressedChange={(pressed) => {
                      setLayout(pressed ? "gallery" : "detailed");
                    }}
                  >
                    {layout === "detailed" ? (
                      <>
                        <List className="h-4 w-4" />
                        <span>Detailed</span>
                      </>
                    ) : (
                      <>
                        <GalleryHorizontal className="h-4 w-4" />
                        <span>Gallery</span>
                      </>
                    )}
                  </Toggle>
                </div>
              </div>
              <div className="flex items-center justify-between gap-2 border-t border-border pt-3 mt-3">
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

                <ThemeToggleButton />
                <SettingsDialog />
              </div>
            </div>
          </div>
        </DrawerPrimitive.Content>
      </DrawerPortal>
    </Drawer>
  );
}
