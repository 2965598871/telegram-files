"use client";
import React from "react";
import { Bot } from "lucide-react";
import { cn } from "@/lib/utils";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { type TelegramChat } from "@/lib/types";
import UseIsMobile from "@/hooks/use-is-mobile";

interface AutoDownloadButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  auto?: TelegramChat["auto"];
}

const AutomationButton = React.forwardRef<
  HTMLButtonElement,
  AutoDownloadButtonProps
>(({ auto, className, ...props }, ref) => {
  const autoEnabled =
    auto &&
    (auto.preload.enabled || auto.download.enabled || auto.transfer.enabled);
  const isMobile = UseIsMobile();

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            ref={ref}
            className={cn(
              "inline-flex h-10 w-36 items-center justify-center gap-2 rounded-[4px] border border-input bg-card px-4 text-sm font-medium text-foreground transition-colors hover:bg-accent",
              isMobile && "w-full",
              className,
            )}
            {...props}
          >
            <span
              className={cn(
                "h-2.5 w-2.5 rounded-full",
                autoEnabled ? "bg-[#103c25]" : "bg-[#91918c]",
              )}
            />
            <Bot className="h-4 w-4" />
            <span>{autoEnabled ? "Automation on" : "Automation off"}</span>
            <div
              className={cn(
                "ml-1 rounded-full px-2 py-1 text-[11px]",
                autoEnabled ? "bg-[#dce7dd] text-[#103c25]" : "bg-muted text-muted-foreground",
              )}
            >
              {autoEnabled ? "Live" : "Idle"}
            </div>
          </button>
        </TooltipTrigger>
        <TooltipContent>
          {autoEnabled
            ? "Automation is enabled, you can disable it by clicking the button"
            : "Automation is disabled, you can enable it by clicking the button"}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
});

AutomationButton.displayName = "AutoDownloadButton";

export { AutomationButton };
