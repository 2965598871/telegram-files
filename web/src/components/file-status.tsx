import { type TelegramFile } from "@/lib/types";
import { Badge } from "@/components/ui/badge";
import React from "react";
import { cn } from "@/lib/utils";
import { TooltipWrapper } from "@/components/ui/tooltip";
import { AnimatePresence, motion } from "framer-motion";
import {
  CheckCircle2,
  Clock,
  Download,
  FolderSync,
  Pause,
  XCircle,
} from "lucide-react";
import useIsMobile from "@/hooks/use-is-mobile";

export const DOWNLOAD_STATUS = {
  idle: {
    icon: Clock,
    className: "bg-[#f0efe9] text-[#62625b]",
    text: "Idle",
  },
  downloading: {
    icon: Download,
    className: "bg-[#f9d7dd] text-[#e60023]",
    text: "Downloading",
  },
  paused: {
    icon: Pause,
    className: "bg-[#f3ead7] text-[#8a5b21]",
    text: "Paused",
  },
  completed: {
    icon: CheckCircle2,
    className: "bg-[#dce7dd] text-[#103c25]",
    text: "Completed",
  },
  error: {
    icon: XCircle,
    className: "bg-[#f6dddd] text-[#9e0a0a]",
    text: "Error",
  },
};

export const TRANSFER_STATUS = {
  idle: {
    icon: Clock,
    className: "bg-[#f0efe9] text-[#62625b]",
    text: "Idle",
  },
  transferring: {
    icon: FolderSync,
    className: "bg-[#f9d7dd] text-[#e60023]",
    text: "Transferring",
  },
  completed: {
    icon: CheckCircle2,
    className: "bg-[#dce7dd] text-[#103c25]",
    text: "Transferred",
  },
  error: {
    icon: XCircle,
    className: "bg-[#f6dddd] text-[#9e0a0a]",
    text: "Transfer Error",
  },
};

export default function FileStatus({
  file,
  className,
}: {
  file: TelegramFile;
  className?: string;
}) {
  const badgeVariants = {
    initial: { opacity: 0, scale: 0.9 },
    animate: {
      opacity: 1,
      scale: 1,
      transition: { type: "spring", stiffness: 300 },
    },
    exit: { opacity: 0, scale: 0.9, transition: { duration: 0.2 } },
  };
  const isMobile = useIsMobile();

  return (
    <div
      className={cn("flex items-center justify-center space-x-2", className)}
    >
      <AnimatePresence>
        {file.transferStatus === "idle" && (
          <motion.div
            key="download-status"
            variants={badgeVariants}
            initial="initial"
            animate="animate"
            exit="exit"
          >
            <TooltipWrapper content="Download Status">
                <Badge
                  className={cn(
                    "h-7 text-xs",
                    DOWNLOAD_STATUS[file.downloadStatus].className,
                    isMobile && "",
                  )}
              >
                {DOWNLOAD_STATUS[file.downloadStatus].text}
              </Badge>
            </TooltipWrapper>
          </motion.div>
        )}
        {file.downloadStatus === "completed" &&
          file.transferStatus &&
          file.transferStatus !== "idle" && (
            <motion.div
              key="transfer-status"
              variants={badgeVariants}
              initial="initial"
              animate="animate"
              exit="exit"
            >
              <TooltipWrapper content="Transfer Status">
                <Badge
                  className={cn(
                    "h-7 text-xs",
                    TRANSFER_STATUS[file.transferStatus].className,
                  )}
                >
                  {TRANSFER_STATUS[file.transferStatus].text}
                </Badge>
              </TooltipWrapper>
            </motion.div>
          )}
      </AnimatePresence>
    </div>
  );
}
