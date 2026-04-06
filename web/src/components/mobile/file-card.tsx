import type { TelegramFile } from "@/lib/types";
import { useFileSpeed } from "@/hooks/use-file-speed";
import { Card, CardContent } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import prettyBytes from "pretty-bytes";
import FileStatus from "@/components/file-status";
import FileControl from "@/components/file-control";
import React from "react";
import FileExtra from "@/components/file-extra";
import FileImage from "../file-image";
import { MobileFileTags } from "@/components/file-tags";
import { TooltipWrapper } from "@/components/ui/tooltip";
import { Badge } from "@/components/ui/badge";
import SpoiledWrapper from "@/components/spoiled-wrapper";
import FileCaptionText from "@/components/file-caption-text";

type FileCardProps = {
  index: number;
  className?: string;
  style?: React.CSSProperties;
  ref?: React.Ref<HTMLDivElement>;
  file: TelegramFile;
  onFileClick: () => void;
  onFileTagsClick?: () => void;
  onTagClick?: (tag: string) => void;
  groupInfo?: {
    count: number;
    index: number;
  };
  layout: "detailed" | "gallery";
};

export function FileCard({
  index,
  className,
  style,
  ref,
  file,
  onFileClick,
  onFileTagsClick,
  onTagClick,
  groupInfo,
  layout,
}: FileCardProps) {
  const { downloadProgress } = useFileSpeed(file);
  const isGalleryLayout = layout === "gallery";
  const showMessageCaption =
    shouldShowMessageCaption(file) && (!groupInfo || groupInfo.index === 0);

  return (
    <Card
      ref={ref}
      data-index={index}
      className={cn(
        "before:ease-[cubic-bezier(0.4,0,0.2,1)] before:will-change:transform relative overflow-hidden rounded-[4px] border border-border before:absolute before:bottom-0 before:left-0 before:top-auto before:z-10 before:h-1.5 before:transform before:rounded-r-full before:bg-primary before:duration-500 before:content-['']",
        downloadProgress > 0 && downloadProgress !== 100
          ? `before:w-progress`
          : "before:w-0",
        className,
      )}
      style={{
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-expect-error
        "--tw-progress-width": `${downloadProgress > 0 && downloadProgress !== 100 ? downloadProgress.toFixed(0) + "%" : "0"}`,
        ...style,
      }}
      onClick={onFileClick}
    >
      <CardContent className="relative z-20 w-full p-3">
        <div
          className={cn(
            "flex items-center gap-4",
            isGalleryLayout && "flex-col justify-center gap-3",
          )}
        >
          {file.reactionCount > 0 && (
            <TooltipWrapper content="Reaction Count">
              <Badge className="absolute right-4 top-4 z-10 rounded-full bg-primary px-2 py-1 text-xs text-primary-foreground">
                {file.reactionCount}
              </Badge>
            </TooltipWrapper>
          )}

          {groupInfo && groupInfo.count > 1 && (
            <Badge
              variant="secondary"
              className="absolute left-4 top-4 z-10 rounded-full px-2 py-1 text-[11px]"
            >
              {groupInfo.index + 1}/{groupInfo.count} same message
            </Badge>
          )}

          <div
            className={cn(
              "overflow-hidden rounded-[4px] bg-muted",
              isGalleryLayout ? "w-full" : "h-20 w-20 min-w-20",
            )}
          >
            <FileImage
              file={file}
              className={cn(
                "h-full w-full object-cover",
                isGalleryLayout ? "aspect-[4/5]" : "h-20 w-20",
              )}
            />
          </div>

          {isGalleryLayout ? (
            <div className="w-full space-y-3 px-1 pb-1">
              {showMessageCaption && (
                <SpoiledWrapper hasSensitiveContent={file.hasSensitiveContent}>
                  <FileCaptionText
                    text={file.caption}
                    className="line-clamp-2 text-sm leading-5 text-foreground"
                    onTagClick={onTagClick}
                  />
                </SpoiledWrapper>
              )}
              <FileExtra
                file={file}
                rowHeight="s"
                ellipsis={true}
                onTagClick={onTagClick}
                suppressCaption={Boolean(groupInfo && groupInfo.index > 0)}
              />
              <div className="flex items-center justify-between gap-3">
                <div className="flex flex-wrap items-center gap-2">
                  <FileStatus file={file} className="justify-start" />
                  {file.loaded && (
                    <MobileFileTags
                      tags={file.tags}
                      onClick={onFileTagsClick}
                    />
                  )}
                </div>
                <span className="text-xs text-muted-foreground">
                  {prettyBytes(file.size)}
                </span>
              </div>
            </div>
          ) : (
            <div className="flex-1 overflow-hidden">
              {showMessageCaption && (
                <SpoiledWrapper hasSensitiveContent={file.hasSensitiveContent}>
                  <FileCaptionText
                    text={file.caption}
                    className="mb-2 line-clamp-2 text-sm leading-5 text-foreground"
                    onTagClick={onTagClick}
                  />
                </SpoiledWrapper>
              )}
              <FileExtra
                file={file}
                rowHeight="s"
                ellipsis={true}
                onTagClick={onTagClick}
                suppressCaption={Boolean(groupInfo && groupInfo.index > 0)}
              />
              <div className="flex items-center justify-between">
                <div className="flex flex-col justify-start gap-0.5">
                  <span className="text-xs text-muted-foreground">
                    {prettyBytes(file.size)} • {file.type}
                  </span>
                  <div className="flex items-center gap-1">
                    <FileStatus file={file} className="justify-start" />
                    {file.loaded && (
                      <MobileFileTags
                        tags={file.tags}
                        onClick={onFileTagsClick}
                      />
                    )}
                  </div>
                </div>

                <div
                  className="flex items-center justify-end"
                  onClick={(e) => e.stopPropagation()}
                >
                  <FileControl file={file} isMobile={true} />
                </div>
              </div>
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

function shouldShowMessageCaption(file: TelegramFile) {
  return (
    (file.type === "photo" || file.type === "video") &&
    file.caption.trim() !== ""
  );
}
