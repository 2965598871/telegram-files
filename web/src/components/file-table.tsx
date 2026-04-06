"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import { Checkbox } from "@/components/ui/checkbox";
import { Button } from "@/components/ui/button";
import { LoaderPinwheel, SquareChevronLeft, WandSparkles } from "lucide-react";
import { useFiles } from "@/hooks/use-files";
import FileNotFount from "@/components/file-not-found";
import type { TelegramFile } from "@/lib/types";
import FileViewer from "@/components/file-viewer";
import FileFilters from "./file-filters";
import { Badge } from "@/components/ui/badge";
import FileBatchControl from "@/components/file-batch-control";
import FileImage from "@/components/file-image";
import FileStatus from "@/components/file-status";
import FileExtra from "@/components/file-extra";
import FileControl from "@/components/file-control";
import FileTags from "@/components/file-tags";
import { Progress } from "@/components/ui/progress";
import { useFileSpeed } from "@/hooks/use-file-speed";
import prettyBytes from "pretty-bytes";
import { cn } from "@/lib/utils";
import { useSettings } from "@/hooks/use-settings";
import SpoiledWrapper from "@/components/spoiled-wrapper";
import FileCaptionText from "@/components/file-caption-text";
import { groupFilesByMessage, type FileGroup } from "@/lib/file-groups";
import { formatDistanceToNow } from "date-fns";

interface FileTableProps {
  accountId: string;
  chatId: string;
  messageThreadId?: number;
  link?: string;
}

export function FileTable({
  accountId,
  chatId,
  messageThreadId,
  link,
}: FileTableProps) {
  const [selectedFiles, setSelectedFiles] = useState<Set<number>>(new Set());
  const loadMoreRef = useRef<HTMLDivElement | null>(null);
  const useFilesProps = useFiles(accountId, chatId, messageThreadId, link);
  const {
    filters,
    updateField,
    handleFilterChange,
    clearFilters,
    isLoading,
    size,
    files,
    hasMore,
    handleLoadMore,
  } = useFilesProps;
  const [currentViewFile, setCurrentViewFile] = useState<
    TelegramFile | undefined
  >();
  const [viewerOpen, setViewerOpen] = useState(false);
  const fileGroups = useMemo(() => groupFilesByMessage(files), [files]);

  useEffect(() => {
    if (files.length === 0 || !currentViewFile) {
      return;
    }

    const index = files.findIndex((file) => file.id === currentViewFile.id);
    if (index === -1) {
      setCurrentViewFile(undefined);
      return;
    }

    const file = files[index]!;
    if (currentViewFile.next === undefined && file.next !== undefined) {
      setCurrentViewFile(file);
    }
  }, [currentViewFile, files]);

  useEffect(() => {
    if (!hasMore || isLoading) {
      return;
    }

    const node = loadMoreRef.current;
    if (!node) {
      return;
    }

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry?.isIntersecting) {
          void handleLoadMore();
        }
      },
      { rootMargin: "400px 0px" },
    );

    observer.observe(node);
    return () => observer.disconnect();
  }, [handleLoadMore, hasMore, isLoading, files.length]);

  const activeFilterCount = Object.entries(filters).filter(([key, value]) => {
    if (["offline", "sort", "order", "dateType", "sizeUnit"].includes(key)) {
      return false;
    }
    if (key === "type") {
      return value !== "media";
    }
    if (typeof value === "string") {
      return value !== "";
    }
    if (typeof value === "boolean") {
      return value;
    }
    if (Array.isArray(value)) {
      return value.length > 0;
    }
    return false;
  }).length;

  const toggleSelectAll = () => {
    if (files.length === 0) {
      return;
    }

    if (selectedFiles.size === files.length) {
      setSelectedFiles(new Set());
      return;
    }

    setSelectedFiles(new Set(files.map((file) => file.id)));
  };

  const handleSelectFile = (fileId: number) => {
    const nextSelected = new Set(selectedFiles);
    if (nextSelected.has(fileId)) {
      nextSelected.delete(fileId);
    } else {
      nextSelected.add(fileId);
    }
    setSelectedFiles(nextSelected);
  };

  const handleTagClick = (tag: string) => {
    void handleFilterChange({
      ...filters,
      search: tag,
    });
  };

  return (
    <>
      {currentViewFile && (
        <FileViewer
          open={viewerOpen}
          onOpenChange={setViewerOpen}
          file={currentViewFile}
          onFileChange={setCurrentViewFile}
          {...useFilesProps}
        />
      )}

      <div className="space-y-4">
        <div className="flex flex-col gap-3 border-b border-border pb-3 md:flex-row md:items-center md:justify-between">
          <div className="flex flex-wrap items-center gap-2">
            {messageThreadId && (
              <Button variant="ghost" onClick={() => window.history.back()}>
                <SquareChevronLeft className="h-4 w-4" />
                Back
              </Button>
            )}

            {link ? (
              <Badge variant="outline" className="gap-1.5">
                <WandSparkles className="h-3 w-3" />
                {link}
              </Badge>
            ) : (
              <>
                <Badge variant="outline" className="capitalize">
                  {filters.type}
                </Badge>
                <Badge variant="outline">
                  {files.length} files
                </Badge>
                {activeFilterCount > 0 && (
                  <Badge variant="secondary">
                    {activeFilterCount} filters
                  </Badge>
                )}
                <FileFilters
                  telegramId={accountId}
                  chatId={chatId}
                  filters={filters}
                  onFiltersChange={handleFilterChange}
                  clearFilters={clearFilters}
                />
              </>
            )}
          </div>

          <Button variant="outline" size="sm" onClick={toggleSelectAll}>
            {selectedFiles.size === files.length && files.length > 0
              ? "Clear selection"
              : "Select visible"}
          </Button>
        </div>

        <FileBatchControl
          files={files}
          selectedFiles={selectedFiles}
          setSelectedFiles={setSelectedFiles}
          updateField={updateField}
        />

        <div className="border border-border rounded-[4px] bg-card p-3 md:p-4">
          {size === 1 && isLoading ? (
            <div className="flex min-h-[60vh] items-center justify-center">
              <LoaderPinwheel
                className="h-5 w-5 animate-spin text-muted-foreground"
                style={{ strokeWidth: "0.8px" }}
              />
            </div>
          ) : files.length === 0 ? (
            <FileNotFount />
          ) : (
            <>
              <div className="columns-1 gap-3 md:columns-2 xl:columns-3 2xl:columns-4">
                {fileGroups.map((group) => (
                  <FilePinGroup
                    key={group.key}
                    group={group}
                    selectedFiles={selectedFiles}
                    onCheckedChange={handleSelectFile}
                    onFileClick={(file) => {
                      setCurrentViewFile(file);
                      setViewerOpen(true);
                    }}
                    onTagClick={handleTagClick}
                    updateField={updateField}
                  />
                ))}
              </div>

              <div ref={loadMoreRef} className="flex justify-center pt-4">
                {hasMore ? (
                  <div className="inline-flex items-center gap-2 text-sm text-muted-foreground">
                    <LoaderPinwheel
                      className="h-3.5 w-3.5 animate-spin"
                      style={{ strokeWidth: "0.8px" }}
                    />
                    Loading...
                  </div>
                ) : (
                  <span className="text-sm text-muted-foreground">
                    End of list
                  </span>
                )}
              </div>
            </>
          )}
        </div>
      </div>
    </>
  );
}

function FilePinGroup({
  group,
  selectedFiles,
  onCheckedChange,
  onFileClick,
  onTagClick,
  updateField,
}: {
  group: FileGroup;
  selectedFiles: Set<number>;
  onCheckedChange: (fileId: number) => void;
  onFileClick: (file: TelegramFile) => void;
  onTagClick: (tag: string) => void;
  updateField: (
    uniqueId: string,
    patch: Partial<TelegramFile>,
  ) => Promise<void>;
}) {
  const grouped = group.files.length > 1;

  if (!grouped) {
    const file = group.files[0]!;
    return (
      <div className="mb-3 break-inside-avoid">
        <div className="overflow-hidden rounded-[4px] border border-border bg-card">
          <FilePinCard
            file={file}
            checked={selectedFiles.has(file.id)}
            onCheckedChange={() => onCheckedChange(file.id)}
            onFileClick={() => onFileClick(file)}
            onTagClick={onTagClick}
            updateField={updateField}
            grouped={false}
          />
        </div>
      </div>
    );
  }

  const firstFile = group.files[0]!;
  const caption =
    group.files.find((file) => file.caption.trim() !== "")?.caption ?? "";
  const totalSize = group.files.reduce((sum, file) => sum + file.size, 0);
  const gridCols = group.files.length >= 5 ? "grid-cols-3" : "grid-cols-2";

  return (
    <div className="mb-3 break-inside-avoid">
      <div className="overflow-hidden rounded-[4px] border border-border bg-card">
        <div className="space-y-3 p-3">
          <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
            <Badge variant="secondary" className="text-xs">
              {group.files.length} items
            </Badge>
            <span>{prettyBytes(totalSize)}</span>
            <span>&bull;</span>
            <span>
              {formatDistanceToNow(new Date(firstFile.date * 1000), {
                addSuffix: true,
              })}
            </span>
          </div>

          {caption && (
            <SpoiledWrapper hasSensitiveContent={firstFile.hasSensitiveContent}>
              <FileCaptionText
                text={caption}
                className="line-clamp-3 text-sm leading-relaxed text-foreground"
                onTagClick={onTagClick}
              />
            </SpoiledWrapper>
          )}

          <div className={cn("grid gap-2", gridCols)}>
            {group.files.map((file, index) => (
              <div
                key={`${file.messageId}-${file.uniqueId}`}
                className="space-y-1.5 rounded-[4px] border border-border p-1.5"
              >
                <div className="relative">
                  <div
                    className="absolute left-2 top-2 z-10"
                    onClick={(event) => event.stopPropagation()}
                  >
                    <Checkbox
                      checked={selectedFiles.has(file.id)}
                      onCheckedChange={() => onCheckedChange(file.id)}
                    />
                  </div>
                  <Badge className="absolute right-2 top-2 z-10 text-[11px]">
                    {index + 1}
                  </Badge>
                  <button
                    type="button"
                    className="block w-full overflow-hidden rounded-[4px] bg-muted text-left"
                    onClick={() => onFileClick(file)}
                  >
                    <div className="aspect-square overflow-hidden">
                      <FileImage
                        file={file}
                        className="h-full w-full object-cover"
                      />
                    </div>
                  </button>
                </div>

                <div className="flex items-center justify-between gap-2">
                  <div className="min-w-0 text-xs text-muted-foreground">
                    <div className="truncate font-medium text-foreground">
                      {file.type === "video" ? "Video" : "Photo"}
                    </div>
                    <div className="truncate">{prettyBytes(file.size)}</div>
                  </div>
                  <div onClick={(event) => event.stopPropagation()}>
                    <FileControl file={file} hovered={true} />
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function FilePinCard({
  file,
  checked,
  onCheckedChange,
  onFileClick,
  onTagClick,
  updateField,
  grouped,
}: {
  file: TelegramFile;
  checked: boolean;
  onCheckedChange: () => void;
  onFileClick: () => void;
  onTagClick: (tag: string) => void;
  updateField: (
    uniqueId: string,
    patch: Partial<TelegramFile>,
  ) => Promise<void>;
  grouped: boolean;
}) {
  const { settings } = useSettings();
  const { downloadProgress, downloadSpeed } = useFileSpeed(file);
  const showMessageCaption = shouldShowMessageCaption(file);

  return (
    <div
      className={cn(
        "transition-colors hover:bg-accent",
        checked && "bg-accent",
      )}
    >
      <div className="relative p-2">
        <div
          className="absolute left-4 top-4 z-10"
          onClick={(event) => event.stopPropagation()}
        >
          <Checkbox checked={checked} onCheckedChange={onCheckedChange} />
        </div>

        {file.reactionCount > 0 && (
          <Badge className="absolute right-4 top-4 z-10 text-xs">
            {file.reactionCount}
          </Badge>
        )}

        <button
          type="button"
          className="block w-full text-left"
          onClick={onFileClick}
        >
          <div
            className={cn(
              "overflow-hidden rounded-[4px] bg-muted",
              getPreviewAspect(file),
            )}
          >
            <FileImage file={file} className="h-full w-full object-cover" />
          </div>
        </button>
      </div>

      <div className="space-y-3 px-3 pb-3 pt-1">
        <div className="space-y-2">
          {showMessageCaption && (
            <SpoiledWrapper hasSensitiveContent={file.hasSensitiveContent}>
              <FileCaptionText
                text={file.caption}
                className="line-clamp-2 text-sm leading-relaxed text-foreground"
                onTagClick={onTagClick}
              />
            </SpoiledWrapper>
          )}
          <FileExtra
            file={file}
            rowHeight="s"
            ellipsis={true}
            onTagClick={onTagClick}
          />
          <div className="flex flex-wrap items-center gap-1.5 text-xs text-muted-foreground">
            <span>{prettyBytes(file.size)}</span>
            <span>&bull;</span>
            <span className="capitalize">{file.type}</span>
            {downloadSpeed > 0 && file.downloadStatus === "downloading" && (
              <>
                <span>&bull;</span>
                <span>
                  {prettyBytes(downloadSpeed, {
                    bits: settings?.speedUnits === "bits",
                  })}
                  /s
                </span>
              </>
            )}
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-1.5">
          <FileStatus file={file} className="justify-start" />
          {file.loaded && (
            <FileTags
              key={`${file.messageId}-${file.uniqueId}`}
              file={file}
              onTagsUpdate={(tags) =>
                updateField(file.uniqueId, { tags: tags.join(",") })
              }
            />
          )}
        </div>

        {downloadProgress > 0 && downloadProgress !== 100 && (
          <div className="space-y-1">
            <Progress value={downloadProgress} />
            <div className="flex items-center justify-between text-xs text-muted-foreground">
              <span>{downloadProgress.toFixed(0)}%</span>
              <span>{prettyBytes(file.downloadedSize)}</span>
            </div>
          </div>
        )}

        <div onClick={(event) => event.stopPropagation()}>
          <FileControl
            file={file}
            downloadSpeed={downloadSpeed}
            hovered={true}
          />
        </div>
      </div>
    </div>
  );
}

function getPreviewAspect(file: TelegramFile) {
  if (file.extra?.width && file.extra?.height) {
    if (file.extra.height > file.extra.width * 1.2) {
      return "aspect-[4/5]";
    }

    if (file.extra.width > file.extra.height * 1.25) {
      return "aspect-[4/3]";
    }
  }

  if (file.type === "video") {
    return "aspect-[4/3]";
  }

  return "aspect-square";
}

function shouldShowMessageCaption(file: TelegramFile) {
  return (
    (file.type === "photo" || file.type === "video") &&
    file.caption.trim() !== ""
  );
}
