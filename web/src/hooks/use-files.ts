import { useCallback, useEffect, useMemo, useState } from "react";
import {
  type DownloadStatus,
  type FileFilter,
  type TelegramFile,
  type Thumbnail,
  type TransferStatus,
} from "@/lib/types";
import useSWRInfinite from "swr/infinite";
import { useWebsocket } from "@/hooks/use-websocket";
import { WebSocketMessageType } from "@/lib/websocket-types";
import { useLocalStorage } from "@/hooks/use-local-storage";
import { getFilesApiPath, isGroupChatId } from "@/lib/chat-target";
import { request } from "@/lib/api";

const DEFAULT_FILTERS: FileFilter = {
  search: "",
  type: "media",
  downloadStatus: undefined,
  transferStatus: undefined,
  alreadyDownloaded: false,
  offline: false,
  tags: [],
};

type FileResponse = {
  files: TelegramFile[];
  count: number;
  nextFromMessageId: number;
};

const getFileCacheKey = (file: TelegramFile) =>
  `${file.telegramId}:${file.chatId}:${file.messageId}:${file.uniqueId}`;

const getFileStatusKey = (fileId: number | undefined, uniqueId: string) =>
  `${fileId ?? 0}:${uniqueId}`;

const getVisibleFileIdsForUniqueId = (
  pages: FileResponse[] | undefined,
  uniqueId: string,
) => {
  const ids = new Set<number>();
  pages?.forEach((page) => {
    page.files.forEach((file) => {
      if (file.uniqueId === uniqueId) {
        ids.add(file.id ?? 0);
      }
    });
  });
  return ids;
};

const rebuildVisiblePages = (
  currentPages: FileResponse[],
  freshFirstPage: FileResponse,
) => {
  if (currentPages.length === 0) {
    return [freshFirstPage];
  }

  const pageLengths = currentPages.map((page, index) =>
    index === 0 ? freshFirstPage.files.length : page.files.length,
  );
  const totalSlots = pageLengths.reduce((sum, pageLength) => sum + pageLength, 0);
  if (totalSlots === 0) {
    return [freshFirstPage];
  }

  const seen = new Set<string>();
  const mergedFiles: TelegramFile[] = [];
  for (const file of [
    ...freshFirstPage.files,
    ...currentPages.flatMap((page) => page.files),
  ]) {
    const fileKey = getFileCacheKey(file);
    if (seen.has(fileKey)) {
      continue;
    }
    seen.add(fileKey);
    mergedFiles.push(file);
  }

  const trimmedFiles = mergedFiles.slice(0, totalSlots);
  const currentLastPage = currentPages[currentPages.length - 1];
  const hasMoreBeyondLoaded =
    mergedFiles.length > trimmedFiles.length ||
    freshFirstPage.count > trimmedFiles.length ||
    (currentLastPage?.nextFromMessageId ?? 0) !== 0;

  let offset = 0;
  return currentPages.map((page, index) => {
    const pageLength = pageLengths[index] ?? 0;
    const pageFiles = trimmedFiles.slice(offset, offset + pageLength);
    offset += pageLength;

    const lastFile = pageFiles[pageFiles.length - 1];
    const isLastPage = index === currentPages.length - 1;
    const hasLoadedNextPage = offset < trimmedFiles.length;

    return {
      ...page,
      files: pageFiles,
      count: isLastPage
        ? hasMoreBeyondLoaded
          ? Math.max(
              trimmedFiles.length + 1,
              freshFirstPage.count,
              currentLastPage?.count ?? 0,
            )
          : trimmedFiles.length
        : page.count,
      nextFromMessageId: lastFile
        ? hasLoadedNextPage || (isLastPage && hasMoreBeyondLoaded)
          ? lastFile.messageId
          : 0
        : 0,
    };
  });
};

export function useFiles(
  accountId: string,
  chatId: string,
  messageThreadId?: number,
  link?: string,
) {
  const noAccountSpecified = accountId === "-1" && chatId === "-1";
  const isGroupChat = isGroupChatId(chatId);
  const url = noAccountSpecified
    ? "/files"
    : getFilesApiPath(accountId, chatId);
  const { lastJsonMessage } = useWebsocket();
  const [latestFilesStatus, setLatestFileStatus] = useState<
    Record<
      string,
      {
        fileId: number;
        downloadStatus: DownloadStatus;
        localPath?: string;
        completionDate?: number;
        downloadedSize: number;
        transferStatus?: TransferStatus;
        thumbnailFile?: Thumbnail;
        removed?: boolean;
      }
    >
  >({});
  const [filters, setFilters, clearFilters] = useLocalStorage<FileFilter>(
    "telegramFileListFilter",
    { ...DEFAULT_FILTERS, offline: noAccountSpecified || isGroupChat },
  );
  const createSearchParams = useCallback(() => {
    const effectiveSort = filters.sort ?? (isGroupChat ? "date" : undefined);
    const effectiveOrder = filters.order ?? (isGroupChat ? "desc" : undefined);
    return new URLSearchParams({
      ...(filters.search && {
        search: window.encodeURIComponent(filters.search),
      }),
      ...(filters.type && { type: filters.type }),
      ...(filters.downloadStatus && { downloadStatus: filters.downloadStatus }),
      ...(filters.transferStatus && { transferStatus: filters.transferStatus }),
      ...(filters.alreadyDownloaded && { alreadyDownloaded: "true" }),
      ...(filters.offline && { offline: "true" }),
      ...(filters.tags.length > 0 && {
        tags: filters.tags.join(","),
      }),
      ...(messageThreadId && { messageThreadId: messageThreadId.toString() }),
      ...(link && { link: window.encodeURIComponent(link) }),
      ...(filters.dateType && { dateType: filters.dateType }),
      ...(filters.dateRange && { dateRange: filters.dateRange.join(",") }),
      ...(filters.sizeRange && { sizeRange: filters.sizeRange.join(",") }),
      ...(filters.sizeUnit && { sizeUnit: filters.sizeUnit }),
      ...(effectiveSort && { sort: effectiveSort }),
      ...(effectiveOrder && { order: effectiveOrder }),
    });
  }, [filters, isGroupChat, link, messageThreadId]);

  const getKey = useCallback(
    (page: number, previousPageData: FileResponse | null) => {
      const effectiveSort = filters.sort ?? (isGroupChat ? "date" : undefined);
      const params = createSearchParams();

      if (page === 0) {
        return `${url}?${params.toString()}`;
      }

      if (!previousPageData) {
        return null;
      }

      params.set("fromMessageId", previousPageData.nextFromMessageId.toString());
      if ((filters.offline || isGroupChat) && previousPageData.files.length > 0) {
        const lastFile = previousPageData.files[previousPageData.files.length - 1];
        if (effectiveSort === "size") {
          params.set("fromSortField", lastFile!.size.toString());
        } else if (effectiveSort === "completion_date") {
          params.set("fromSortField", lastFile!.completionDate.toString());
        } else if (effectiveSort === "date") {
          params.set("fromSortField", lastFile!.date.toString());
        } else if (effectiveSort === "reaction_count") {
          params.set("fromSortField", lastFile!.reactionCount.toString());
        }
      }
      return `${url}?${params.toString()}`;
    },
    [createSearchParams, filters.offline, isGroupChat, filters.sort, url],
  );

  const {
    data: pages,
    isLoading,
    isValidating,
    size,
    setSize,
    error,
    mutate,
  } = useSWRInfinite<FileResponse, Error>(getKey, {
    revalidateFirstPage: false,
    keepPreviousData: true,
    revalidateOnFocus: false,
    revalidateOnReconnect: false,
  });

  const refreshVisiblePages = useCallback(async () => {
    const firstPageKey = getKey(0, null);
    if (!firstPageKey) {
      return;
    }

    const freshFirstPage = await request<FileResponse>(firstPageKey);
    await mutate((currentPages) => {
      if (!currentPages || currentPages.length === 0) {
        return [freshFirstPage];
      }
      return rebuildVisiblePages(currentPages, freshFirstPage);
    }, false);
  }, [getKey, mutate]);

  const isLoadingMore = Boolean(pages) && size > (pages?.length ?? 0);
  const isInitialLoading = !pages && (isLoading || isValidating);
  const isRefreshing = Boolean(pages) && isValidating && !isLoadingMore;

  useEffect(() => {
    if (lastJsonMessage?.type !== WebSocketMessageType.FILE_STATUS) {
      return;
    }
    const data = lastJsonMessage.data as {
      fileId: number;
      uniqueId: string;
      downloadStatus: DownloadStatus;
      localPath: string;
      completionDate: number;
      downloadedSize: number;
      transferStatus?: TransferStatus;
      thumbnailFile?: Thumbnail;
      removed?: boolean;
    };

    const visibleFileIds = getVisibleFileIdsForUniqueId(pages, data.uniqueId);
    const exactStatusKey = getFileStatusKey(data.fileId, data.uniqueId);
    const aliasStatusKey =
      visibleFileIds.size === 1
        ? getFileStatusKey([...visibleFileIds][0], data.uniqueId)
        : null;

    if (data.removed) {
      setLatestFileStatus((prev) => ({
        ...prev,
        [exactStatusKey]: {
          fileId: data.fileId,
          downloadStatus: "idle",
          localPath: undefined,
          completionDate: undefined,
          downloadedSize: 0,
          transferStatus: "idle",
          removed: true,
        },
        ...(aliasStatusKey && aliasStatusKey !== exactStatusKey
          ? {
              [aliasStatusKey]: {
                fileId: data.fileId,
                downloadStatus: "idle",
                localPath: undefined,
                completionDate: undefined,
                downloadedSize: 0,
                transferStatus: "idle",
                removed: true,
              },
            }
          : {}),
      }));
      return;
    }

    setLatestFileStatus((prev) => ({
      ...prev,
      [exactStatusKey]: {
        fileId: data.fileId,
        downloadStatus:
          data.downloadStatus ?? prev[exactStatusKey]?.downloadStatus,
        localPath: data.localPath ?? prev[exactStatusKey]?.localPath,
        completionDate:
          data.completionDate ?? prev[exactStatusKey]?.completionDate,
        downloadedSize:
          data.downloadedSize ?? prev[exactStatusKey]?.downloadedSize,
        transferStatus:
          data.transferStatus ?? prev[exactStatusKey]?.transferStatus,
        thumbnailFile: data.thumbnailFile ?? prev[exactStatusKey]?.thumbnailFile,
      },
      ...(aliasStatusKey && aliasStatusKey !== exactStatusKey
        ? {
            [aliasStatusKey]: {
              fileId: data.fileId,
              downloadStatus:
                data.downloadStatus ?? prev[aliasStatusKey]?.downloadStatus,
              localPath: data.localPath ?? prev[aliasStatusKey]?.localPath,
              completionDate:
                data.completionDate ?? prev[aliasStatusKey]?.completionDate,
              downloadedSize:
                data.downloadedSize ?? prev[aliasStatusKey]?.downloadedSize,
              transferStatus: prev[aliasStatusKey]?.transferStatus,
              thumbnailFile:
                data.thumbnailFile ?? prev[aliasStatusKey]?.thumbnailFile,
            },
          }
        : {}),
    }));
  }, [lastJsonMessage, pages]);

  useEffect(() => {
    if (!accountId || !chatId) {
      return;
    }
    if (lastJsonMessage?.type !== WebSocketMessageType.CHAT_UPDATE) {
      return;
    }

    const data = lastJsonMessage.data as {
      telegramId?: string;
      chatId?: string;
    };
    if ((data.telegramId ?? "") !== accountId) {
      return;
    }
    if (!isGroupChat && (data.chatId ?? "") !== chatId) {
      return;
    }

    const timeoutId = window.setTimeout(() => {
      void refreshVisiblePages();
    }, 300);

    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [accountId, chatId, isGroupChat, lastJsonMessage, refreshVisiblePages]);

  useEffect(() => {
    if ((noAccountSpecified || isGroupChat) && !filters.offline) {
      setFilters((prev) => ({
        ...prev,
        offline: true,
      }));
    }
  }, [filters.offline, isGroupChat, noAccountSpecified, setFilters]);

  const files = useMemo(() => {
    if (!pages) return [];
    const files: TelegramFile[] = [];
    pages.forEach((page) => {
      page.files.forEach((file) => {
        const statusKey = getFileStatusKey(file.id, file.uniqueId);
        if (file.originalDeleted && latestFilesStatus[statusKey]?.removed) {
          return;
        }
        files.push({
          ...file,
          id: latestFilesStatus[statusKey]?.fileId ?? file.id,
          downloadStatus:
            latestFilesStatus[statusKey]?.downloadStatus ??
            file.downloadStatus,
          localPath:
            latestFilesStatus[statusKey]?.localPath ?? file.localPath,
          completionDate:
            latestFilesStatus[statusKey]?.completionDate ??
            file.completionDate,
          downloadedSize:
            latestFilesStatus[statusKey]?.downloadedSize ??
            file.downloadedSize,
          transferStatus:
            latestFilesStatus[statusKey]?.transferStatus ??
            file.transferStatus,
          thumbnailFile:
            latestFilesStatus[statusKey]?.thumbnailFile ??
            file.thumbnailFile,
        });
      });
    });
    files.forEach((file, index) => {
      file.prev = files[index - 1];
      file.next = files[index + 1];
    });
    return files;
  }, [pages, latestFilesStatus]);

  const hasMore = useMemo(() => {
    if (!pages || pages.length === 0) return true;

    const fetchedCount = pages.reduce((acc, d) => acc + d.files.length, 0);
    const lastPage = pages[pages.length - 1];
    let hasMore = false;
    if (lastPage) {
      const count = lastPage.count;
      hasMore = count > fetchedCount && lastPage.nextFromMessageId !== 0;
    }
    return hasMore;
  }, [pages]);

  const handleLoadMore = async () => {
    if (isLoading || isValidating || !hasMore || error) return;
    await setSize(size + 1);
  };

  const handleFilterChange = async (newFilters: FileFilter) => {
    if (
      Object.keys(newFilters).every(
        (key) =>
          newFilters[key as keyof FileFilter] ===
          filters[key as keyof FileFilter],
      )
    ) {
      return;
    }
    setFilters(newFilters);
    await setSize(1);
  };

  const updateField = async (
    uniqueId: string,
    patch: Partial<TelegramFile>,
  ) => {
    await mutate((pages) => {
      if (!pages) return [];

      return pages.map((page) => {
        const newFiles = page.files.map((file) =>
          file.uniqueId === uniqueId ? { ...file, ...patch } : file,
        );
        return {
          ...page,
          files: newFiles,
        };
      });
    }, false);
  };

  const reload = async () => {
    setLatestFileStatus({});
    if (!pages || pages.length === 0) {
      await mutate();
      return;
    }
    await refreshVisiblePages();
  };

  return {
    size,
    files,
    filters,
    isLoading: isInitialLoading || isLoadingMore,
    isRefreshing,
    isLoadingMore,
    reload,
    updateField,
    handleFilterChange,
    clearFilters,
    handleLoadMore,
    hasMore,
  };
}
