import type { TelegramFile } from "@/lib/types";

export type FileGroup = {
  key: string;
  files: TelegramFile[];
};

export function getFileGroupKey(file: TelegramFile): string {
  const isGroupedMedia =
    (file.type === "photo" || file.type === "video") && file.mediaAlbumId !== 0;
  if (isGroupedMedia) {
    return `album:${file.chatId}:${file.mediaAlbumId}`;
  }
  return `message:${file.chatId}:${file.messageId}`;
}

export function groupFilesByMessage(files: TelegramFile[]): FileGroup[] {
  const groups = new Map<string, FileGroup>();
  const seenFiles = new Map<string, Set<string>>();

  for (const file of files) {
    const key = getFileGroupKey(file);
    const fileKey = file.uniqueId || `${file.chatId}:${file.messageId}:${file.id}`;

    let group = groups.get(key);
    if (!group) {
      group = { key, files: [] };
      groups.set(key, group);
      seenFiles.set(key, new Set());
    }

    const groupSeenFiles = seenFiles.get(key)!;
    if (groupSeenFiles.has(fileKey)) {
      continue;
    }

    groupSeenFiles.add(fileKey);
    group.files.push(file);
  }

  return Array.from(groups.values());
}
