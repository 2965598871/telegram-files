import { useMemo, useState } from "react";
import useSWR from "swr";
import { POST } from "@/lib/api";
import { type TelegramChat } from "@/lib/types";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useToast } from "@/hooks/use-toast";

type ChatGroupDialogProps = {
  accountId?: string;
  onSaved?: () => Promise<unknown> | void;
};

export default function ChatGroupDialog({
  accountId,
  onSaved,
}: ChatGroupDialogProps) {
  const { toast } = useToast();
  const [open, setOpen] = useState(false);
  const [editingGroupId, setEditingGroupId] = useState<string | undefined>();
  const [name, setName] = useState("");
  const [selectedChatIds, setSelectedChatIds] = useState<string[]>([]);
  const [isSaving, setIsSaving] = useState(false);
  const [deletingGroupId, setDeletingGroupId] = useState<string | undefined>();

  const { data: groups, mutate: mutateGroups } = useSWR<TelegramChat[]>(
    accountId ? `/telegram/${accountId}/chat-groups?query=` : null,
  );
  const { data: chats } = useSWR<TelegramChat[]>(
    accountId
      ? `/telegram/${accountId}/chats?query=&archived=false&chatId=`
      : null,
  );

  const availableChats = useMemo(
    () => (chats ?? []).filter((chat) => chat.kind !== "group"),
    [chats],
  );
  const trimmedName = name.trim();
  const canSave = trimmedName.length > 0 && selectedChatIds.length >= 2;

  const resetForm = () => {
    setEditingGroupId(undefined);
    setName("");
    setSelectedChatIds([]);
  };

  const syncData = async () => {
    await mutateGroups();
    await onSaved?.();
  };

  const handleToggleChat = (chatId: string) => {
    setSelectedChatIds((prev) =>
      prev.includes(chatId)
        ? prev.filter((item) => item !== chatId)
        : [...prev, chatId],
    );
  };

  const handleEdit = (group: TelegramChat) => {
    setEditingGroupId(group.groupId);
    setName(group.name);
    setSelectedChatIds([...(group.chatIds ?? [])]);
  };

  const handleSave = async () => {
    if (!accountId) {
      return;
    }

    if (!canSave) {
      toast({
        variant: "error",
        description: "A group chat needs a name and at least 2 chats.",
      });
      return;
    }

    setIsSaving(true);
    try {
      await POST(
        editingGroupId
          ? `/telegram/${accountId}/chat-groups/${editingGroupId}`
          : `/telegram/${accountId}/chat-groups`,
        {
          name,
          chatIds: selectedChatIds.map((chatId) => Number(chatId)),
        },
      );
      toast({
        variant: "success",
        title: editingGroupId ? "Group updated" : "Group created",
      });
      await syncData();
      resetForm();
      setOpen(false);
    } catch (error) {
      toast({
        variant: "error",
        description:
          error instanceof Error ? error.message : "Failed to save group chat",
      });
    } finally {
      setIsSaving(false);
    }
  };

  const handleDelete = async (group: TelegramChat) => {
    if (!accountId || !group.groupId) {
      return;
    }

    setDeletingGroupId(group.groupId);
    try {
      await POST(`/telegram/${accountId}/chat-groups/${group.groupId}/delete`);
      toast({
        variant: "success",
        title: "Group deleted",
      });
      if (editingGroupId === group.groupId) {
        resetForm();
      }
      await syncData();
    } catch (error) {
      toast({
        variant: "error",
        description:
          error instanceof Error
            ? error.message
            : "Failed to delete group chat",
      });
    } finally {
      setDeletingGroupId(undefined);
    }
  };

  return (
    <Dialog
      open={open}
      onOpenChange={(nextOpen) => {
        setOpen(nextOpen);
        if (!nextOpen) {
          resetForm();
        }
      }}
    >
      <DialogTrigger asChild>
        <Button variant="outline" size="sm" className="w-full">
          Manage groups
        </Button>
      </DialogTrigger>
      <DialogContent className="max-h-[85vh] overflow-y-auto sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>Named group chats</DialogTitle>
          <DialogDescription>
            Create saved chat bundles with a custom name so users can browse
            multiple chats from one board.
          </DialogDescription>
        </DialogHeader>

        <div className="grid gap-6 md:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
          <div className="space-y-3 rounded-[4px] border border-border p-4">
            <div className="flex items-center justify-between">
              <p className="text-sm font-medium">Existing groups</p>
              <Badge variant="secondary">{groups?.length ?? 0}</Badge>
            </div>
            <div className="space-y-2">
              {(groups ?? []).length === 0 ? (
                <p className="text-sm text-muted-foreground">
                  No group chats yet.
                </p>
              ) : (
                (groups ?? []).map((group) => (
                  <div
                    key={group.id}
                    className="rounded-[4px] border border-border p-3"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="space-y-1">
                        <p className="text-sm font-medium">{group.name}</p>
                        <p className="text-xs text-muted-foreground">
                          {group.memberCount ?? group.chatIds?.length ?? 0}{" "}
                          chats
                        </p>
                      </div>
                      <div className="flex gap-2">
                        <Button
                          type="button"
                          size="sm"
                          variant="outline"
                          onClick={() => handleEdit(group)}
                        >
                          Edit
                        </Button>
                        <Button
                          type="button"
                          size="sm"
                          variant="destructive"
                          disabled={deletingGroupId === group.groupId}
                          onClick={() => {
                            void handleDelete(group);
                          }}
                        >
                          Delete
                        </Button>
                      </div>
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>

          <div className="space-y-4 rounded-[4px] border border-border p-4">
            <div className="flex items-center justify-between gap-2">
              <p className="text-sm font-medium">
                {editingGroupId ? "Edit group" : "Create group"}
              </p>
              {editingGroupId && (
                <Button variant="ghost" size="sm" onClick={resetForm}>
                  New group
                </Button>
              )}
            </div>

            <div className="space-y-2">
              <p className="text-xs font-semibold uppercase tracking-[0.08em] text-muted-foreground">
                Group name
              </p>
              <Input
                value={name}
                onChange={(event) => setName(event.target.value)}
                placeholder="Favorites, Workspaces, Research..."
              />
            </div>

            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <p className="text-xs font-semibold uppercase tracking-[0.08em] text-muted-foreground">
                  Chats
                </p>
                <Badge variant="outline">
                  {selectedChatIds.length} selected
                </Badge>
              </div>
              <div className="max-h-72 space-y-2 overflow-y-auto rounded-[4px] border border-border p-3">
                {availableChats.length === 0 ? (
                  <p className="text-sm text-muted-foreground">
                    No chats available.
                  </p>
                ) : (
                  availableChats.map((chat) => (
                    <label
                      key={chat.id}
                      className="flex cursor-pointer items-center gap-3 rounded-[4px] border border-border px-3 py-2"
                    >
                      <Checkbox
                        checked={selectedChatIds.includes(chat.id)}
                        onCheckedChange={() => handleToggleChat(chat.id)}
                      />
                      <div className="min-w-0">
                        <p className="truncate text-sm font-medium">
                          {chat.name}
                        </p>
                        <p className="truncate text-xs text-muted-foreground">
                          {chat.type}
                        </p>
                      </div>
                    </label>
                  ))
                )}
              </div>
              {selectedChatIds.length > 0 && selectedChatIds.length < 2 ? (
                <p className="text-xs text-destructive">
                  Group chats need at least 2 chats.
                </p>
              ) : null}
            </div>

            <Button
              type="button"
              disabled={isSaving || !canSave}
              onClick={() => void handleSave()}
            >
              {editingGroupId ? "Save group" : "Create group"}
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
