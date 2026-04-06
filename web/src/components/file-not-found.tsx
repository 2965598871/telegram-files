import { FolderSearch } from "lucide-react";

export default function FileNotFount() {
  return (
    <div className="flex min-h-[55vh] items-center justify-center px-4 py-10">
      <div className="flex max-w-md flex-col items-center gap-4 rounded-[4px] bg-muted px-8 py-10 text-center">
        <div className="flex h-16 w-16 items-center justify-center rounded-full bg-card">
          <FolderSearch className="h-7 w-7 text-muted-foreground" />
        </div>
        <div className="space-y-2">
          <h3 className="text-2xl font-semibold">No pins yet</h3>
          <p className="text-sm text-muted-foreground">
            Try a different filter, search, or chat.
          </p>
        </div>
      </div>
    </div>
  );
}
