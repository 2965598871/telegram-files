import { type TelegramAccount } from "@/lib/types";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
import { Circle, PhoneCall } from "lucide-react";
import { Spoiler } from "spoiled";
import AccountDeleteDialog from "@/components/account-delete-dialog";
import { AccountDialog } from "@/components/account-dialog";
import { Button } from "@/components/ui/button";

interface AccountListProps {
  accounts: TelegramAccount[];
  onSelectAccount: (accountId: string) => void;
}

export function AccountList({ accounts, onSelectAccount }: AccountListProps) {
  return (
    <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
      {accounts.map((account) => (
        <div
          key={account.id}
          className="group relative cursor-pointer border border-border rounded-[4px] bg-card transition-colors hover:bg-accent"
          onClick={(_e) => {
            onSelectAccount(account.id);
          }}
        >
          <AccountDeleteDialog
            telegramId={account.id}
            className="absolute right-3 top-3 z-10 hidden group-hover:inline-flex"
          />
          <div className="p-4">
            <div className="mb-4 flex items-center gap-3">
              <Avatar className="h-10 w-10 rounded-[4px]">
                <AvatarImage src={`data:image/jpeg;base64,${account.avatar}`} />
                <AvatarFallback className="rounded-[4px]">{account.name[0]}</AvatarFallback>
              </Avatar>
              <div className="min-w-0 flex-1">
                <h3 className="truncate text-sm font-bold">{account.name}</h3>
                <p className="truncate text-xs text-muted-foreground">
                  {account.status === "active" ? (
                    <span className="inline-flex items-center gap-1">
                      <PhoneCall className="h-3 w-3" />
                      <Spoiler>{account.phoneNumber}</Spoiler>
                    </span>
                  ) : (
                    "Authorization required"
                  )}
                </p>
              </div>
              <Badge
                variant={account.status === "active" ? "default" : "secondary"}
                className="gap-1.5 text-xs"
              >
                <Circle
                  className={`h-2 w-2 ${account.status === "active" ? "fill-current" : "text-muted-foreground"}`}
                />
                {account.status}
              </Badge>
            </div>

            <p className="mb-3 truncate text-xs text-muted-foreground">
              {account.rootPath}
            </p>

            <div className="flex items-center gap-2">
              <Button variant="default" size="sm">
                Open board
              </Button>
              {account.status === "inactive" && (
                <AccountDialog>
                  <Button variant="outline" size="sm">
                    Activate
                  </Button>
                </AccountDialog>
              )}
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}
