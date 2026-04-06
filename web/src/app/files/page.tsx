"use client";
import Files from "@/components/files";
import ThemeToggleButton from "@/components/theme-toggle-button";
import Link from "next/link";
import TelegramIcon from "@/components/telegram-icon";

export default function AccountPage() {
  return (
    <div className="app-shell px-4 py-4 md:px-6 md:py-6">
      <div className="mb-4 border-b border-border pb-3">
        <div className="flex items-center justify-between gap-4">
          <Link
            href="/"
            className="inline-flex items-center gap-2"
          >
            <div className="flex h-8 w-8 items-center justify-center rounded-[4px] bg-foreground text-background">
              <TelegramIcon className="h-4 w-4" />
            </div>
            <div>
              <h1 className="text-base font-bold">Library</h1>
            </div>
          </Link>

          <ThemeToggleButton />
        </div>
      </div>
      <Files accountId="-1" chatId="-1" />
    </div>
  );
}
