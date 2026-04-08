import * as React from "react";
import { type CSSProperties, useEffect, useState } from "react";
import { format } from "date-fns";
import {
  ArrowDownNarrowWide,
  ArrowUpNarrowWide,
  Calendar as CalendarRange,
  Filter,
  X,
} from "lucide-react";
import {
  type DownloadStatus,
  type FileFilter,
  type FileType,
  type SortFields,
  type TransferStatus,
} from "@/lib/types";
import { Button } from "./ui/button";
import {
  Drawer,
  DrawerDescription,
  DrawerFooter,
  DrawerOverlay,
  DrawerPortal,
  DrawerTitle,
  DrawerTrigger,
} from "./ui/drawer";
import { Drawer as DrawerPrimitive } from "vaul";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Label } from "@/components/ui/label";
import { Calendar } from "@/components/ui/calendar";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { RangeSlider } from "@/components/ui/slider";
import { cn, split } from "@/lib/utils";
import { Input } from "@/components/ui/input";
import FileTypeFilter from "@/components/file-type-filter";
import FileStatusFilter from "@/components/file-status-filter";
import { Switch } from "@/components/ui/switch";
import useIsMobile from "@/hooks/use-is-mobile";
import { TagsSelector } from "@/components/ui/tags-selector";
import { useSettings } from "@/hooks/use-settings";

const SearchFilter = ({
  search,
  onChange,
}: {
  search: string;
  onChange: (search: string) => void;
}) => {
  const [localSearch, setLocalSearch] = useState(search);

  const handleChange = (value: string) => {
    setLocalSearch(value);
    onChange(value);
  };

  return (
    <div className="space-y-2">
      <Label className="text-xs font-semibold uppercase tracking-[0.08em] text-muted-foreground">
        Keyword
      </Label>
      <div className="relative">
        <Input
          placeholder="Search with name or caption"
          value={localSearch}
          onChange={(e) => handleChange(e.target.value)}
        />
        {search && (
          <Button
            variant="ghost"
            size="icon"
            className="absolute right-2 top-1/2 h-8 w-8 -translate-y-1/2 rounded-full text-muted-foreground hover:bg-muted hover:text-foreground"
            onClick={() => handleChange("")}
          >
            <X className="h-4 w-4" />
          </Button>
        )}
      </div>
    </div>
  );
};

interface TagsFilterProps {
  tags: string[];
  onChange: (tags: string[]) => void;
}

const TagsFilter = ({ tags, onChange }: TagsFilterProps) => {
  const { settings } = useSettings();

  return (
    <div className="space-y-2">
      <Label className="text-xs font-semibold uppercase tracking-[0.08em] text-muted-foreground">
        Tags
      </Label>
      <TagsSelector
        value={tags}
        onChangeAction={onChange}
        tags={split(",", settings?.tags)}
      />
    </div>
  );
};

interface DateFilterProps {
  dateType: "sent" | "downloaded" | undefined;
  dateRange: [string, string] | undefined;
  onChange: (type: "sent" | "downloaded", range: [string, string]) => void;
}

const DateFilter = ({ dateType, dateRange, onChange }: DateFilterProps) => {
  const [open, setOpen] = useState(false);
  const isMobile = useIsMobile();
  const [localType, setLocalType] = useState<"sent" | "downloaded">(
    dateType ?? "sent",
  );
  const [localRange, setLocalRange] = useState<
    [Date | undefined, Date | undefined]
  >([
    dateRange?.[0] ? new Date(dateRange[0]) : undefined,
    dateRange?.[1] ? new Date(dateRange[1]) : undefined,
  ]);

  const handleTypeChange = (type: "sent" | "downloaded") => {
    setLocalType(type);
  };

  const handleRangeSelect = (range?: {
    from: Date | undefined;
    to?: Date | undefined;
  }) => {
    if (!range) return;

    setLocalRange([range.from, range.to]);
    if (range.from && range.to) {
      onChange(localType, [
        format(range.from, "yyyy-MM-dd"),
        format(range.to, "yyyy-MM-dd"),
      ]);
    }
  };

  const getDisplayText = () => {
    if (!dateRange?.[0] && !dateRange?.[1]) return "Select date range";
    if (dateRange[0] && dateRange[1]) {
      return `${format(new Date(dateRange[0]), "LLL dd, y")} - ${format(new Date(dateRange[1]), "LLL dd, y")}`;
    }
    return "Date range selected";
  };

  return (
    <div className="space-y-2">
      <Label className="text-xs font-semibold uppercase tracking-[0.08em] text-muted-foreground">
        Date
      </Label>
      <Popover open={open} onOpenChange={setOpen}>
        <PopoverTrigger asChild>
          <Button
            variant="outline"
            className="w-full justify-start text-left font-normal text-muted-foreground"
          >
            <CalendarRange className="mr-2 h-4 w-4" />
            <span className="flex-1">{getDisplayText()}</span>
            <span className="ml-2 rounded-full bg-muted px-2.5 py-1 text-xs text-foreground">
              {localType === "downloaded" ? "Download" : "Sent"}
            </span>
          </Button>
        </PopoverTrigger>
        <PopoverContent
          className="w-auto rounded-[4px] p-4"
          side={isMobile ? undefined : "right"}
          modal={true}
        >
          <div className="space-y-4">
            <div className="flex gap-2">
              <Button
                size="sm"
                variant={localType === "sent" ? "default" : "outline"}
                onClick={() => handleTypeChange("sent")}
                className="flex-1"
              >
                Sent Date
              </Button>
              <Button
                size="sm"
                variant={localType === "downloaded" ? "default" : "outline"}
                onClick={() => handleTypeChange("downloaded")}
                className="flex-1"
              >
                Downloaded
              </Button>
            </div>
            <div className="rounded-[4px] border border-border p-2">
              <Calendar
                mode="range"
                selected={{
                  from: localRange[0],
                  to: localRange[1],
                }}
                onSelect={handleRangeSelect}
                numberOfMonths={2}
                defaultMonth={localRange[0] ?? new Date()}
              />
            </div>
          </div>
        </PopoverContent>
      </Popover>
    </div>
  );
};

interface SizeFilterProps {
  sizeRange: [number, number] | undefined;
  sizeUnit: "KB" | "MB" | "GB" | undefined;
  onChange: (range: [number, number], unit: "KB" | "MB" | "GB") => void;
}

const SizeFilter = ({ sizeRange, sizeUnit, onChange }: SizeFilterProps) => {
  const defaultRange: [number, number] = [0, 1000];
  const [localRange, setLocalRange] = useState<[number, number]>(
    sizeRange ?? defaultRange,
  );
  const [localUnit, setLocalUnit] = useState<"KB" | "MB" | "GB">(
    sizeUnit ?? "MB",
  );

  const handleChange = (newValue: number[]) => {
    const range: [number, number] = [newValue[0]!, newValue[1]!];
    setLocalRange(range);
    onChange(range, localUnit);
  };

  const handleUnitChange = (unit: "KB" | "MB" | "GB") => {
    setLocalUnit(unit);
    onChange(localRange, unit);
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <Label className="text-xs font-semibold uppercase tracking-[0.08em] text-muted-foreground">
          File Size
        </Label>
        <Popover>
          <PopoverTrigger asChild>
            <Button variant="outline" size="sm">
              {localUnit}
            </Button>
          </PopoverTrigger>
          <PopoverContent
            className="w-auto rounded-[4px] p-2"
            align="center"
            modal={true}
          >
            <div className="flex flex-col gap-2">
              <Button
                size="sm"
                variant={localUnit === "KB" ? "default" : "outline"}
                onClick={() => handleUnitChange("KB")}
              >
                KB
              </Button>
              <Button
                size="sm"
                variant={localUnit === "MB" ? "default" : "outline"}
                onClick={() => handleUnitChange("MB")}
              >
                MB
              </Button>
              <Button
                size="sm"
                variant={localUnit === "GB" ? "default" : "outline"}
                onClick={() => handleUnitChange("GB")}
              >
                GB
              </Button>
            </div>
          </PopoverContent>
        </Popover>
      </div>
      <div
        className="px-2 pt-2"
        onPointerDown={(e) => {
          e.stopPropagation();
        }}
      >
        <RangeSlider
          value={localRange}
          min={0}
          max={1000}
          step={1}
          minStepsBetweenThumbs={1}
          className="w-full"
          onValueChange={handleChange}
        />
      </div>
      <div className="flex justify-between text-sm">
        <span className="text-muted-foreground">
          {localRange[0]} {localUnit}
        </span>
        <span className="text-muted-foreground">
          {localRange[1]} {localUnit}
        </span>
      </div>
    </div>
  );
};

interface SortFilterProps {
  sort: SortFields | undefined;
  order: "asc" | "desc" | undefined;
  onChange: (sort: SortFields, order: "asc" | "desc") => void;
}

const SortFilter = ({ sort, order, onChange }: SortFilterProps) => {
  const currentSort = sort ?? "date";
  const currentOrder = order ?? "desc";

  const sortOptions = [
    { value: "date", label: "Sent Date" },
    { value: "completion_date", label: "Downloaded Date" },
    { value: "size", label: "File Size" },
    { value: "reaction_count", label: "Reaction Count" },
  ] as const;

  return (
    <div className="space-y-2">
      <Label className="text-xs font-semibold uppercase tracking-[0.08em] text-muted-foreground">
        Sort
      </Label>
      <div className="flex gap-2">
        <Select
          value={currentSort}
          onValueChange={(newSort: typeof currentSort) =>
            onChange(newSort, currentOrder)
          }
        >
          <SelectTrigger className="flex-1">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {sortOptions.map((option) => (
              <SelectItem key={option.value} value={option.value}>
                {option.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Button
          variant="outline"
          size="icon"
          onClick={() =>
            onChange(currentSort, currentOrder === "asc" ? "desc" : "asc")
          }
          className={cn("h-9 w-9")}
        >
          {currentOrder === "asc" ? (
            <ArrowUpNarrowWide className="h-4 w-4" />
          ) : (
            <ArrowDownNarrowWide className="h-4 w-4" />
          )}
        </Button>
      </div>
    </div>
  );
};

interface FileFiltersProps {
  telegramId: string;
  chatId: string;
  filters: FileFilter;
  onFiltersChange: (filters: FileFilter) => void;
  clearFilters: () => void;
}

export default function FileFilters({
  telegramId,
  chatId,
  filters,
  onFiltersChange,
  clearFilters,
}: FileFiltersProps) {
  const noAccountSpecified = telegramId === "-1" && chatId === "-1";
  const [localFilters, setLocalFilters] = useState<FileFilter>(filters);
  const isMobile = useIsMobile();
  const [open, setOpen] = useState(false);

  useEffect(() => {
    setLocalFilters(filters);
  }, [filters]);

  const filterCount = Object.entries(filters).filter(([key, value]) => {
    if (["offline", "sort", "order", "dateType", "sizeUnit"].includes(key))
      return false;
    if (typeof value === "string") return value !== "";
    if (typeof value === "boolean") return value;
    if (Array.isArray(value)) return value.length > 0;
    return false;
  }).length;

  const handleSearchChange = (search: string) => {
    setLocalFilters((prev) => ({ ...prev, search }));
  };

  const handleTypeChange = (type: FileType | "all") => {
    setLocalFilters((prev) => ({ ...prev, type }));
  };

  const handleStatusChange = (
    downloadStatus?: DownloadStatus,
    transferStatus?: TransferStatus,
  ) => {
    setLocalFilters((prev) => ({
      ...prev,
      downloadStatus,
      transferStatus,
    }));
  };

  const handleTagsChange = (tags: string[]) => {
    setLocalFilters((prev) => ({ ...prev, tags }));
  };

  const handleDateChange = (
    dateType: "sent" | "downloaded",
    dateRange: [string, string],
  ) => {
    setLocalFilters((prev) => ({ ...prev, dateType, dateRange }));
  };

  const handleSizeChange = (
    sizeRange: [number, number],
    sizeUnit: "KB" | "MB" | "GB",
  ) => {
    setLocalFilters((prev) => ({ ...prev, sizeRange, sizeUnit }));
  };

  const handleSortChange = (sort: SortFields, order: "asc" | "desc") => {
    setLocalFilters((prev) => ({ ...prev, sort, order }));
  };

  const handleApply = () => {
    onFiltersChange(localFilters);
    setOpen(false);
  };

  const handleClear = () => {
    clearFilters();
    setOpen(false);
  };

  const panelClassName =
    "rounded-[4px] border border-border bg-muted p-4";

  return (
    <Drawer
      open={open}
      onOpenChange={setOpen}
      direction={isMobile ? "bottom" : "left"}
      shouldScaleBackground={isMobile}
      preventScrollRestoration={true}
    >
      <DrawerTrigger asChild>
        <Button
          variant="outline"
          className={cn("relative gap-2", isMobile && "z-50 w-10 px-0")}
        >
          <Filter className="h-5 w-5" />
          {!isMobile && "Filters"}
          {filterCount > 0 && (
            <span className="absolute -left-1 -top-1 flex h-6 w-6 items-center justify-center rounded-full bg-primary text-xs text-primary-foreground">
              {filterCount}
            </span>
          )}
        </Button>
      </DrawerTrigger>
      <DrawerPortal>
        <DrawerOverlay className="bg-black/30 dark:bg-black/50" />
        <DrawerPrimitive.Content
          className={cn(
            isMobile
              ? "fixed inset-x-0 bottom-0 z-50 mt-24 flex h-auto max-h-screen flex-col rounded-t-[28px] border border-border bg-background"
              : "fixed bottom-2 left-2 top-2 z-50 flex w-[400px] outline-none",
          )}
          style={
            isMobile
              ? {}
              : ({ "--initial-transform": "calc(100% + 8px)" } as CSSProperties)
          }
        >
          {isMobile && (
            <div className="mx-auto mt-4 h-2 w-[100px] rounded-full bg-muted" />
          )}
          <div className="no-scrollbar flex h-full w-full grow flex-col overflow-auto rounded-[4px] bg-background">
            <div className="flex-1 p-6 pb-[132px]">
              <div className="space-y-5">
                <div className="space-y-2">
                  <div className="flex items-center justify-between gap-3">
                    <DrawerTitle className="text-2xl">Filters</DrawerTitle>
                    {!noAccountSpecified && (
                      <div className="flex items-center gap-3 rounded-full bg-muted px-3 py-2">
                        <Label
                          htmlFor="offline"
                          className="cursor-pointer text-sm text-muted-foreground"
                        >
                          Offline
                        </Label>
                        <Switch
                          id="offline"
                          checked={localFilters.offline}
                          onCheckedChange={(checked) => {
                            setLocalFilters((prev) => ({
                              ...prev,
                              offline: checked,
                            }));
                          }}
                        />
                      </div>
                    )}
                  </div>
                  <DrawerDescription>
                    Search live Telegram data or switch to your downloaded archive.
                  </DrawerDescription>
                </div>

                <div className="space-y-4 overflow-y-auto p-0.5">
                  <div className={panelClassName}>
                    <SearchFilter
                      search={localFilters.search}
                      onChange={handleSearchChange}
                    />
                  </div>

                  <div className={panelClassName}>
                    <FileTypeFilter
                      offline={localFilters.offline}
                      telegramId={telegramId}
                      chatId={chatId}
                      type={filters.type}
                      onChange={handleTypeChange}
                    />
                  </div>

                  {!localFilters.offline && (
                    <div className="space-y-4">
                      <div className={cn(panelClassName, "flex items-center justify-between gap-3")}>
                        <div>
                          <Label htmlFor="notDownload" className="text-sm font-medium text-foreground">
                            Only not downloaded
                          </Label>
                          <p className="mt-1 text-xs text-muted-foreground">
                            Focus on files still waiting to be saved.
                          </p>
                        </div>
                        <Switch
                          id="notDownload"
                          checked={localFilters.downloadStatus === "idle"}
                          onCheckedChange={(checked) => {
                            setLocalFilters((prev) => ({
                              ...prev,
                              downloadStatus: checked ? "idle" : undefined,
                              alreadyDownloaded: checked ? false : prev.alreadyDownloaded,
                            }));
                          }}
                          aria-label="Not Download"
                        />
                      </div>

                      <div className={cn(panelClassName, "flex items-center justify-between gap-3")}>
                        <div>
                          <Label htmlFor="alreadyDownloaded" className="text-sm font-medium text-foreground">
                            Only already downloaded
                          </Label>
                          <p className="mt-1 text-xs text-muted-foreground">
                            Show live Telegram files that already exist in your archive.
                          </p>
                        </div>
                        <Switch
                          id="alreadyDownloaded"
                          checked={Boolean(localFilters.alreadyDownloaded)}
                          onCheckedChange={(checked) => {
                            setLocalFilters((prev) => ({
                              ...prev,
                              alreadyDownloaded: checked,
                              downloadStatus: checked ? undefined : prev.downloadStatus,
                            }));
                          }}
                          aria-label="Already Downloaded"
                        />
                      </div>
                    </div>
                  )}

                  {localFilters.offline && (
                    <>
                      <div className={panelClassName}>
                        <FileStatusFilter
                          downloadStatus={localFilters.downloadStatus}
                          transferStatus={localFilters.transferStatus}
                          onChange={handleStatusChange}
                        />
                      </div>

                      <div className={panelClassName}>
                        <TagsFilter
                          tags={localFilters.tags}
                          onChange={handleTagsChange}
                        />
                      </div>

                      <div className={panelClassName}>
                        <DateFilter
                          dateType={localFilters.dateType}
                          dateRange={localFilters.dateRange}
                          onChange={handleDateChange}
                        />
                      </div>

                      <div className={panelClassName}>
                        <SizeFilter
                          sizeRange={localFilters.sizeRange}
                          sizeUnit={localFilters.sizeUnit}
                          onChange={handleSizeChange}
                        />
                      </div>

                      <div className={panelClassName}>
                        <SortFilter
                          sort={localFilters.sort}
                          order={localFilters.order}
                          onChange={handleSortChange}
                        />
                      </div>
                    </>
                  )}
                </div>
              </div>
            </div>

            <DrawerFooter className="fixed bottom-0 left-0 right-0 border-t border-border bg-background/95 ">
              <Button onClick={handleApply}>Apply filters</Button>
              <Button variant="outline" onClick={handleClear}>
                Clear filters
              </Button>
            </DrawerFooter>
          </div>
        </DrawerPrimitive.Content>
      </DrawerPortal>
    </Drawer>
  );
}
