import React from "react";
import useSWR from "swr";
import { Github, RefreshCw } from "lucide-react";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import Link from "next/link";
import TGDuck16HeyOut from "@/components/animations/tg-duck16_hey_out.json";
import dynamic from "next/dynamic";

interface VersionData {
  version: string;
}

interface GitHubReleaseData {
  tag_name: string;
}

const fetcher = (url: string) => fetch(url).then((res) => res.json());

const Lottie = dynamic(() => import("lottie-react"), { ssr: false });
export default function About() {
  const { data: apiData, error: apiError } = useSWR<VersionData, Error>(
    "/version",
  );
  const { data: githubData, error: githubError } = useSWR<
    GitHubReleaseData,
    Error
  >(
    "https://api.github.com/repos/jarvis2f/telegram-files/releases/latest",
    fetcher,
  );

  const projectInfo = {
    repository: "https://github.com/jarvis2f/telegram-files",
    author: "Jarvis2f",
  };

  const currentVersion = apiData?.version ?? "Unknown";
  const isNewVersionAvailable =
    githubData && githubData.tag_name !== currentVersion;

  return (
    <div className="flex justify-center md:h-full md:items-center">
      <Card className="w-full max-w-2xl overflow-hidden border-border bg-card">
        <CardHeader>
          <CardTitle>About This Project</CardTitle>
          <CardDescription>
            A self-hosted Telegram file downloader for continuous, stable, and
            unattended downloads.
          </CardDescription>
        </CardHeader>
        <CardContent className="relative">
          <Lottie
            className="absolute bottom-3 right-3 h-28 w-28"
            animationData={TGDuck16HeyOut}
            loop={true}
          />
          <div className="space-y-4">
            <div className="flex flex-col items-center justify-center rounded-[4px] bg-muted p-4">
              <p className="text-sm font-medium text-muted-foreground">Author</p>
              <p>{projectInfo.author}</p>
            </div>

            <div className="flex flex-col items-center justify-center rounded-[4px] bg-muted p-4">
              <p className="mb-1 text-sm font-medium text-muted-foreground">
                Current Version
              </p>
              {apiError ? (
                <p className="text-red-500">Failed to load current version</p>
              ) : !apiData ? (
                <div className="flex items-center space-x-2">
                  <RefreshCw className="animate-spin text-muted-foreground" size={16} />
                  <span>Loading...</span>
                </div>
              ) : (
                <p className="rounded-full bg-card px-3 py-1">
                  {currentVersion}
                </p>
              )}
            </div>

            <div className="flex flex-col items-center justify-center rounded-[4px] bg-muted p-4">
              <p className="mb-1 text-sm font-medium text-muted-foreground">
                Latest Version
              </p>
              {githubError ? (
                <p className="text-red-500">Failed to load release data</p>
              ) : !githubData ? (
                <div className="flex items-center space-x-2">
                  <RefreshCw className="animate-spin text-muted-foreground" size={16} />
                  <span>Loading...</span>
                </div>
              ) : (
                <p className="rounded-full bg-card px-3 py-1">
                  {githubData.tag_name}
                </p>
              )}
            </div>

            {isNewVersionAvailable && (
              <div className="rounded-[4px] border border-border bg-muted px-4 py-3">
                <p className="text-sm text-foreground">
                  A new version ({githubData?.tag_name}) is available! Update
                  now.
                </p>
              </div>
            )}

            <div className="flex items-center justify-center space-x-2">
              <Link
                href={projectInfo.repository}
                target="_blank"
                rel="noopener noreferrer"
              >
                <Github className="h-6 w-6" />
              </Link>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
