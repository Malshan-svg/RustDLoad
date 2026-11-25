import { FormEvent, useEffect, useMemo, useRef, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { save } from "@tauri-apps/plugin-dialog";
import "./App.css";

type DownloadStatus = "queued" | "downloading" | "paused" | "completed" | "failed" | "link-expired" | "cancelled";
type DownloadFilter = "all" | "downloading" | "queued" | "completed";
type DownloadAction = "pause" | "resume" | "cancel" | "retry";
type DownloadSort = "recent" | "name" | "size" | "progress";
type DownloadGrouping = "none" | "status";
type AppPage = "downloads" | "settings";
type IconName =
  | "download"
  | "arrowDown"
  | "queue"
  | "check"
  | "settings"
  | "search"
  | "plus"
  | "more"
  | "play"
  | "pause"
  | "copy"
  | "folder"
  | "retry"
  | "cancel";

type Download = {
  id: string;
  filename: string;
  url?: string;
  downloadedBytes: number;
  totalBytes: number | null;
  status: DownloadStatus;
  error: string | null;
};

type DownloadUpdate = Omit<Download, "url">;
type StartedDownload = Pick<Download, "id" | "filename"> & { duplicate: boolean; skipped: boolean };
type FileExistsPolicy = "rename" | "overwrite" | "skip";
type DownloadSettings = { maxConcurrentDownloads: number; fileExistsPolicy: FileExistsPolicy };
type TransferSample = { bytes: number; at: number };
type DownloadGroup = { id: string; label: string; downloads: Download[] };

const filters: { id: DownloadFilter; label: string; icon: IconName }[] = [
  { id: "all", label: "All", icon: "download" },
  { id: "downloading", label: "Downloading", icon: "arrowDown" },
  { id: "queued", label: "Queued", icon: "queue" },
  { id: "completed", label: "Completed", icon: "check" },
];

const statusOrder: DownloadStatus[] = ["downloading", "queued", "paused", "link-expired", "failed", "completed", "cancelled"];

function normalizedUrlForDuplicateCheck(value: string): string {
  try {
    const url = new URL(value);
    url.hash = "";
    return url.href;
  } catch {
    return value.trim();
  }
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let value = bytes / 1024;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  return `${value.toFixed(value >= 100 ? 0 : 1)} ${units[unitIndex]}`;
}

function formatSpeed(bytesPerSecond: number): string {
  return `${formatBytes(bytesPerSecond)}/s`;
}

function formatEta(seconds: number): string {
  const remaining = Math.max(0, Math.ceil(seconds));
  const hours = Math.floor(remaining / 3600);
  const minutes = Math.floor((remaining % 3600) / 60);
  const secs = remaining % 60;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${secs}s`;
  return `${secs}s`;
}

function transferRate(samples: TransferSample[], now: number): number | null {
  if (samples.length < 2) return null;
  const recent = samples.filter((sample) => now - sample.at <= 8_000);
  const first = recent[0];
  const last = recent[recent.length - 1];
  const elapsed = last && first ? last.at - first.at : 0;
  if (
    recent.length < 2 ||
    !first ||
    !last ||
    now - last.at > 3_000 ||
    elapsed < 750 ||
    last.bytes <= first.bytes
  ) {
    return null;
  }
  const rate = ((last.bytes - first.bytes) * 1000) / elapsed;
  return Number.isFinite(rate) && rate > 0 ? rate : null;
}

function Icon({ name }: { name: IconName }) {
  const paths: Record<IconName, React.ReactNode> = {
    download: <><path d="M12 3v12" /><path d="m7 10 5 5 5-5" /><path d="M5 20h14" /></>,
    arrowDown: <><path d="M12 3v12" /><path d="m7 10 5 5 5-5" /></>,
    queue: <><path d="M9 6h11" /><path d="M9 12h11" /><path d="M9 18h11" /><path d="M4 6h.01" /><path d="M4 12h.01" /><path d="M4 18h.01" /></>,
    check: <><path d="m5 12 4 4L19 6" /></>,
    settings: <><path d="M12 8a4 4 0 1 0 0 8 4 4 0 0 0 0-8Z" /><path d="m19.4 15 .1.1a1.7 1.7 0 0 1-2.4 2.4L17 17.4a1.7 1.7 0 0 0-2.9 1.2v.2a1.7 1.7 0 0 1-3.4 0v-.2a1.7 1.7 0 0 0-2.9-1.2l-.1.1a1.7 1.7 0 0 1-2.4-2.4l.1-.1a1.7 1.7 0 0 0-1.2-2.9H4a1.7 1.7 0 0 1 0-3.4h.2a1.7 1.7 0 0 0 1.2-2.9l-.1-.1a1.7 1.7 0 0 1 2.4-2.4l.1.1a1.7 1.7 0 0 0 2.9-1.2V4a1.7 1.7 0 0 1 3.4 0v.2a1.7 1.7 0 0 0 2.9 1.2l.1-.1a1.7 1.7 0 0 1 2.4 2.4l-.1.1a1.7 1.7 0 0 0 1.2 2.9h.2a1.7 1.7 0 0 1 0 3.4h-.2a1.7 1.7 0 0 0-1.2.9Z" /></>,
    search: <><circle cx="11" cy="11" r="7" /><path d="m20 20-4-4" /></>,
    plus: <><path d="M12 5v14" /><path d="M5 12h14" /></>,
    more: <><circle cx="5" cy="12" r="1" /><circle cx="12" cy="12" r="1" /><circle cx="19" cy="12" r="1" /></>,
    play: <><path d="m8 5 11 7-11 7z" /></>,
    pause: <><path d="M8 5v14" /><path d="M16 5v14" /></>,
    copy: <><rect x="8" y="8" width="12" height="13" rx="1.5" /><path d="M16 8V5H5v12h3" /></>,
    folder: <><path d="M3 7.5h7l2 2h9v9H3z" /><path d="M3 7.5V5h7l2 2" /></>,
    retry: <><path d="M20 7v5h-5" /><path d="M19 12a7 7 0 1 0 1 4" /></>,
    cancel: <><path d="m6 6 12 12M18 6 6 18" /></>,
  };
  return <svg aria-hidden="true" className="icon" fill="none" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.8" viewBox="0 0 24 24">{paths[name]}</svg>;
}

function sourceFor(download: Download): string | null {
  if (!download.url) return null;
  try {
    return new URL(download.url).hostname;
  } catch {
    return null;
  }
}

function progressFor(download: Download): number | null {
  if (!download.totalBytes || download.totalBytes <= 0) return null;
  return Math.min(100, Math.round((download.downloadedBytes / download.totalBytes) * 100));
}

function upsertDownload(downloads: Download[], update: DownloadUpdate): Download[] {
  const index = downloads.findIndex((download) => download.id === update.id);
  if (index === -1) return [update, ...downloads];
  return downloads.map((download) =>
    download.id === update.id ? { ...update, url: download.url } : download,
  );
}

function isActive(status: DownloadStatus): boolean {
  return status === "queued" || status === "downloading" || status === "paused";
}

function statusLabel(status: DownloadStatus): string {
  if (status === "completed") return "Complete";
  if (status === "failed") return "Failed";
  if (status === "link-expired") return "Link expired";
  if (status === "cancelled") return "Cancelled";
  if (status === "paused") return "Paused";
  if (status === "queued") return "Queued";
  return "Downloading";
}

function App() {
  const [url, setUrl] = useState("");
  const [downloads, setDownloads] = useState<Download[]>([]);
  const [filter, setFilter] = useState<DownloadFilter>("all");
  const [sortBy, setSortBy] = useState<DownloadSort>("recent");
  const [groupBy, setGroupBy] = useState<DownloadGrouping>("none");
  const [search, setSearch] = useState("");
  const [transferSamples, setTransferSamples] = useState<Record<string, TransferSample[]>>({});
  const [clock, setClock] = useState(() => performance.now());
  const [errorMessage, setErrorMessage] = useState("");
  const [isStarting, setIsStarting] = useState(false);
  const [busyActionId, setBusyActionId] = useState<string | null>(null);
  const [page, setPage] = useState<AppPage>("downloads");
  const [maxConcurrentDownloads, setMaxConcurrentDownloads] = useState(3);
  const [concurrencyDraft, setConcurrencyDraft] = useState("3");
  const [fileExistsPolicy, setFileExistsPolicy] = useState<FileExistsPolicy>("rename");
  const [settingsError, setSettingsError] = useState("");
  const [filePolicyError, setFilePolicyError] = useState("");
  const [isSavingSettings, setIsSavingSettings] = useState(false);
  const [isSavingFilePolicy, setIsSavingFilePolicy] = useState(false);
  const [contextMenu, setContextMenu] = useState<{ downloadId: string; x: number; y: number } | null>(null);
  const [noticeMessage, setNoticeMessage] = useState("");
  const urlInputRef = useRef<HTMLInputElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    let disposed = false;
    let unlisten: (() => void) | undefined;

    const subscribe = async () => {
      try {
        const stopListening = await listen<DownloadUpdate>("download-update", ({ payload }) => {
          setDownloads((current) => upsertDownload(current, payload));
          const sample = { bytes: payload.downloadedBytes, at: performance.now() };
          setClock(sample.at);
          setTransferSamples((current) => {
            const previous = current[payload.id] ?? [];
            if (payload.status !== "downloading") {
              if (!previous.length) return current;
              const next = { ...current };
              delete next[payload.id];
              return next;
            }
            const last = previous[previous.length - 1];
            if (last && sample.bytes < last.bytes) {
              return { ...current, [payload.id]: [sample] };
            }
            if (last && sample.bytes === last.bytes) return current;
            return { ...current, [payload.id]: [...previous, sample].slice(-40) };
          });
        });
        if (disposed) {
          stopListening();
          return;
        }
        unlisten = stopListening;

        const restored = await invoke<Download[]>("list_downloads");
        if (!disposed) {
          setDownloads((current) => {
            const currentIds = new Set(current.map((download) => download.id));
            return [...current, ...restored.filter((download) => !currentIds.has(download.id))];
          });
        }
        const settings = await invoke<DownloadSettings>("get_download_settings");
        if (!disposed) {
          setMaxConcurrentDownloads(settings.maxConcurrentDownloads);
          setConcurrencyDraft(String(settings.maxConcurrentDownloads));
          setFileExistsPolicy(settings.fileExistsPolicy);
        }
      } catch (error) {
        setErrorMessage(`Could not initialize downloads: ${String(error)}`);
      }
    };

    void subscribe();
    return () => {
      disposed = true;
      unlisten?.();
    };
  }, []);

  const downloadingCount = useMemo(
    () => downloads.filter((download) => download.status === "downloading").length,
    [downloads],
  );
  const queuedCount = useMemo(
    () => downloads.filter((download) => download.status === "queued" || download.status === "paused").length,
    [downloads],
  );
  const completedCount = useMemo(
    () => downloads.filter((download) => download.status === "completed").length,
    [downloads],
  );
  const hasDownloading = downloads.some((download) => download.status === "downloading");
  const totalSpeed = useMemo(
    () =>
      downloads.reduce((total, download) => {
        if (download.status !== "downloading") return total;
        return total + (transferRate(transferSamples[download.id] ?? [], clock) ?? 0);
      }, 0),
    [clock, downloads, transferSamples],
  );

  useEffect(() => {
    if (!hasDownloading) return;
    const timer = window.setInterval(() => setClock(performance.now()), 1000);
    return () => window.clearInterval(timer);
  }, [hasDownloading]);

  useEffect(() => {
    function handleShortcut(event: KeyboardEvent) {
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "f") {
        event.preventDefault();
        searchInputRef.current?.focus();
      } else if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "l") {
        event.preventDefault();
        urlInputRef.current?.focus();
      } else if (event.key === "Escape") {
        setContextMenu(null);
      }
    }

    window.addEventListener("keydown", handleShortcut);
    return () => window.removeEventListener("keydown", handleShortcut);
  }, []);

  const visibleDownloads = useMemo(() => {
    const normalizedSearch = search.trim().toLocaleLowerCase();
    const filtered = downloads.filter((download) => {
      if (filter === "downloading" && download.status !== "downloading") return false;
      if (filter === "queued" && download.status !== "queued" && download.status !== "paused") return false;
      if (filter === "completed" && download.status !== "completed") return false;
      if (!normalizedSearch) return true;
      return [download.filename, download.url ?? "", download.error ?? "", statusLabel(download.status)]
        .some((value) => value.toLocaleLowerCase().includes(normalizedSearch));
    });
    if (sortBy === "name") {
      return [...filtered].sort((a, b) => a.filename.localeCompare(b.filename));
    }
    if (sortBy === "size") {
      return [...filtered].sort((a, b) => {
        if (a.totalBytes === null) return b.totalBytes === null ? 0 : 1;
        if (b.totalBytes === null) return -1;
        return b.totalBytes - a.totalBytes;
      });
    }
    if (sortBy === "progress") {
      return [...filtered].sort((a, b) => (progressFor(b) ?? -1) - (progressFor(a) ?? -1));
    }
    return filtered;
  }, [downloads, filter, search, sortBy]);

  const downloadGroups = useMemo<DownloadGroup[]>(() => {
    if (groupBy === "none") return [{ id: "all", label: "", downloads: visibleDownloads }];
    return statusOrder
      .map((status) => ({
        id: status,
        label: statusLabel(status),
        downloads: visibleDownloads.filter((download) => download.status === status),
      }))
      .filter((group) => group.downloads.length > 0);
  }, [groupBy, visibleDownloads]);

  async function enqueueDownload(submittedUrl: string, clearInput = false) {
    setErrorMessage("");
    setNoticeMessage("");
    if (clearInput) setIsStarting(true);
    try {
      const normalizedUrl = normalizedUrlForDuplicateCheck(submittedUrl);
      const duplicate = downloads.find(
        (download) =>
          download.url &&
          normalizedUrlForDuplicateCheck(download.url) === normalizedUrl,
      );
      if (duplicate) {
        setNoticeMessage(`${duplicate.filename} is already in your download list.`);
        if (clearInput) setUrl("");
        return;
      }
      const defaultPath = await invoke<string>("suggest_download_destination", { url: submittedUrl });
      const destinationPath = await save({ defaultPath, title: "Save download as" });
      if (destinationPath === null) return;
      const started = await invoke<StartedDownload>("start_download", { url: submittedUrl, destinationPath });
      if (started.duplicate) {
        setDownloads(await invoke<Download[]>("list_downloads"));
        setNoticeMessage(`${started.filename} is already in your download list.`);
        if (clearInput) setUrl("");
        return;
      }
      if (started.skipped) {
        setNoticeMessage(`${started.filename} already exists; the download was skipped.`);
        if (clearInput) setUrl("");
        return;
      }
      const newDownload = { id: started.id, filename: started.filename };
      setDownloads((current) => {
        const existing = current.some((download) => download.id === started.id);
        if (existing) {
          return current.map((download) =>
            download.id === started.id ? { ...download, url: submittedUrl } : download,
          );
        }
        return [
          {
            ...newDownload,
            url: submittedUrl,
            downloadedBytes: 0,
            totalBytes: null,
            status: "queued",
            error: null,
          },
          ...current,
        ];
      });
      if (clearInput) setUrl("");
    } catch (error) {
      setErrorMessage(String(error));
    } finally {
      if (clearInput) setIsStarting(false);
    }
  }

  async function startDownload(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const submittedUrl = url.trim();
    if (!submittedUrl || isStarting) return;
    await enqueueDownload(submittedUrl, true);
  }

  async function runDownloadAction(download: Download, action: DownloadAction) {
    if (busyActionId) return;
    setBusyActionId(download.id);
    setErrorMessage("");
    setNoticeMessage("");
    try {
      if (action === "pause") await invoke("pause_download", { id: download.id });
      else if (action === "resume") await invoke("resume_download", { id: download.id });
      else if (action === "cancel") await invoke("cancel_download", { id: download.id });
      else if (download.url) await invoke("retry_download", { id: download.id });
      else throw new Error("The original link is unavailable, so this download cannot be retried.");
    } catch (error) {
      setErrorMessage(String(error));
    } finally {
      setBusyActionId(null);
    }
  }

  async function updateExpiredDownloadUrl(download: Download) {
    const newUrl = window.prompt("Enter the refreshed download URL", download.url ?? "");
    if (!newUrl?.trim() || busyActionId) return;
    setBusyActionId(download.id);
    setErrorMessage("");
    setNoticeMessage("");
    try {
      await invoke("update_download_url", { id: download.id, newUrl: newUrl.trim() });
      setDownloads(await invoke<Download[]>("list_downloads"));
      setNoticeMessage("Updated link; download resumed.");
    } catch (error) {
      setErrorMessage(String(error));
    } finally {
      setBusyActionId(null);
    }
  }

  function showContextMenu(downloadId: string, x: number, y: number) {
    const menuWidth = 236;
    const menuHeight = 280;
    setContextMenu({
      downloadId,
      x: Math.max(8, Math.min(x, window.innerWidth - menuWidth - 8)),
      y: Math.max(8, Math.min(y, window.innerHeight - menuHeight - 8)),
    });
  }

  async function copyDownloadLink(download: Download) {
    setContextMenu(null);
    setErrorMessage("");
    if (!download.url) {
      setErrorMessage("The original link is unavailable, so it cannot be copied.");
      return;
    }

    try {
      if (!navigator.clipboard?.writeText) {
        throw new Error("Clipboard access is unavailable.");
      }
      await navigator.clipboard.writeText(download.url);
      setNoticeMessage("Download link copied to clipboard.");
    } catch (error) {
      setErrorMessage(`Could not copy the download link: ${String(error)}`);
    }
  }

  async function openDownloadLocation(download: Download) {
    setContextMenu(null);
    setErrorMessage("");
    setNoticeMessage("");
    try {
      await invoke("open_download_location", { id: download.id });
    } catch (error) {
      setErrorMessage(String(error));
    }
  }

  async function saveSettings(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const limit = Number(concurrencyDraft);
    if (!Number.isInteger(limit) || limit < 1 || limit > 10) {
      setSettingsError("Choose a whole number between 1 and 10.");
      return;
    }

    setSettingsError("");
    setIsSavingSettings(true);
    try {
      const saved = await invoke<DownloadSettings>("set_max_concurrent_downloads", { limit });
      setMaxConcurrentDownloads(saved.maxConcurrentDownloads);
      setConcurrencyDraft(String(saved.maxConcurrentDownloads));
      setFileExistsPolicy(saved.fileExistsPolicy);
    } catch (error) {
      setSettingsError(String(error));
    } finally {
      setIsSavingSettings(false);
    }
  }

  async function saveFileExistsPolicy(policy: FileExistsPolicy) {
    setFilePolicyError("");
    setIsSavingFilePolicy(true);
    try {
      const saved = await invoke<DownloadSettings>("set_file_exists_policy", { policy });
      setMaxConcurrentDownloads(saved.maxConcurrentDownloads);
      setConcurrencyDraft(String(saved.maxConcurrentDownloads));
      setFileExistsPolicy(saved.fileExistsPolicy);
    } catch (error) {
      setFileExistsPolicy(fileExistsPolicy);
      setFilePolicyError(String(error));
    } finally {
      setIsSavingFilePolicy(false);
    }
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="brand-mark"><Icon name="download" /></span>
          <span className="brand-name">RustDLoad</span>
        </div>

        <div className="sidebar-section-label">LIBRARY</div>
        <nav className="nav-list" aria-label="Download filters">
          {filters.map((item) => {
            const count =
              item.id === "all"
                ? downloads.length
                : item.id === "downloading"
                  ? downloadingCount
                  : item.id === "queued"
                    ? queuedCount
                    : completedCount;
            return (
              <button
                aria-current={filter === item.id && page === "downloads" ? "page" : undefined}
                className={`nav-item ${filter === item.id && page === "downloads" ? "selected" : ""}`}
                key={item.id}
                onClick={() => {
                  setPage("downloads");
                  setFilter(item.id);
                }}
                type="button"
              >
                <Icon name={item.icon} />
                <span>{item.label}</span>
                <span className="nav-count">{count}</span>
              </button>
            );
          })}
          <div className="sidebar-section-label tools-label">PREFERENCES</div>
          <button
            aria-current={page === "settings" ? "page" : undefined}
            className={`nav-item ${page === "settings" ? "selected" : ""}`}
            onClick={() => setPage("settings")}
            type="button"
          >
            <Icon name="settings" />
            <span>Settings</span>
          </button>
        </nav>

        <div className="sidebar-footer">
          <span>Concurrent downloads</span>
          <strong>{maxConcurrentDownloads}</strong>
        </div>
      </aside>

      <main className="main-panel">
        <header className="topbar">
          <div className="breadcrumb">
            <span>RustDLoad</span>
            <span className="breadcrumb-divider">/</span>
            <strong>{page === "settings" ? "Settings" : "Downloads"}</strong>
          </div>
          <div className="topbar-hints">
            <span><kbd>Ctrl</kbd>+<kbd>F</kbd> Search</span>
            <span><kbd>Ctrl</kbd>+<kbd>L</kbd> Add URL</span>
          </div>
        </header>

        <section className={`content ${page === "settings" ? "settings-content" : ""}`}>
          {page === "settings" ? (
            <>
              <div className="section-heading">
                <div>
                  <h1>Settings</h1>
                  <p>Configure how RustDLoad manages your transfers.</p>
                </div>
              </div>
              <form className="settings-card" onSubmit={saveSettings}>
                <div className="settings-copy">
                  <h2>Concurrent downloads</h2>
                  <p>Choose how many downloads may transfer at the same time.</p>
                </div>
                <label className="settings-field" htmlFor="max-concurrent-downloads">
                  <span>Maximum active transfers</span>
                  <input
                    id="max-concurrent-downloads"
                    max={10}
                    min={1}
                    onChange={(event) => setConcurrencyDraft(event.currentTarget.value)}
                    step={1}
                    type="number"
                    value={concurrencyDraft}
                    required
                  />
                </label>
                <p className="settings-note">
                  Lowering this limit won’t stop running downloads; queued downloads wait for capacity.
                </p>
                {settingsError && <p className="settings-error" role="alert">{settingsError}</p>}
                <button className="start-button settings-save" disabled={isSavingSettings} type="submit">
                  {isSavingSettings ? "Saving…" : "Save settings"}
                </button>
              </form>
              <section className="settings-card file-policy-card">
                <div className="settings-copy">
                  <h2>When a file already exists</h2>
                  <p>Choose how RustDLoad handles a destination name that is already in use.</p>
                </div>
                <label className="settings-field" htmlFor="file-exists-policy">
                  <span>File exists behavior</span>
                  <select
                    disabled={isSavingFilePolicy}
                    id="file-exists-policy"
                    onChange={(event) => {
                      const value = event.currentTarget.value;
                      if (value !== "rename" && value !== "overwrite" && value !== "skip") return;
                      const policy = value;
                      setFileExistsPolicy(policy);
                      void saveFileExistsPolicy(policy);
                    }}
                    value={fileExistsPolicy}
                  >
                    <option value="rename">Keep both (rename new file)</option>
                    <option value="overwrite">Replace existing file</option>
                    <option value="skip">Skip the new download</option>
                  </select>
                </label>
                {filePolicyError && <p className="settings-error" role="alert">{filePolicyError}</p>}
              </section>
            </>
          ) : (
            <>
              <div className="download-toolbar">
                <div className="section-heading">
                  <div>
                    <h1>Downloads</h1>
                    <p>{downloads.length} {downloads.length === 1 ? "item" : "items"} · {downloadingCount} downloading · {queuedCount} queued</p>
                  </div>
                </div>
                <form className="add-download-card" onSubmit={startDownload}>
                  <label className="visually-hidden" htmlFor="download-url">Download URL</label>
                  <input
                    autoComplete="url"
                    id="download-url"
                    onChange={(event) => setUrl(event.currentTarget.value)}
                    placeholder="Paste a direct file URL"
                    ref={urlInputRef}
                    type="url"
                    value={url}
                    required
                  />
                  <button className="start-button" disabled={isStarting} type="submit">
                    <Icon name="plus" />
                    {isStarting ? "Choosing location…" : "Add download"}
                  </button>
                </form>
              </div>

              {errorMessage && (
                <div className="error-banner" role="alert">
                  <span>{errorMessage}</span>
                  <button aria-label="Dismiss error" onClick={() => setErrorMessage("")} type="button">×</button>
                </div>
              )}
              {noticeMessage && (
                <div className="notice-banner" role="status" aria-live="polite">
                  <span>{noticeMessage}</span>
                  <button aria-label="Dismiss notification" onClick={() => setNoticeMessage("")} type="button">×</button>
                </div>
              )}

              <div className="table-toolbar">
                <div className="filter-title">
                  <h2>{filters.find((item) => item.id === filter)?.label ?? "Downloads"}</h2>
                  <span>{visibleDownloads.length}</span>
                </div>
                <label className="download-search">
                  <Icon name="search" />
                  <input
                    aria-label="Search downloads"
                    onChange={(event) => setSearch(event.currentTarget.value)}
                    placeholder="Search"
                    ref={searchInputRef}
                    type="search"
                    value={search}
                  />
                </label>
                <label className="select-control">
                  <span>Sort</span>
                  <select aria-label="Sort downloads" onChange={(event) => setSortBy(event.currentTarget.value as DownloadSort)} value={sortBy}>
                    <option value="recent">Recently added</option>
                    <option value="name">Name</option>
                    <option value="size">File size</option>
                    <option value="progress">Progress</option>
                  </select>
                </label>
                <label className="select-control">
                  <span>Group</span>
                  <select aria-label="Group downloads" onChange={(event) => setGroupBy(event.currentTarget.value as DownloadGrouping)} value={groupBy}>
                    <option value="none">None</option>
                    <option value="status">Status</option>
                  </select>
                </label>
              </div>

              <div className="table-scroll">
                <table className="download-table">
                  <thead>
                    <tr>
                      <th scope="col">Name</th>
                      <th scope="col">Size</th>
                      <th scope="col">Progress</th>
                      <th scope="col">Speed</th>
                      <th scope="col">ETA</th>
                      <th scope="col">Status</th>
                      <th scope="col"><span className="visually-hidden">Actions</span></th>
                    </tr>
                  </thead>
                  {downloadGroups.map((group) => (
                    <tbody aria-label={group.label || "Downloads"} key={group.id}>
                      {group.label && (
                        <tr className="group-row">
                          <th colSpan={7} scope="rowgroup">
                            {group.label}<span>{group.downloads.length}</span>
                          </th>
                        </tr>
                      )}
                      {group.downloads.map((download) => {
                        const progress = progressFor(download);
                        const speed =
                          download.status === "downloading"
                            ? transferRate(transferSamples[download.id] ?? [], clock)
                            : null;
                        const eta =
                          speed !== null && download.totalBytes !== null
                            ? (download.totalBytes - download.downloadedBytes) / speed
                            : null;
                        const source = sourceFor(download);
                        const progressWidth =
                          progress !== null
                            ? `${progress}%`
                            : download.status === "completed"
                              ? "100%"
                              : "0%";
                        return (
                          <tr
                            className={`download-row ${download.status}`}
                            key={download.id}
                            onContextMenu={(event) => {
                              event.preventDefault();
                              showContextMenu(download.id, event.clientX, event.clientY);
                            }}
                          >
                            <td className="name-cell">
                              <span className={`file-status-icon ${download.status}`}>
                                <Icon name={download.status === "completed" ? "check" : download.status === "downloading" ? "arrowDown" : "download"} />
                              </span>
                              <span className="name-text">
                                <span title={download.filename}>{download.filename}</span>
                                {source && <small title={download.url}>{source}</small>}
                              </span>
                            </td>
                            <td className="size-cell numeric">
                              {download.totalBytes === null ? "Unknown" : formatBytes(download.totalBytes)}
                            </td>
                            <td className="progress-cell">
                              <div
                                aria-label={progress === null ? `${formatBytes(download.downloadedBytes)} downloaded, total unknown` : `${progress}% complete`}
                                className="progress-track"
                              >
                                <div
                                  className={`progress-fill ${download.status}`}
                                  style={{ width: progressWidth }}
                                />
                              </div>
                              <div className="progress-meta numeric">
                                <span>
                                  {download.totalBytes === null
                                    ? `${formatBytes(download.downloadedBytes)} downloaded`
                                    : `${formatBytes(download.downloadedBytes)} of ${formatBytes(download.totalBytes)}`}
                                </span>
                                <span>{progress === null ? "—" : `${progress}%`}</span>
                              </div>
                            </td>
                            <td className="numeric speed-cell">
                              {speed === null
                                ? download.status === "downloading" ? "Measuring…" : "—"
                                : formatSpeed(speed)}
                            </td>
                            <td className="numeric">
                              {eta !== null && eta >= 0 ? formatEta(eta) : "—"}
                            </td>
                            <td>
                              <span className={`status-label ${download.status}`} title={download.error ?? undefined}>
                                <span className="status-dot" />
                                {statusLabel(download.status)}
                              </span>
                            </td>
                            <td className="action-cell">
                              <div className="row-actions">
                                {isActive(download.status) && (
                                  <button
                                    aria-label={`${download.status === "paused" ? "Resume" : "Pause"} ${download.filename}`}
                                    className="row-action-button transfer-action"
                                    disabled={busyActionId !== null}
                                    onClick={() =>
                                      void runDownloadAction(
                                        download,
                                        download.status === "paused" ? "resume" : "pause",
                                      )
                                    }
                                    title={download.status === "paused" ? "Resume download" : "Pause download"}
                                    type="button"
                                  >
                                    <Icon name={download.status === "paused" ? "play" : "pause"} />
                                  </button>
                                )}
                                <button
                                  aria-haspopup="menu"
                                  aria-label={`Actions for ${download.filename}`}
                                  className="row-action-button more-button"
                                  disabled={busyActionId !== null}
                                  onClick={(event) => showContextMenu(download.id, event.clientX, event.clientY)}
                                  title="More actions"
                                  type="button"
                                >
                                  <Icon name="more" />
                                </button>
                              </div>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  ))}
                </table>
                {visibleDownloads.length === 0 && (
                  <div className="empty-state">
                    <strong>{search.trim() ? "No matching downloads" : `No ${filter === "all" ? "" : `${filters.find((item) => item.id === filter)?.label.toLowerCase()} `}downloads`}</strong>
                    <span>{search.trim() ? "Try a different search." : "Add a direct file URL to start a download."}</span>
                  </div>
                )}
              </div>

              {contextMenu && (() => {
                const download = downloads.find((item) => item.id === contextMenu.downloadId);
                if (!download) return null;
                const closeMenu = () => setContextMenu(null);
                const runAction = (action: DownloadAction) => {
                  closeMenu();
                  void runDownloadAction(download, action);
                };
                return (
                  <div className="context-layer" onClick={closeMenu} onContextMenu={(event) => { event.preventDefault(); closeMenu(); }}>
                    <div
                      className="context-menu"
                      role="menu"
                      style={{ left: contextMenu.x, top: contextMenu.y }}
                      onClick={(event) => event.stopPropagation()}
                    >
                      {download.status === "paused" && (
                        <button onClick={() => runAction("resume")} role="menuitem" type="button">
                          <Icon name="play" />
                          Start / Resume
                        </button>
                      )}
                      {download.status === "link-expired" && (
                        <button onClick={() => void updateExpiredDownloadUrl(download)} role="menuitem" type="button">
                          <Icon name="retry" />
                          Update expired URL
                        </button>
                      )}
                      {download.url && (download.status === "failed" || download.status === "cancelled") && (
                        <button onClick={() => runAction("retry")} role="menuitem" type="button">
                          <Icon name="retry" />
                          Retry
                        </button>
                      )}
                      {(download.status === "downloading" || download.status === "queued") && (
                        <button onClick={() => runAction("pause")} role="menuitem" type="button">
                          <Icon name="pause" />
                          Pause
                        </button>
                      )}
                      {isActive(download.status) && (
                        <button className="danger-menu-item" onClick={() => runAction("cancel")} role="menuitem" type="button">
                          <Icon name="cancel" />
                          Cancel
                        </button>
                      )}
                      {(isActive(download.status) || download.status === "failed" || download.status === "link-expired" || download.status === "cancelled") && (
                        <div className="context-menu-divider" role="separator" />
                      )}
                      {download.url && (
                        <button onClick={() => void copyDownloadLink(download)} role="menuitem" type="button">
                          <Icon name="copy" />
                          Copy download link
                        </button>
                      )}
                      <button onClick={() => void openDownloadLocation(download)} role="menuitem" type="button">
                        <Icon name="folder" />
                        Open download location
                      </button>
                    </div>
                  </div>
                );
              })()}
            </>
          )}
        </section>

        <footer className="statusbar" role="status" aria-live="polite">
          <div className="statusbar-item">
            <span className={`status-dot ${downloadingCount > 0 ? "live" : ""}`} />
            <strong>{downloadingCount > 0 && totalSpeed === 0 ? "Measuring…" : formatSpeed(totalSpeed)}</strong>
            <span>Total speed</span>
          </div>
          <span className="statusbar-divider" />
          <div className="statusbar-item"><strong>{downloadingCount}</strong><span>downloading</span></div>
          <div className="statusbar-item"><strong>{queuedCount}</strong><span>queued</span></div>
          <span className="statusbar-spacer" />
          <span className="statusbar-limit">{maxConcurrentDownloads} max connections</span>
        </footer>
      </main>
    </div>
  );
}

export default App;
