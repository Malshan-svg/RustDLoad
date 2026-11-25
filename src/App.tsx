import { FormEvent, useEffect, useMemo, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import "./App.css";

type DownloadStatus = "queued" | "downloading" | "paused" | "completed" | "failed" | "cancelled";
type DownloadFilter = "all" | "active" | "completed";
type DownloadAction = "pause" | "resume" | "cancel" | "retry";
type DownloadSort = "recent" | "name" | "size" | "progress";
type DownloadGrouping = "none" | "status";

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
type StartedDownload = Pick<Download, "id" | "filename">;
type TransferSample = { bytes: number; at: number };
type DownloadGroup = { id: string; label: string; downloads: Download[] };

const filters: { id: DownloadFilter; label: string }[] = [
  { id: "all", label: "All downloads" },
  { id: "active", label: "Active" },
  { id: "completed", label: "Completed" },
];

const statusOrder: DownloadStatus[] = ["downloading", "queued", "paused", "failed", "completed", "cancelled"];

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
  if (samples.length < 3) return null;
  const recent = samples.filter((sample) => now - sample.at <= 8_000);
  const first = recent[0];
  const last = recent[recent.length - 1];
  if (recent.length < 3 || !first || !last || now - last.at > 4_000 || last.at - first.at < 2_000 || last.bytes <= first.bytes) {
    return null;
  }
  return ((last.bytes - first.bytes) * 1000) / (last.at - first.at);
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

  useEffect(() => {
    let disposed = false;
    let unlisten: (() => void) | undefined;

    const subscribe = async () => {
      try {
        const stopListening = await listen<DownloadUpdate>("download-update", ({ payload }) => {
          setDownloads((current) => upsertDownload(current, payload));
          const sample = { bytes: payload.downloadedBytes, at: performance.now() };
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
            const nextSamples =
              last && sample.at - last.at < 750
                ? [...previous.slice(0, -1), sample]
                : [...previous, sample];
            return { ...current, [payload.id]: nextSamples.slice(-12) };
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

  const activeCount = useMemo(() => downloads.filter((download) => isActive(download.status)).length, [downloads]);
  const completedCount = useMemo(
    () => downloads.filter((download) => download.status === "completed").length,
    [downloads],
  );
  const hasDownloading = downloads.some((download) => download.status === "downloading");

  useEffect(() => {
    if (!hasDownloading) return;
    const timer = window.setInterval(() => setClock(performance.now()), 1000);
    return () => window.clearInterval(timer);
  }, [hasDownloading]);

  const visibleDownloads = useMemo(() => {
    const normalizedSearch = search.trim().toLocaleLowerCase();
    const filtered = downloads.filter((download) => {
      if (filter === "active" && !isActive(download.status)) return false;
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
    if (clearInput) setIsStarting(true);
    try {
      const started = await invoke<StartedDownload>("start_download", { url: submittedUrl });
      setDownloads((current) => {
        const existing = current.some((download) => download.id === started.id);
        if (existing) {
          return current.map((download) =>
            download.id === started.id ? { ...download, url: submittedUrl } : download,
          );
        }
        return [
          {
            ...started,
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
    try {
      if (action === "pause") await invoke("pause_download", { id: download.id });
      else if (action === "resume") await invoke("resume_download", { id: download.id });
      else if (action === "cancel") await invoke("cancel_download", { id: download.id });
      else if (download.url) await enqueueDownload(download.url);
      else throw new Error("The original link is unavailable, so this download cannot be retried.");
    } catch (error) {
      setErrorMessage(String(error));
    } finally {
      setBusyActionId(null);
    }
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark" aria-hidden="true">
            <span />
            <span />
            <span />
          </div>
          <div>
            <div className="brand-name">RustDLoad</div>
            <div className="brand-caption">DOWNLOAD MANAGER</div>
          </div>
        </div>

        <div className="sidebar-section-label">LIBRARY</div>
        <nav className="nav-list" aria-label="Download filters">
          {filters.map((item) => {
            const count = item.id === "active" ? activeCount : item.id === "completed" ? completedCount : downloads.length;
            return (
              <button
                className={`nav-item ${filter === item.id ? "selected" : ""}`}
                key={item.id}
                onClick={() => setFilter(item.id)}
                type="button"
              >
                <span className={`nav-icon nav-icon-${item.id}`} aria-hidden="true">
                  {item.id === "all" ? "↓" : item.id === "active" ? "◷" : "✓"}
                </span>
                <span>{item.label}</span>
                <span className="nav-count">{count}</span>
              </button>
            );
          })}
        </nav>

        <div className="sidebar-footer">
          <span className="engine-indicator" />
          <div>
            <div className="engine-title">Download engine</div>
            <div className="engine-caption">Up to 3 downloads at once</div>
          </div>
          <span className="engine-state">READY</span>
        </div>
      </aside>

      <main className="main-panel">
        <header className="topbar">
          <div className="breadcrumb">
            <span>Workspace</span>
            <span className="breadcrumb-divider">/</span>
            <strong>Downloads</strong>
          </div>
          <div className="open-source-tag">
            <span className="tag-dot" />
            Free &amp; open source
          </div>
        </header>

        <section className="content">
          <div className="page-heading">
            <div>
              <div className="eyebrow">YOUR LIBRARY</div>
              <h1>Downloads</h1>
              <p className="page-subtitle">Keep your files moving. Add a link to get started.</p>
            </div>
            <div className="download-summary">
              <div className="summary-icon" aria-hidden="true">↓</div>
              <div>
                <div className="summary-value">{activeCount}</div>
                <div className="summary-label">ACTIVE JOBS</div>
              </div>
            </div>
          </div>

          <form className="add-download-card" onSubmit={startDownload}>
            <label htmlFor="download-url">Add a download</label>
            <div className="url-entry">
              <span className="url-icon" aria-hidden="true">↗</span>
              <input
                autoComplete="url"
                id="download-url"
                onChange={(event) => setUrl(event.currentTarget.value)}
                placeholder="Paste a direct file link (https://...)"
                type="url"
                value={url}
                required
              />
              <button className="start-button" disabled={isStarting} type="submit">
                <span aria-hidden="true">{isStarting ? "…" : "↓"}</span>
                {isStarting ? "Starting" : "Start download"}
              </button>
            </div>
            <div className="form-hint">
              <span className="hint-lock" aria-hidden="true">⌑</span>
              Files are saved to your Downloads folder
              <span className="hint-separator">·</span>
              HTTP and HTTPS links supported
              <span className="hint-separator">·</span>
              Up to 3 run at once
            </div>
          </form>

          {errorMessage && (
            <div className="error-banner" role="alert">
              <span>{errorMessage}</span>
              <button aria-label="Dismiss error" onClick={() => setErrorMessage("")} type="button">×</button>
            </div>
          )}

          <div className="list-toolbar">
            <div>
              <h2>{filter === "all" ? "Recent downloads" : filter === "active" ? "Active downloads" : "Completed downloads"}</h2>
              <span className="list-count">{visibleDownloads.length} {visibleDownloads.length === 1 ? "item" : "items"}</span>
            </div>
            <div className="list-meta">
              <span className="live-dot" />
              LIVE UPDATES
            </div>
          </div>

          <div className="list-controls">
            <label className="download-search">
              <span className="control-icon" aria-hidden="true">⌕</span>
              <input
                aria-label="Search downloads"
                onChange={(event) => setSearch(event.currentTarget.value)}
                placeholder="Search downloads"
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
                <option value="none">No grouping</option>
                <option value="status">By status</option>
              </select>
            </label>
          </div>

          {visibleDownloads.length === 0 ? (
            <div className="empty-state">
              <div className="empty-art" aria-hidden="true">
                <div className="empty-file">
                  <span className="file-fold" />
                  <span className="file-line line-one" />
                  <span className="file-line line-two" />
                  <span className="file-line line-three" />
                </div>
                <div className="empty-arrow">↓</div>
                <div className="empty-ground" />
              </div>
              <h3>{search.trim() ? "No matching downloads" : downloads.length === 0 ? "Your download list is empty" : `No ${filter} downloads`}</h3>
              <p>{search.trim() ? "Try a different name, address or status." : "Paste a direct file link above and it will appear here."}</p>
            </div>
          ) : (
            <div className="download-list">
              {downloadGroups.map((group) => (
                <section aria-label={group.label || "Downloads"} className="download-group" key={group.id}>
                  {group.label && (
                    <h3 className="group-heading">
                      <span>{group.label}</span>
                      <span>{group.downloads.length}</span>
                    </h3>
                  )}
                  {group.downloads.map((download) => {
                const progress = progressFor(download);
                const isBusy = busyActionId !== null;
                const speed = download.status === "downloading"
                  ? transferRate(transferSamples[download.id] ?? [], clock)
                  : null;
                const source = sourceFor(download);
                const eta = speed !== null && download.totalBytes !== null
                  ? (download.totalBytes - download.downloadedBytes) / speed
                  : null;
                const progressWidth =
                  progress !== null
                    ? `${progress}%`
                    : download.status === "completed"
                      ? "100%"
                      : download.status === "downloading"
                        ? "34%"
                        : "0%";
                return (
                  <article className="download-row" key={download.id}>
                    <div className={`file-icon ${download.status}`} aria-hidden="true">
                      {download.status === "completed"
                        ? "✓"
                        : download.status === "failed"
                          ? "!"
                          : download.status === "cancelled"
                            ? "×"
                            : download.status === "paused"
                              ? "Ⅱ"
                              : "↓"}
                    </div>
                    <div className="download-details">
                      <div className="download-title-line">
                        <div className="download-name">
                          <h3 title={download.filename}>{download.filename}</h3>
                          {source && <span className="download-source" title={download.url}>{source}</span>}
                        </div>
                        <span className={`status-pill ${download.status}`}>
                          <span className="status-dot" />
                          {statusLabel(download.status)}
                        </span>
                      </div>
                      <div className="progress-track" aria-label={progress === null ? "Download progress unknown" : `${progress}% complete`}>
                        <div
                          className={`progress-fill ${download.status} ${progress === null && download.status === "downloading" ? "indeterminate" : ""}`}
                          style={{ width: progressWidth }}
                        />
                      </div>
                      <div className="download-meta">
                        {download.error ? (
                          <span className="download-error">{download.error}</span>
                        ) : (
                          <span>
                            {download.totalBytes !== null
                              ? `${formatBytes(download.downloadedBytes)} of ${formatBytes(download.totalBytes)}`
                              : `${formatBytes(download.downloadedBytes)} downloaded · size unknown`}
                          </span>
                        )}
                        {progress !== null && <span className="progress-percent">{progress}%</span>}
                      </div>
                      {download.status === "downloading" && (
                        <div className="transfer-meta" aria-live="polite">
                          <span>{speed === null ? "Measuring speed…" : formatSpeed(speed)}</span>
                          {eta !== null && eta >= 0 && <span>About {formatEta(eta)} left</span>}
                        </div>
                      )}
                    </div>
                    <div className="row-actions">
                      {(download.status === "downloading" || download.status === "queued") && (
                        <button
                          className="row-action-button"
                          disabled={isBusy}
                          onClick={() => void runDownloadAction(download, "pause")}
                          type="button"
                        >
                          {busyActionId === download.id ? "Working…" : "Pause"}
                        </button>
                      )}
                      {download.status === "paused" && (
                        <button
                          className="row-action-button primary"
                          disabled={isBusy}
                          onClick={() => void runDownloadAction(download, "resume")}
                          type="button"
                        >
                          {busyActionId === download.id ? "Working…" : "Resume"}
                        </button>
                      )}
                      {download.status === "failed" && download.url && (
                        <button
                          className="row-action-button"
                          disabled={isBusy}
                          onClick={() => void runDownloadAction(download, "retry")}
                          type="button"
                        >
                          Retry
                        </button>
                      )}
                      {isActive(download.status) && (
                        <button
                          aria-label={`Cancel ${download.filename}`}
                          className="row-action-button cancel"
                          disabled={isBusy}
                          onClick={() => void runDownloadAction(download, "cancel")}
                          type="button"
                        >
                          Cancel
                        </button>
                      )}
                    </div>
                  </article>
                );
                  })}
                </section>
              ))}
            </div>
          )}

          <footer className="content-footer">
            <span>Built with Rust + Tauri</span>
            <span className="footer-separator">·</span>
            <span>Downloads are stored locally on your device</span>
          </footer>
        </section>
      </main>
    </div>
  );
}

export default App;
