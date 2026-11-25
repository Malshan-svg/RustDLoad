import { FormEvent, useEffect, useMemo, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import "./App.css";

type DownloadStatus = "queued" | "downloading" | "paused" | "completed" | "failed" | "cancelled";
type DownloadFilter = "all" | "active" | "completed";
type DownloadAction = "pause" | "resume" | "cancel" | "retry";

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

const filters: { id: DownloadFilter; label: string }[] = [
  { id: "all", label: "All downloads" },
  { id: "active", label: "Active" },
  { id: "completed", label: "Completed" },
];

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

function App() {
  const [url, setUrl] = useState("");
  const [downloads, setDownloads] = useState<Download[]>([]);
  const [filter, setFilter] = useState<DownloadFilter>("all");
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
        });
        if (disposed) stopListening();
        else unlisten = stopListening;
      } catch (error) {
        setErrorMessage(`Could not receive download updates: ${String(error)}`);
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
  const visibleDownloads = useMemo(
    () =>
      downloads.filter((download) => {
        if (filter === "active") return isActive(download.status);
        if (filter === "completed") return download.status === "completed";
        return true;
      }),
    [downloads, filter],
  );

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
            <div className="engine-caption">Ready for your links</div>
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
              <h3>{filter === "all" ? "Your download list is empty" : `No ${filter} downloads`}</h3>
              <p>Paste a direct file link above and it will appear here.</p>
            </div>
          ) : (
            <div className="download-list">
              {visibleDownloads.map((download) => {
                const progress = progressFor(download);
                const isBusy = busyActionId !== null;
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
                        <h3 title={download.filename}>{download.filename}</h3>
                        <span className={`status-pill ${download.status}`}>
                          <span className="status-dot" />
                          {download.status === "completed"
                            ? "Complete"
                            : download.status === "failed"
                              ? "Failed"
                              : download.status === "cancelled"
                                ? "Cancelled"
                                : download.status === "paused"
                                  ? "Paused"
                                  : download.status === "queued"
                                    ? "Queued"
                                    : "Downloading"}
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
                          <span>{formatBytes(download.downloadedBytes)}{download.totalBytes ? ` of ${formatBytes(download.totalBytes)}` : " downloaded"}</span>
                        )}
                        {progress !== null && <span className="progress-percent">{progress}%</span>}
                      </div>
                    </div>
                    <div className="row-actions">
                      {download.status === "downloading" && (
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
