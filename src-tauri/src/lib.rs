use futures_util::StreamExt;
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
#[cfg(any(target_os = "windows", target_os = "macos", unix))]
use std::process::Command;
use std::{
    collections::{HashMap, HashSet},
    io::ErrorKind,
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tauri::{AppHandle, Emitter, Manager, RunEvent, Runtime, State};
use tokio::{
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::{Notify, watch},
};
use uuid::Uuid;

const DEFAULT_MAX_CONCURRENT_DOWNLOADS: usize = 3;
const MIN_CONCURRENT_DOWNLOADS: usize = 1;
const MAX_CONCURRENT_DOWNLOADS: usize = 10;
const MAX_SEGMENTS_PER_DOWNLOAD: usize = 4;
const MIN_SEGMENTED_DOWNLOAD_SIZE: u64 = 2 * 1024 * 1024;
const MAX_NETWORK_RETRIES: u32 = 6;
const PERSISTED_FORMAT_VERSION: u32 = 1;
const SEGMENT_RESUME_VERSION: u32 = 1;
#[cfg(not(test))]
const STALL_TIMEOUT: Duration = Duration::from_secs(15);
#[cfg(test)]
const STALL_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct DownloadUpdate {
    id: String,
    filename: String,
    downloaded_bytes: u64,
    total_bytes: Option<u64>,
    status: String,
    error: Option<String>,
}

struct DownloadContext {
    id: String,
    url: String,
    filename: String,
    destination: PathBuf,
    partial_path: PathBuf,
}

struct DownloadFailure<'a> {
    id: &'a str,
    filename: &'a str,
    path: &'a Path,
    downloaded_bytes: u64,
    total_bytes: Option<u64>,
    message: String,
}

struct SegmentRequest {
    client: reqwest::Client,
    app: Arc<dyn UpdateSink>,
    manager: DownloadManager,
    url: String,
    partial_path: PathBuf,
    filename: String,
    id: String,
    segment_index: usize,
    segment_end: u64,
    position: u64,
    total: u64,
    validator: Option<String>,
    progress: Arc<AtomicU64>,
    last_progress_update: Arc<Mutex<Instant>>,
    tracker: SegmentResumeTracker,
    control: watch::Receiver<Control>,
}

struct SegmentedDownloadContext<'a> {
    client: &'a reqwest::Client,
    app: &'a Arc<dyn UpdateSink>,
    manager: &'a DownloadManager,
    id: &'a str,
    url: &'a str,
    filename: &'a str,
    path: &'a Path,
    sidecar_path: &'a Path,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SegmentPosition {
    start: u64,
    end: u64,
    position: u64,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SegmentResumeState {
    version: u32,
    url: String,
    final_url: String,
    total_size: u64,
    validator: Option<String>,
    segments: Vec<SegmentPosition>,
}

#[derive(Clone)]
struct SegmentResumeTracker {
    state: Arc<tokio::sync::Mutex<SegmentResumeState>>,
    path: PathBuf,
    persistence_lock: Arc<tokio::sync::Mutex<()>>,
    last_saved: Arc<tokio::sync::Mutex<Instant>>,
}

fn remember_validator(manager: &DownloadManager, id: &str, validator: Option<String>) {
    let updated = manager
        .entries
        .lock()
        .map(|mut entries| {
            if let Some(entry) = entries.get_mut(id) {
                entry.validator = validator;
                true
            } else {
                false
            }
        })
        .unwrap_or(false);
    if updated && let Err(error) = manager.persist() {
        eprintln!("{error}");
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DownloadSnapshot {
    id: String,
    filename: String,
    url: String,
    downloaded_bytes: u64,
    total_bytes: Option<u64>,
    status: String,
    error: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StartedDownload {
    id: String,
    filename: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Control {
    Running,
    Paused,
    Cancelled,
}

struct DownloadEntry {
    url: String,
    filename: String,
    destination: PathBuf,
    status: String,
    downloaded_bytes: u64,
    total_bytes: Option<u64>,
    validator: Option<String>,
    error: Option<String>,
    retry_count: u32,
    auto_offline: bool,
    control: watch::Sender<Control>,
    task_lock: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Clone)]
struct DownloadManager {
    entries: Arc<Mutex<HashMap<String, DownloadEntry>>>,
    persistence_file: Option<PathBuf>,
    persistence_lock: Arc<Mutex<()>>,
    settings_file: Option<PathBuf>,
    settings_lock: Arc<Mutex<()>>,
    admission: Arc<DownloadAdmission>,
}

struct AdmissionState {
    active: usize,
    limit: usize,
}

struct DownloadAdmission {
    state: Mutex<AdmissionState>,
    changed: Notify,
}

struct DownloadPermit {
    admission: Arc<DownloadAdmission>,
}

impl Drop for DownloadPermit {
    fn drop(&mut self) {
        if let Ok(mut state) = self.admission.state.lock() {
            state.active = state.active.saturating_sub(1);
        }
        self.admission.changed.notify_one();
    }
}

impl DownloadAdmission {
    fn new(limit: usize) -> Self {
        Self {
            state: Mutex::new(AdmissionState { active: 0, limit }),
            changed: Notify::new(),
        }
    }

    fn limit(&self) -> Result<usize, String> {
        self.state
            .lock()
            .map(|state| state.limit)
            .map_err(|_| "Download settings are unavailable.".to_string())
    }

    fn set_limit(&self, limit: usize) -> Result<(), String> {
        self.state
            .lock()
            .map_err(|_| "Download settings are unavailable.".to_string())?
            .limit = limit;
        self.changed.notify_waiters();
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedDownload {
    id: String,
    url: String,
    filename: String,
    #[serde(default)]
    destination: Option<String>,
    status: String,
    downloaded_bytes: u64,
    total_bytes: Option<u64>,
    #[serde(default)]
    validator: Option<String>,
    #[serde(default)]
    retry_count: u32,
    error: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedSettings {
    #[serde(default)]
    version: u32,
    max_concurrent_downloads: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedQueue {
    #[serde(default)]
    version: u32,
    downloads: Vec<PersistedDownload>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PersistedQueueDocument {
    Versioned(PersistedQueue),
    Legacy(Vec<PersistedDownload>),
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DownloadSettings {
    max_concurrent_downloads: usize,
}

fn validate_max_concurrent_downloads(limit: usize) -> Result<usize, String> {
    if !(MIN_CONCURRENT_DOWNLOADS..=MAX_CONCURRENT_DOWNLOADS).contains(&limit) {
        return Err(format!(
            "Maximum concurrent downloads must be between {MIN_CONCURRENT_DOWNLOADS} and {MAX_CONCURRENT_DOWNLOADS}."
        ));
    }
    Ok(limit)
}

trait UpdateSink: Send + Sync {
    fn emit_download_update(&self, update: DownloadUpdate);
}

impl<R: Runtime> UpdateSink for AppHandle<R> {
    fn emit_download_update(&self, update: DownloadUpdate) {
        if let Err(error) = self.emit("download-update", update) {
            eprintln!("Could not send download update to the UI: {error}");
        }
    }
}

impl<T: UpdateSink + ?Sized> UpdateSink for Arc<T> {
    fn emit_download_update(&self, update: DownloadUpdate) {
        (**self).emit_download_update(update);
    }
}

fn emit_update<S: UpdateSink + ?Sized>(sink: &S, update: DownloadUpdate) {
    sink.emit_download_update(update);
}

fn update_entry(
    manager: &DownloadManager,
    id: &str,
    filename: &str,
    status: &str,
    downloaded_bytes: u64,
    total_bytes: Option<u64>,
    error: Option<String>,
) -> Result<String, String> {
    let mut entries = manager
        .entries
        .lock()
        .map_err(|_| "Download state is unavailable.".to_string())?;
    let entry = entries
        .get_mut(id)
        .ok_or_else(|| format!("Download {id} is no longer available."))?;
    let status = match *entry.control.borrow() {
        Control::Running if matches!(status, "downloading" | "paused") => "downloading",
        Control::Paused if matches!(status, "downloading" | "paused") => "paused",
        Control::Cancelled if matches!(status, "downloading" | "paused") => "cancelled",
        _ => status,
    };
    entry.filename = filename.to_string();
    entry.status = status.to_string();
    entry.downloaded_bytes = downloaded_bytes;
    entry.total_bytes = total_bytes;
    entry.error = error;
    Ok(status.to_string())
}

impl DownloadManager {
    fn persist(&self) -> Result<(), String> {
        let Some(path) = self.persistence_file.as_deref() else {
            return Ok(());
        };
        let _persistence_guard = self
            .persistence_lock
            .lock()
            .map_err(|_| "Download persistence is unavailable.".to_string())?;
        let mut snapshot: Vec<_> = {
            let entries = self
                .entries
                .lock()
                .map_err(|_| "Download state is unavailable.".to_string())?;
            entries
                .iter()
                .map(|(id, entry)| PersistedDownload {
                    id: id.clone(),
                    url: entry.url.clone(),
                    filename: entry.filename.clone(),
                    destination: Some(entry.destination.to_string_lossy().into_owned()),
                    status: entry.status.clone(),
                    downloaded_bytes: entry.downloaded_bytes,
                    total_bytes: entry.total_bytes,
                    validator: entry.validator.clone(),
                    retry_count: entry.retry_count,
                    error: entry.error.clone(),
                })
                .collect()
        };
        snapshot.sort_by(|left, right| left.id.cmp(&right.id));
        persist_snapshot(path, &snapshot)
    }
}

fn persist_snapshot(path: &Path, snapshot: &[PersistedDownload]) -> Result<(), String> {
    let queue = PersistedQueue {
        version: PERSISTED_FORMAT_VERSION,
        downloads: snapshot.to_vec(),
    };
    let serialized = serde_json::to_vec_pretty(&queue)
        .map_err(|error| format!("Could not serialize the download queue: {error}"))?;
    persist_json(path, &serialized, "download queue")
}

fn persist_json(path: &Path, serialized: &[u8], label: &str) -> Result<(), String> {
    let temporary_path = path.with_extension("json.tmp");
    let backup_path = path.with_extension("json.bak");
    std::fs::write(&temporary_path, serialized)
        .map_err(|error| format!("Could not write the {label}: {error}"))?;

    if path.exists() {
        if backup_path.exists() {
            std::fs::remove_file(path)
                .map_err(|error| format!("Could not replace the {label}: {error}"))?;
        } else {
            std::fs::rename(path, &backup_path)
                .map_err(|error| format!("Could not preserve the previous {label}: {error}"))?;
        }
    }
    if let Err(error) = std::fs::rename(&temporary_path, path) {
        if backup_path.exists() && !path.exists() {
            let _ = std::fs::rename(&backup_path, path);
        }
        return Err(format!("Could not save the {label}: {error}"));
    }
    if backup_path.exists() {
        let _ = std::fs::remove_file(backup_path);
    }
    Ok(())
}

fn read_max_concurrent_downloads(path: &Path) -> Result<usize, String> {
    let backup_path = path.with_extension("json.bak");
    let read = |file: &Path| -> Result<usize, String> {
        let bytes = std::fs::read(file)
            .map_err(|error| format!("Could not read download settings: {error}"))?;
        let settings: PersistedSettings = serde_json::from_slice(&bytes)
            .map_err(|error| format!("Could not parse download settings: {error}"))?;
        if settings.version > PERSISTED_FORMAT_VERSION {
            return Err("The saved download settings use a newer unsupported format.".to_string());
        }
        validate_max_concurrent_downloads(settings.max_concurrent_downloads)
    };

    if path.exists() {
        match read(path) {
            Ok(limit) => return Ok(limit),
            Err(error) if backup_path.exists() => {
                eprintln!("{error}; trying the previous settings snapshot.");
            }
            Err(error) => return Err(error),
        }
    }
    if backup_path.exists() {
        return read(&backup_path);
    }
    Ok(DEFAULT_MAX_CONCURRENT_DOWNLOADS)
}

fn persist_max_concurrent_downloads(path: &Path, limit: usize) -> Result<(), String> {
    let settings = PersistedSettings {
        version: PERSISTED_FORMAT_VERSION,
        max_concurrent_downloads: validate_max_concurrent_downloads(limit)?,
    };
    let serialized = serde_json::to_vec_pretty(&settings)
        .map_err(|error| format!("Could not serialize download settings: {error}"))?;
    persist_json(path, &serialized, "download settings")
}

fn restore_download_manager(
    persistence_file: PathBuf,
    download_dir: &Path,
) -> Result<(DownloadManager, Vec<PersistedDownload>), String> {
    std::fs::create_dir_all(download_dir)
        .map_err(|error| format!("Could not create the Downloads folder: {error}"))?;
    remove_stale_segment_directories(download_dir);

    let backup_file = persistence_file.with_extension("json.bak");
    let records = read_persisted_queue(&persistence_file, &backup_file)?;
    let settings_file = persistence_file.with_file_name("settings.json");
    let max_concurrent_downloads =
        read_max_concurrent_downloads(&settings_file).unwrap_or_else(|error| {
            eprintln!("{error}; using the default download limit.");
            DEFAULT_MAX_CONCURRENT_DOWNLOADS
        });
    let manager = DownloadManager {
        entries: Arc::default(),
        persistence_file: Some(persistence_file),
        persistence_lock: Arc::default(),
        settings_file: Some(settings_file),
        settings_lock: Arc::default(),
        admission: Arc::new(DownloadAdmission::new(max_concurrent_downloads)),
    };
    let mut resumable = Vec::new();
    let mut seen_ids = HashSet::new();

    for mut record in records {
        if Uuid::parse_str(&record.id).is_err()
            || !seen_ids.insert(record.id.clone())
            || record.filename.is_empty()
            || record
                .filename
                .chars()
                .any(|character| matches!(character, '/' | '\\'))
            || !matches!(
                Path::new(&record.filename)
                    .components()
                    .collect::<Vec<_>>()
                    .as_slice(),
                [Component::Normal(_)]
            )
        {
            eprintln!("Ignoring an invalid persisted download record.");
            continue;
        }
        let valid_url = reqwest::Url::parse(&record.url)
            .map(|url| matches!(url.scheme(), "http" | "https"))
            .unwrap_or(false);
        if !valid_url {
            record.status = "failed".to_string();
            record.error = Some("The saved download URL is invalid.".to_string());
        }

        let destination = record
            .destination
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| download_dir.join(&record.filename));
        let Some(destination_filename) = destination.file_name().and_then(|part| part.to_str())
        else {
            eprintln!("Ignoring a download record with an invalid destination.");
            continue;
        };
        if destination_filename != record.filename || destination.parent().is_none() {
            eprintln!("Ignoring a download record with a mismatched destination.");
            continue;
        }
        record.destination = Some(destination.to_string_lossy().into_owned());
        let destination_directory = destination.parent().unwrap();
        remove_stale_segment_directory(destination_directory, &record.id);
        let partial_path = partial_path_for(&destination, &record.id);
        let sidecar_path = segment_sidecar_path(&destination);
        let sidecar_present = sidecar_path.exists();
        let sidecar_state = if matches!(
            record.status.as_str(),
            "queued" | "downloading" | "paused" | "link-expired"
        ) {
            match read_segment_resume_state(&sidecar_path) {
                Ok(state) => state.filter(|state| state.url == record.url),
                Err(error) => {
                    record.status = "failed".to_string();
                    record.error = Some(error);
                    None
                }
            }
        } else {
            None
        };
        if let Some(state) = sidecar_state.as_ref() {
            record.total_bytes = Some(state.total_size);
            record.validator = state.validator.clone();
        }
        let partial_size = if matches!(
            record.status.as_str(),
            "queued" | "downloading" | "paused" | "link-expired"
        ) {
            match std::fs::metadata(&partial_path) {
                Ok(metadata)
                    if metadata.is_file()
                        && sidecar_state
                            .as_ref()
                            .is_some_and(|state| metadata.len() == state.total_size) =>
                {
                    Some(segment_progress(sidecar_state.as_ref().unwrap()))
                }
                Ok(metadata)
                    if metadata.is_file() && sidecar_state.is_none() && !sidecar_present =>
                {
                    Some(metadata.len())
                }
                Ok(metadata) if metadata.is_file() => {
                    eprintln!(
                        "Restarting segmented download {} because its preallocated file is incomplete.",
                        record.id
                    );
                    Some(0)
                }
                Ok(_) => {
                    record.status = "failed".to_string();
                    record.error = Some("The partial download path is not a file.".to_string());
                    None
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    match std::fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(false)
                        .open(&partial_path)
                    {
                        Ok(_) => Some(0),
                        Err(error) => {
                            record.status = "failed".to_string();
                            record.error = Some(format!(
                                "Could not restore partial download {}: {error}",
                                record.id
                            ));
                            None
                        }
                    }
                }
                Err(error) => {
                    record.status = "failed".to_string();
                    record.error = Some(format!("Could not inspect the partial download: {error}"));
                    None
                }
            }
        } else {
            None
        };
        let completed_size = if record.status == "completed" {
            std::fs::metadata(&destination)
                .ok()
                .filter(|metadata| metadata.is_file())
                .map(|metadata| metadata.len())
        } else {
            None
        };
        if reconcile_persisted_status(&mut record, partial_size, completed_size) {
            resumable.push(record.clone());
        }

        let (sender, receiver) = watch::channel(if record.status == "paused" {
            Control::Paused
        } else {
            Control::Running
        });
        manager
            .entries
            .lock()
            .map_err(|_| "Download state is unavailable while restoring the queue.".to_string())?
            .insert(
                record.id.clone(),
                DownloadEntry {
                    url: record.url.clone(),
                    filename: record.filename.clone(),
                    destination,
                    status: record.status.clone(),
                    downloaded_bytes: record.downloaded_bytes,
                    total_bytes: record.total_bytes,
                    validator: record.validator.clone(),
                    error: record.error.clone(),
                    retry_count: record.retry_count,
                    auto_offline: false,
                    control: sender,
                    task_lock: Arc::new(tokio::sync::Mutex::new(())),
                },
            );
        drop(receiver);
    }

    manager.persist()?;
    Ok((manager, resumable))
}

fn remove_stale_segment_directories(download_dir: &Path) {
    let entries = match std::fs::read_dir(download_dir) {
        Ok(entries) => entries,
        Err(error) => {
            eprintln!("Could not inspect stale download segments: {error}");
            return;
        }
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(id) = name
            .strip_prefix('.')
            .and_then(|name| name.strip_suffix(".segments"))
        else {
            continue;
        };
        if Uuid::parse_str(id).is_ok() {
            remove_stale_segment_directory(download_dir, id);
        }
    }
}

fn remove_stale_segment_directory(directory: &Path, id: &str) {
    let path = directory.join(format!(".{id}.segments"));
    if path.is_dir()
        && let Err(error) = std::fs::remove_dir_all(path)
    {
        eprintln!("Could not remove stale segments for download {id}: {error}");
    }
}

fn partial_path_for(destination: &Path, id: &str) -> PathBuf {
    destination
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!(".{id}.part"))
}

fn segment_sidecar_path(destination: &Path) -> PathBuf {
    let filename = destination
        .file_name()
        .map(|filename| filename.to_string_lossy())
        .unwrap_or_default();
    destination.with_file_name(format!("{filename}.rdl"))
}

fn segment_progress(state: &SegmentResumeState) -> u64 {
    state
        .segments
        .iter()
        .map(|segment| segment.position.saturating_sub(segment.start))
        .sum()
}

fn valid_segment_resume_state(state: &SegmentResumeState) -> bool {
    if state.version != SEGMENT_RESUME_VERSION || state.total_size == 0 || state.segments.is_empty()
    {
        return false;
    }

    let mut next_start = 0;
    for segment in &state.segments {
        if segment.start != next_start
            || segment.end < segment.start
            || segment.position < segment.start
            || segment.position > segment.end.saturating_add(1)
        {
            return false;
        }
        next_start = segment.end.saturating_add(1);
    }
    next_start == state.total_size
}

fn read_segment_resume_state(path: &Path) -> Result<Option<SegmentResumeState>, String> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("Could not read the resume sidecar: {error}")),
    };
    match serde_json::from_slice::<SegmentResumeState>(&bytes) {
        Ok(state) if valid_segment_resume_state(&state) => Ok(Some(state)),
        Ok(_) | Err(_) => {
            eprintln!("Ignoring an invalid segmented-download resume sidecar.");
            Ok(None)
        }
    }
}

async fn persist_segment_resume_state(
    path: &Path,
    state: &SegmentResumeState,
) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(state)
        .map_err(|error| format!("Could not serialize the resume sidecar: {error}"))?;
    let temporary_path = path.with_file_name(format!(
        "{}.tmp",
        path.file_name()
            .map(|filename| filename.to_string_lossy())
            .unwrap_or_default()
    ));
    let mut file = tokio::fs::File::create(&temporary_path)
        .await
        .map_err(|error| format!("Could not create the resume sidecar: {error}"))?;
    file.write_all(&bytes)
        .await
        .map_err(|error| format!("Could not write the resume sidecar: {error}"))?;
    file.sync_all()
        .await
        .map_err(|error| format!("Could not flush the resume sidecar: {error}"))?;
    drop(file);
    tokio::fs::rename(&temporary_path, path)
        .await
        .map_err(|error| format!("Could not save the resume sidecar: {error}"))
}

async fn remove_segment_sidecar(path: &Path) -> Result<(), String> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "Could not remove the download resume sidecar {}: {error}",
            path.display()
        )),
    }
}

impl SegmentResumeTracker {
    async fn checkpoint(&self, index: usize, position: u64, force: bool) -> Result<(), String> {
        let _persistence_guard = self.persistence_lock.lock().await;
        let mut last_saved = self.last_saved.lock().await;
        if !force && last_saved.elapsed() < Duration::from_secs(1) {
            return Ok(());
        }
        let snapshot = {
            let mut state = self.state.lock().await;
            state.segments[index].position = position;
            state.clone()
        };
        persist_segment_resume_state(&self.path, &snapshot).await?;
        *last_saved = Instant::now();
        Ok(())
    }

    async fn persist(&self) -> Result<(), String> {
        let _persistence_guard = self.persistence_lock.lock().await;
        let snapshot = self.state.lock().await.clone();
        persist_segment_resume_state(&self.path, &snapshot).await?;
        *self.last_saved.lock().await = Instant::now();
        Ok(())
    }

    async fn snapshot(&self) -> SegmentResumeState {
        self.state.lock().await.clone()
    }
}

fn read_persisted_queue(
    persistence_file: &Path,
    backup_file: &Path,
) -> Result<Vec<PersistedDownload>, String> {
    let read = |path: &Path| -> Result<Vec<PersistedDownload>, String> {
        let contents = std::fs::read(path)
            .map_err(|error| format!("Could not read the download queue: {error}"))?;
        let document: PersistedQueueDocument = serde_json::from_slice(&contents)
            .map_err(|error| format!("Could not parse the download queue: {error}"))?;
        match document {
            PersistedQueueDocument::Legacy(downloads) => Ok(downloads),
            PersistedQueueDocument::Versioned(queue)
                if queue.version <= PERSISTED_FORMAT_VERSION =>
            {
                Ok(queue.downloads)
            }
            PersistedQueueDocument::Versioned(_) => {
                Err("The saved download queue uses a newer unsupported format.".to_string())
            }
        }
    };
    if persistence_file.exists() {
        match read(persistence_file) {
            Ok(records) => return Ok(records),
            Err(error) if backup_file.exists() => {
                eprintln!("{error}; trying the previous queue snapshot.");
            }
            Err(error) => return Err(error),
        }
    }
    if backup_file.exists() {
        return read(backup_file);
    }
    Ok(Vec::new())
}

fn reconcile_persisted_status(
    record: &mut PersistedDownload,
    partial_size: Option<u64>,
    completed_size: Option<u64>,
) -> bool {
    match record.status.as_str() {
        "queued" | "downloading" | "paused" => {
            record.status = "paused".to_string();
            record.downloaded_bytes = partial_size.unwrap_or(0);
            record.error = None;
            true
        }
        "completed"
            if completed_size == Some(record.downloaded_bytes)
                && record
                    .total_bytes
                    .is_none_or(|total| total == record.downloaded_bytes) =>
        {
            false
        }
        "completed" => {
            record.status = "failed".to_string();
            record.error = Some(
                "The saved completed file is missing or its size no longer matches.".to_string(),
            );
            false
        }
        "failed" | "cancelled" => false,
        "link-expired" => {
            record.downloaded_bytes = partial_size.unwrap_or(record.downloaded_bytes);
            false
        }
        _ => {
            record.status = "failed".to_string();
            record.error = Some("The saved download status is invalid.".to_string());
            false
        }
    }
}

fn publish_update<S: UpdateSink + ?Sized>(
    app: &S,
    manager: &DownloadManager,
    mut update: DownloadUpdate,
) {
    update.status = match update_entry(
        manager,
        &update.id,
        &update.filename,
        &update.status,
        update.downloaded_bytes,
        update.total_bytes,
        update.error.clone(),
    ) {
        Ok(status) => status,
        Err(state_error) => {
            eprintln!("{state_error}");
            update.status.clone()
        }
    };
    if let Err(persist_error) = manager.persist() {
        eprintln!("{persist_error}");
    }
    emit_update(app, update);
}

macro_rules! publish_update {
    ($app:expr, $manager:expr, $id:expr, $filename:expr, $status:expr, $downloaded_bytes:expr, $total_bytes:expr, $error:expr $(,)?) => {
        publish_update(
            $app,
            $manager,
            DownloadUpdate {
                id: $id.to_string(),
                filename: $filename.to_string(),
                downloaded_bytes: $downloaded_bytes,
                total_bytes: $total_bytes,
                status: $status.to_string(),
                error: $error,
            },
        )
    };
}

fn filename_from_url(url: &reqwest::Url) -> String {
    let raw = url
        .path_segments()
        .and_then(|mut segments| segments.rfind(|segment| !segment.is_empty()))
        .unwrap_or("download");
    let decoded = percent_decode_str(raw).decode_utf8_lossy();
    let safe: String = decoded
        .chars()
        .map(|character| {
            if character.is_control()
                || matches!(
                    character,
                    '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*'
                )
            {
                '_'
            } else {
                character
            }
        })
        .collect();
    let safe = safe.trim_end_matches([' ', '.']);

    if safe.is_empty() || safe == "." || safe == ".." {
        "download".to_string()
    } else if is_reserved_windows_name(safe) {
        format!("_{safe}")
    } else {
        safe.to_string()
    }
}

fn resolve_destination(destination: &Path) -> Result<PathBuf, String> {
    let filename = destination
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| "Choose a file name for the download.".to_string())?;
    if !matches!(
        Path::new(filename)
            .components()
            .collect::<Vec<_>>()
            .as_slice(),
        [Component::Normal(_)]
    ) {
        return Err("The selected download destination is invalid.".to_string());
    }
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| "The selected download destination is invalid.".to_string())?;
    let parent = std::fs::canonicalize(parent)
        .map_err(|error| format!("Could not access the selected folder: {error}"))?;
    if !parent.is_dir() {
        return Err("The selected download folder is not a directory.".to_string());
    }
    Ok(parent.join(filename))
}

fn destinations_equal(left: &Path, right: &Path) -> bool {
    #[cfg(windows)]
    {
        left.to_string_lossy()
            .eq_ignore_ascii_case(&right.to_string_lossy())
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

fn ensure_destination_available(
    destination: &Path,
    entries: &HashMap<String, DownloadEntry>,
) -> Result<(), String> {
    if destination.exists() {
        return Err("A file already exists at the selected destination.".to_string());
    }
    if entries.values().any(|entry| {
        !matches!(entry.status.as_str(), "failed" | "cancelled")
            && destinations_equal(&entry.destination, destination)
    }) {
        return Err("Another download is already using the selected destination.".to_string());
    }
    Ok(())
}

fn suggested_destination(
    directory: &Path,
    proposed_filename: &str,
    entries: &HashMap<String, DownloadEntry>,
) -> Result<PathBuf, String> {
    (0..10_000)
        .map(|number| directory.join(numbered_filename(proposed_filename, number)))
        .find(|candidate| {
            !candidate.exists()
                && !entries.values().any(|entry| {
                    !matches!(entry.status.as_str(), "failed" | "cancelled")
                        && destinations_equal(&entry.destination, candidate)
                })
        })
        .ok_or_else(|| "Could not choose a unique download filename.".to_string())
}

fn is_reserved_windows_name(filename: &str) -> bool {
    let stem = Path::new(filename)
        .file_stem()
        .and_then(|part| part.to_str())
        .unwrap_or("")
        .to_ascii_uppercase();
    matches!(
        stem.as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

fn numbered_filename(filename: &str, number: u32) -> String {
    if number == 0 {
        return filename.to_string();
    }

    let path = Path::new(filename);
    let stem = path
        .file_stem()
        .and_then(|part| part.to_str())
        .unwrap_or("download");
    match path.extension().and_then(|part| part.to_str()) {
        Some(extension) => format!("{stem} ({number}).{extension}"),
        None => format!("{stem} ({number})"),
    }
}

fn parse_content_range(value: &str) -> Option<(u64, u64, u64)> {
    let range = value.strip_prefix("bytes ")?;
    let (bounds, total) = range.split_once('/')?;
    let (start, end) = bounds.split_once('-')?;
    let start = start.parse().ok()?;
    let end = end.parse().ok()?;
    let total = total.parse().ok()?;
    (start <= end && end < total).then_some((start, end, total))
}

fn segment_ranges(total: u64, count: usize) -> Vec<(u64, u64)> {
    let count = (count as u64).min(total);
    if count == 0 {
        return Vec::new();
    }

    let base_size = total / count;
    let remainder = total % count;
    let mut start = 0;
    (0..count)
        .map(|index| {
            let size = base_size + u64::from(index < remainder);
            let range = (start, start + size - 1);
            start += size;
            range
        })
        .collect()
}

fn valid_segment_response(
    status: reqwest::StatusCode,
    content_range: Option<(u64, u64, u64)>,
    expected_start: u64,
    expected_end: u64,
    expected_total: u64,
) -> bool {
    status == reqwest::StatusCode::PARTIAL_CONTENT
        && content_range == Some((expected_start, expected_end, expected_total))
}

fn valid_range_probe(
    status: reqwest::StatusCode,
    content_range: Option<(u64, u64, u64)>,
    body_length: usize,
) -> Option<u64> {
    let Some((0, 0, total)) = content_range else {
        return None;
    };
    (status == reqwest::StatusCode::PARTIAL_CONTENT
        && total >= MIN_SEGMENTED_DOWNLOAD_SIZE
        && body_length == 1)
        .then_some(total)
}

fn validators_compatible(expected: Option<&str>, actual: Option<&str>) -> bool {
    match (expected, actual) {
        (Some(expected), actual) => actual == Some(expected),
        (None, _) => true,
    }
}

fn status_is_range_not_satisfiable(response: &reqwest::Response) -> bool {
    response.status() == reqwest::StatusCode::RANGE_NOT_SATISFIABLE
}

fn response_validator(response: &reqwest::Response) -> Option<String> {
    if let Some(etag) = response
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|value| value.to_str().ok())
        && !etag.starts_with("W/")
    {
        return Some(etag.to_string());
    }
    response
        .headers()
        .get(reqwest::header::LAST_MODIFIED)?
        .to_str()
        .ok()
        .map(str::to_string)
}

async fn remove_partial_file(path: &Path) -> Result<(), String> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "Could not remove incomplete download {}: {error}",
            path.display()
        )),
    }
}

async fn finish_failed<S: UpdateSink + ?Sized>(
    app: &S,
    manager: &DownloadManager,
    failure: DownloadFailure<'_>,
) {
    let DownloadFailure {
        id,
        filename,
        path,
        downloaded_bytes,
        total_bytes,
        message,
    } = failure;
    let preserve_partial = message.starts_with("Not enough disk space:")
        || message.starts_with("Could not check free disk space:")
        || message.starts_with("Disk full:")
        || message.starts_with("Retry limit reached");
    let sidecar_path = match manager.entries.lock() {
        Ok(entries) => entries
            .get(id)
            .map(|entry| segment_sidecar_path(&entry.destination)),
        Err(_) => {
            eprintln!("Download state is unavailable while cleaning up the resume sidecar.");
            None
        }
    };
    let message = if preserve_partial {
        message
    } else {
        match remove_partial_file(path).await {
            Ok(()) => {
                if let Some(sidecar_path) = sidecar_path.as_deref()
                    && let Err(error) = remove_segment_sidecar(sidecar_path).await
                {
                    eprintln!("{error}");
                }
                let cancelled = match manager.entries.lock() {
                    Ok(entries) => entries
                        .get(id)
                        .map(|entry| *entry.control.borrow() == Control::Cancelled)
                        .unwrap_or(false),
                    Err(_) => {
                        eprintln!("Download state is unavailable while reporting failure.");
                        false
                    }
                };
                if cancelled {
                    publish_update!(
                        app,
                        manager,
                        id,
                        filename,
                        "cancelled",
                        0,
                        total_bytes,
                        None,
                    );
                    return;
                }
                message
            }
            Err(cleanup_error) => format!("{message}; {cleanup_error}"),
        }
    };
    publish_update!(
        app,
        manager,
        id,
        filename,
        "failed",
        downloaded_bytes,
        total_bytes,
        Some(message),
    );
}

macro_rules! finish_failed {
    ($app:expr, $manager:expr, $id:expr, $filename:expr, $path:expr, $downloaded_bytes:expr, $total_bytes:expr, $message:expr $(,)?) => {
        finish_failed(
            $app,
            $manager,
            DownloadFailure {
                id: $id,
                filename: $filename,
                path: $path,
                downloaded_bytes: $downloaded_bytes,
                total_bytes: $total_bytes,
                message: $message,
            },
        )
    };
}

enum SlotResult {
    Acquired(DownloadPermit),
    Cancelled,
}

enum RetryResult {
    Retry,
    Interrupted,
    Exhausted,
}

struct RetryContext<'a> {
    id: &'a str,
    filename: &'a str,
    downloaded_bytes: u64,
    total_bytes: Option<u64>,
    message: &'a str,
    offline: bool,
}

fn retry_delay(attempt: u32) -> Duration {
    let seconds = 1u64
        .checked_shl(attempt.saturating_sub(1).min(5))
        .unwrap_or(30)
        .min(30);
    if cfg!(test) {
        Duration::from_millis(seconds.min(2) * 10)
    } else {
        Duration::from_secs(seconds)
    }
}

async fn wait_before_retry<S: UpdateSink + ?Sized>(
    app: &S,
    manager: &DownloadManager,
    retry: RetryContext<'_>,
    retry_attempts: &mut u32,
    control: &mut watch::Receiver<Control>,
) -> RetryResult {
    let RetryContext {
        id,
        filename,
        downloaded_bytes,
        total_bytes,
        message,
        offline,
    } = retry;
    *retry_attempts = retry_attempts.saturating_add(1);
    let attempt = *retry_attempts;
    if attempt > MAX_NETWORK_RETRIES {
        if !offline {
            return RetryResult::Exhausted;
        }
        *retry_attempts = 0;
    }
    let delay = if attempt > MAX_NETWORK_RETRIES {
        Duration::from_secs(30)
    } else {
        retry_delay(attempt)
    };
    {
        let mut entries = match manager.entries.lock() {
            Ok(entries) => entries,
            Err(_) => {
                eprintln!("Download state is unavailable while retrying.");
                return RetryResult::Exhausted;
            }
        };
        let Some(entry) = entries.get_mut(id) else {
            eprintln!("Download {id} is no longer available while retrying.");
            return RetryResult::Exhausted;
        };
        entry.retry_count = entry.retry_count.saturating_add(1);
        entry.auto_offline = offline;
        if offline {
            entry.control.send_replace(Control::Paused);
        }
    }
    let status = if offline { "paused" } else { "downloading" };
    let delay_label = if delay.as_secs() > 0 {
        format!("{}s", delay.as_secs())
    } else {
        format!("{}ms", delay.as_millis())
    };
    let description = if attempt > MAX_NETWORK_RETRIES {
        "Offline. Still unable to reach the server; retrying in 30s.".to_string()
    } else {
        let detail = if offline { "Offline" } else { message };
        format!("{detail}. Retrying in {delay_label} (attempt {attempt}/{MAX_NETWORK_RETRIES}).")
    };
    publish_update!(
        app,
        manager,
        id,
        filename,
        status,
        downloaded_bytes,
        total_bytes,
        Some(description),
    );
    if let Err(error) = manager.persist() {
        eprintln!("{error}");
    }
    if offline {
        let _ = control.borrow_and_update();
    }

    tokio::select! {
        _ = tokio::time::sleep(delay) => {}
        changed = control.changed() => {
            if changed.is_err() || *control.borrow() != Control::Running {
                return RetryResult::Interrupted;
            }
            return RetryResult::Interrupted;
        }
    }
    if offline {
        let resumed = match manager.entries.lock() {
            Ok(mut entries) => {
                if let Some(entry) = entries.get_mut(id)
                    && entry.auto_offline
                    && *control.borrow() == Control::Paused
                {
                    entry.auto_offline = false;
                    entry.control.send_replace(Control::Running);
                    true
                } else {
                    false
                }
            }
            Err(_) => {
                eprintln!("Download state is unavailable while recovering the connection.");
                false
            }
        };
        if !resumed {
            return RetryResult::Interrupted;
        }
        let _ = control.borrow_and_update();
        publish_update!(
            app,
            manager,
            id,
            filename,
            "downloading",
            downloaded_bytes,
            total_bytes,
            None,
        );
    }
    RetryResult::Retry
}

fn pause_active_downloads(manager: &DownloadManager) -> Result<(), String> {
    let entries = manager
        .entries
        .lock()
        .map_err(|_| "Download state is unavailable during shutdown.".to_string())?;
    for entry in entries.values() {
        if entry.status == "downloading" {
            entry.control.send_replace(Control::Paused);
        }
    }
    Ok(())
}

async fn acquire_download_slot(
    admission: &Arc<DownloadAdmission>,
    receiver: &mut watch::Receiver<Control>,
) -> Result<SlotResult, String> {
    loop {
        let control = *receiver.borrow();
        match control {
            Control::Running => {}
            Control::Cancelled => return Ok(SlotResult::Cancelled),
            Control::Paused => {
                if receiver.changed().await.is_err() {
                    return Ok(SlotResult::Cancelled);
                }
                continue;
            }
        }

        let notified = admission.changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        {
            let mut state = admission
                .state
                .lock()
                .map_err(|_| "Download settings are unavailable.".to_string())?;
            if state.active < state.limit {
                state.active += 1;
                return Ok(SlotResult::Acquired(DownloadPermit {
                    admission: admission.clone(),
                }));
            }
        }

        tokio::select! {
            _ = &mut notified => {}
            changed = receiver.changed() => {
                if changed.is_err() {
                    return Ok(SlotResult::Cancelled);
                }
            }
        }
    }
}

async fn finalize_file(destination: &Path, partial_path: &Path) -> Result<(), String> {
    tokio::fs::hard_link(partial_path, destination)
        .await
        .map_err(|error| {
            if error.kind() == ErrorKind::AlreadyExists {
                "A file already exists at the selected destination; it was not overwritten."
                    .to_string()
            } else {
                format!("Could not finalize the download: {error}")
            }
        })?;
    tokio::fs::remove_file(partial_path)
        .await
        .map_err(|error| format!("Could not finalize the download: {error}"))
}

enum SegmentedAttempt {
    Completed(u64),
    Fallback,
    Retryable(String, bool),
    Paused(u64, u64, Option<String>),
    Cancelled(u64, u64),
}

enum SegmentWorkerResult {
    Completed,
    Invalid,
    Retryable(String, bool),
    Controlled(Control),
    Failed(String),
}

fn is_offline_error(error: &reqwest::Error) -> bool {
    error.is_connect() || error.is_timeout()
}

async fn fetch_segment(request: SegmentRequest) -> SegmentWorkerResult {
    let SegmentRequest {
        client,
        app,
        manager,
        url,
        partial_path,
        filename,
        id,
        segment_index,
        segment_end,
        position,
        total,
        validator,
        progress,
        last_progress_update,
        tracker,
        mut control,
    } = request;
    if position > segment_end {
        return SegmentWorkerResult::Completed;
    }
    if *control.borrow() != Control::Running {
        return SegmentWorkerResult::Controlled(*control.borrow());
    }

    let mut request = client
        .get(url)
        .header(
            reqwest::header::RANGE,
            format!("bytes={position}-{segment_end}"),
        )
        .header(reqwest::header::ACCEPT_ENCODING, "identity");
    if let Some(validator) = validator.as_deref() {
        request = request.header(reqwest::header::IF_RANGE, validator);
    }
    let response = tokio::select! {
        result = request.send() => match result {
            Ok(response) => response,
            Err(error) => {
                return SegmentWorkerResult::Retryable(error.to_string(), is_offline_error(&error));
            }
        },
        changed = control.changed() => {
            if changed.is_err() || *control.borrow() != Control::Running {
                return SegmentWorkerResult::Controlled(*control.borrow());
            }
            return SegmentWorkerResult::Invalid;
        }
    };

    let content_range = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_content_range);
    if !valid_segment_response(
        response.status(),
        content_range,
        position,
        segment_end,
        total,
    ) || !validators_compatible(
        validator.as_deref(),
        response_validator(&response).as_deref(),
    ) || response
        .content_length()
        .is_some_and(|length| length != segment_end - position + 1)
    {
        return SegmentWorkerResult::Invalid;
    }

    let mut file = match tokio::fs::OpenOptions::new()
        .write(true)
        .open(partial_path)
        .await
    {
        Ok(file) => file,
        Err(error) => {
            return SegmentWorkerResult::Failed(disk_error_message(
                "Could not open the partial download for a segment",
                error,
            ));
        }
    };
    if let Err(error) = file.seek(std::io::SeekFrom::Start(position)).await {
        return SegmentWorkerResult::Failed(format!(
            "Could not position a download segment: {error}"
        ));
    };
    let mut stream = response.bytes_stream();
    let mut written = 0u64;
    loop {
        tokio::select! {
            chunk = tokio::time::timeout(STALL_TIMEOUT, stream.next()) => match chunk {
                Ok(Some(Ok(chunk))) => {
                    if written.saturating_add(chunk.len() as u64) > segment_end - position + 1 {
                        return SegmentWorkerResult::Invalid;
                    }
                    if let Err(error) = file.write_all(&chunk).await {
                        return SegmentWorkerResult::Failed(disk_error_message(
                            "Could not write a download segment",
                            error,
                        ));
                    }
                    written += chunk.len() as u64;
                    let downloaded = progress.fetch_add(chunk.len() as u64, Ordering::Relaxed)
                        + chunk.len() as u64;
                    if let Ok(mut last_update) = last_progress_update.lock()
                        && last_update.elapsed() >= Duration::from_millis(200)
                    {
                            publish_update!(
                                &app,
                                &manager,
                                &id,
                                &filename,
                                "downloading",
                                downloaded,
                                Some(total),
                                None,
                            );
                            *last_update = Instant::now();
                    }
                    let checkpoint_due = {
                        tracker.last_saved.lock().await.elapsed() >= Duration::from_secs(1)
                    };
                    if checkpoint_due {
                        if let Err(error) = file.sync_data().await {
                            return SegmentWorkerResult::Failed(format!(
                                "Could not checkpoint a download segment: {error}"
                            ));
                        }
                        if let Err(error) = tracker
                            .checkpoint(segment_index, position + written, false)
                            .await
                        {
                            return SegmentWorkerResult::Failed(error);
                        }
                    }
                }
                Ok(Some(Err(error))) => {
                    if let Err(checkpoint_error) = checkpoint_segment_failure(
                        &mut file,
                        &tracker,
                        segment_index,
                        position + written,
                    ).await {
                        return SegmentWorkerResult::Failed(checkpoint_error);
                    }
                    return SegmentWorkerResult::Retryable(
                        error.to_string(),
                        is_offline_error(&error),
                    );
                }
                Ok(None) => break,
                Err(_) => {
                    if let Err(error) = checkpoint_segment_failure(
                        &mut file,
                        &tracker,
                        segment_index,
                        position + written,
                    ).await {
                        return SegmentWorkerResult::Failed(error);
                    }
                    return SegmentWorkerResult::Retryable(
                        "No data received from the server before the 15-second stall timeout."
                            .to_string(),
                        false,
                    );
                }
            },
            changed = control.changed() => {
                if changed.is_err() || *control.borrow() != Control::Running {
                    let state = *control.borrow();
                    if let Err(error) = file.flush().await {
                        return SegmentWorkerResult::Failed(disk_error_message(
                            "Could not flush a paused download segment",
                            error,
                        ));
                    }
                    if let Err(error) = file.sync_data().await {
                        return SegmentWorkerResult::Failed(disk_error_message(
                            "Could not checkpoint a download segment",
                            error,
                        ));
                    }
                    if let Err(error) = tracker
                        .checkpoint(segment_index, position + written, true)
                        .await
                    {
                        return SegmentWorkerResult::Failed(error);
                    }
                    return SegmentWorkerResult::Controlled(state);
                }
                return SegmentWorkerResult::Invalid;
            }
        }
    }
    if let Err(error) = file.flush().await {
        return SegmentWorkerResult::Failed(disk_error_message(
            "Could not flush a download segment",
            error,
        ));
    }
    if let Err(error) = file.sync_data().await {
        return SegmentWorkerResult::Failed(disk_error_message(
            "Could not checkpoint a download segment",
            error,
        ));
    }
    if written != segment_end - position + 1 {
        SegmentWorkerResult::Retryable(
            "The server closed the connection before the requested range was complete.".to_string(),
            false,
        )
    } else {
        match tracker
            .checkpoint(segment_index, segment_end + 1, true)
            .await
        {
            Ok(()) => SegmentWorkerResult::Completed,
            Err(error) => SegmentWorkerResult::Failed(error),
        }
    }
}

async fn checkpoint_segment_failure(
    file: &mut tokio::fs::File,
    tracker: &SegmentResumeTracker,
    segment_index: usize,
    next_position: u64,
) -> Result<(), String> {
    file.flush()
        .await
        .map_err(|error| disk_error_message("Could not flush a failed download segment", error))?;
    file.sync_data().await.map_err(|error| {
        disk_error_message("Could not checkpoint a failed download segment", error)
    })?;
    tracker.checkpoint(segment_index, next_position, true).await
}

async fn create_preallocated_partial(path: &Path, total: u64) -> Result<(), String> {
    ensure_disk_space(path, total, true)?;
    let file = tokio::fs::File::create(path)
        .await
        .map_err(|error| disk_error_message("Could not prepare the partial download", error))?;
    file.set_len(total)
        .await
        .map_err(|error| disk_error_message("Could not preallocate the partial download", error))?;
    file.sync_all()
        .await
        .map_err(|error| disk_error_message("Could not flush the preallocated download", error))
}

fn enough_disk_space(required: u64, available: u64, reclaimable: u64) -> bool {
    required <= available.saturating_add(reclaimable)
}

fn ensure_disk_space(path: &Path, required: u64, replacing_partial: bool) -> Result<(), String> {
    if required == 0 {
        return Ok(());
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let available = fs2::available_space(parent)
        .map_err(|error| format!("Could not check free disk space: {error}"))?;
    let reclaimable = if replacing_partial {
        std::fs::metadata(path)
            .ok()
            .filter(|metadata| metadata.is_file())
            .map(|metadata| metadata.len())
            .unwrap_or(0)
    } else {
        0
    };
    if !enough_disk_space(required, available, reclaimable) {
        return Err(format!(
            "Not enough disk space: need {required} bytes, only {} bytes are available.",
            available.saturating_add(reclaimable)
        ));
    }
    Ok(())
}

fn disk_error_message(context: &str, error: std::io::Error) -> String {
    if error.kind() == ErrorKind::StorageFull {
        format!("Disk full: {error}")
    } else {
        format!("{context}: {error}")
    }
}

fn new_segment_resume_state(
    url: &str,
    final_url: &str,
    total: u64,
    validator: Option<String>,
) -> SegmentResumeState {
    SegmentResumeState {
        version: SEGMENT_RESUME_VERSION,
        url: url.to_string(),
        final_url: final_url.to_string(),
        total_size: total,
        validator,
        segments: segment_ranges(total, MAX_SEGMENTS_PER_DOWNLOAD)
            .into_iter()
            .map(|(start, end)| SegmentPosition {
                start,
                end,
                position: start,
            })
            .collect(),
    }
}

async fn try_segmented_download(
    context: SegmentedDownloadContext<'_>,
    control: &mut watch::Receiver<Control>,
) -> Result<SegmentedAttempt, String> {
    let SegmentedDownloadContext {
        client,
        app,
        manager,
        id,
        url,
        filename,
        path,
        sidecar_path,
    } = context;
    let old_state = read_segment_resume_state(sidecar_path)?;
    let old_progress = old_state.as_ref().map(segment_progress).unwrap_or(0);
    if *control.borrow() != Control::Running {
        return Ok(match *control.borrow() {
            Control::Paused => SegmentedAttempt::Paused(
                old_progress,
                old_state
                    .as_ref()
                    .map(|state| state.total_size)
                    .unwrap_or(0),
                old_state.and_then(|state| state.validator),
            ),
            Control::Cancelled => SegmentedAttempt::Cancelled(
                old_progress,
                old_state.map(|state| state.total_size).unwrap_or(0),
            ),
            Control::Running => unreachable!(),
        });
    }

    let probe_request = client
        .get(url)
        .header(reqwest::header::RANGE, "bytes=0-0")
        .header(reqwest::header::ACCEPT_ENCODING, "identity");
    let probe = tokio::select! {
        result = probe_request.send() => match result {
            Ok(response) => response,
            Err(error) => {
                return Ok(SegmentedAttempt::Retryable(
                    error.to_string(),
                    is_offline_error(&error),
                ));
            }
        },
        changed = control.changed() => {
            return Ok(match *control.borrow() {
                Control::Paused => SegmentedAttempt::Paused(
                    old_progress,
                    old_state.as_ref().map(|state| state.total_size).unwrap_or(0),
                    old_state.as_ref().and_then(|state| state.validator.clone()),
                ),
                Control::Cancelled => SegmentedAttempt::Cancelled(
                    old_progress,
                    old_state.as_ref().map(|state| state.total_size).unwrap_or(0),
                ),
                Control::Running => {
                    if changed.is_err() {
                        SegmentedAttempt::Cancelled(
                            old_progress,
                            old_state.as_ref().map(|state| state.total_size).unwrap_or(0),
                        )
                    } else {
                        SegmentedAttempt::Fallback
                    }
                }
            });
        }
    };
    let probe_status = probe.status();
    let probe_range = probe
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_content_range);
    let final_url = probe.url().to_string();
    let Some((0, 0, total)) = probe_range else {
        if sidecar_path.exists() {
            create_preallocated_partial(path, 0).await?;
            remove_segment_sidecar(sidecar_path).await?;
        }
        return Ok(SegmentedAttempt::Fallback);
    };
    if probe_status != reqwest::StatusCode::PARTIAL_CONTENT || total < MIN_SEGMENTED_DOWNLOAD_SIZE {
        if sidecar_path.exists() {
            create_preallocated_partial(path, 0).await?;
            remove_segment_sidecar(sidecar_path).await?;
        }
        return Ok(SegmentedAttempt::Fallback);
    }
    let validator = response_validator(&probe);
    if probe.content_length().is_some_and(|length| length != 1) {
        if sidecar_path.exists() {
            create_preallocated_partial(path, 0).await?;
            remove_segment_sidecar(sidecar_path).await?;
        }
        return Ok(SegmentedAttempt::Fallback);
    }
    let mut probe_stream = probe.bytes_stream();
    let mut probe_body_length = 0usize;
    loop {
        tokio::select! {
            chunk = probe_stream.next() => match chunk {
                Some(Ok(chunk)) => {
                    probe_body_length = probe_body_length.saturating_add(chunk.len());
                    if probe_body_length > 1 {
                        break;
                    }
                }
                Some(Err(_)) => return Ok(SegmentedAttempt::Fallback),
                None => break,
            },
            changed = control.changed() => {
                return Ok(match *control.borrow() {
                    Control::Paused => SegmentedAttempt::Paused(
                        old_progress,
                        total,
                        old_state.as_ref().and_then(|state| state.validator.clone()),
                    ),
                    Control::Cancelled => SegmentedAttempt::Cancelled(0, total),
                    Control::Running => {
                        if changed.is_err() {
                            SegmentedAttempt::Cancelled(0, total)
                        } else {
                            SegmentedAttempt::Fallback
                        }
                    }
                });
            }
        }
    }
    if valid_range_probe(probe_status, probe_range, probe_body_length) != Some(total) {
        if sidecar_path.exists() {
            create_preallocated_partial(path, 0).await?;
            remove_segment_sidecar(sidecar_path).await?;
        }
        return Ok(SegmentedAttempt::Fallback);
    }
    remember_validator(manager, id, validator.clone());

    let partial_size = tokio::fs::metadata(path)
        .await
        .ok()
        .filter(|metadata| metadata.is_file())
        .map(|metadata| metadata.len());
    let resume_state = old_state.filter(|state| {
        state.url == url
            && state.final_url == final_url
            && state.total_size == total
            && state.validator == validator
            && partial_size == Some(total)
    });
    let state = if let Some(state) = resume_state {
        state
    } else {
        ensure_disk_space(path, total, true)?;
        let state = new_segment_resume_state(url, &final_url, total, validator.clone());
        persist_segment_resume_state(sidecar_path, &state).await?;
        create_preallocated_partial(path, total).await?;
        state
    };
    let tracker = SegmentResumeTracker {
        state: Arc::new(tokio::sync::Mutex::new(state.clone())),
        path: sidecar_path.to_path_buf(),
        persistence_lock: Arc::default(),
        last_saved: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };
    let initial_progress = segment_progress(&state);
    publish_update!(
        app,
        manager,
        id,
        filename,
        "downloading",
        initial_progress,
        Some(total),
        None,
    );
    let mut workers = tokio::task::JoinSet::new();
    let progress = Arc::new(AtomicU64::new(initial_progress));
    let last_progress_update = Arc::new(Mutex::new(Instant::now() - Duration::from_secs(1)));
    for (index, segment) in state.segments.iter().enumerate() {
        if segment.position > segment.end {
            continue;
        }
        workers.spawn(fetch_segment(SegmentRequest {
            client: client.clone(),
            app: app.clone(),
            manager: manager.clone(),
            url: final_url.clone(),
            partial_path: path.to_path_buf(),
            filename: filename.to_string(),
            id: id.to_string(),
            segment_index: index,
            segment_end: segment.end,
            position: segment.position,
            total,
            validator: validator.clone(),
            progress: progress.clone(),
            last_progress_update: last_progress_update.clone(),
            tracker: tracker.clone(),
            control: control.clone(),
        }));
    }

    let mut attempt = SegmentedAttempt::Completed(total);
    let mut worker_error = None;
    while let Some(result) = workers.join_next().await {
        match result {
            Ok(SegmentWorkerResult::Completed) => {
                if !matches!(attempt, SegmentedAttempt::Paused(_, _, _)) {
                    publish_update!(
                        app,
                        manager,
                        id,
                        filename,
                        "downloading",
                        progress.load(Ordering::Relaxed),
                        Some(total),
                        None,
                    );
                }
            }
            Ok(SegmentWorkerResult::Controlled(Control::Paused)) => {
                attempt = SegmentedAttempt::Paused(
                    progress.load(Ordering::Relaxed),
                    total,
                    validator.clone(),
                );
            }
            Ok(SegmentWorkerResult::Controlled(Control::Cancelled)) => {
                attempt = SegmentedAttempt::Cancelled(progress.load(Ordering::Relaxed), total);
                break;
            }
            Ok(SegmentWorkerResult::Failed(error)) => {
                worker_error = Some(error);
                break;
            }
            Ok(SegmentWorkerResult::Retryable(message, offline)) => {
                attempt = SegmentedAttempt::Retryable(message, offline);
                break;
            }
            Ok(SegmentWorkerResult::Controlled(Control::Running))
            | Ok(SegmentWorkerResult::Invalid) => match *control.borrow() {
                Control::Paused => {
                    attempt = SegmentedAttempt::Paused(
                        progress.load(Ordering::Relaxed),
                        total,
                        validator.clone(),
                    );
                }
                Control::Cancelled => {
                    attempt = SegmentedAttempt::Cancelled(progress.load(Ordering::Relaxed), total);
                    break;
                }
                Control::Running if !matches!(attempt, SegmentedAttempt::Paused(_, _, _)) => {
                    attempt = SegmentedAttempt::Fallback;
                    break;
                }
                Control::Running => {}
            },
            Err(_) => match *control.borrow() {
                Control::Paused => {
                    attempt = SegmentedAttempt::Paused(
                        progress.load(Ordering::Relaxed),
                        total,
                        validator.clone(),
                    );
                }
                Control::Cancelled => {
                    attempt = SegmentedAttempt::Cancelled(progress.load(Ordering::Relaxed), total);
                    break;
                }
                Control::Running if !matches!(attempt, SegmentedAttempt::Paused(_, _, _)) => {
                    attempt = SegmentedAttempt::Fallback;
                    break;
                }
                Control::Running => {}
            },
        }
    }
    if matches!(
        attempt,
        SegmentedAttempt::Fallback
            | SegmentedAttempt::Retryable(_, _)
            | SegmentedAttempt::Cancelled(_, _)
    ) {
        workers.abort_all();
    }
    while workers.join_next().await.is_some() {}
    if matches!(attempt, SegmentedAttempt::Paused(_, _, _)) {
        let _ = control.borrow_and_update();
    }
    if let Some(error) = worker_error {
        return Err(error);
    }
    tracker.persist().await?;
    let saved_state = tracker.snapshot().await;
    let saved_progress = segment_progress(&saved_state);

    if matches!(attempt, SegmentedAttempt::Completed(_)) && saved_progress != total {
        attempt = SegmentedAttempt::Fallback;
    }
    if matches!(attempt, SegmentedAttempt::Paused(_, _, _)) && saved_progress == total {
        attempt = SegmentedAttempt::Completed(total);
    }
    if matches!(attempt, SegmentedAttempt::Fallback) {
        create_preallocated_partial(path, 0).await?;
        remove_segment_sidecar(sidecar_path).await?;
    } else if matches!(attempt, SegmentedAttempt::Paused(_, _, _)) {
        attempt = SegmentedAttempt::Paused(saved_progress, total, validator);
    }
    Ok(attempt)
}

async fn download_file<R: Runtime>(
    app: AppHandle<R>,
    manager: DownloadManager,
    download: DownloadContext,
    control: watch::Receiver<Control>,
) {
    let task_lock = match manager.entries.lock() {
        Ok(entries) => entries
            .get(&download.id)
            .map(|entry| entry.task_lock.clone()),
        Err(_) => None,
    };
    let Some(task_lock) = task_lock else {
        eprintln!(
            "Download {} disappeared before its transfer could start.",
            download.id
        );
        return;
    };
    let _task_guard = task_lock.lock().await;
    let app: Arc<dyn UpdateSink> = Arc::new(app);
    download_file_inner(app, manager, download, control).await;
}

async fn download_file_inner(
    app: Arc<dyn UpdateSink>,
    manager: DownloadManager,
    download: DownloadContext,
    mut control: watch::Receiver<Control>,
) {
    let DownloadContext {
        id,
        url,
        filename,
        destination,
        partial_path: path,
    } = download;
    let client = match reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(15))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            finish_failed!(
                &app,
                &manager,
                &id,
                &filename,
                &path,
                0,
                None,
                format!("Could not configure the download connection: {error}"),
            )
            .await;
            return;
        }
    };
    let sidecar_path = segment_sidecar_path(&destination);
    let mut validator = manager
        .entries
        .lock()
        .ok()
        .and_then(|entries| entries.get(&id).and_then(|entry| entry.validator.clone()));
    let mut total_bytes = None;
    let mut last_update = Instant::now();
    let mut force_sequential = false;
    let mut retry_attempts = 0;

    loop {
        let offset = if sidecar_path.exists() {
            match read_segment_resume_state(&sidecar_path) {
                Ok(Some(state)) if state.url == url => segment_progress(&state),
                Ok(_) => 0,
                Err(error) => {
                    finish_failed!(&app, &manager, &id, &filename, &path, 0, total_bytes, error,)
                        .await;
                    return;
                }
            }
        } else {
            match tokio::fs::metadata(&path).await {
                Ok(metadata) => metadata.len(),
                Err(error) => {
                    finish_failed!(
                        &app,
                        &manager,
                        &id,
                        &filename,
                        &path,
                        0,
                        total_bytes,
                        format!("Could not inspect the partial download: {error}"),
                    )
                    .await;
                    return;
                }
            }
        };

        let _slot = match acquire_download_slot(&manager.admission, &mut control).await {
            Ok(SlotResult::Acquired(slot)) => slot,
            Ok(SlotResult::Cancelled) => {
                if let Err(error) = remove_partial_file(&path).await {
                    publish_update!(
                        &app,
                        &manager,
                        &id,
                        &filename,
                        "failed",
                        offset,
                        total_bytes,
                        Some(error),
                    );
                } else {
                    if let Err(error) = remove_segment_sidecar(&sidecar_path).await {
                        eprintln!("{error}");
                    }
                    publish_update!(
                        &app,
                        &manager,
                        &id,
                        &filename,
                        "cancelled",
                        0,
                        total_bytes,
                        None,
                    );
                }
                return;
            }
            Err(error) => {
                finish_failed!(
                    &app,
                    &manager,
                    &id,
                    &filename,
                    &path,
                    offset,
                    total_bytes,
                    error,
                )
                .await;
                return;
            }
        };
        publish_update!(
            &app,
            &manager,
            &id,
            &filename,
            "downloading",
            offset,
            total_bytes,
            None,
        );

        if !force_sequential && (offset == 0 || sidecar_path.exists()) {
            match try_segmented_download(
                SegmentedDownloadContext {
                    client: &client,
                    app: &app,
                    manager: &manager,
                    id: &id,
                    url: &url,
                    filename: &filename,
                    path: &path,
                    sidecar_path: &sidecar_path,
                },
                &mut control,
            )
            .await
            {
                Ok(SegmentedAttempt::Completed(total)) => {
                    match finalize_file(&destination, &path).await {
                        Ok(()) => {
                            if let Err(error) = remove_segment_sidecar(&sidecar_path).await {
                                eprintln!("{error}");
                            }
                            publish_update!(
                                &app,
                                &manager,
                                &id,
                                &filename,
                                "completed",
                                total,
                                Some(total),
                                None,
                            );
                        }
                        Err(error) => {
                            finish_failed!(
                                &app,
                                &manager,
                                &id,
                                &filename,
                                &path,
                                total,
                                Some(total),
                                error,
                            )
                            .await;
                        }
                    }
                    return;
                }
                Ok(SegmentedAttempt::Fallback) => {
                    force_sequential = true;
                    continue;
                }
                Ok(SegmentedAttempt::Retryable(message, offline)) => {
                    let progress = manager
                        .entries
                        .lock()
                        .ok()
                        .and_then(|entries| entries.get(&id).map(|entry| entry.downloaded_bytes))
                        .unwrap_or(offset);
                    drop(_slot);
                    match wait_before_retry(
                        &*app,
                        &manager,
                        RetryContext {
                            id: &id,
                            filename: &filename,
                            downloaded_bytes: progress,
                            total_bytes,
                            message: &message,
                            offline,
                        },
                        &mut retry_attempts,
                        &mut control,
                    )
                    .await
                    {
                        RetryResult::Retry | RetryResult::Interrupted => continue,
                        RetryResult::Exhausted => {
                            finish_failed!(
                                &app,
                                &manager,
                                &id,
                                &filename,
                                &path,
                                progress,
                                total_bytes,
                                format!(
                                    "Retry limit reached after {MAX_NETWORK_RETRIES} attempts: {message}"
                                ),
                            )
                            .await;
                            return;
                        }
                    }
                }
                Ok(SegmentedAttempt::Paused(progress, total, segment_validator)) => {
                    if segment_validator.is_some() {
                        validator = segment_validator.clone();
                        remember_validator(&manager, &id, segment_validator);
                    }
                    publish_update!(
                        &app,
                        &manager,
                        &id,
                        &filename,
                        "paused",
                        progress,
                        Some(total),
                        None,
                    );
                    continue;
                }
                Ok(SegmentedAttempt::Cancelled(progress, total)) => {
                    if let Err(error) = remove_partial_file(&path).await {
                        finish_failed!(
                            &app,
                            &manager,
                            &id,
                            &filename,
                            &path,
                            progress,
                            Some(total),
                            error,
                        )
                        .await;
                    } else {
                        if let Err(error) = remove_segment_sidecar(&sidecar_path).await {
                            eprintln!("{error}");
                        }
                        publish_update!(
                            &app,
                            &manager,
                            &id,
                            &filename,
                            "cancelled",
                            0,
                            Some(total),
                            None,
                        );
                    }
                    return;
                }
                Err(error) => {
                    finish_failed!(
                        &app,
                        &manager,
                        &id,
                        &filename,
                        &path,
                        offset,
                        total_bytes,
                        error,
                    )
                    .await;
                    return;
                }
            }
        }

        let can_resume = offset > 0;
        let mut request = client.get(&url);
        if can_resume {
            request = request.header(reqwest::header::RANGE, format!("bytes={offset}-"));
            if let Some(validator) = validator.as_deref() {
                request = request.header(reqwest::header::IF_RANGE, validator);
            }
        }
        let response = tokio::select! {
            result = request.send() => match result {
                Ok(response) => response,
                Err(error) => {
                    let message = error.to_string();
                    drop(_slot);
                    match wait_before_retry(
                        &*app,
                        &manager,
                        RetryContext {
                            id: &id,
                            filename: &filename,
                            downloaded_bytes: offset,
                            total_bytes,
                            message: &message,
                            offline: is_offline_error(&error),
                        },
                        &mut retry_attempts,
                        &mut control,
                    ).await {
                        RetryResult::Retry | RetryResult::Interrupted => continue,
                        RetryResult::Exhausted => {
                            finish_failed!(
                                &app,
                                &manager,
                                &id,
                                &filename,
                                &path,
                                offset,
                                total_bytes,
                                format!(
                                    "Retry limit reached after {MAX_NETWORK_RETRIES} attempts: {message}"
                                ),
                            ).await;
                            return;
                        }
                    }
                }
            },
            changed = control.changed() => {
                if changed.is_err() || *control.borrow() == Control::Cancelled {
                    if let Err(error) = remove_partial_file(&path).await {
                        finish_failed!(&app, &manager, &id, &filename, &path, offset, total_bytes, error).await;
                    } else {
                        if let Err(error) = remove_segment_sidecar(&sidecar_path).await {
                            eprintln!("{error}");
                        }
                        publish_update!(&app, &manager, &id, &filename, "cancelled", 0, total_bytes, None);
                    }
                    return;
                }
                if *control.borrow() == Control::Paused {
                    publish_update!(&app, &manager, &id, &filename, "paused", offset, total_bytes, None);
                }
                continue;
            }
        };

        if can_resume && status_is_range_not_satisfiable(&response) {
            drop(response);
            if let Err(error) = tokio::fs::File::create(&path).await {
                finish_failed!(
                    &app,
                    &manager,
                    &id,
                    &filename,
                    &path,
                    offset,
                    total_bytes,
                    disk_error_message("Could not restart the download", error),
                )
                .await;
                return;
            }
            validator = None;
            total_bytes = None;
            continue;
        }

        if matches!(
            response.status(),
            reqwest::StatusCode::FORBIDDEN | reqwest::StatusCode::GONE
        ) {
            let status = response.status();
            publish_update!(
                &app,
                &manager,
                &id,
                &filename,
                "link-expired",
                offset,
                total_bytes,
                Some(format!(
                    "Link expired (HTTP {status}). Update the URL to continue this download."
                )),
            );
            return;
        }

        if let Err(error) = response.error_for_status_ref() {
            finish_failed!(
                &app,
                &manager,
                &id,
                &filename,
                &path,
                offset,
                total_bytes,
                error.to_string(),
            )
            .await;
            return;
        }

        let status = response.status();
        let mut range_end = None;
        let truncate = if can_resume && status == reqwest::StatusCode::PARTIAL_CONTENT {
            let content_range = response
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .and_then(parse_content_range);
            let current_validator = response_validator(&response);
            let valid_range = content_range
                .map(|(start, _, _)| start == offset)
                .unwrap_or(false);
            let valid_validator = validator
                .as_deref()
                .map(|expected| current_validator.as_deref() == Some(expected))
                .unwrap_or(true);
            if !valid_range || !valid_validator {
                drop(response);
                if let Err(error) = tokio::fs::File::create(&path).await {
                    finish_failed!(
                        &app,
                        &manager,
                        &id,
                        &filename,
                        &path,
                        offset,
                        total_bytes,
                        disk_error_message("Could not restart the download", error),
                    )
                    .await;
                    return;
                }
                validator = None;
                total_bytes = None;
                continue;
            }
            if let Some((_, end, total)) = content_range {
                range_end = Some(end);
                total_bytes = Some(total);
            }
            if validator.is_none() {
                validator = current_validator;
            }
            false
        } else if status == reqwest::StatusCode::OK {
            true
        } else if status == reqwest::StatusCode::PARTIAL_CONTENT {
            let range = response
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .and_then(parse_content_range);
            if !matches!(range, Some((0, _, _))) {
                finish_failed!(
                    &app,
                    &manager,
                    &id,
                    &filename,
                    &path,
                    offset,
                    total_bytes,
                    "Server returned an invalid Content-Range response.".to_string(),
                )
                .await;
                return;
            }
            if let Some((_, end, total)) = range {
                range_end = Some(end);
                total_bytes = Some(total);
            }
            validator = response_validator(&response);
            true
        } else {
            finish_failed!(
                &app,
                &manager,
                &id,
                &filename,
                &path,
                offset,
                total_bytes,
                format!("Server returned an unsupported HTTP status: {status}."),
            )
            .await;
            return;
        };

        let required_space = if truncate {
            response.content_length().or(total_bytes).unwrap_or(0)
        } else if let Some(end) = range_end {
            end.saturating_sub(offset).saturating_add(1)
        } else {
            total_bytes
                .map(|total| total.saturating_sub(offset))
                .unwrap_or(0)
        };
        if let Err(error) = ensure_disk_space(&path, required_space, truncate) {
            finish_failed!(
                &app,
                &manager,
                &id,
                &filename,
                &path,
                offset,
                total_bytes,
                error,
            )
            .await;
            return;
        }

        if truncate {
            if let Err(error) = tokio::fs::File::create(&path).await {
                finish_failed!(
                    &app,
                    &manager,
                    &id,
                    &filename,
                    &path,
                    offset,
                    total_bytes,
                    disk_error_message("Could not restart the download", error),
                )
                .await;
                return;
            }
            if status == reqwest::StatusCode::OK {
                total_bytes = response.content_length();
            }
            validator = response_validator(&response);
        }
        remember_validator(&manager, &id, validator.clone());

        let mut file = match tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(!truncate)
            .truncate(truncate)
            .open(&path)
            .await
        {
            Ok(file) => file,
            Err(error) => {
                finish_failed!(
                    &app,
                    &manager,
                    &id,
                    &filename,
                    &path,
                    offset,
                    total_bytes,
                    disk_error_message("Could not open the partial download", error),
                )
                .await;
                return;
            }
        };

        let mut downloaded_bytes = if truncate { 0 } else { offset };
        let mut stream = response.bytes_stream();
        publish_update!(
            &app,
            &manager,
            &id,
            &filename,
            "downloading",
            downloaded_bytes,
            total_bytes,
            None,
        );
        let mut paused = false;
        let mut cancelled = false;
        let mut stream_error: Option<(String, bool, bool)> = None;

        loop {
            tokio::select! {
                chunk = tokio::time::timeout(STALL_TIMEOUT, stream.next()) => match chunk {
                    Ok(Some(Ok(chunk))) => {
                        if let Err(error) = file.write_all(&chunk).await {
                            stream_error = Some((disk_error_message("Disk write failed", error), false, false));
                            break;
                        }
                        downloaded_bytes += chunk.len() as u64;
                        if last_update.elapsed() >= Duration::from_millis(200) {
                            publish_update!(
                                &app, &manager, &id, &filename, "downloading",
                                downloaded_bytes, total_bytes, None,
                            );
                            last_update = Instant::now();
                        }
                    }
                    Ok(Some(Err(error))) => {
                        stream_error = Some((
                            error.to_string(),
                            true,
                            is_offline_error(&error),
                        ));
                        break;
                    }
                    Ok(None) => break,
                    Err(_) => {
                        stream_error = Some((
                            "No data received from the server before the 15-second stall timeout."
                                .to_string(),
                            true,
                            false,
                        ));
                        break;
                    }
                },
                changed = control.changed() => {
                    if changed.is_err() || *control.borrow() == Control::Cancelled {
                        cancelled = true;
                        break;
                    }
                    if *control.borrow() == Control::Paused {
                        paused = true;
                        break;
                    }
                }
            }
        }

        if let Err(error) = file.flush().await {
            stream_error = Some((
                disk_error_message("Could not flush the partial download", error),
                false,
                false,
            ));
        }
        drop(file);
        drop(stream);

        match *control.borrow() {
            Control::Cancelled => cancelled = true,
            Control::Paused => paused = true,
            Control::Running => {}
        }

        if cancelled {
            if let Err(error) = remove_partial_file(&path).await {
                finish_failed!(
                    &app,
                    &manager,
                    &id,
                    &filename,
                    &path,
                    downloaded_bytes,
                    total_bytes,
                    error,
                )
                .await;
            } else {
                if let Err(error) = remove_segment_sidecar(&sidecar_path).await {
                    eprintln!("{error}");
                }
                publish_update!(
                    &app,
                    &manager,
                    &id,
                    &filename,
                    "cancelled",
                    0,
                    total_bytes,
                    None,
                );
            }
            return;
        }

        if paused {
            let actual_bytes = match tokio::fs::metadata(&path).await {
                Ok(metadata) => metadata.len(),
                Err(error) => {
                    finish_failed!(
                        &app,
                        &manager,
                        &id,
                        &filename,
                        &path,
                        downloaded_bytes,
                        total_bytes,
                        format!("Could not inspect the paused download: {error}"),
                    )
                    .await;
                    return;
                }
            };
            publish_update!(
                &app,
                &manager,
                &id,
                &filename,
                "paused",
                actual_bytes,
                total_bytes,
                None,
            );
            continue;
        }

        if let Some((error, retryable, offline)) = stream_error {
            if retryable {
                drop(_slot);
                match wait_before_retry(
                    &*app,
                    &manager,
                    RetryContext {
                        id: &id,
                        filename: &filename,
                        downloaded_bytes,
                        total_bytes,
                        message: &error,
                        offline,
                    },
                    &mut retry_attempts,
                    &mut control,
                )
                .await
                {
                    RetryResult::Retry | RetryResult::Interrupted => continue,
                    RetryResult::Exhausted => {
                        finish_failed!(
                            &app,
                            &manager,
                            &id,
                            &filename,
                            &path,
                            downloaded_bytes,
                            total_bytes,
                            format!(
                                "Retry limit reached after {MAX_NETWORK_RETRIES} attempts: {error}"
                            ),
                        )
                        .await;
                        return;
                    }
                }
            }
            finish_failed!(
                &app,
                &manager,
                &id,
                &filename,
                &path,
                downloaded_bytes,
                total_bytes,
                error,
            )
            .await;
            return;
        }

        let actual_bytes = match tokio::fs::metadata(&path).await {
            Ok(metadata) => metadata.len(),
            Err(error) => {
                finish_failed!(
                    &app,
                    &manager,
                    &id,
                    &filename,
                    &path,
                    downloaded_bytes,
                    total_bytes,
                    format!("Could not verify the completed download: {error}"),
                )
                .await;
                return;
            }
        };
        if let Some(end) = range_end {
            let expected_segment = end.saturating_sub(offset) + 1;
            if actual_bytes.saturating_sub(if truncate { 0 } else { offset }) != expected_segment {
                finish_failed!(
                    &app,
                    &manager,
                    &id,
                    &filename,
                    &path,
                    actual_bytes,
                    total_bytes,
                    "The server returned a partial response with an unexpected byte count."
                        .to_string(),
                )
                .await;
                return;
            }
            if total_bytes.is_some_and(|total| end + 1 < total) {
                continue;
            }
        }
        if total_bytes.is_some_and(|total| total != actual_bytes) {
            finish_failed!(
                &app,
                &manager,
                &id,
                &filename,
                &path,
                actual_bytes,
                total_bytes,
                "The downloaded byte count did not match the server response.".to_string(),
            )
            .await;
            return;
        }
        match finalize_file(&destination, &path).await {
            Ok(()) => {
                publish_update!(
                    &app,
                    &manager,
                    &id,
                    &filename,
                    "completed",
                    actual_bytes,
                    total_bytes.or(Some(actual_bytes)),
                    None,
                );
            }
            Err(error) => {
                finish_failed!(
                    &app,
                    &manager,
                    &id,
                    &filename,
                    &path,
                    actual_bytes,
                    total_bytes,
                    error,
                )
                .await
            }
        }
        return;
    }
}

fn control_download(
    app: &AppHandle,
    manager: &DownloadManager,
    id: &str,
    action: Control,
    status: &str,
) -> Result<(), String> {
    let update = {
        let mut entries = manager
            .entries
            .lock()
            .map_err(|_| "Download state is unavailable.".to_string())?;
        let entry = entries
            .get_mut(id)
            .ok_or_else(|| format!("Download {id} was not found."))?;
        if let Err(error) = validate_transition(&entry.status, status) {
            if status == "paused" && entry.status == "paused" {
                return Ok(());
            }
            return Err(error);
        }
        if status == "paused" && entry.status == "paused" {
            return Ok(());
        }
        entry.control.send_replace(action);
        entry.auto_offline = false;
        entry.status = status.to_string();
        entry.error = None;
        DownloadUpdate {
            id: id.to_string(),
            filename: entry.filename.clone(),
            downloaded_bytes: entry.downloaded_bytes,
            total_bytes: entry.total_bytes,
            status: status.to_string(),
            error: None,
        }
    };
    emit_update(app, update);
    if let Err(error) = manager.persist() {
        eprintln!("{error}");
    }
    Ok(())
}

fn validate_transition(current: &str, target: &str) -> Result<(), String> {
    if matches!(
        current,
        "completed" | "failed" | "cancelled" | "link-expired"
    ) {
        return Err(format!(
            "Cannot {target} a download that is already {current}."
        ));
    }
    if matches!(target, "downloading" | "queued") && current != "paused" {
        return Err("Only a paused download can be resumed.".to_string());
    }
    Ok(())
}

#[tauri::command]
fn suggest_download_destination(
    app: AppHandle,
    manager: State<'_, DownloadManager>,
    url: String,
) -> Result<String, String> {
    let parsed_url = reqwest::Url::parse(&url).map_err(|error| error.to_string())?;
    if !matches!(parsed_url.scheme(), "http" | "https") {
        return Err("Only HTTP and HTTPS download links are supported.".to_string());
    }
    let download_dir = app
        .path()
        .download_dir()
        .map_err(|error| format!("Could not find the Downloads folder: {error}"))?;
    std::fs::create_dir_all(&download_dir)
        .map_err(|error| format!("Could not create the Downloads folder: {error}"))?;
    let entries = manager
        .entries
        .lock()
        .map_err(|_| "Download state is unavailable.".to_string())?;
    suggested_destination(&download_dir, &filename_from_url(&parsed_url), &entries)
        .map(|path| path.to_string_lossy().into_owned())
}

#[tauri::command]
async fn start_download(
    app: AppHandle,
    manager: State<'_, DownloadManager>,
    url: String,
    destination_path: String,
) -> Result<StartedDownload, String> {
    let parsed_url = reqwest::Url::parse(&url).map_err(|error| error.to_string())?;
    if !matches!(parsed_url.scheme(), "http" | "https") {
        return Err("Only HTTP and HTTPS download links are supported.".to_string());
    }

    let destination = resolve_destination(Path::new(&destination_path))?;
    let filename = destination
        .file_name()
        .and_then(|part| part.to_str())
        .ok_or_else(|| "The selected download filename is invalid.".to_string())?
        .to_string();
    let id = Uuid::new_v4().to_string();
    let path = partial_path_for(&destination, &id);

    let (sender, receiver) = watch::channel(Control::Running);
    {
        let mut entries = manager
            .entries
            .lock()
            .map_err(|_| "Download state is unavailable.".to_string())?;
        ensure_destination_available(&destination, &entries)?;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| format!("Could not create the partial download: {error}"))?;
        entries.insert(
            id.clone(),
            DownloadEntry {
                url: parsed_url.to_string(),
                filename: filename.clone(),
                destination: destination.clone(),
                status: "queued".to_string(),
                downloaded_bytes: 0,
                total_bytes: None,
                validator: None,
                error: None,
                retry_count: 0,
                auto_offline: false,
                control: sender,
                task_lock: Arc::new(tokio::sync::Mutex::new(())),
            },
        );
    }
    publish_update!(&app, &manager, &id, &filename, "queued", 0, None, None);

    let app_handle = app.clone();
    let task_manager = manager.inner().clone();
    let download = DownloadContext {
        id: id.clone(),
        url: parsed_url.to_string(),
        filename: filename.clone(),
        destination: destination.clone(),
        partial_path: path,
    };
    tauri::async_runtime::spawn(async move {
        download_file(app_handle, task_manager, download, receiver).await;
    });

    Ok(StartedDownload { id, filename })
}

#[tauri::command]
fn list_downloads(manager: State<'_, DownloadManager>) -> Result<Vec<DownloadSnapshot>, String> {
    let entries = manager
        .entries
        .lock()
        .map_err(|_| "Download state is unavailable.".to_string())?;
    let mut downloads: Vec<_> = entries
        .iter()
        .map(|(id, entry)| DownloadSnapshot {
            id: id.clone(),
            filename: entry.filename.clone(),
            url: entry.url.clone(),
            downloaded_bytes: entry.downloaded_bytes,
            total_bytes: entry.total_bytes,
            status: entry.status.clone(),
            error: entry.error.clone(),
        })
        .collect();
    downloads.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(downloads)
}

#[tauri::command]
fn pause_download(
    app: AppHandle,
    manager: State<'_, DownloadManager>,
    id: String,
) -> Result<(), String> {
    control_download(&app, &manager, &id, Control::Paused, "paused")
}

#[tauri::command]
fn resume_download(
    app: AppHandle,
    manager: State<'_, DownloadManager>,
    id: String,
) -> Result<(), String> {
    control_download(&app, &manager, &id, Control::Running, "queued")
}

#[tauri::command]
fn cancel_download(
    app: AppHandle,
    manager: State<'_, DownloadManager>,
    id: String,
) -> Result<(), String> {
    control_download(&app, &manager, &id, Control::Cancelled, "cancelled")
}

struct RetryDownload {
    id: String,
    url: String,
    filename: String,
    destination: PathBuf,
    partial_path: PathBuf,
    downloaded_bytes: u64,
    total_bytes: Option<u64>,
    control: watch::Receiver<Control>,
}

struct ReplacementUrlProbe {
    final_url: String,
    total_size: Option<u64>,
    validator: Option<String>,
    supports_ranges: bool,
}

async fn probe_replacement_url(url: &reqwest::Url) -> Result<ReplacementUrlProbe, String> {
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|error| format!("Could not prepare the replacement URL check: {error}"))?
        .get(url.clone())
        .header(reqwest::header::RANGE, "bytes=0-0")
        .header(reqwest::header::ACCEPT_ENCODING, "identity")
        .send()
        .await
        .map_err(|error| format!("Could not reach the replacement URL: {error}"))?;
    let status = response.status();
    if matches!(
        status,
        reqwest::StatusCode::FORBIDDEN | reqwest::StatusCode::GONE
    ) {
        return Err(format!(
            "The replacement URL is also expired (HTTP {status})."
        ));
    }
    if status == reqwest::StatusCode::PARTIAL_CONTENT {
        let content_range = response
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(parse_content_range);
        let final_url = response.url().to_string();
        let validator = response_validator(&response);
        let body = response
            .bytes()
            .await
            .map_err(|error| format!("Could not check the replacement URL response: {error}"))?;
        let Some((0, 0, total_size)) = content_range else {
            return Err("The replacement URL returned an invalid byte-range probe.".to_string());
        };
        if body.len() != 1 {
            return Err("The replacement URL returned an invalid byte-range body.".to_string());
        }
        return Ok(ReplacementUrlProbe {
            final_url,
            total_size: Some(total_size),
            validator,
            supports_ranges: true,
        });
    }
    if status == reqwest::StatusCode::OK {
        let probe = ReplacementUrlProbe {
            final_url: response.url().to_string(),
            total_size: response.content_length(),
            validator: response_validator(&response),
            supports_ranges: false,
        };
        return Ok(probe);
    }
    Err(format!(
        "The replacement URL returned an unsupported HTTP status: {status}."
    ))
}

async fn prepare_download_url_update(
    manager: &DownloadManager,
    id: &str,
    new_url: &str,
) -> Result<DownloadContext, String> {
    let parsed_url = reqwest::Url::parse(new_url).map_err(|error| error.to_string())?;
    if !matches!(parsed_url.scheme(), "http" | "https") {
        return Err("Only HTTP and HTTPS download links are supported.".to_string());
    }
    let (filename, destination, old_total, old_validator) = {
        let entries = manager
            .entries
            .lock()
            .map_err(|_| "Download state is unavailable.".to_string())?;
        let entry = entries
            .get(id)
            .ok_or_else(|| format!("Download {id} was not found."))?;
        if entry.status != "link-expired" {
            return Err("Only downloads with an expired link can be updated.".to_string());
        }
        (
            entry.filename.clone(),
            entry.destination.clone(),
            entry.total_bytes,
            entry.validator.clone(),
        )
    };
    let probe = probe_replacement_url(&parsed_url).await?;
    let path = partial_path_for(&destination, id);
    let sidecar_path = segment_sidecar_path(&destination);
    let old_state = read_segment_resume_state(&sidecar_path)?;
    let expected_total = old_state
        .as_ref()
        .map(|state| state.total_size)
        .or(old_total);
    let expected_validator = old_state
        .as_ref()
        .and_then(|state| state.validator.clone())
        .or(old_validator);
    let partial_size = tokio::fs::metadata(&path)
        .await
        .ok()
        .filter(|metadata| metadata.is_file())
        .map(|metadata| metadata.len());
    let resumable = probe.supports_ranges
        && probe.total_size == expected_total
        && expected_validator.is_some()
        && probe.validator == expected_validator
        && match (old_state.as_ref(), partial_size, probe.total_size) {
            (Some(_), Some(size), Some(total)) => size == total,
            (None, Some(size), Some(total)) => size <= total,
            _ => false,
        };
    let downloaded_bytes = if resumable {
        if let Some(mut state) = old_state {
            state.url = parsed_url.to_string();
            state.final_url = probe.final_url.clone();
            state.validator = probe.validator.clone();
            persist_segment_resume_state(&sidecar_path, &state).await?;
            segment_progress(&state)
        } else {
            partial_size.unwrap_or(0)
        }
    } else {
        create_preallocated_partial(&path, 0).await?;
        remove_segment_sidecar(&sidecar_path).await?;
        0
    };

    {
        let mut entries = manager
            .entries
            .lock()
            .map_err(|_| "Download state is unavailable.".to_string())?;
        let entry = entries
            .get_mut(id)
            .ok_or_else(|| format!("Download {id} was not found."))?;
        entry.url = parsed_url.to_string();
        entry.status = "queued".to_string();
        entry.downloaded_bytes = downloaded_bytes;
        entry.total_bytes = probe.total_size;
        entry.validator = probe.validator;
        entry.error = None;
        entry.auto_offline = false;
        entry.control.send_replace(Control::Running);
    }
    manager.persist()?;
    Ok(DownloadContext {
        id: id.to_string(),
        url: parsed_url.to_string(),
        filename,
        destination: destination.clone(),
        partial_path: path,
    })
}

fn prepare_download_retry(manager: &DownloadManager, id: &str) -> Result<RetryDownload, String> {
    let mut entries = manager
        .entries
        .lock()
        .map_err(|_| "Download state is unavailable.".to_string())?;
    let entry = entries
        .get(id)
        .ok_or_else(|| format!("Download {id} was not found."))?;
    if !matches!(entry.status.as_str(), "failed" | "cancelled") {
        return Err("Only failed or cancelled downloads can be retried.".to_string());
    }

    let destination = entry.destination.clone();
    let url = entry.url.clone();
    ensure_destination_available(&destination, &entries)?;
    let partial_path = partial_path_for(&destination, id);
    let partial_size = match std::fs::metadata(&partial_path) {
        Ok(metadata) if metadata.is_file() => metadata.len(),
        Ok(_) => return Err("The partial download path is not a file.".to_string()),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&partial_path)
                .map_err(|error| format!("Could not recreate the partial download: {error}"))?;
            0
        }
        Err(error) => {
            return Err(format!("Could not inspect the partial download: {error}"));
        }
    };
    let sidecar = read_segment_resume_state(&segment_sidecar_path(&destination))?;
    let downloaded_bytes = match sidecar {
        Some(state)
            if state.url == url
                && partial_size == state.total_size
                && valid_segment_resume_state(&state) =>
        {
            segment_progress(&state)
        }
        _ => partial_size,
    };

    let entry = entries
        .get_mut(id)
        .ok_or_else(|| format!("Download {id} was not found."))?;
    entry.control.send_replace(Control::Running);
    let control = entry.control.subscribe();
    entry.status = "queued".to_string();
    entry.downloaded_bytes = downloaded_bytes;
    entry.error = None;

    Ok(RetryDownload {
        id: id.to_string(),
        url: entry.url.clone(),
        filename: entry.filename.clone(),
        destination,
        partial_path,
        downloaded_bytes,
        total_bytes: entry.total_bytes,
        control,
    })
}

#[tauri::command]
async fn retry_download(
    app: AppHandle,
    manager: State<'_, DownloadManager>,
    id: String,
) -> Result<(), String> {
    let manager = manager.inner().clone();
    let task_lock = {
        let entries = manager
            .entries
            .lock()
            .map_err(|_| "Download state is unavailable.".to_string())?;
        let entry = entries
            .get(&id)
            .ok_or_else(|| format!("Download {id} was not found."))?;
        if !matches!(entry.status.as_str(), "failed" | "cancelled") {
            return Err("Only failed or cancelled downloads can be retried.".to_string());
        }
        entry.task_lock.clone()
    };
    let _task_guard = task_lock.lock().await;
    let retry = prepare_download_retry(&manager, &id)?;
    publish_update!(
        &app,
        &manager,
        &retry.id,
        &retry.filename,
        "queued",
        retry.downloaded_bytes,
        retry.total_bytes,
        None,
    );

    let task_manager = manager.clone();
    let download = DownloadContext {
        id: retry.id,
        url: retry.url,
        filename: retry.filename,
        destination: retry.destination,
        partial_path: retry.partial_path,
    };
    tauri::async_runtime::spawn(async move {
        download_file(app, task_manager, download, retry.control).await;
    });
    Ok(())
}

#[tauri::command]
async fn update_download_url(
    app: AppHandle,
    manager: State<'_, DownloadManager>,
    id: String,
    new_url: String,
) -> Result<(), String> {
    let manager = manager.inner().clone();
    let task_lock = {
        let entries = manager
            .entries
            .lock()
            .map_err(|_| "Download state is unavailable.".to_string())?;
        let entry = entries
            .get(&id)
            .ok_or_else(|| format!("Download {id} was not found."))?;
        if entry.status != "link-expired" {
            return Err("Only downloads with an expired link can be updated.".to_string());
        }
        entry.task_lock.clone()
    };
    let _task_guard = task_lock.lock().await;
    let download = prepare_download_url_update(&manager, &id, &new_url).await?;
    let (filename, downloaded_bytes, total_bytes, receiver) = {
        let entries = manager
            .entries
            .lock()
            .map_err(|_| "Download state is unavailable.".to_string())?;
        let entry = entries
            .get(&id)
            .ok_or_else(|| format!("Download {id} was not found."))?;
        (
            entry.filename.clone(),
            entry.downloaded_bytes,
            entry.total_bytes,
            entry.control.subscribe(),
        )
    };
    publish_update!(
        &app,
        &manager,
        &id,
        &filename,
        "queued",
        downloaded_bytes,
        total_bytes,
        None,
    );

    let task_manager = manager.clone();
    tauri::async_runtime::spawn(async move {
        download_file(app, task_manager, download, receiver).await;
    });
    Ok(())
}

#[tauri::command]
fn open_download_location(manager: State<'_, DownloadManager>, id: String) -> Result<(), String> {
    let directory = {
        let entries = manager
            .entries
            .lock()
            .map_err(|_| "Download state is unavailable.".to_string())?;
        let entry = entries
            .get(&id)
            .ok_or_else(|| format!("Download {id} was not found."))?;
        entry
            .destination
            .parent()
            .ok_or_else(|| "The download has no parent folder.".to_string())?
            .to_path_buf()
    };
    if !directory.is_dir() {
        return Err(format!(
            "The download folder does not exist: {}",
            directory.display()
        ));
    }

    #[cfg(target_os = "windows")]
    let result = Command::new("explorer.exe").arg(&directory).spawn();
    #[cfg(target_os = "macos")]
    let result = Command::new("open").arg(&directory).spawn();
    #[cfg(all(unix, not(target_os = "macos")))]
    let result = Command::new("xdg-open").arg(&directory).spawn();
    #[cfg(not(any(target_os = "windows", target_os = "macos", unix)))]
    return Err("Opening the download folder is not supported on this platform.".to_string());

    #[cfg(any(target_os = "windows", target_os = "macos", unix))]
    result
        .map(|_| ())
        .map_err(|error| format!("Could not open the download folder: {error}"))
}

#[tauri::command]
fn get_download_settings(manager: State<'_, DownloadManager>) -> Result<DownloadSettings, String> {
    Ok(DownloadSettings {
        max_concurrent_downloads: manager.admission.limit()?,
    })
}

#[tauri::command]
fn set_max_concurrent_downloads(
    manager: State<'_, DownloadManager>,
    limit: usize,
) -> Result<DownloadSettings, String> {
    let limit = validate_max_concurrent_downloads(limit)?;
    let _settings_guard = manager
        .settings_lock
        .lock()
        .map_err(|_| "Download settings are unavailable.".to_string())?;
    if let Some(path) = manager.settings_file.as_deref() {
        persist_max_concurrent_downloads(path, limit)?;
    }
    manager.admission.set_limit(limit)?;
    Ok(DownloadSettings {
        max_concurrent_downloads: limit,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        Control, DownloadAdmission, DownloadContext, DownloadEntry, DownloadFailure,
        DownloadManager, DownloadUpdate, PersistedDownload, PersistedSettings, RetryContext,
        RetryResult, SEGMENT_RESUME_VERSION, SegmentPosition, SegmentResumeState, SlotResult,
        UpdateSink, acquire_download_slot, download_file_inner, enough_disk_space,
        ensure_destination_available, ensure_disk_space, filename_from_url, finalize_file,
        finish_failed, is_reserved_windows_name, numbered_filename, parse_content_range,
        partial_path_for, pause_active_downloads, persist_max_concurrent_downloads,
        persist_segment_resume_state, persist_snapshot, prepare_download_retry,
        prepare_download_url_update, read_max_concurrent_downloads, read_persisted_queue,
        read_segment_resume_state, reconcile_persisted_status, restore_download_manager,
        segment_progress, segment_ranges, segment_sidecar_path, valid_range_probe,
        valid_segment_response, valid_segment_resume_state, validate_max_concurrent_downloads,
        validate_transition, validators_compatible, wait_before_retry,
    };
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        thread::{self, JoinHandle},
        time::Duration,
    };

    static HTTP_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn http_test_guard() -> std::sync::MutexGuard<'static, ()> {
        HTTP_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum ServerBehavior {
        SupportsRanges,
        IgnoresRanges,
        MalformedSegmentRanges,
        ChangesValidator,
        ShortRangeBodies,
        DropsSegmentConnections,
        HtmlErrorPage,
        Forbidden,
        Gone,
        SlowRanges,
        SlowStreamingRanges,
        StallsSegmentBodies,
        StallsSequentialBody,
    }

    struct LocalHttpServer {
        url: String,
        range_requests: Arc<AtomicUsize>,
        identity_range_requests: Arc<AtomicUsize>,
        requested_ranges: Arc<Mutex<Vec<(u64, u64)>>>,
        slow: Arc<AtomicBool>,
        shutdown: Arc<AtomicBool>,
        server_thread: Option<JoinHandle<()>>,
    }

    impl LocalHttpServer {
        fn start(body: Vec<u8>, behavior: ServerBehavior) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let address = listener.local_addr().unwrap();
            let body = Arc::new(body);
            let range_requests = Arc::new(AtomicUsize::new(0));
            let identity_range_requests = Arc::new(AtomicUsize::new(0));
            let requested_ranges = Arc::new(Mutex::new(Vec::new()));
            let slow = Arc::new(AtomicBool::new(matches!(
                behavior,
                ServerBehavior::SlowRanges
                    | ServerBehavior::SlowStreamingRanges
                    | ServerBehavior::StallsSegmentBodies
                    | ServerBehavior::StallsSequentialBody
            )));
            let drop_once = Arc::new(AtomicBool::new(true));
            let shutdown = Arc::new(AtomicBool::new(false));

            let thread_body = body.clone();
            let thread_range_requests = range_requests.clone();
            let thread_identity_range_requests = identity_range_requests.clone();
            let thread_requested_ranges = requested_ranges.clone();
            let thread_slow = slow.clone();
            let thread_drop_once = drop_once.clone();
            let thread_shutdown = shutdown.clone();
            let server_thread = thread::spawn(move || {
                while !thread_shutdown.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let body = thread_body.clone();
                            let range_requests = thread_range_requests.clone();
                            let identity_range_requests = thread_identity_range_requests.clone();
                            let requested_ranges = thread_requested_ranges.clone();
                            let slow = thread_slow.clone();
                            let drop_once = thread_drop_once.clone();
                            thread::spawn(move || {
                                serve_download_request(
                                    stream,
                                    body,
                                    behavior,
                                    range_requests,
                                    identity_range_requests,
                                    requested_ranges,
                                    slow,
                                    drop_once,
                                );
                            });
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(2));
                        }
                        Err(_) => break,
                    }
                }
            });

            Self {
                url: format!("http://{address}/payload.bin"),
                range_requests,
                identity_range_requests,
                requested_ranges,
                slow,
                shutdown,
                server_thread: Some(server_thread),
            }
        }
    }

    impl Drop for LocalHttpServer {
        fn drop(&mut self) {
            self.slow.store(false, Ordering::Release);
            self.shutdown.store(true, Ordering::Release);
            if let Some(server_thread) = self.server_thread.take() {
                let _ = server_thread.join();
            }
        }
    }

    fn serve_download_request(
        mut stream: TcpStream,
        body: Arc<Vec<u8>>,
        behavior: ServerBehavior,
        range_requests: Arc<AtomicUsize>,
        identity_range_requests: Arc<AtomicUsize>,
        requested_ranges: Arc<Mutex<Vec<(u64, u64)>>>,
        slow: Arc<AtomicBool>,
        drop_once: Arc<AtomicBool>,
    ) {
        let _ = stream.set_nonblocking(false);
        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
        let mut request = Vec::new();
        let mut chunk = [0; 1024];
        loop {
            let Ok(length) = stream.read(&mut chunk) else {
                return;
            };
            if length == 0 {
                return;
            }
            request.extend_from_slice(&chunk[..length]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }

        let request = String::from_utf8_lossy(&request);
        let accepts_identity = request.lines().any(|line| {
            line.split_once(':').is_some_and(|(name, value)| {
                name.eq_ignore_ascii_case("accept-encoding")
                    && value.trim().eq_ignore_ascii_case("identity")
            })
        });
        let range = request.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if !name.eq_ignore_ascii_case("range") {
                return None;
            }
            let bounds = value.trim().strip_prefix("bytes=")?;
            let (start, end) = bounds.split_once('-')?;
            let start = start.parse::<u64>().ok()?;
            let end = if end.is_empty() {
                body.len().checked_sub(1)? as u64
            } else {
                end.parse::<u64>().ok()?
            };
            Some((start, end))
        });
        if range.is_some() {
            range_requests.fetch_add(1, Ordering::Relaxed);
            if accepts_identity {
                identity_range_requests.fetch_add(1, Ordering::Relaxed);
            }
            requested_ranges.lock().unwrap().push(range.unwrap());
        }
        let is_probe = range == Some((0, 0));
        let ignores_range = matches!(
            behavior,
            ServerBehavior::IgnoresRanges | ServerBehavior::StallsSequentialBody
        );
        let (status, mut response_body, content_range) = match behavior {
            ServerBehavior::HtmlErrorPage => (
                200,
                b"<!doctype html><title>Temporary error</title>".to_vec(),
                None,
            ),
            ServerBehavior::Forbidden => (403, b"Forbidden".to_vec(), None),
            ServerBehavior::Gone => (410, b"Link expired".to_vec(), None),
            _ => match range {
                Some((start, end)) if !ignores_range && end < body.len() as u64 && start <= end => {
                    let response_range =
                        if behavior == ServerBehavior::MalformedSegmentRanges && !is_probe {
                            format!("bytes {}-{}/{}", start + 1, end + 1, body.len())
                        } else {
                            format!("bytes {start}-{end}/{}", body.len())
                        };
                    let content = &body[start as usize..=end as usize];
                    (206, content.to_vec(), Some(response_range))
                }
                _ => (200, body.as_ref().clone(), None),
            },
        };
        let short_range_body =
            behavior == ServerBehavior::ShortRangeBodies && range.is_some() && !is_probe;
        if short_range_body {
            response_body.truncate((response_body.len() / 2).max(1));
        }
        let drops_segment_connection = behavior == ServerBehavior::DropsSegmentConnections
            && range.is_some()
            && !is_probe
            && drop_once.swap(false, Ordering::AcqRel);
        let etag = if behavior == ServerBehavior::ChangesValidator && range.is_some() && !is_probe {
            "\"changed\""
        } else {
            "\"stable\""
        };
        let reason = match status {
            206 => "Partial Content",
            403 => "Forbidden",
            410 => "Gone",
            _ => "OK",
        };
        let mut headers = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\nAccept-Ranges: bytes\r\nETag: {etag}\r\n",
            if drops_segment_connection {
                content_range
                    .as_ref()
                    .and_then(|_| range.map(|(start, end)| end - start + 1))
                    .unwrap_or(response_body.len() as u64) as usize
            } else {
                response_body.len()
            }
        );
        if behavior == ServerBehavior::HtmlErrorPage {
            headers.push_str("Content-Type: text/html\r\n");
        }
        if let Some(content_range) = content_range {
            headers.push_str(&format!("Content-Range: {content_range}\r\n"));
        }
        headers.push_str("\r\n");
        if stream.write_all(headers.as_bytes()).is_err() {
            return;
        }
        if behavior == ServerBehavior::SlowRanges && range.is_some() {
            while slow.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(2));
            }
        }
        if behavior == ServerBehavior::StallsSegmentBodies && range.is_some() && !is_probe {
            while slow.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(2));
            }
        }
        if behavior == ServerBehavior::StallsSequentialBody && range.is_none() {
            while slow.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(2));
            }
        }
        if behavior == ServerBehavior::SlowStreamingRanges && range.is_some() && !is_probe {
            for body_chunk in response_body.chunks(64 * 1024) {
                if stream.write_all(body_chunk).is_err() {
                    return;
                }
                if slow.load(Ordering::Acquire) {
                    thread::sleep(Duration::from_millis(100));
                }
            }
            return;
        }
        if drops_segment_connection {
            let _ = stream.write_all(&response_body[..response_body.len() / 2]);
            return;
        }
        let _ = stream.write_all(&response_body);
    }

    struct TestDownload {
        id: String,
        manager: DownloadManager,
        control_sender: tokio::sync::watch::Sender<Control>,
        control_receiver: tokio::sync::watch::Receiver<Control>,
        directory: std::path::PathBuf,
        partial_path: std::path::PathBuf,
        final_path: std::path::PathBuf,
    }

    struct TestUpdateSink;

    impl UpdateSink for TestUpdateSink {
        fn emit_download_update(&self, _update: DownloadUpdate) {}
    }

    struct RecordingUpdateSink {
        updates: Arc<Mutex<Vec<DownloadUpdate>>>,
    }

    impl UpdateSink for RecordingUpdateSink {
        fn emit_download_update(&self, update: DownloadUpdate) {
            self.updates.lock().unwrap().push(update);
        }
    }

    impl TestDownload {
        fn new() -> Self {
            let id = uuid::Uuid::new_v4().to_string();
            let directory = std::env::temp_dir().join(format!("rustdload-http-test-{id}"));
            std::fs::create_dir_all(&directory).unwrap();
            let partial_path = directory.join(format!(".{id}.part"));
            let final_path = directory.join("payload.bin");
            std::fs::write(&partial_path, b"").unwrap();
            let (control_sender, control_receiver) = tokio::sync::watch::channel(Control::Running);
            let mut entries = std::collections::HashMap::new();
            entries.insert(
                id.clone(),
                DownloadEntry {
                    url: String::new(),
                    filename: "payload.bin".to_string(),
                    destination: final_path.clone(),
                    status: "queued".to_string(),
                    downloaded_bytes: 0,
                    total_bytes: None,
                    validator: None,
                    error: None,
                    retry_count: 0,
                    auto_offline: false,
                    control: control_sender.clone(),
                    task_lock: Arc::new(tokio::sync::Mutex::new(())),
                },
            );
            let manager = DownloadManager {
                entries: Arc::new(Mutex::new(entries)),
                persistence_file: None,
                persistence_lock: Arc::default(),
                settings_file: None,
                settings_lock: Arc::default(),
                admission: Arc::new(DownloadAdmission::new(3)),
            };
            Self {
                id,
                manager,
                control_sender,
                control_receiver,
                directory,
                partial_path,
                final_path,
            }
        }

        fn status(&self) -> String {
            self.manager.entries.lock().unwrap()[&self.id]
                .status
                .clone()
        }
    }

    impl Drop for TestDownload {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.directory);
        }
    }

    fn start_test_download(
        server: &LocalHttpServer,
        test_download: &TestDownload,
    ) -> tokio::task::JoinHandle<()> {
        let manager = test_download.manager.clone();
        let id = test_download.id.clone();
        let destination = test_download.final_path.clone();
        let partial_path = test_download.partial_path.clone();
        let control_receiver = test_download.control_receiver.clone();
        let url = server.url.clone();
        let app: Arc<dyn UpdateSink> = Arc::new(TestUpdateSink);
        tokio::spawn(async move {
            download_file_inner(
                app,
                manager,
                DownloadContext {
                    id,
                    url,
                    filename: "payload.bin".to_string(),
                    destination,
                    partial_path,
                },
                control_receiver,
            )
            .await;
        })
    }

    async fn wait_for_range_requests(server: &LocalHttpServer, minimum: usize) {
        for _ in 0..100_000 {
            if server.range_requests.load(Ordering::Acquire) >= minimum {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("server did not receive {minimum} range requests");
    }

    async fn wait_for_retry_count(test_download: &TestDownload, minimum: u32) {
        for _ in 0..10_000 {
            if test_download.manager.entries.lock().unwrap()[&test_download.id].retry_count
                >= minimum
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        panic!("download did not reach retry count {minimum}");
    }

    async fn wait_for_status(test_download: &TestDownload, status: &str) {
        for _ in 0..100_000 {
            if test_download.status() == status {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("download did not reach status {status}");
    }

    async fn wait_for_segment_checkpoint(test_download: &TestDownload) -> SegmentResumeState {
        let sidecar = segment_sidecar_path(&test_download.final_path);
        for _ in 0..10_000 {
            if let Ok(Some(state)) = read_segment_resume_state(&sidecar)
                && segment_progress(&state) > 0
            {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        panic!("no segmented progress was checkpointed");
    }

    async fn wait_for_paused_checkpoint(test_download: &TestDownload) -> SegmentResumeState {
        let sidecar = segment_sidecar_path(&test_download.final_path);
        for _ in 0..10_000 {
            if let Ok(Some(state)) = read_segment_resume_state(&sidecar) {
                let entries = test_download.manager.entries.lock().unwrap();
                let entry = &entries[&test_download.id];
                if entry.status == "paused" && entry.downloaded_bytes == segment_progress(&state) {
                    return state;
                }
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        panic!("paused download progress was not persisted consistently");
    }

    fn test_payload(length: usize) -> Vec<u8> {
        (0..length)
            .map(|index| (index.wrapping_mul(31) % 251) as u8)
            .collect()
    }

    #[test]
    fn encoded_path_characters_cannot_escape_the_download_filename() {
        let url = reqwest::Url::parse("https://example.com/a%2Fb%3Fc.txt").unwrap();
        assert_eq!(filename_from_url(&url), "a_b_c.txt");
    }

    #[test]
    fn windows_device_names_are_prefixed() {
        let url = reqwest::Url::parse("https://example.com/CON.txt").unwrap();
        assert_eq!(filename_from_url(&url), "_CON.txt");
        assert!(is_reserved_windows_name("LPT1"));
    }

    #[test]
    fn duplicate_names_keep_the_extension() {
        assert_eq!(numbered_filename("archive.tar.gz", 2), "archive.tar (2).gz");
    }

    #[test]
    fn content_range_requires_valid_bounds_and_total() {
        assert_eq!(parse_content_range("bytes 12-19/42"), Some((12, 19, 42)));
        assert_eq!(parse_content_range("bytes 12-42/42"), None);
        assert_eq!(parse_content_range("bytes */42"), None);
        assert_eq!(parse_content_range("items 12-19/42"), None);
    }

    #[test]
    fn segmented_download_requires_a_valid_single_byte_range_probe() {
        let partial = reqwest::StatusCode::PARTIAL_CONTENT;
        let ok_range = Some((0, 0, super::MIN_SEGMENTED_DOWNLOAD_SIZE));
        assert_eq!(
            valid_range_probe(partial, ok_range, 1),
            Some(super::MIN_SEGMENTED_DOWNLOAD_SIZE)
        );
        assert_eq!(
            valid_range_probe(reqwest::StatusCode::OK, ok_range, 1),
            None
        );
        assert_eq!(valid_range_probe(partial, Some((1, 1, 100)), 1), None);
        assert_eq!(valid_range_probe(partial, Some((0, 0, 100)), 1), None);
        assert_eq!(valid_range_probe(partial, ok_range, 2), None);
    }

    #[test]
    fn segmented_ranges_cover_and_reassemble_the_original_bytes() {
        let original: Vec<u8> = (0..=100).collect();
        let ranges = segment_ranges(original.len() as u64, 4);
        let mut assembled = Vec::new();
        let mut next_start = 0;
        for (start, end) in &ranges {
            assert_eq!(*start, next_start);
            assembled.extend_from_slice(&original[*start as usize..=*end as usize]);
            next_start = end + 1;
        }
        assert_eq!(next_start, original.len() as u64);
        assert_eq!(assembled, original);
    }

    #[test]
    fn segmented_resume_progress_includes_every_saved_range() {
        let state = SegmentResumeState {
            version: SEGMENT_RESUME_VERSION,
            url: "http://127.0.0.1/file".to_string(),
            final_url: "http://127.0.0.1/file".to_string(),
            total_size: 40,
            validator: None,
            segments: vec![
                SegmentPosition {
                    start: 0,
                    end: 9,
                    position: 10,
                },
                SegmentPosition {
                    start: 10,
                    end: 19,
                    position: 15,
                },
                SegmentPosition {
                    start: 20,
                    end: 29,
                    position: 30,
                },
                SegmentPosition {
                    start: 30,
                    end: 39,
                    position: 35,
                },
            ],
        };
        assert!(valid_segment_resume_state(&state));
        assert_eq!(segment_progress(&state), 30);
    }

    #[tokio::test]
    async fn segmented_resume_sidecar_is_atomically_replaced() {
        let directory =
            std::env::temp_dir().join(format!("rustdload-sidecar-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let destination = directory.join("payload.bin");
        let sidecar = segment_sidecar_path(&destination);
        let mut state = SegmentResumeState {
            version: SEGMENT_RESUME_VERSION,
            url: "http://127.0.0.1/payload.bin".to_string(),
            final_url: "http://127.0.0.1/payload.bin".to_string(),
            total_size: 20,
            validator: Some("\"stable\"".to_string()),
            segments: vec![SegmentPosition {
                start: 0,
                end: 19,
                position: 0,
            }],
        };
        persist_segment_resume_state(&sidecar, &state)
            .await
            .unwrap();
        state.segments[0].position = 12;
        persist_segment_resume_state(&sidecar, &state)
            .await
            .unwrap();

        let restored = read_segment_resume_state(&sidecar).unwrap().unwrap();
        assert_eq!(segment_progress(&restored), 12);
        assert_eq!(restored.validator.as_deref(), Some("\"stable\""));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn segmented_response_validation_rejects_mismatched_ranges() {
        let partial = reqwest::StatusCode::PARTIAL_CONTENT;
        assert!(valid_segment_response(
            partial,
            Some((10, 19, 100)),
            10,
            19,
            100
        ));
        assert!(!valid_segment_response(
            partial,
            Some((0, 9, 100)),
            10,
            19,
            100
        ));
        assert!(!valid_segment_response(
            reqwest::StatusCode::OK,
            Some((10, 19, 100)),
            10,
            19,
            100
        ));
    }

    #[test]
    fn segmented_responses_reject_changed_validators() {
        assert!(validators_compatible(Some("\"one\""), Some("\"one\"")));
        assert!(!validators_compatible(Some("\"one\""), Some("\"two\"")));
        assert!(!validators_compatible(Some("\"one\""), None));
        assert!(validators_compatible(None, None));
    }

    #[test]
    fn download_state_transitions_reject_resume_and_control_after_terminal_states() {
        assert!(validate_transition("queued", "paused").is_ok());
        assert!(validate_transition("paused", "downloading").is_ok());
        assert!(validate_transition("paused", "queued").is_ok());
        assert!(validate_transition("downloading", "downloading").is_err());
        assert!(validate_transition("queued", "queued").is_err());
        assert!(validate_transition("completed", "paused").is_err());
        assert!(validate_transition("cancelled", "downloading").is_err());
    }

    #[test]
    fn shutdown_pauses_active_downloads_before_exit() {
        let test_download = TestDownload::new();
        {
            let mut entries = test_download.manager.entries.lock().unwrap();
            let entry = entries.get_mut(&test_download.id).unwrap();
            entry.control.send_replace(Control::Running);
            entry.status = "downloading".to_string();
        }

        pause_active_downloads(&test_download.manager).unwrap();

        assert!(*test_download.control_receiver.borrow() == Control::Paused);
    }

    #[test]
    fn retry_reuses_destination_and_preserves_partial_progress() {
        let test_download = TestDownload::new();
        std::fs::write(&test_download.partial_path, b"saved").unwrap();
        {
            let mut entries = test_download.manager.entries.lock().unwrap();
            let entry = entries.get_mut(&test_download.id).unwrap();
            entry.url = "https://example.com/payload.bin".to_string();
            entry.status = "failed".to_string();
            entry.error = Some("temporary error".to_string());
            entry.control.send_replace(Control::Cancelled);
        }

        let retry = prepare_download_retry(&test_download.manager, &test_download.id).unwrap();

        assert_eq!(retry.id, test_download.id);
        assert_eq!(retry.url, "https://example.com/payload.bin");
        assert_eq!(retry.destination, test_download.final_path);
        assert_eq!(retry.downloaded_bytes, 5);
        assert!(*retry.control.borrow() == Control::Running);
        let entries = test_download.manager.entries.lock().unwrap();
        let entry = &entries[&test_download.id];
        assert_eq!(entry.status, "queued");
        assert_eq!(entry.error, None);
    }

    #[tokio::test]
    async fn retry_resumes_to_the_saved_destination() {
        let _guard = http_test_guard();
        let payload = test_payload(64 * 1024);
        let server = LocalHttpServer::start(payload.clone(), ServerBehavior::SupportsRanges);
        let test_download = TestDownload::new();
        let partial_bytes = 512;
        std::fs::write(&test_download.partial_path, &payload[..partial_bytes]).unwrap();
        {
            let mut entries = test_download.manager.entries.lock().unwrap();
            let entry = entries.get_mut(&test_download.id).unwrap();
            entry.url = server.url.clone();
            entry.status = "failed".to_string();
            entry.total_bytes = Some(payload.len() as u64);
            entry.validator = Some("\"stable\"".to_string());
            entry.control.send_replace(Control::Cancelled);
        }

        let retry = prepare_download_retry(&test_download.manager, &test_download.id).unwrap();
        let app: Arc<dyn UpdateSink> = Arc::new(TestUpdateSink);
        download_file_inner(
            app,
            test_download.manager.clone(),
            DownloadContext {
                id: retry.id,
                url: retry.url,
                filename: retry.filename,
                destination: retry.destination,
                partial_path: retry.partial_path,
            },
            retry.control,
        )
        .await;

        assert_eq!(test_download.status(), "completed");
        assert_eq!(std::fs::read(&test_download.final_path).unwrap(), payload);
    }

    #[test]
    fn retry_recreates_missing_partial_for_cancelled_download() {
        let test_download = TestDownload::new();
        std::fs::remove_file(&test_download.partial_path).unwrap();
        {
            let mut entries = test_download.manager.entries.lock().unwrap();
            let entry = entries.get_mut(&test_download.id).unwrap();
            entry.status = "cancelled".to_string();
            entry.control.send_replace(Control::Cancelled);
        }

        let retry = prepare_download_retry(&test_download.manager, &test_download.id).unwrap();

        assert_eq!(retry.destination, test_download.final_path);
        assert_eq!(retry.downloaded_bytes, 0);
        assert!(test_download.partial_path.exists());
        assert!(*retry.control.borrow() == Control::Running);
    }

    #[test]
    fn retry_rejects_downloads_that_are_not_failed_or_cancelled() {
        let test_download = TestDownload::new();

        assert!(
            prepare_download_retry(&test_download.manager, &test_download.id)
                .err()
                .unwrap()
                .contains("Only failed or cancelled")
        );
    }

    #[tokio::test]
    async fn offline_retry_pauses_with_reason_and_automatically_resumes() {
        let _guard = http_test_guard();
        let test_download = TestDownload::new();
        let updates = Arc::new(Mutex::new(Vec::new()));
        let app = RecordingUpdateSink {
            updates: updates.clone(),
        };
        let mut control = test_download.control_receiver.clone();
        let mut retry_attempts = 0;

        let result = wait_before_retry(
            &app,
            &test_download.manager,
            RetryContext {
                id: &test_download.id,
                filename: "payload.bin",
                downloaded_bytes: 0,
                total_bytes: Some(100),
                message: "connection failed",
                offline: true,
            },
            &mut retry_attempts,
            &mut control,
        )
        .await;

        assert!(matches!(result, RetryResult::Retry));
        assert_eq!(test_download.status(), "downloading");
        assert_eq!(
            test_download.manager.entries.lock().unwrap()[&test_download.id].retry_count,
            1
        );
        assert!(*control.borrow() == Control::Running);
        assert!(updates.lock().unwrap().iter().any(|update| {
            update.status == "paused"
                && update
                    .error
                    .as_deref()
                    .is_some_and(|error| error.contains("Offline"))
        }));
    }

    #[tokio::test]
    async fn network_retry_count_is_capped_for_non_offline_failures() {
        let _guard = http_test_guard();
        let test_download = TestDownload::new();
        let app = TestUpdateSink;
        let mut control = test_download.control_receiver.clone();
        let mut retry_attempts = 0;

        for _ in 0..super::MAX_NETWORK_RETRIES {
            let result = wait_before_retry(
                &app,
                &test_download.manager,
                RetryContext {
                    id: &test_download.id,
                    filename: "payload.bin",
                    downloaded_bytes: 0,
                    total_bytes: Some(100),
                    message: "connection closed",
                    offline: false,
                },
                &mut retry_attempts,
                &mut control,
            )
            .await;
            assert!(matches!(result, RetryResult::Retry));
        }
        let result = wait_before_retry(
            &app,
            &test_download.manager,
            RetryContext {
                id: &test_download.id,
                filename: "payload.bin",
                downloaded_bytes: 0,
                total_bytes: Some(100),
                message: "connection closed",
                offline: false,
            },
            &mut retry_attempts,
            &mut control,
        )
        .await;

        assert!(matches!(result, RetryResult::Exhausted));
        assert_eq!(
            test_download.manager.entries.lock().unwrap()[&test_download.id].retry_count,
            super::MAX_NETWORK_RETRIES
        );
    }

    fn persisted_download(status: &str, downloaded_bytes: u64) -> PersistedDownload {
        PersistedDownload {
            id: "c04e4566-0d5e-47c6-8863-9f2d6d327849".to_string(),
            url: "https://example.com/archive.zip".to_string(),
            filename: "archive.zip".to_string(),
            destination: None,
            status: status.to_string(),
            downloaded_bytes,
            total_bytes: Some(12),
            validator: None,
            retry_count: 0,
            error: None,
        }
    }

    #[test]
    fn interrupted_download_recovers_paused_at_actual_partial_size() {
        let mut record = persisted_download("downloading", 9);
        assert!(reconcile_persisted_status(&mut record, Some(7), None));
        assert_eq!(record.status, "paused");
        assert_eq!(record.downloaded_bytes, 7);
        assert_eq!(record.total_bytes, Some(12));
    }

    #[test]
    fn interrupted_download_with_missing_partial_restarts_at_zero() {
        let mut record = persisted_download("paused", 9);
        assert!(reconcile_persisted_status(&mut record, Some(0), None));
        assert_eq!(record.status, "paused");
        assert_eq!(record.downloaded_bytes, 0);
    }

    #[test]
    fn completed_download_is_restored_only_when_file_size_matches() {
        let mut complete = persisted_download("completed", 12);
        assert!(!reconcile_persisted_status(&mut complete, None, Some(12)));
        assert_eq!(complete.status, "completed");
        assert!(complete.error.is_none());

        let mut incomplete = persisted_download("completed", 12);
        assert!(!reconcile_persisted_status(&mut incomplete, None, Some(11)));
        assert_eq!(incomplete.status, "failed");
        assert!(incomplete.error.is_some());

        let mut missing = persisted_download("completed", 12);
        assert!(!reconcile_persisted_status(&mut missing, None, None));
        assert_eq!(missing.status, "failed");

        let mut expired = persisted_download("link-expired", 12);
        assert!(!reconcile_persisted_status(&mut expired, Some(5), None));
        assert_eq!(expired.status, "link-expired");
        assert_eq!(expired.downloaded_bytes, 5);
    }

    #[test]
    fn destination_collision_is_rejected_without_overwriting_existing_content() {
        let directory =
            std::env::temp_dir().join(format!("rustdload-destination-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let destination = directory.join("archive.zip");
        std::fs::write(&destination, b"existing").unwrap();

        assert!(
            ensure_destination_available(&destination, &std::collections::HashMap::new()).is_err()
        );
        assert_eq!(std::fs::read(&destination).unwrap(), b"existing");

        std::fs::remove_file(&destination).unwrap();
        let (control, _) = tokio::sync::watch::channel(Control::Running);
        let entries = [(
            "active".to_string(),
            DownloadEntry {
                url: String::new(),
                filename: "archive.zip".to_string(),
                destination: destination.clone(),
                status: "queued".to_string(),
                downloaded_bytes: 0,
                total_bytes: None,
                validator: None,
                error: None,
                retry_count: 0,
                auto_offline: false,
                control,
                task_lock: Arc::new(tokio::sync::Mutex::new(())),
            },
        )]
        .into_iter()
        .collect();
        assert!(ensure_destination_available(&destination, &entries).is_err());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn finalize_collision_preserves_existing_file_and_partial() {
        let directory =
            std::env::temp_dir().join(format!("rustdload-finalize-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let destination = directory.join("archive.zip");
        let partial = directory.join(".partial");
        std::fs::write(&destination, b"existing").unwrap();
        std::fs::write(&partial, b"new data").unwrap();

        let result = finalize_file(&destination, &partial).await;
        assert!(result.is_err());
        assert_eq!(std::fs::read(&destination).unwrap(), b"existing");
        assert_eq!(std::fs::read(&partial).unwrap(), b"new data");
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn persisted_queue_round_trips_and_uses_backup_when_primary_is_invalid() {
        let directory =
            std::env::temp_dir().join(format!("rustdload-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let queue_file = directory.join("downloads.json");
        let backup_file = directory.join("downloads.json.bak");
        let mut record = persisted_download("paused", 7);
        record.validator = Some("\"stable\"".to_string());
        record.retry_count = 2;

        persist_snapshot(&queue_file, std::slice::from_ref(&record)).unwrap();
        let restored = read_persisted_queue(&queue_file, &backup_file).unwrap();
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].id, record.id);
        assert_eq!(restored[0].validator, record.validator);
        assert_eq!(restored[0].retry_count, 2);

        std::fs::write(&queue_file, b"{").unwrap();
        std::fs::write(&backup_file, serde_json::to_vec(&[record]).unwrap()).unwrap();
        let recovered = read_persisted_queue(&queue_file, &backup_file).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].status, "paused");
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn legacy_queue_and_settings_are_migrated_to_versioned_formats() {
        let directory =
            std::env::temp_dir().join(format!("rustdload-migration-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let queue_file = directory.join("downloads.json");
        let settings_file = directory.join("settings.json");
        let backup_file = queue_file.with_extension("json.bak");
        let record = persisted_download("paused", 7);
        let mut old_record = serde_json::to_value(&record).unwrap();
        old_record.as_object_mut().unwrap().remove("retryCount");
        let old_record: PersistedDownload = serde_json::from_value(old_record).unwrap();
        assert_eq!(old_record.retry_count, 0);

        std::fs::write(&queue_file, serde_json::to_vec(&[&record]).unwrap()).unwrap();
        assert_eq!(
            read_persisted_queue(&queue_file, &backup_file).unwrap()[0].id,
            record.id
        );
        persist_snapshot(&queue_file, std::slice::from_ref(&record)).unwrap();
        let queue: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&queue_file).unwrap()).unwrap();
        assert_eq!(queue["version"], super::PERSISTED_FORMAT_VERSION);

        std::fs::write(&settings_file, br#"{"maxConcurrentDownloads":5}"#).unwrap();
        assert_eq!(read_max_concurrent_downloads(&settings_file).unwrap(), 5);
        persist_max_concurrent_downloads(&settings_file, 5).unwrap();
        let settings: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&settings_file).unwrap()).unwrap();
        assert_eq!(settings["version"], super::PERSISTED_FORMAT_VERSION);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn concurrent_download_limit_validates_bounds() {
        assert_eq!(validate_max_concurrent_downloads(1).unwrap(), 1);
        assert_eq!(validate_max_concurrent_downloads(10).unwrap(), 10);
        assert!(validate_max_concurrent_downloads(0).is_err());
        assert!(validate_max_concurrent_downloads(11).is_err());
    }

    #[test]
    fn concurrent_download_limit_persists_and_defaults_when_missing() {
        let directory =
            std::env::temp_dir().join(format!("rustdload-settings-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let settings_file = directory.join("settings.json");

        assert_eq!(
            read_max_concurrent_downloads(&settings_file).unwrap(),
            super::DEFAULT_MAX_CONCURRENT_DOWNLOADS
        );
        persist_max_concurrent_downloads(&settings_file, 7).unwrap();
        assert_eq!(read_max_concurrent_downloads(&settings_file).unwrap(), 7);
        std::fs::write(&settings_file, b"{").unwrap();
        let backup_file = settings_file.with_extension("json.bak");
        std::fs::write(
            &backup_file,
            serde_json::to_vec(&PersistedSettings {
                version: super::PERSISTED_FORMAT_VERSION,
                max_concurrent_downloads: 5,
            })
            .unwrap(),
        )
        .unwrap();
        assert_eq!(read_max_concurrent_downloads(&settings_file).unwrap(), 5);
        assert!(persist_max_concurrent_downloads(&settings_file, 0).is_err());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn restored_download_manager_uses_persisted_concurrency_limit() {
        let directory =
            std::env::temp_dir().join(format!("rustdload-settings-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let settings_file = directory.join("settings.json");
        let persistence_file = directory.join("downloads.json");
        let download_dir = directory.join("downloads");

        persist_max_concurrent_downloads(&settings_file, 7).unwrap();
        let (manager, _) = super::restore_download_manager(persistence_file, &download_dir)
            .expect("download manager should restore persisted settings");

        assert_eq!(manager.admission.limit().unwrap(), 7);
        drop(manager);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn chosen_destination_and_partial_resume_survive_restart() {
        let _http_guard = http_test_guard();
        let root = std::env::temp_dir().join(format!("rustdload-resume-{}", uuid::Uuid::new_v4()));
        let chosen_directory = root.join("chosen-folder");
        std::fs::create_dir_all(&chosen_directory).unwrap();
        let download_dir = root.join("default-downloads");
        let persistence_file = root.join("downloads.json");
        let payload = test_payload(300_000);
        let server = LocalHttpServer::start(payload.clone(), ServerBehavior::SupportsRanges);
        let id = uuid::Uuid::new_v4().to_string();
        let destination = chosen_directory.join("custom-name.bin");
        let partial = partial_path_for(&destination, &id);
        let prefix = &payload[..64];
        std::fs::write(&partial, prefix).unwrap();

        let mut record = persisted_download("downloading", prefix.len() as u64);
        record.id = id.clone();
        record.url = server.url.clone();
        record.filename = "custom-name.bin".to_string();
        record.destination = Some(destination.to_string_lossy().into_owned());
        persist_snapshot(&persistence_file, &[record]).unwrap();

        let (manager, recovered) =
            restore_download_manager(persistence_file, &download_dir).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(
            recovered[0].destination.as_deref(),
            Some(destination.to_str().unwrap())
        );
        let receiver = {
            let mut entries = manager.entries.lock().unwrap();
            let entry = entries.get_mut(&id).unwrap();
            assert_eq!(entry.destination, destination);
            assert_eq!(entry.status, "paused");
            assert_eq!(entry.downloaded_bytes, prefix.len() as u64);
            entry.control.send_replace(Control::Running);
            entry.control.subscribe()
        };
        let app: Arc<dyn UpdateSink> = Arc::new(TestUpdateSink);
        download_file_inner(
            app,
            manager,
            DownloadContext {
                id,
                url: server.url.clone(),
                filename: "custom-name.bin".to_string(),
                destination: destination.clone(),
                partial_path: partial,
            },
            receiver,
        )
        .await;

        assert_eq!(std::fs::read(&destination).unwrap(), payload);
        assert!(server.range_requests.load(Ordering::Acquire) > 0);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn paused_queued_download_waits_without_taking_a_slot() {
        let admission = std::sync::Arc::new(DownloadAdmission::new(1));
        let (sender, mut receiver) = tokio::sync::watch::channel(Control::Paused);
        let task_admission = admission.clone();
        let waiter =
            tokio::spawn(
                async move { acquire_download_slot(&task_admission, &mut receiver).await },
            );

        tokio::task::yield_now().await;
        assert_eq!(admission.state.lock().unwrap().active, 0);
        sender.send_replace(Control::Running);
        let result = waiter.await.unwrap().unwrap();
        assert!(matches!(result, SlotResult::Acquired(_)));
        assert_eq!(admission.state.lock().unwrap().active, 1);
    }

    #[tokio::test]
    async fn lowering_limit_waits_for_existing_transfer_before_admitting_queue() {
        let admission = std::sync::Arc::new(DownloadAdmission::new(2));
        let mut active_slots = Vec::new();
        for _ in 0..2 {
            let (_sender, mut receiver) = tokio::sync::watch::channel(Control::Running);
            active_slots.push(
                acquire_download_slot(&admission, &mut receiver)
                    .await
                    .unwrap(),
            );
        }
        admission.set_limit(1).unwrap();
        let (sender, mut receiver) = tokio::sync::watch::channel(Control::Running);
        let waiting_admission = admission.clone();
        let waiter =
            tokio::spawn(
                async move { acquire_download_slot(&waiting_admission, &mut receiver).await },
            );
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        drop(active_slots.pop());
        assert!(!waiter.is_finished());
        drop(active_slots);
        assert!(matches!(
            waiter.await.unwrap().unwrap(),
            SlotResult::Acquired(_)
        ));
        sender.send_replace(Control::Cancelled);
    }

    #[tokio::test]
    async fn raising_limit_admits_a_waiting_download() {
        let admission = std::sync::Arc::new(DownloadAdmission::new(1));
        let (_active_sender, mut active_receiver) = tokio::sync::watch::channel(Control::Running);
        let active_slot = acquire_download_slot(&admission, &mut active_receiver)
            .await
            .unwrap();
        let (sender, mut receiver) = tokio::sync::watch::channel(Control::Running);
        let waiting_admission = admission.clone();
        let waiter =
            tokio::spawn(
                async move { acquire_download_slot(&waiting_admission, &mut receiver).await },
            );

        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        admission.set_limit(2).unwrap();
        assert!(matches!(
            waiter.await.unwrap().unwrap(),
            SlotResult::Acquired(_)
        ));
        drop(active_slot);
        sender.send_replace(Control::Cancelled);
    }

    #[tokio::test]
    async fn local_http_range_server_writes_exact_bytes_to_preallocated_file() {
        let _guard = http_test_guard();
        let payload = test_payload(super::MIN_SEGMENTED_DOWNLOAD_SIZE as usize + 37);
        let server = LocalHttpServer::start(payload.clone(), ServerBehavior::SupportsRanges);
        let test_download = TestDownload::new();

        start_test_download(&server, &test_download).await.unwrap();

        assert_eq!(test_download.status(), "completed");
        assert_eq!(std::fs::read(&test_download.final_path).unwrap(), payload);
        assert!(
            server.range_requests.load(Ordering::Acquire) >= 5,
            "observed only {} range requests",
            server.range_requests.load(Ordering::Acquire)
        );
        assert_eq!(
            server.identity_range_requests.load(Ordering::Acquire),
            server.range_requests.load(Ordering::Acquire)
        );
    }

    #[tokio::test]
    async fn local_http_invalid_range_servers_fall_back_to_sequential_downloads() {
        let _guard = http_test_guard();
        for behavior in [
            ServerBehavior::IgnoresRanges,
            ServerBehavior::MalformedSegmentRanges,
            ServerBehavior::ChangesValidator,
        ] {
            let payload = test_payload(super::MIN_SEGMENTED_DOWNLOAD_SIZE as usize + 53);
            let server = LocalHttpServer::start(payload.clone(), behavior);
            let test_download = TestDownload::new();

            start_test_download(&server, &test_download).await.unwrap();

            assert_eq!(
                test_download.status(),
                "completed",
                "download error: {:?}",
                test_download.manager.entries.lock().unwrap()[&test_download.id].error
            );
            assert_eq!(std::fs::read(&test_download.final_path).unwrap(), payload);
        }
    }

    #[tokio::test]
    async fn local_http_short_range_bodies_fall_back_to_sequential_downloads() {
        let _guard = http_test_guard();
        let payload = test_payload(super::MIN_SEGMENTED_DOWNLOAD_SIZE as usize + 59);
        let server = LocalHttpServer::start(payload.clone(), ServerBehavior::ShortRangeBodies);
        let test_download = TestDownload::new();

        start_test_download(&server, &test_download).await.unwrap();

        assert_eq!(
            test_download.status(),
            "completed",
            "download error: {:?}",
            test_download.manager.entries.lock().unwrap()[&test_download.id].error
        );
        assert_eq!(std::fs::read(&test_download.final_path).unwrap(), payload);
        assert!(server.range_requests.load(Ordering::Acquire) > 1);
    }

    #[tokio::test]
    async fn local_http_dropped_segment_connections_retry_and_resume_the_range() {
        let _guard = http_test_guard();
        let payload = test_payload(super::MIN_SEGMENTED_DOWNLOAD_SIZE as usize + 61);
        let server =
            LocalHttpServer::start(payload.clone(), ServerBehavior::DropsSegmentConnections);
        let test_download = TestDownload::new();

        start_test_download(&server, &test_download).await.unwrap();

        assert_eq!(
            test_download.status(),
            "completed",
            "download error: {:?}",
            test_download.manager.entries.lock().unwrap()[&test_download.id].error
        );
        assert_eq!(std::fs::read(&test_download.final_path).unwrap(), payload);
        assert!(server.range_requests.load(Ordering::Acquire) > 1);
        assert_eq!(
            test_download.manager.entries.lock().unwrap()[&test_download.id].retry_count,
            1
        );
    }

    #[tokio::test]
    async fn local_http_stalled_segments_time_out_and_resume_after_retry() {
        let _guard = http_test_guard();
        let payload = test_payload(super::MIN_SEGMENTED_DOWNLOAD_SIZE as usize + 71);
        let server = LocalHttpServer::start(payload.clone(), ServerBehavior::StallsSegmentBodies);
        let test_download = TestDownload::new();
        let task = start_test_download(&server, &test_download);

        wait_for_retry_count(&test_download, 1).await;
        server.slow.store(false, Ordering::Release);
        task.await.unwrap();

        assert_eq!(
            test_download.status(),
            "completed",
            "download error: {:?}",
            test_download.manager.entries.lock().unwrap()[&test_download.id].error
        );
        assert_eq!(std::fs::read(&test_download.final_path).unwrap(), payload);
        assert!(server.range_requests.load(Ordering::Acquire) > 5);
    }

    #[tokio::test]
    async fn local_http_stalled_sequential_response_retries_after_timeout() {
        let _guard = http_test_guard();
        let payload = test_payload(super::MIN_SEGMENTED_DOWNLOAD_SIZE as usize + 77);
        let server = LocalHttpServer::start(payload.clone(), ServerBehavior::StallsSequentialBody);
        let test_download = TestDownload::new();
        let task = start_test_download(&server, &test_download);

        wait_for_retry_count(&test_download, 1).await;
        server.slow.store(false, Ordering::Release);
        task.await.unwrap();

        assert_eq!(
            test_download.status(),
            "completed",
            "download error: {:?}",
            test_download.manager.entries.lock().unwrap()[&test_download.id].error
        );
        assert_eq!(std::fs::read(&test_download.final_path).unwrap(), payload);
        assert_eq!(
            test_download.manager.entries.lock().unwrap()[&test_download.id].retry_count,
            1
        );
    }

    #[tokio::test]
    async fn local_http_forbidden_link_is_marked_expired_without_completing_a_file() {
        let _guard = http_test_guard();
        let server = LocalHttpServer::start(
            test_payload(super::MIN_SEGMENTED_DOWNLOAD_SIZE as usize + 63),
            ServerBehavior::Forbidden,
        );
        let test_download = TestDownload::new();

        start_test_download(&server, &test_download).await.unwrap();

        assert_eq!(test_download.status(), "link-expired");
        assert!(
            test_download.manager.entries.lock().unwrap()[&test_download.id]
                .error
                .as_deref()
                .is_some_and(|error| !error.is_empty())
        );
        assert!(!test_download.final_path.exists());
    }

    #[tokio::test]
    async fn local_http_gone_link_is_marked_expired_without_completing_a_file() {
        let _guard = http_test_guard();
        let server = LocalHttpServer::start(
            test_payload(super::MIN_SEGMENTED_DOWNLOAD_SIZE as usize + 65),
            ServerBehavior::Gone,
        );
        let test_download = TestDownload::new();

        start_test_download(&server, &test_download).await.unwrap();

        assert_eq!(test_download.status(), "link-expired");
        assert!(
            test_download.manager.entries.lock().unwrap()[&test_download.id]
                .error
                .as_deref()
                .is_some_and(|error| !error.is_empty())
        );
        assert!(!test_download.final_path.exists());
    }

    #[tokio::test]
    async fn replacement_url_preserves_progress_when_size_and_validator_match() {
        let _guard = http_test_guard();
        let payload = test_payload(super::MIN_SEGMENTED_DOWNLOAD_SIZE as usize + 73);
        let server = LocalHttpServer::start(payload.clone(), ServerBehavior::SupportsRanges);
        let test_download = TestDownload::new();
        let ranges = segment_ranges(payload.len() as u64, super::MAX_SEGMENTS_PER_DOWNLOAD);
        let old_url = "https://expired.example/payload.bin";
        let old_state = SegmentResumeState {
            version: SEGMENT_RESUME_VERSION,
            url: old_url.to_string(),
            final_url: old_url.to_string(),
            total_size: payload.len() as u64,
            validator: Some("\"stable\"".to_string()),
            segments: ranges
                .iter()
                .enumerate()
                .map(|(index, (start, end))| SegmentPosition {
                    start: *start,
                    end: *end,
                    position: if index == 0 { end + 1 } else { *start },
                })
                .collect(),
        };
        std::fs::write(&test_download.partial_path, vec![0; payload.len()]).unwrap();
        let sidecar = segment_sidecar_path(&test_download.final_path);
        persist_segment_resume_state(&sidecar, &old_state)
            .await
            .unwrap();
        {
            let mut entries = test_download.manager.entries.lock().unwrap();
            let entry = entries.get_mut(&test_download.id).unwrap();
            entry.url = old_url.to_string();
            entry.status = "link-expired".to_string();
            entry.downloaded_bytes = segment_progress(&old_state);
            entry.total_bytes = Some(payload.len() as u64);
            entry.validator = old_state.validator.clone();
            entry.error = Some("Link expired".to_string());
        }

        let download =
            prepare_download_url_update(&test_download.manager, &test_download.id, &server.url)
                .await
                .unwrap();

        assert_eq!(download.url, server.url);
        assert_eq!(
            test_download.manager.entries.lock().unwrap()[&test_download.id].downloaded_bytes,
            segment_progress(&old_state)
        );
        let updated_state = read_segment_resume_state(&sidecar).unwrap().unwrap();
        assert_eq!(updated_state.url, server.url);
        assert_eq!(updated_state.final_url, server.url);
    }

    #[tokio::test]
    async fn replacement_url_restarts_when_validator_changes() {
        let _guard = http_test_guard();
        let payload = test_payload(super::MIN_SEGMENTED_DOWNLOAD_SIZE as usize + 75);
        let server = LocalHttpServer::start(payload.clone(), ServerBehavior::SupportsRanges);
        let test_download = TestDownload::new();
        let sidecar = segment_sidecar_path(&test_download.final_path);
        let ranges = segment_ranges(payload.len() as u64, super::MAX_SEGMENTS_PER_DOWNLOAD);
        let old_state = SegmentResumeState {
            version: SEGMENT_RESUME_VERSION,
            url: "https://expired.example/payload.bin".to_string(),
            final_url: "https://expired.example/payload.bin".to_string(),
            total_size: payload.len() as u64,
            validator: Some("\"old\"".to_string()),
            segments: ranges
                .iter()
                .map(|(start, end)| SegmentPosition {
                    start: *start,
                    end: *end,
                    position: end + 1,
                })
                .collect(),
        };
        std::fs::write(&test_download.partial_path, vec![0; payload.len()]).unwrap();
        persist_segment_resume_state(&sidecar, &old_state)
            .await
            .unwrap();
        {
            let mut entries = test_download.manager.entries.lock().unwrap();
            let entry = entries.get_mut(&test_download.id).unwrap();
            entry.url = old_state.url.clone();
            entry.status = "link-expired".to_string();
            entry.downloaded_bytes = payload.len() as u64;
            entry.total_bytes = Some(payload.len() as u64);
            entry.validator = old_state.validator.clone();
        }

        prepare_download_url_update(&test_download.manager, &test_download.id, &server.url)
            .await
            .unwrap();

        assert_eq!(
            test_download.manager.entries.lock().unwrap()[&test_download.id].downloaded_bytes,
            0
        );
        assert_eq!(
            std::fs::metadata(&test_download.partial_path)
                .unwrap()
                .len(),
            0
        );
        assert!(!sidecar.exists());
    }

    #[test]
    fn disk_space_check_accounts_for_replaced_partial_files() {
        assert!(enough_disk_space(100, 50, 50));
        assert!(!enough_disk_space(101, 50, 50));
        assert!(enough_disk_space(0, 0, 0));
    }

    #[test]
    fn disk_space_preflight_rejects_an_impossible_allocation() {
        let test_download = TestDownload::new();
        let error = ensure_disk_space(&test_download.partial_path, u64::MAX, false).unwrap_err();

        assert!(error.starts_with("Not enough disk space:"));
    }

    #[tokio::test]
    async fn insufficient_disk_space_preserves_partial_download_state() {
        let test_download = TestDownload::new();
        std::fs::write(&test_download.partial_path, b"saved bytes").unwrap();

        finish_failed(
            &TestUpdateSink,
            &test_download.manager,
            DownloadFailure {
                id: &test_download.id,
                filename: "payload.bin",
                path: &test_download.partial_path,
                downloaded_bytes: 11,
                total_bytes: Some(100),
                message: "Not enough disk space: need 100 bytes.".to_string(),
            },
        )
        .await;

        assert_eq!(
            std::fs::read(&test_download.partial_path).unwrap(),
            b"saved bytes"
        );
    }

    #[tokio::test]
    async fn local_http_html_error_fixture_returns_a_success_status_and_html_type() {
        let _guard = http_test_guard();
        let server = LocalHttpServer::start(
            test_payload(super::MIN_SEGMENTED_DOWNLOAD_SIZE as usize + 67),
            ServerBehavior::HtmlErrorPage,
        );

        let response = reqwest::get(&server.url).await.unwrap();

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert!(
            response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("text/html"))
        );
        assert!(response.text().await.unwrap().contains("Temporary error"));
    }

    #[tokio::test]
    async fn local_http_download_can_pause_during_range_probe_and_resume() {
        let _guard = http_test_guard();
        let payload = test_payload(8 * 1024 * 1024);
        let server = LocalHttpServer::start(payload.clone(), ServerBehavior::SlowRanges);
        let test_download = TestDownload::new();
        let task = start_test_download(&server, &test_download);

        wait_for_range_requests(&server, 1).await;
        test_download.control_sender.send_replace(Control::Paused);
        wait_for_status(&test_download, "paused").await;
        assert!(!task.is_finished());

        server.slow.store(false, Ordering::Release);
        test_download.control_sender.send_replace(Control::Running);
        task.await.unwrap();

        assert_eq!(
            test_download.status(),
            "completed",
            "download error: {:?}",
            test_download.manager.entries.lock().unwrap()[&test_download.id].error
        );
        assert_eq!(std::fs::read(&test_download.final_path).unwrap(), payload);
    }

    #[tokio::test]
    async fn local_http_segmented_pause_preserves_all_progress_and_resumes() {
        let _guard = http_test_guard();
        let payload = test_payload(8 * 1024 * 1024);
        let server = LocalHttpServer::start(payload.clone(), ServerBehavior::SlowStreamingRanges);
        let test_download = TestDownload::new();
        let task = start_test_download(&server, &test_download);

        let checkpoint = wait_for_segment_checkpoint(&test_download).await;
        assert!(segment_progress(&checkpoint) > 0);
        test_download.control_sender.send_replace(Control::Paused);
        let paused_state = wait_for_paused_checkpoint(&test_download).await;

        let sidecar = segment_sidecar_path(&test_download.final_path);
        let paused_progress = segment_progress(&paused_state);
        assert!(paused_progress > 0);
        assert!(paused_progress < payload.len() as u64);
        assert!(
            paused_state
                .segments
                .iter()
                .any(|segment| segment.position > segment.start)
        );
        assert_eq!(
            std::fs::metadata(&test_download.partial_path)
                .unwrap()
                .len(),
            payload.len() as u64
        );
        assert_eq!(
            test_download.manager.entries.lock().unwrap()[&test_download.id].downloaded_bytes,
            paused_progress
        );
        assert_eq!(
            test_download.manager.entries.lock().unwrap()[&test_download.id].validator,
            Some("\"stable\"".to_string())
        );

        server.slow.store(false, Ordering::Release);
        test_download.control_sender.send_replace(Control::Running);
        task.await.unwrap();

        assert_eq!(test_download.status(), "completed");
        assert_eq!(std::fs::read(&test_download.final_path).unwrap(), payload);
        assert!(!sidecar.exists());
    }

    #[tokio::test]
    async fn interrupted_segmented_download_restores_sidecar_and_only_requests_missing_ranges() {
        let _guard = http_test_guard();
        let payload = test_payload(8 * 1024 * 1024);
        let server = LocalHttpServer::start(payload.clone(), ServerBehavior::SlowStreamingRanges);
        let test_download = TestDownload::new();
        let task = start_test_download(&server, &test_download);
        let sidecar = segment_sidecar_path(&test_download.final_path);
        let saved_state = wait_for_segment_checkpoint(&test_download).await;
        assert!(segment_progress(&saved_state) > 0);

        let request_count = server.range_requests.load(Ordering::Acquire);
        task.abort();
        let _ = task.await;
        server.slow.store(false, Ordering::Release);

        let persistence_file = test_download.directory.join("downloads.json");
        let download_dir = test_download.directory.join("downloads");
        std::fs::create_dir_all(&download_dir).unwrap();
        let mut record = persisted_download("downloading", 0);
        record.id = test_download.id.clone();
        record.url = server.url.clone();
        record.filename = "payload.bin".to_string();
        record.destination = Some(test_download.final_path.to_string_lossy().into_owned());
        persist_snapshot(&persistence_file, &[record]).unwrap();

        let (manager, recovered) =
            restore_download_manager(persistence_file, &download_dir).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].status, "paused");
        assert_eq!(
            recovered[0].downloaded_bytes,
            segment_progress(&saved_state)
        );
        let receiver = {
            let mut entries = manager.entries.lock().unwrap();
            let entry = entries.get_mut(&test_download.id).unwrap();
            assert_eq!(entry.status, "paused");
            entry.control.send_replace(Control::Running);
            entry.control.subscribe()
        };
        let restored_manager = manager.clone();
        let app: Arc<dyn UpdateSink> = Arc::new(TestUpdateSink);
        download_file_inner(
            app,
            manager,
            DownloadContext {
                id: test_download.id.clone(),
                url: server.url.clone(),
                filename: "payload.bin".to_string(),
                destination: test_download.final_path.clone(),
                partial_path: test_download.partial_path.clone(),
            },
            receiver,
        )
        .await;

        assert_eq!(
            restored_manager.entries.lock().unwrap()[&test_download.id].status,
            "completed",
            "download error: {:?}",
            restored_manager.entries.lock().unwrap()[&test_download.id].error
        );
        assert_eq!(std::fs::read(&test_download.final_path).unwrap(), payload);
        assert!(!sidecar.exists());
        let resumed_ranges = server.requested_ranges.lock().unwrap();
        let resumed_ranges = &resumed_ranges[request_count..];
        for segment in &saved_state.segments {
            if segment.position <= segment.end {
                assert!(
                    resumed_ranges.contains(&(segment.position, segment.end)),
                    "missing resumed range {}-{}",
                    segment.position,
                    segment.end
                );
            }
        }
    }

    #[tokio::test]
    async fn segmented_resume_restarts_from_zero_when_validator_changes() {
        let _guard = http_test_guard();
        let payload = test_payload(super::MIN_SEGMENTED_DOWNLOAD_SIZE as usize + 19);
        let server = LocalHttpServer::start(payload.clone(), ServerBehavior::SupportsRanges);
        let test_download = TestDownload::new();
        let sidecar = segment_sidecar_path(&test_download.final_path);
        let ranges = segment_ranges(payload.len() as u64, super::MAX_SEGMENTS_PER_DOWNLOAD);
        let old_state = SegmentResumeState {
            version: SEGMENT_RESUME_VERSION,
            url: server.url.clone(),
            final_url: server.url.clone(),
            total_size: payload.len() as u64,
            validator: Some("\"old-validator\"".to_string()),
            segments: ranges
                .iter()
                .enumerate()
                .map(|(index, (start, end))| SegmentPosition {
                    start: *start,
                    end: *end,
                    position: if index == 0 { *end + 1 } else { *start },
                })
                .collect(),
        };
        std::fs::write(&test_download.partial_path, vec![0xA5; payload.len()]).unwrap();
        persist_segment_resume_state(&sidecar, &old_state)
            .await
            .unwrap();
        assert!(sidecar.exists());
        assert_eq!(
            read_segment_resume_state(&sidecar)
                .unwrap()
                .unwrap()
                .validator,
            old_state.validator
        );
        {
            let mut entries = test_download.manager.entries.lock().unwrap();
            let entry = entries.get_mut(&test_download.id).unwrap();
            entry.url = server.url.clone();
            entry.status = "paused".to_string();
            entry.total_bytes = Some(payload.len() as u64);
            entry.validator = old_state.validator.clone();
            entry.control.send_replace(Control::Running);
        }
        let mut control_receiver = test_download.control_receiver.clone();
        let _ = control_receiver.borrow_and_update();

        let app: Arc<dyn UpdateSink> = Arc::new(TestUpdateSink);
        download_file_inner(
            app,
            test_download.manager.clone(),
            DownloadContext {
                id: test_download.id.clone(),
                url: server.url.clone(),
                filename: "payload.bin".to_string(),
                destination: test_download.final_path.clone(),
                partial_path: test_download.partial_path.clone(),
            },
            control_receiver,
        )
        .await;

        assert_eq!(
            test_download.status(),
            "completed",
            "download error: {:?}; ranges: {:?}",
            test_download.manager.entries.lock().unwrap()[&test_download.id].error,
            server.requested_ranges.lock().unwrap()
        );
        assert_eq!(std::fs::read(&test_download.final_path).unwrap(), payload);
        assert!(!sidecar.exists());
        let requests = server.requested_ranges.lock().unwrap();
        for expected in ranges {
            assert!(
                requests.contains(&expected),
                "restart did not request the full range {}-{}",
                expected.0,
                expected.1
            );
        }
    }

    #[tokio::test]
    async fn local_http_download_can_be_cancelled_during_range_probe() {
        let _guard = http_test_guard();
        let server =
            LocalHttpServer::start(test_payload(8 * 1024 * 1024), ServerBehavior::SlowRanges);
        let test_download = TestDownload::new();
        let task = start_test_download(&server, &test_download);

        wait_for_range_requests(&server, 1).await;
        test_download
            .control_sender
            .send_replace(Control::Cancelled);
        server.slow.store(false, Ordering::Release);
        task.await.unwrap();

        assert_eq!(test_download.status(), "cancelled");
        assert!(!test_download.final_path.exists());
        assert!(!test_download.partial_path.exists());
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let exit_started = Arc::new(AtomicBool::new(false));
    let exit_started_for_run = exit_started.clone();
    let app = tauri::Builder::default()
        .setup(|app| {
            let app_data_dir = app.path().app_data_dir()?;
            std::fs::create_dir_all(&app_data_dir)?;
            let download_dir = app.path().download_dir()?;
            let persistence_file = app_data_dir.join("downloads.json");
            let (manager, recovered) = restore_download_manager(persistence_file, &download_dir)
                .map_err(std::io::Error::other)?;
            app.manage(manager.clone());

            for record in recovered {
                let receiver = manager
                    .entries
                    .lock()
                    .map_err(|_| {
                        std::io::Error::other("Download state is unavailable during recovery.")
                    })?
                    .get(&record.id)
                    .map(|entry| entry.control.subscribe())
                    .ok_or_else(|| {
                        std::io::Error::other("Recovered download disappeared from the queue.")
                    })?;
                let task_manager = manager.clone();
                let task_app = app.handle().clone();
                let destination = record
                    .destination
                    .as_deref()
                    .map(PathBuf::from)
                    .unwrap_or_else(|| download_dir.join(&record.filename));
                let partial_path = partial_path_for(&destination, &record.id);
                let download = DownloadContext {
                    id: record.id.clone(),
                    url: record.url.clone(),
                    filename: record.filename.clone(),
                    destination,
                    partial_path,
                };
                tauri::async_runtime::spawn(async move {
                    download_file(task_app, task_manager, download, receiver).await;
                });
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            suggest_download_destination,
            start_download,
            pause_download,
            resume_download,
            cancel_download,
            retry_download,
            update_download_url,
            open_download_location,
            list_downloads,
            get_download_settings,
            set_max_concurrent_downloads
        ])
        .plugin(tauri_plugin_dialog::init())
        .build(tauri::generate_context!())
        .expect("error while running tauri application");
    app.run(move |app_handle, event| {
        if let RunEvent::ExitRequested { api, .. } = event {
            api.prevent_exit();
            if exit_started_for_run.swap(true, Ordering::AcqRel) {
                return;
            }

            let Some(manager_state) = app_handle.try_state::<DownloadManager>() else {
                app_handle.exit(0);
                return;
            };
            let manager = manager_state.inner().clone();
            if let Err(error) = pause_active_downloads(&manager) {
                eprintln!("{error}");
            }
            let exit_app = app_handle.clone();
            tauri::async_runtime::spawn(async move {
                let deadline = Instant::now() + Duration::from_secs(10);
                loop {
                    let active = match manager.entries.lock() {
                        Ok(entries) => entries.values().any(|entry| entry.status == "downloading"),
                        Err(_) => {
                            eprintln!("Download state is unavailable while waiting for shutdown.");
                            false
                        }
                    };
                    if !active {
                        break;
                    }
                    if Instant::now() >= deadline {
                        eprintln!(
                            "Timed out waiting for active downloads to checkpoint before shutdown."
                        );
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                if let Err(error) = manager.persist() {
                    eprintln!("{error}");
                }
                exit_app.exit(0);
            });
        }
    });
}
