use futures_util::StreamExt;
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::ErrorKind,
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tauri::{AppHandle, Emitter, Manager, Runtime, State};
use tokio::{
    io::AsyncWriteExt,
    sync::{Notify, watch},
};
use uuid::Uuid;

const DEFAULT_MAX_CONCURRENT_DOWNLOADS: usize = 3;
const MIN_CONCURRENT_DOWNLOADS: usize = 1;
const MAX_CONCURRENT_DOWNLOADS: usize = 10;
const MAX_SEGMENTS_PER_DOWNLOAD: usize = 4;
const MIN_SEGMENTED_DOWNLOAD_SIZE: u64 = 2 * 1024 * 1024;

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
    status: String,
    downloaded_bytes: u64,
    total_bytes: Option<u64>,
    error: Option<String>,
    control: watch::Sender<Control>,
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
    status: String,
    downloaded_bytes: u64,
    total_bytes: Option<u64>,
    error: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedSettings {
    max_concurrent_downloads: usize,
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
                    status: entry.status.clone(),
                    downloaded_bytes: entry.downloaded_bytes,
                    total_bytes: entry.total_bytes,
                    error: entry.error.clone(),
                })
                .collect()
        };
        snapshot.sort_by(|left, right| left.id.cmp(&right.id));
        persist_snapshot(path, &snapshot)
    }
}

fn persist_snapshot(path: &Path, snapshot: &[PersistedDownload]) -> Result<(), String> {
    let serialized = serde_json::to_vec_pretty(snapshot)
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

        let partial_path = download_dir.join(format!(".{}.part", record.id));
        let partial_size = if matches!(record.status.as_str(), "queued" | "downloading" | "paused")
        {
            match std::fs::metadata(&partial_path) {
                Ok(metadata) if metadata.is_file() => Some(metadata.len()),
                Ok(_) => {
                    record.status = "failed".to_string();
                    record.error = Some("The partial download path is not a file.".to_string());
                    None
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    std::fs::File::create(&partial_path).map_err(|error| {
                        format!("Could not restore partial download {}: {error}", record.id)
                    })?;
                    Some(0)
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
            std::fs::metadata(download_dir.join(&record.filename))
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
                    status: record.status.clone(),
                    downloaded_bytes: record.downloaded_bytes,
                    total_bytes: record.total_bytes,
                    error: record.error.clone(),
                    control: sender,
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
            if entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false) {
                if let Err(error) = std::fs::remove_dir_all(entry.path()) {
                    eprintln!("Could not remove stale segments for download {id}: {error}");
                }
            }
        }
    }
}

fn read_persisted_queue(
    persistence_file: &Path,
    backup_file: &Path,
) -> Result<Vec<PersistedDownload>, String> {
    let read = |path: &Path| -> Result<Vec<PersistedDownload>, String> {
        let contents = std::fs::read(path)
            .map_err(|error| format!("Could not read the download queue: {error}"))?;
        serde_json::from_slice(&contents)
            .map_err(|error| format!("Could not parse the download queue: {error}"))
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
    id: &str,
    filename: &str,
    status: &str,
    downloaded_bytes: u64,
    total_bytes: Option<u64>,
    error: Option<String>,
) {
    let status = match update_entry(
        manager,
        id,
        filename,
        status,
        downloaded_bytes,
        total_bytes,
        error.clone(),
    ) {
        Ok(status) => status,
        Err(state_error) => {
            eprintln!("{state_error}");
            status.to_string()
        }
    };
    if let Err(persist_error) = manager.persist() {
        eprintln!("{persist_error}");
    }
    emit_update(
        app,
        DownloadUpdate {
            id: id.to_string(),
            filename: filename.to_string(),
            downloaded_bytes,
            total_bytes,
            status,
            error,
        },
    );
}

fn filename_from_url(url: &reqwest::Url) -> String {
    let raw = url
        .path_segments()
        .and_then(|segments| segments.filter(|segment| !segment.is_empty()).next_back())
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
    let safe = safe.trim_end_matches(|character| character == ' ' || character == '.');

    if safe.is_empty() || safe == "." || safe == ".." {
        "download".to_string()
    } else if is_reserved_windows_name(&safe) {
        format!("_{safe}")
    } else {
        safe.to_string()
    }
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
        (Some(expected), Some(actual)) => expected == actual,
        _ => true,
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
    {
        if !etag.starts_with("W/") {
            return Some(etag.to_string());
        }
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
    id: &str,
    filename: &str,
    path: &Path,
    downloaded_bytes: u64,
    total_bytes: Option<u64>,
    message: String,
) {
    let message = match remove_partial_file(path).await {
        Ok(()) => {
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
                publish_update(
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
    };
    publish_update(
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

enum SlotResult {
    Acquired(DownloadPermit),
    Cancelled,
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

async fn finalize_file(
    directory: &Path,
    initial_filename: &str,
    partial_path: &Path,
) -> Result<String, String> {
    for number in 0..10_000 {
        let filename = numbered_filename(initial_filename, number);
        let destination = directory.join(&filename);
        match tokio::fs::hard_link(partial_path, &destination).await {
            Ok(()) => {
                tokio::fs::remove_file(partial_path)
                    .await
                    .map_err(|error| format!("Could not finalize the download: {error}"))?;
                return Ok(filename);
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(format!("Could not finalize the download: {error}")),
        }
    }
    Err("Could not choose a unique final filename.".to_string())
}

enum SegmentedAttempt {
    Completed(u64),
    Fallback,
    Paused(u64, u64),
    Cancelled(u64, u64),
}

enum SegmentWorkerResult {
    Completed(usize, u64),
    Invalid,
    Controlled(Control),
}

async fn fetch_segment(
    client: reqwest::Client,
    app: Arc<dyn UpdateSink>,
    manager: DownloadManager,
    url: String,
    path: PathBuf,
    index: usize,
    filename: String,
    id: String,
    start: u64,
    end: u64,
    total: u64,
    validator: Option<String>,
    progress: Arc<AtomicU64>,
    last_progress_update: Arc<Mutex<Instant>>,
    mut control: watch::Receiver<Control>,
) -> SegmentWorkerResult {
    if *control.borrow() != Control::Running {
        return SegmentWorkerResult::Controlled(*control.borrow());
    }

    let mut request = client
        .get(url)
        .header(reqwest::header::RANGE, format!("bytes={start}-{end}"))
        .header(reqwest::header::ACCEPT_ENCODING, "identity");
    if let Some(validator) = validator.as_deref() {
        request = request.header(reqwest::header::IF_RANGE, validator);
    }
    let response = tokio::select! {
        result = request.send() => match result {
            Ok(response) => response,
            Err(_) => return SegmentWorkerResult::Invalid,
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
    if !valid_segment_response(response.status(), content_range, start, end, total)
        || !validators_compatible(
            validator.as_deref(),
            response_validator(&response).as_deref(),
        )
        || response
            .content_length()
            .is_some_and(|length| length != end - start + 1)
    {
        return SegmentWorkerResult::Invalid;
    }

    let mut file = match tokio::fs::File::create(path).await {
        Ok(file) => file,
        Err(_) => return SegmentWorkerResult::Invalid,
    };
    let mut stream = response.bytes_stream();
    let mut written = 0u64;
    loop {
        tokio::select! {
            chunk = stream.next() => match chunk {
                Some(Ok(chunk)) => {
                    if written.saturating_add(chunk.len() as u64) > end - start + 1
                        || file.write_all(&chunk).await.is_err()
                    {
                        return SegmentWorkerResult::Invalid;
                    }
                    written += chunk.len() as u64;
                    let downloaded = progress.fetch_add(chunk.len() as u64, Ordering::Relaxed)
                        + chunk.len() as u64;
                    if let Ok(mut last_update) = last_progress_update.lock() {
                        if last_update.elapsed() >= Duration::from_millis(200) {
                            publish_update(
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
                    }
                }
                Some(Err(_)) => return SegmentWorkerResult::Invalid,
                None => break,
            },
            changed = control.changed() => {
                if changed.is_err() || *control.borrow() != Control::Running {
                    return SegmentWorkerResult::Controlled(*control.borrow());
                }
                return SegmentWorkerResult::Invalid;
            }
        }
    }
    if file.flush().await.is_err() || written != end - start + 1 {
        SegmentWorkerResult::Invalid
    } else {
        SegmentWorkerResult::Completed(index, written)
    }
}

async fn try_segmented_download(
    client: &reqwest::Client,
    app: &Arc<dyn UpdateSink>,
    manager: &DownloadManager,
    id: &str,
    url: &str,
    filename: &str,
    directory: &Path,
    path: &Path,
    control: &mut watch::Receiver<Control>,
) -> Result<SegmentedAttempt, String> {
    if *control.borrow() != Control::Running {
        return Ok(match *control.borrow() {
            Control::Paused => SegmentedAttempt::Paused(0, 0),
            Control::Cancelled => SegmentedAttempt::Cancelled(0, 0),
            Control::Running => unreachable!(),
        });
    }

    let probe_request = client
        .get(url)
        .header(reqwest::header::RANGE, "bytes=0-0")
        .header(reqwest::header::ACCEPT_ENCODING, "identity");
    let probe = tokio::select! {
        result = probe_request.send() => result.map_err(|error| error.to_string())?,
        changed = control.changed() => {
            return Ok(match *control.borrow() {
                Control::Paused => SegmentedAttempt::Paused(0, 0),
                Control::Cancelled => SegmentedAttempt::Cancelled(0, 0),
                Control::Running => {
                    if changed.is_err() {
                        SegmentedAttempt::Cancelled(0, 0)
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
    let Some((0, 0, total)) = probe_range else {
        return Ok(SegmentedAttempt::Fallback);
    };
    if probe_status != reqwest::StatusCode::PARTIAL_CONTENT || total < MIN_SEGMENTED_DOWNLOAD_SIZE {
        return Ok(SegmentedAttempt::Fallback);
    }
    let validator = response_validator(&probe);
    if probe.content_length().is_some_and(|length| length != 1) {
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
                    Control::Paused => SegmentedAttempt::Paused(0, total),
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
        return Ok(SegmentedAttempt::Fallback);
    }

    let segment_directory = directory.join(format!(".{id}.segments"));
    match tokio::fs::remove_dir_all(&segment_directory).await {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(format!("Could not prepare download segments: {error}")),
    }
    tokio::fs::create_dir(&segment_directory)
        .await
        .map_err(|error| format!("Could not prepare download segments: {error}"))?;

    let ranges = segment_ranges(total, MAX_SEGMENTS_PER_DOWNLOAD);
    publish_update(
        app,
        manager,
        id,
        filename,
        "downloading",
        0,
        Some(total),
        None,
    );
    let mut workers = tokio::task::JoinSet::new();
    let progress = Arc::new(AtomicU64::new(0));
    let last_progress_update = Arc::new(Mutex::new(Instant::now()));
    for (index, (start, end)) in ranges.iter().copied().enumerate() {
        workers.spawn(fetch_segment(
            client.clone(),
            app.clone(),
            manager.clone(),
            url.to_string(),
            segment_directory.join(index.to_string()),
            index,
            filename.to_string(),
            id.to_string(),
            start,
            end,
            total,
            validator.clone(),
            progress.clone(),
            last_progress_update.clone(),
            control.clone(),
        ));
    }

    let mut completed_bytes = 0u64;
    let mut completed_segments = vec![false; ranges.len()];
    let mut attempt = SegmentedAttempt::Completed(total);
    while let Some(result) = workers.join_next().await {
        match result {
            Ok(SegmentWorkerResult::Completed(index, bytes)) => {
                completed_segments[index] = true;
                completed_bytes += bytes;
                publish_update(
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
            Ok(SegmentWorkerResult::Controlled(Control::Paused)) => {
                attempt = SegmentedAttempt::Paused(completed_bytes, total);
                break;
            }
            Ok(SegmentWorkerResult::Controlled(Control::Cancelled)) => {
                attempt = SegmentedAttempt::Cancelled(completed_bytes, total);
                break;
            }
            Ok(SegmentWorkerResult::Controlled(Control::Running))
            | Ok(SegmentWorkerResult::Invalid) => {
                attempt = SegmentedAttempt::Fallback;
                break;
            }
            Err(_) => {
                attempt = SegmentedAttempt::Fallback;
                break;
            }
        }
    }
    if !matches!(attempt, SegmentedAttempt::Completed(_)) {
        workers.abort_all();
    }
    while workers.join_next().await.is_some() {}

    if matches!(attempt, SegmentedAttempt::Completed(_)) && completed_bytes != total {
        attempt = SegmentedAttempt::Fallback;
    }
    let assembly_count = match &attempt {
        SegmentedAttempt::Completed(_) => ranges.len(),
        SegmentedAttempt::Paused(_, _) => completed_segments
            .iter()
            .take_while(|completed| **completed)
            .count(),
        _ => 0,
    };
    if assembly_count > 0 {
        let mut output = tokio::fs::File::create(path)
            .await
            .map_err(|error| format!("Could not assemble the download: {error}"))?;
        for index in 0..assembly_count {
            let mut segment = tokio::fs::File::open(segment_directory.join(index.to_string()))
                .await
                .map_err(|error| format!("Could not read a download segment: {error}"))?;
            tokio::select! {
                result = tokio::io::copy(&mut segment, &mut output) => {
                    result.map_err(|error| format!("Could not assemble the download: {error}"))?;
                }
                changed = control.changed() => {
                    let state = *control.borrow();
                    if changed.is_err() || state == Control::Cancelled {
                        attempt = SegmentedAttempt::Cancelled(completed_bytes, total);
                    } else if state == Control::Paused {
                        attempt = SegmentedAttempt::Paused(completed_bytes, total);
                    } else {
                        attempt = SegmentedAttempt::Fallback;
                    }
                    break;
                }
            }
        }
        output
            .flush()
            .await
            .map_err(|error| format!("Could not assemble the download: {error}"))?;
        drop(output);
    }
    if matches!(attempt, SegmentedAttempt::Paused(_, _)) {
        let progress = tokio::fs::metadata(path)
            .await
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        attempt = SegmentedAttempt::Paused(progress, total);
    }
    if matches!(attempt, SegmentedAttempt::Fallback) {
        tokio::fs::File::create(path)
            .await
            .map_err(|error| format!("Could not reset the partial download: {error}"))?;
    }
    tokio::fs::remove_dir_all(&segment_directory)
        .await
        .map_err(|error| format!("Could not clean up download segments: {error}"))?;
    Ok(attempt)
}

async fn download_file<R: Runtime>(
    app: AppHandle<R>,
    manager: DownloadManager,
    id: String,
    url: String,
    initial_filename: String,
    directory: PathBuf,
    path: PathBuf,
    control: watch::Receiver<Control>,
) {
    let app: Arc<dyn UpdateSink> = Arc::new(app);
    download_file_inner(
        app,
        manager,
        id,
        url,
        initial_filename,
        directory,
        path,
        control,
    )
    .await;
}

async fn download_file_inner(
    app: Arc<dyn UpdateSink>,
    manager: DownloadManager,
    id: String,
    url: String,
    initial_filename: String,
    directory: PathBuf,
    path: PathBuf,
    mut control: watch::Receiver<Control>,
) {
    let client = reqwest::Client::new();
    let mut filename = initial_filename;
    let mut validator: Option<String> = None;
    let mut total_bytes = None;
    let mut last_update = Instant::now();

    loop {
        let offset = match tokio::fs::metadata(&path).await {
            Ok(metadata) => metadata.len(),
            Err(error) => {
                finish_failed(
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
        };

        let _slot = match acquire_download_slot(&manager.admission, &mut control).await {
            Ok(SlotResult::Acquired(slot)) => slot,
            Ok(SlotResult::Cancelled) => {
                if let Err(error) = remove_partial_file(&path).await {
                    publish_update(
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
                    publish_update(
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
                finish_failed(
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
        publish_update(
            &app,
            &manager,
            &id,
            &filename,
            "downloading",
            offset,
            total_bytes,
            None,
        );

        if offset == 0 {
            match try_segmented_download(
                &client,
                &app,
                &manager,
                &id,
                &url,
                &filename,
                &directory,
                &path,
                &mut control,
            )
            .await
            {
                Ok(SegmentedAttempt::Completed(total)) => {
                    match finalize_file(&directory, &filename, &path).await {
                        Ok(final_filename) => {
                            filename = final_filename;
                            publish_update(
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
                            finish_failed(
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
                Ok(SegmentedAttempt::Fallback) => {}
                Ok(SegmentedAttempt::Paused(progress, total)) => {
                    publish_update(
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
                        finish_failed(
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
                        publish_update(
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
                    finish_failed(
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
                    finish_failed(
                        &app, &manager, &id, &filename, &path, offset, total_bytes, error.to_string()
                    ).await;
                    return;
                }
            },
            changed = control.changed() => {
                if changed.is_err() || *control.borrow() == Control::Cancelled {
                    if let Err(error) = remove_partial_file(&path).await {
                        finish_failed(&app, &manager, &id, &filename, &path, offset, total_bytes, error).await;
                    } else {
                        publish_update(&app, &manager, &id, &filename, "cancelled", 0, total_bytes, None);
                    }
                    return;
                }
                if *control.borrow() == Control::Paused {
                    publish_update(&app, &manager, &id, &filename, "paused", offset, total_bytes, None);
                }
                continue;
            }
        };

        if can_resume && status_is_range_not_satisfiable(&response) {
            drop(response);
            if let Err(error) = tokio::fs::File::create(&path).await {
                finish_failed(
                    &app,
                    &manager,
                    &id,
                    &filename,
                    &path,
                    offset,
                    total_bytes,
                    format!("Could not restart the download: {error}"),
                )
                .await;
                return;
            }
            validator = None;
            total_bytes = None;
            continue;
        }

        if let Err(error) = response.error_for_status_ref() {
            finish_failed(
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
                    finish_failed(
                        &app,
                        &manager,
                        &id,
                        &filename,
                        &path,
                        offset,
                        total_bytes,
                        format!("Could not restart the download: {error}"),
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
                finish_failed(
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
            finish_failed(
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

        if truncate {
            if let Err(error) = tokio::fs::File::create(&path).await {
                finish_failed(
                    &app,
                    &manager,
                    &id,
                    &filename,
                    &path,
                    offset,
                    total_bytes,
                    format!("Could not restart the download: {error}"),
                )
                .await;
                return;
            }
            if status == reqwest::StatusCode::OK {
                total_bytes = response.content_length();
            }
            validator = response_validator(&response);
        }

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
                finish_failed(
                    &app,
                    &manager,
                    &id,
                    &filename,
                    &path,
                    offset,
                    total_bytes,
                    format!("Could not open the partial download: {error}"),
                )
                .await;
                return;
            }
        };

        let mut downloaded_bytes = if truncate { 0 } else { offset };
        let mut stream = response.bytes_stream();
        publish_update(
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
        let mut stream_error = None;

        loop {
            tokio::select! {
                chunk = stream.next() => match chunk {
                    Some(Ok(chunk)) => {
                        if let Err(error) = file.write_all(&chunk).await {
                            stream_error = Some(error.to_string());
                            break;
                        }
                        downloaded_bytes += chunk.len() as u64;
                        if last_update.elapsed() >= Duration::from_millis(200) {
                            publish_update(
                                &app, &manager, &id, &filename, "downloading",
                                downloaded_bytes, total_bytes, None,
                            );
                            last_update = Instant::now();
                        }
                    }
                    Some(Err(error)) => {
                        stream_error = Some(error.to_string());
                        break;
                    }
                    None => break,
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
            stream_error = Some(format!("Could not flush the partial download: {error}"));
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
                finish_failed(
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
                publish_update(
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
                    finish_failed(
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
            publish_update(
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

        if let Some(error) = stream_error {
            finish_failed(
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
                finish_failed(
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
                finish_failed(
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
            finish_failed(
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
        match finalize_file(&directory, &filename, &path).await {
            Ok(final_filename) => {
                filename = final_filename;
                publish_update(
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
                finish_failed(
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
    if matches!(current, "completed" | "failed" | "cancelled") {
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
async fn start_download(
    app: AppHandle,
    manager: State<'_, DownloadManager>,
    url: String,
) -> Result<StartedDownload, String> {
    let parsed_url = reqwest::Url::parse(&url).map_err(|error| error.to_string())?;
    if !matches!(parsed_url.scheme(), "http" | "https") {
        return Err("Only HTTP and HTTPS download links are supported.".to_string());
    }

    let proposed_filename = filename_from_url(&parsed_url);
    let download_dir = app
        .path()
        .download_dir()
        .map_err(|error| format!("Could not find the Downloads folder: {error}"))?;
    tokio::fs::create_dir_all(&download_dir)
        .await
        .map_err(|error| format!("Could not create the Downloads folder: {error}"))?;

    let filename = {
        let entries = manager
            .entries
            .lock()
            .map_err(|_| "Download state is unavailable.".to_string())?;
        (0..10_000)
            .map(|number| numbered_filename(&proposed_filename, number))
            .find(|candidate| {
                !download_dir.join(candidate).exists()
                    && !entries.values().any(|entry| {
                        entry.filename == *candidate
                            && !matches!(entry.status.as_str(), "failed" | "cancelled")
                    })
            })
            .ok_or_else(|| "Could not choose a unique download filename.".to_string())?
    };
    let id = Uuid::new_v4().to_string();
    let path = download_dir.join(format!(".{id}.part"));
    tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .await
        .map_err(|error| format!("Could not create the partial download: {error}"))?;

    let (sender, receiver) = watch::channel(Control::Running);
    manager
        .entries
        .lock()
        .map_err(|_| "Download state is unavailable.".to_string())?
        .insert(
            id.clone(),
            DownloadEntry {
                url: parsed_url.to_string(),
                filename: filename.clone(),
                status: "queued".to_string(),
                downloaded_bytes: 0,
                total_bytes: None,
                error: None,
                control: sender,
            },
        );
    publish_update(&app, &manager, &id, &filename, "queued", 0, None, None);

    let app_handle = app.clone();
    let task_manager = manager.inner().clone();
    let download_url = parsed_url.to_string();
    let task_id = id.clone();
    let task_filename = filename.clone();
    tauri::async_runtime::spawn(async move {
        download_file(
            app_handle,
            task_manager,
            task_id,
            download_url,
            task_filename,
            download_dir,
            path,
            receiver,
        )
        .await;
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
        Control, DownloadAdmission, DownloadEntry, DownloadManager, DownloadUpdate,
        PersistedDownload, PersistedSettings, SlotResult, UpdateSink, acquire_download_slot,
        download_file_inner, filename_from_url, is_reserved_windows_name, numbered_filename,
        parse_content_range, persist_max_concurrent_downloads, persist_snapshot,
        read_max_concurrent_downloads, read_persisted_queue, reconcile_persisted_status,
        segment_ranges, valid_range_probe, valid_segment_response,
        validate_max_concurrent_downloads, validate_transition, validators_compatible,
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

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum ServerBehavior {
        SupportsRanges,
        IgnoresRanges,
        MalformedSegmentRanges,
        ChangesValidator,
        SlowRanges,
    }

    struct LocalHttpServer {
        url: String,
        range_requests: Arc<AtomicUsize>,
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
            let slow = Arc::new(AtomicBool::new(behavior == ServerBehavior::SlowRanges));
            let shutdown = Arc::new(AtomicBool::new(false));

            let thread_body = body.clone();
            let thread_range_requests = range_requests.clone();
            let thread_slow = slow.clone();
            let thread_shutdown = shutdown.clone();
            let server_thread = thread::spawn(move || {
                while !thread_shutdown.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let body = thread_body.clone();
                            let range_requests = thread_range_requests.clone();
                            let slow = thread_slow.clone();
                            thread::spawn(move || {
                                serve_download_request(
                                    stream,
                                    body,
                                    behavior,
                                    range_requests,
                                    slow,
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
        slow: Arc<AtomicBool>,
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
        let range = request.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if !name.eq_ignore_ascii_case("range") {
                return None;
            }
            let bounds = value.trim().strip_prefix("bytes=")?;
            let (start, end) = bounds.split_once('-')?;
            Some((start.parse::<u64>().ok()?, end.parse::<u64>().ok()?))
        });
        if range.is_some() {
            range_requests.fetch_add(1, Ordering::Relaxed);
        }
        let is_probe = range == Some((0, 0));
        let ignores_range = behavior == ServerBehavior::IgnoresRanges;
        let (status, response_body, content_range) = match range {
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
        };
        let etag = if behavior == ServerBehavior::ChangesValidator && range.is_some() && !is_probe {
            "\"changed\""
        } else {
            "\"stable\""
        };
        let reason = if status == 206 {
            "Partial Content"
        } else {
            "OK"
        };
        let mut headers = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\nAccept-Ranges: bytes\r\nETag: {etag}\r\n",
            response_body.len()
        );
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
                    status: "queued".to_string(),
                    downloaded_bytes: 0,
                    total_bytes: None,
                    error: None,
                    control: control_sender.clone(),
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
        let directory = test_download.directory.clone();
        let partial_path = test_download.partial_path.clone();
        let control_receiver = test_download.control_receiver.clone();
        let url = server.url.clone();
        let app: Arc<dyn UpdateSink> = Arc::new(TestUpdateSink);
        tokio::spawn(async move {
            download_file_inner(
                app,
                manager,
                id,
                url,
                "payload.bin".to_string(),
                directory,
                partial_path,
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

    async fn wait_for_status(test_download: &TestDownload, status: &str) {
        for _ in 0..100_000 {
            if test_download.status() == status {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("download did not reach status {status}");
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
        assert!(validators_compatible(Some("\"one\""), None));
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

    fn persisted_download(status: &str, downloaded_bytes: u64) -> PersistedDownload {
        PersistedDownload {
            id: "c04e4566-0d5e-47c6-8863-9f2d6d327849".to_string(),
            url: "https://example.com/archive.zip".to_string(),
            filename: "archive.zip".to_string(),
            status: status.to_string(),
            downloaded_bytes,
            total_bytes: Some(12),
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
    }

    #[test]
    fn persisted_queue_round_trips_and_uses_backup_when_primary_is_invalid() {
        let directory =
            std::env::temp_dir().join(format!("rustdload-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let queue_file = directory.join("downloads.json");
        let backup_file = directory.join("downloads.json.bak");
        let record = persisted_download("paused", 7);

        persist_snapshot(&queue_file, std::slice::from_ref(&record)).unwrap();
        let restored = read_persisted_queue(&queue_file, &backup_file).unwrap();
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].id, record.id);

        std::fs::write(&queue_file, b"{").unwrap();
        std::fs::write(&backup_file, serde_json::to_vec(&[record]).unwrap()).unwrap();
        let recovered = read_persisted_queue(&queue_file, &backup_file).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].status, "paused");
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
                max_concurrent_downloads: 5,
            })
            .unwrap(),
        )
        .unwrap();
        assert_eq!(read_max_concurrent_downloads(&settings_file).unwrap(), 5);
        assert!(persist_max_concurrent_downloads(&settings_file, 0).is_err());
        std::fs::remove_dir_all(directory).unwrap();
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
    async fn local_http_range_server_downloads_and_assembles_exact_bytes() {
        let _guard = HTTP_TEST_LOCK.lock().unwrap();
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
    }

    #[tokio::test]
    async fn local_http_invalid_range_servers_fall_back_to_sequential_downloads() {
        let _guard = HTTP_TEST_LOCK.lock().unwrap();
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
    async fn local_http_download_can_pause_during_range_probe_and_resume() {
        let _guard = HTTP_TEST_LOCK.lock().unwrap();
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
    async fn local_http_download_can_be_cancelled_during_range_probe() {
        let _guard = HTTP_TEST_LOCK.lock().unwrap();
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
    tauri::Builder::default()
        .setup(|app| {
            let app_data_dir = app.path().app_data_dir()?;
            std::fs::create_dir_all(&app_data_dir)?;
            let download_dir = app.path().download_dir()?;
            let persistence_file = app_data_dir.join("downloads.json");
            let (manager, recovered) = restore_download_manager(persistence_file, &download_dir)
                .map_err(|error| std::io::Error::other(error))?;
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
                let partial_path = download_dir.join(format!(".{}.part", record.id));
                let task_directory = download_dir.clone();
                let task_id = record.id.clone();
                let task_url = record.url.clone();
                let task_filename = record.filename.clone();
                tauri::async_runtime::spawn(async move {
                    download_file(
                        task_app,
                        task_manager,
                        task_id,
                        task_url,
                        task_filename,
                        task_directory,
                        partial_path,
                        receiver,
                    )
                    .await;
                });
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            start_download,
            pause_download,
            resume_download,
            cancel_download,
            list_downloads,
            get_download_settings,
            set_max_concurrent_downloads
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
