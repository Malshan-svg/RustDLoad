use futures_util::StreamExt;
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::ErrorKind,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::{
    io::AsyncWriteExt,
    sync::{OwnedSemaphorePermit, Semaphore, watch},
};
use uuid::Uuid;

const MAX_CONCURRENT_DOWNLOADS: usize = 3;

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
    slots: Arc<Semaphore>,
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

fn emit_update(app: &AppHandle, update: DownloadUpdate) {
    if let Err(error) = app.emit("download-update", update) {
        eprintln!("Could not send download update to the UI: {error}");
    }
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
    let temporary_path = path.with_extension("json.tmp");
    let backup_path = path.with_extension("json.bak");
    std::fs::write(&temporary_path, serialized)
        .map_err(|error| format!("Could not write the download queue: {error}"))?;

    if path.exists() {
        if backup_path.exists() {
            std::fs::remove_file(path)
                .map_err(|error| format!("Could not replace the download queue: {error}"))?;
        } else {
            std::fs::rename(path, &backup_path).map_err(|error| {
                format!("Could not preserve the previous download queue: {error}")
            })?;
        }
    }
    if let Err(error) = std::fs::rename(&temporary_path, path) {
        if backup_path.exists() && !path.exists() {
            let _ = std::fs::rename(&backup_path, path);
        }
        return Err(format!("Could not save the download queue: {error}"));
    }
    if backup_path.exists() {
        let _ = std::fs::remove_file(backup_path);
    }
    Ok(())
}

fn restore_download_manager(
    persistence_file: PathBuf,
    download_dir: &Path,
) -> Result<(DownloadManager, Vec<PersistedDownload>), String> {
    std::fs::create_dir_all(download_dir)
        .map_err(|error| format!("Could not create the Downloads folder: {error}"))?;

    let backup_file = persistence_file.with_extension("json.bak");
    let records = read_persisted_queue(&persistence_file, &backup_file)?;
    let manager = DownloadManager {
        entries: Arc::default(),
        persistence_file: Some(persistence_file),
        persistence_lock: Arc::default(),
        slots: Arc::new(Semaphore::new(MAX_CONCURRENT_DOWNLOADS)),
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

fn publish_update(
    app: &AppHandle,
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

async fn finish_failed(
    app: &AppHandle,
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
    Acquired(OwnedSemaphorePermit),
    Cancelled,
}

async fn acquire_download_slot(
    semaphore: &Arc<Semaphore>,
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

        tokio::select! {
            permit = semaphore.clone().acquire_owned() => {
                let permit = permit.map_err(|error| format!("Could not acquire a download slot: {error}"))?;
                let control = *receiver.borrow();
                if control == Control::Running {
                    return Ok(SlotResult::Acquired(permit));
                }
                drop(permit);
            }
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

async fn download_file(
    app: AppHandle,
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

        let _slot = match acquire_download_slot(&manager.slots, &mut control).await {
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

#[cfg(test)]
mod tests {
    use super::{
        Control, PersistedDownload, SlotResult, acquire_download_slot, filename_from_url,
        is_reserved_windows_name, numbered_filename, parse_content_range, persist_snapshot,
        read_persisted_queue, reconcile_persisted_status, validate_transition,
    };

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

    #[tokio::test]
    async fn paused_queued_download_waits_without_taking_a_slot() {
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let (sender, mut receiver) = tokio::sync::watch::channel(Control::Paused);
        let task_semaphore = semaphore.clone();
        let waiter =
            tokio::spawn(
                async move { acquire_download_slot(&task_semaphore, &mut receiver).await },
            );

        tokio::task::yield_now().await;
        assert_eq!(semaphore.available_permits(), 1);
        sender.send_replace(Control::Running);
        let result = waiter.await.unwrap().unwrap();
        assert!(matches!(result, SlotResult::Acquired(_)));
        assert_eq!(semaphore.available_permits(), 0);
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
            list_downloads
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
