use futures_util::StreamExt;
use percent_encoding::percent_decode_str;
use serde::Serialize;
use std::{
    collections::HashMap,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::{io::AsyncWriteExt, sync::watch};
use uuid::Uuid;

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
    filename: String,
    status: String,
    downloaded_bytes: u64,
    total_bytes: Option<u64>,
    control: watch::Sender<Control>,
}

#[derive(Clone, Default)]
struct DownloadManager {
    entries: Arc<Mutex<HashMap<String, DownloadEntry>>>,
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
    Ok(status.to_string())
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
    let status = match update_entry(manager, id, filename, status, downloaded_bytes, total_bytes) {
        Ok(status) => status,
        Err(state_error) => {
            eprintln!("{state_error}");
            status.to_string()
        }
    };
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

enum WaitResult {
    Running,
    Cancelled,
}

async fn wait_until_running(receiver: &mut watch::Receiver<Control>) -> WaitResult {
    loop {
        let control = *receiver.borrow();
        match control {
            Control::Running => return WaitResult::Running,
            Control::Cancelled => return WaitResult::Cancelled,
            Control::Paused => {
                if receiver.changed().await.is_err() {
                    return WaitResult::Cancelled;
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

        match wait_until_running(&mut control).await {
            WaitResult::Cancelled => {
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
            WaitResult::Running => {}
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
    Ok(())
}

fn validate_transition(current: &str, target: &str) -> Result<(), String> {
    if matches!(current, "completed" | "failed" | "cancelled") {
        return Err(format!(
            "Cannot {target} a download that is already {current}."
        ));
    }
    if target == "downloading" && current != "paused" {
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
                filename: filename.clone(),
                status: "queued".to_string(),
                downloaded_bytes: 0,
                total_bytes: None,
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
    control_download(&app, &manager, &id, Control::Running, "downloading")
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
        filename_from_url, is_reserved_windows_name, numbered_filename, parse_content_range,
        validate_transition,
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
        assert!(validate_transition("downloading", "downloading").is_err());
        assert!(validate_transition("completed", "paused").is_err());
        assert!(validate_transition("cancelled", "downloading").is_err());
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(DownloadManager::default())
        .invoke_handler(tauri::generate_handler![
            start_download,
            pause_download,
            resume_download,
            cancel_download
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
