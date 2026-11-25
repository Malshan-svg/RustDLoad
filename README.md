# RustDLoad

A free, open-source download manager built with Rust and Tauri.

## Current features

- Start HTTP and HTTPS downloads from a URL.
- Prompt for a destination with the native save dialog before enqueueing each new download; the URL-derived filename and system Downloads folder are used as the default.
- Sanitize URL and server-provided filenames, including path separators, Windows device names, trailing dots/spaces, and excessive lengths.
- Resume partial files beside the chosen destination across app restarts using a versioned `.rdl` sidecar.
- Verify completed files against the server-reported size and reject HTML/text responses masquerading as binary downloads.
- Show live byte progress and completion or error status.
- Choose whether existing files are renamed, replaced after a successful transfer, or cause the new download to be skipped.
- Pause, resume, and cancel active downloads.
- Retry failed or cancelled downloads at their saved destination without reopening the save dialog.
- Retry transient network failures with capped exponential backoff; pause with an Offline reason on connection failures and retry automatically when connectivity returns.
- Detect stalled responses, preserve durable progress on disk-full errors, and check available disk space before allocating known-size downloads.
- Mark HTTP 403/410 responses as expired links; supply a replacement URL to resume only when the server size and validator still match.
- Detect duplicate HTTP/HTTPS URLs before opening the save dialog and report existing downloads to the UI; resolve destination collisions using the configured file-exists policy.
- Right-click a download to start/resume or pause it, retry it, copy its URL, open its file or destination folder, rename it, or remove it from the list.
- Open executable and script downloads only after an explicit warning; completed Windows downloads receive a Mark-of-the-Web zone identifier.
- Export a diagnostics report with app version, platform, settings, and recent logs with URL credentials and query values redacted.
- Choose a persistent maximum of 1–10 concurrent downloads (default: 3); additional jobs wait in the queue.
- Apply persistent global and per-download bandwidth limits in KB/s; zero or empty means unlimited.
- Schedule downloads for an exact future start time. Scheduled items survive restart and start automatically when the deadline arrives and a slot is available.
- Apply limit changes without interrupting running transfers. When lowering the limit, queued jobs wait until the active count is below the new limit.
- Resume from the saved byte offset when the server supports HTTP Range requests.
- Use up to four parallel byte-range segments for downloads of at least 2 MiB, writing directly to byte offsets in a preallocated partial file. An atomic `.rdl` sidecar checkpoints each range and validates the final URL, size, and validator before resuming; unsupported or inconsistent ranges restart safely as a sequential download.
- Persist the queue across restarts, recover interrupted downloads as paused, and validate saved completed files before restoring their completed status.
- Query the restored queue and per-download response, segment, retry, timestamp, and event details through the `list_downloads` and `get_download_details` Tauri commands.
- Manage completed or inactive records through validated `rename_download`, `remove_from_list`, `open_file`, and `open_containing_folder` commands.

## Development

Install the prerequisites for [Tauri 2](https://tauri.app/start/prerequisites/), then run:

```powershell
npm install
npm run tauri dev
```

Run the Rust unit and local HTTP integration tests with:

```powershell
cargo test --manifest-path src-tauri\Cargo.toml
```

Create a production build with:

```powershell
npm run tauri build
```

## Roadmap

Queue and settings metadata are versioned in the Tauri app-data directory; legacy queue arrays and settings objects without a version are migrated when saved. Partial files are kept beside the selected destination so interrupted sequential downloads resume from their actual file size, while segmented downloads resume from the durable positions recorded for every range. Completed downloads remain completed only when the saved destination exists with the recorded size. The file-exists policy defaults to keeping both files under a unique name; replacement happens only after a transfer completes, and the final link operation does not overwrite unexpectedly. Frontends should call `list_downloads` after registering their event listener to populate the queue immediately on startup. If range responses are unsupported, inconsistent, or invalid, the downloader discards the attempted segmented data and falls back to one sequential request. The configurable concurrency setting continues to limit the number of active download jobs; each eligible job may issue up to four segment requests.

The `browser-extension/` directory contains a Chromium link-handoff prototype. It prepares a request locally but cannot send downloads to the desktop app until a Native Messaging bridge is implemented. Advanced browser handoff and scheduling beyond exact local start times are still planned.
