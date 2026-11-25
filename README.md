# RustDLoad

A free, open-source download manager built with Rust and Tauri.

## Current features

- Start HTTP and HTTPS downloads from a URL.
- Stream downloads to the system Downloads folder.
- Show live byte progress and completion or error status.
- Avoid overwriting files by choosing a unique filename.
- Pause, resume, and cancel active downloads.
- Choose a persistent maximum of 1–10 concurrent downloads (default: 3); additional jobs wait in the queue.
- Apply limit changes without interrupting running transfers. When lowering the limit, queued jobs wait until the active count is below the new limit.
- Resume from the saved byte offset when the server supports HTTP Range requests.
- Use up to four parallel byte-range segments for downloads of at least 2 MiB, but only after a one-byte range probe confirms valid range support; unsupported or invalid range responses restart safely as a sequential download.
- Persist the queue across restarts, recover interrupted downloads as paused, and validate saved completed files before restoring their completed status.
- Query the restored queue through the additive `list_downloads` Tauri command.

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

Queue metadata is stored in the Tauri app-data directory. On restart, interrupted downloads are restored as paused and resume from the actual partial-file size; completed downloads remain completed only when the saved file exists with the recorded size. Frontends should call `list_downloads` after registering their event listener to populate the queue immediately on startup. Segmented transfers retain contiguous completed bytes when paused; if range responses are unsupported, inconsistent, or invalid, the downloader discards the attempted segments and falls back to one sequential request. The configurable concurrency setting continues to limit the number of active download jobs; each eligible job may issue up to four segment requests.

The `browser-extension/` directory contains a Chromium link-handoff prototype. It prepares a request locally but cannot send downloads to the desktop app until a Native Messaging bridge is implemented. Scheduled start times and end-to-end browser integration are still planned.
