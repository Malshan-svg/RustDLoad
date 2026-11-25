# RustDLoad

A free, open-source download manager built with Rust and Tauri.

## Current features

- Start HTTP and HTTPS downloads from a URL.
- Stream downloads to the system Downloads folder.
- Show live byte progress and completion or error status.
- Avoid overwriting files by choosing a unique filename.
- Pause, resume, and cancel active downloads.
- Run up to three downloads at once while additional jobs wait in the queue.
- Resume from the saved byte offset when the server supports HTTP Range requests.
- Persist the queue across restarts, recover interrupted downloads as paused, and validate saved completed files before restoring their completed status.
- Query the restored queue through the additive `list_downloads` Tauri command.

## Development

Install the prerequisites for [Tauri 2](https://tauri.app/start/prerequisites/), then run:

```powershell
npm install
npm run tauri dev
```

Create a production build with:

```powershell
npm run tauri build
```

## Roadmap

Queue metadata is stored in the Tauri app-data directory. On restart, interrupted downloads are restored as paused and resume from the actual partial-file size; completed downloads remain completed only when the saved file exists with the recorded size. Frontends should call `list_downloads` after registering their event listener to populate the queue immediately on startup. Servers that do not support byte-range requests may require a download to restart from the beginning.

The `browser-extension/` directory contains a Chromium link-handoff prototype. It prepares a request locally but cannot send downloads to the desktop app until a Native Messaging bridge is implemented. Segmented downloads, scheduled start times, and end-to-end browser integration are still planned.
