# RustDLoad

A free, open-source download manager built with Rust and Tauri.

## Current features

- Start HTTP and HTTPS downloads from a URL.
- Stream downloads to the system Downloads folder.
- Show live byte progress and completion or error status.
- Avoid overwriting files by choosing a unique filename.
- Pause, resume, and cancel active downloads.
- Resume from the saved byte offset when the server supports HTTP Range requests.

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

The download queue is currently held in memory and is not restored after restarting the app. Servers that do not support byte-range requests may require a download to restart from the beginning. Persistent queue recovery, segmented downloads, scheduling, and browser integration are planned.
