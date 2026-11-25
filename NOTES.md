# Overnight work notes

## Checkpoint

- Created branch `overnight/reliability`; `main` was not modified.
- Committed the existing work as `29f78e9` (`feat: improve download reliability and controls`). The requested concurrency and segmented-download themes already exist in the earlier `055e150` commit, while the remaining uncommitted changes combine destination handling, retries, progress reporting, and UI controls.
- Push to `origin` was attempted and rejected because GitHub authentication is unavailable in this environment. The user chose to continue locally; do not retry the push unless asked.

## Baseline validation

- `cargo fmt --manifest-path src-tauri\Cargo.toml -- --check`: passed.
- `cargo test --manifest-path src-tauri\Cargo.toml`: 32 passed at baseline; 37 passed after Phase 1 task 1.
- `npm run build`: passed at baseline and after Phase 1 task 1.
- `cargo clippy --manifest-path src-tauri\Cargo.toml -- -D warnings`: passed after cleaning the existing warnings and grouping transfer/update state into context structs; passed again after Phase 1 task 1.

## Dependencies

- `tauri-plugin-dialog` and `@tauri-apps/plugin-dialog`: provide the native save dialog for choosing each download's destination.
- `fs2`: provides cross-platform available-disk-space checks before allocating or extending download files.

## Work log

- Phase 1, task 1 is complete locally: segmented downloads now write directly into a preallocated partial file and persist atomic `.rdl` resume sidecars with per-range positions, URL, final URL, size, and validator. Resume requests only unfinished ranges; mismatched metadata or validators restart safely, and invalid or unsupported ranges fall back to sequential downloading.
- Queue and settings persistence now write versioned formats while accepting legacy queue arrays and settings objects. Application exit pauses active downloads, waits up to 10 seconds for checkpointing, persists the queue, then exits.
- Task 1 verification: the focused interrupted-download recovery test passed; all 37 Rust tests passed; Rust formatting, strict Clippy, whitespace checks, and the frontend production build passed.
- Phase 1, task 2 adds local HTTP server modes and coverage for ignored ranges, malformed ranges, changed validators, short range bodies, dropped segment connections, slow/stalling transfers, and 403/410 responses. The HTML-error fixture is verified to return status 200 and `text/html`; downloader classification is deferred to task 6, where final-file integrity handling is introduced.
- No HTTP server dependency was added; the existing in-process TCP fixture provides the required local-only wire behavior.
- Task 2 validation: all 10 targeted local HTTP tests passed; the full Rust suite passed with 42 tests, and formatting, strict Clippy, whitespace checks, and the frontend build passed.
- Phase 1, task 3 adds identity-encoded range requests, 15-second stalled-body detection, per-transfer exponential retry limits with a persisted retry count, automatic Offline pause/retry behavior, Link expired status and replacement-URL validation, and available-space checks before known-size allocation. Disk-full and insufficient-space failures preserve partial data and segmented checkpoints.
- Replacement URLs preserve progress only when range probing succeeds and both size and validator match; otherwise the partial is reset before starting from the new URL.
- Task 3 validation: segmented and sequential stall tests, transient range retry, offline pause/resume, retry-cap, replacement-URL match/mismatch, disk-space preflight/preservation, and expired-link tests passed. The full Rust suite passed with 51 tests; strict Clippy, formatting, whitespace checks, and the frontend build passed.
- Phase 1, task 4: interrupted records already reconcile to Paused and have regression coverage; initial and restored display names come from the selected destination, with a custom-name restore assertion. Duplicate URL adds are now caught in the UI before the save dialog and rechecked atomically by the backend; the result identifies the existing download. Destination filename collisions continue to return a clear error.
- The reported temporary `TMPF*.tmp` display name was not reproducible in the current flow: the queue entry is created with the selected destination filename before transfer startup, and progress updates retain that entry name.
- Task 4 validation: duplicate URLs (including fragment-only differences), interrupted restore, and destination collision tests passed; the full Rust suite passed with 52 tests, strict Clippy, formatting, whitespace checks, and frontend build all passed.
- The remainder of Phase 1 is not complete. Continue in the provided task order and do not begin Phase 2 until Phase 1 is fully validated.
- No internet-hosted test files or real downloads have been used during validation.
- Phase 1, task 5 adds sanitized URL and `Content-Disposition` filenames: path separators and traversal markers cannot escape the selected folder, reserved Windows device names are prefixed, trailing dots/spaces are removed, and long names are shortened safely. An automatic URL-derived name may be replaced by the server's sanitized filename; an existing destination forces a unique name and moves any segmented checkpoint with it.
- The persisted file-exists setting now supports keeping both files (default), replacing the existing file only after the new transfer is complete, or skipping a destination that already exists. The policy is stored in settings and on each download record so retries/restores retain behavior; legacy settings and queue records default safely to rename.
- Task 5 validation: URL/header filename normalization, local HTTP filename adoption, checkpoint relocation, existing-file collision handling, overwrite/skip finalization, and settings migration/persistence tests passed. All 59 Rust tests, strict Clippy, formatting/whitespace checks, and the frontend production build passed.

## Manual test checklist

- [ ] Download a large file with segments; pause, close the app, relaunch, resume, and verify its hash.
- [ ] Test a server that ignores Range requests.
- [ ] Test offline/online switching and sleep/wake recovery.
- [ ] Test duplicate URL handling and an expired link.
- [ ] Update an expired link with matching and changed validators; verify progress is retained or reset appropriately.
- [ ] Verify insufficient free space leaves partial files and resume metadata intact.
- [ ] Test the Settings screen and saved concurrency limit.
- [ ] Test each file-exists policy against an existing destination and confirm no unintended data loss.
- [ ] Verify URL-derived and server-provided filenames on Windows with unusual characters and long names.

## Known limitations

- Offline recovery is request-driven rather than based on an operating-system network-change event; active transfers detect connection/timeouts, pause as Offline, and retry with backoff.
- The HTTP 200 HTML-error fixture is covered at the wire level, but classifying that body as an error is part of Phase 1 task 6 and remains pending.
