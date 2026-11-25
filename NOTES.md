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
- Phase 1, task 6 verifies the completed partial file is regular and exactly matches the expected total size before finalizing both sequential and segmented downloads. Responses labeled as HTML/text are rejected for non-text filenames with an explicit error rather than being marked completed; ordinary text filenames remain allowed.
- Task 6's local HTTP regression exercises the previously deferred status-200 HTML error through the downloader, not just at the wire level. Final size and content-type checks are covered by focused tests.
- Phase 1, task 7 adds `get_download_details` with response metadata, current segment progress, timestamps, retry and connection counts, last error, and a persisted 50-event history. Structured JSON tracing rotates daily under app data. Diagnostics export includes app version, platform, non-secret settings, and the 100 newest log lines; URL user-info, query values, and fragments are redacted. A Settings action exports the report.
- Diagnostics tests cover detail snapshots, event-history retention, URL redaction, newest-log ordering, and report content.
- Phase 1, task 8 adds validated rename and removal actions, optional completed-file deletion, default-app opening, and containing-folder opening. Destructive or filesystem actions reject active downloads; rename also moves the resume sidecar and rolls back on persistence errors. Executable and script extensions require an explicit frontend confirmation; URL copying remains frontend-only.
- Phase 1, task 9 validates download identifiers and URL sizes/schemes, restricts the capability to event listening and the save dialog, and applies a restrictive CSP. Completed Windows downloads receive a `Zone.Identifier` marking them as Internet-originated.
- Phase 1 completion gate: all 73 Rust tests passed, including Windows Mark-of-the-Web coverage; strict Clippy, Rust formatting, whitespace/config validation, and the frontend production build passed. No live internet downloads were used.
- Phase 2 scheduling and bandwidth controls persist under the existing versioned settings file. Bandwidth limits are validated as bytes per second, with zero or empty values representing unlimited; token-bucket throttling is applied to sequential and segmented response streams.
- Scheduled downloads persist an optional future start timestamp and restore as `Scheduled`. A one-second backend monitor starts due entries once a download slot is available. The frontend provides schedule-time selection and "Start now" for scheduled rows.
- Bulk text import reads up to 256 KB and 1,000 unique HTTP/HTTPS entries, removes duplicate list lines, and reports invalid line numbers. The frontend uses native file/folder dialogs and queues each imported URL into the selected folder with the existing duplicate and file-policy logic.

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
- [ ] Rename and remove completed downloads; confirm remove-without-delete preserves the file and delete-file removes it.
- [ ] Open a downloaded document and confirm executable/script downloads require an additional warning confirmation.
- [ ] Export diagnostics and verify report contents are useful while signed URL credentials and query values are absent.
- [ ] Set global and per-download bandwidth limits and verify local transfers stay near the configured KB/s ceilings.
- [ ] Schedule a download for a future minute, close and relaunch the app, and confirm it starts automatically when due.

## Known limitations

- Offline recovery is request-driven rather than based on an operating-system network-change event; active transfers detect connection/timeouts, pause as Offline, and retry with backoff.
- Manual desktop smoke tests remain outstanding, including real-server concurrency/range behavior and the new context-menu/diagnostics flows.
