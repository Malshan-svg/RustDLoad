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

## Work log

- Phase 1, task 1 is complete locally: segmented downloads now write directly into a preallocated partial file and persist atomic `.rdl` resume sidecars with per-range positions, URL, final URL, size, and validator. Resume requests only unfinished ranges; mismatched metadata or validators restart safely, and invalid or unsupported ranges fall back to sequential downloading.
- Queue and settings persistence now write versioned formats while accepting legacy queue arrays and settings objects. Application exit pauses active downloads, waits up to 10 seconds for checkpointing, persists the queue, then exits.
- Task 1 verification: the focused interrupted-download recovery test passed; all 37 Rust tests passed; Rust formatting, strict Clippy, whitespace checks, and the frontend production build passed.
- The remainder of Phase 1 is not complete. Continue in the provided task order and do not begin Phase 2 until Phase 1 is fully validated.
- No internet-hosted test files or real downloads have been used during validation.

## Manual test checklist

- [ ] Download a large file with segments; pause, close the app, relaunch, resume, and verify its hash.
- [ ] Test a server that ignores Range requests.
- [ ] Test offline/online switching and sleep/wake recovery.
- [ ] Test duplicate URL handling and an expired link.
- [ ] Test the Settings screen and saved concurrency limit.
