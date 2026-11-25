# Overnight work notes

## Checkpoint

- Created branch `overnight/reliability`; `main` was not modified.
- Committed the existing work as `29f78e9` (`feat: improve download reliability and controls`). The requested concurrency and segmented-download themes already exist in the earlier `055e150` commit, while the remaining uncommitted changes combine destination handling, retries, progress reporting, and UI controls.
- Push to `origin` was attempted and rejected because GitHub authentication is unavailable in this environment. Do not force-push; retry after credentials are configured.

## Baseline validation

- `cargo fmt --manifest-path src-tauri\Cargo.toml -- --check`: passed.
- `cargo test --manifest-path src-tauri\Cargo.toml`: 32 passed.
- `npm run build`: passed.
- `cargo clippy --manifest-path src-tauri\Cargo.toml -- -D warnings`: passed after cleaning the existing warnings and grouping transfer/update state into context structs.

## Dependencies

- `tauri-plugin-dialog` and `@tauri-apps/plugin-dialog`: provide the native save dialog for choosing each download's destination.

## Work log

- Phase 1 reliability work has not started yet; the lint baseline is now clear, and crash-safe segmented resume is next.
- No internet-hosted test files or real downloads have been used during validation.

## Manual test checklist

- [ ] Download a large file with segments; pause, close the app, relaunch, resume, and verify its hash.
- [ ] Test a server that ignores Range requests.
- [ ] Test offline/online switching and sleep/wake recovery.
- [ ] Test duplicate URL handling and an expired link.
- [ ] Test the Settings screen and saved concurrency limit.
