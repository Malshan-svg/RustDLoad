# RustDLoad browser link handoff (prototype)

This is an isolated Chromium Manifest V3 prototype for Chrome and Edge. The
toolbar popup lets you explicitly paste a direct link and prepare a JSON
start-download request. It does **not** intercept downloads, inspect tabs,
contact websites, or send anything to the desktop app.

## Load locally

1. Open `chrome://extensions` (Chrome) or `edge://extensions` (Edge).
2. Enable **Developer mode**.
3. Choose **Load unpacked** and select this `browser-extension` directory.
4. Open the RustDLoad toolbar button, paste a direct HTTP(S) link, and select
   **Prepare request**. The resulting JSON can be selected and copied manually.

The extension is only a request-preparation demo. Loading it does not install a
desktop bridge or start a download.

## Permissions and user action

The manifest requests no permissions and no host access. It does not use
`downloads`, `tabs`, `activeTab`, `storage`, `contextMenus`, or
`nativeMessaging`. The browser opens the popup only after the user clicks its
toolbar button; a second explicit **Prepare request** action is required.
Credentials embedded in a URL are rejected. Only HTTP and HTTPS URLs are
accepted.

## Desktop capability and missing bridge

The current desktop app has an internal Tauri command, `start_download`, which
accepts `{ "url": "https://…" }` and resolves to `{ "id": "...", "filename":
"..." }`. It is registered for the app's own frontend, not exposed to browser
extensions. Progress is emitted as the Tauri `download-update` event only
inside that app. There is currently no browser-facing IPC/native host,
registration, or authentication/consent bridge. Therefore this prototype
cannot hand a request to the app end-to-end.

### Proposed v1 bridge contract

For a future desktop bridge, this prototype prepares the following envelope
(the extension currently displays it locally and does not transmit it):

```json
{
  "version": 1,
  "requestId": "a UUID generated for this request",
  "type": "start_download",
  "payload": {
    "url": "https://example.com/file.zip"
  }
}
```

The proposed transport is Chromium Native Messaging with host name
`io.github.malshan-svg.rustdload`. A future bridge would need a registered
native-messaging host manifest whose `allowed_origins` contains the installed
extension ID, plus an executable that validates the envelope, allows only
HTTP(S), forwards the URL to the desktop download engine (equivalent to
Tauri's `start_download`), and replies to the same request ID. Chromium's
native-messaging framing (4-byte message length followed by UTF-8 JSON) is
handled by the browser API, not included in the JSON envelope.

Expected one-request/one-response JSON contract:

```json
{
  "version": 1,
  "requestId": "same UUID as request",
  "ok": true,
  "download": {
    "id": "desktop download ID",
    "filename": "file.zip"
  }
}
```

On rejection or failure, the bridge should instead reply:

```json
{
  "version": 1,
  "requestId": "same UUID as request",
  "ok": false,
  "error": "Human-readable reason"
}
```

An adapter should map Tauri's `{ url }` input and `{ id, filename }` result to
these envelopes; this wire format is a proposed contract, not an existing
desktop API. Progress notifications, pause/resume/cancel, browser cookies, and
authenticated page downloads are out of scope. Native Messaging is deliberately
not enabled in this prototype because no registered, working desktop host
exists.

## Lightweight checks

From the repository root, validate the manifest JSON and JavaScript syntax with:

```powershell
Get-Content browser-extension\manifest.json -Raw | ConvertFrom-Json
node --check browser-extension\background.js
node --check browser-extension\popup.js
```
