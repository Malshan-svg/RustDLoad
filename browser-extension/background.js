const REQUEST_TYPE = "prepare-start-download";

chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  if (!message || message.type !== REQUEST_TYPE) return;

  try {
    const url = new URL(message.url);
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      throw new Error("Only HTTP and HTTPS links are supported.");
    }
    if (url.username || url.password) {
      throw new Error("Links containing embedded credentials are not supported.");
    }

    sendResponse({
      ok: true,
      request: {
        version: 1,
        requestId: crypto.randomUUID(),
        type: "start_download",
        payload: { url: url.href },
      },
    });
  } catch (error) {
    sendResponse({
      ok: false,
      error: error instanceof Error ? error.message : "The link is invalid.",
    });
  }
});
