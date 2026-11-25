const form = document.querySelector("#request-form");
const urlInput = document.querySelector("#url");
const requestOutput = document.querySelector("#request");
const status = document.querySelector("#status");
const selectButton = document.querySelector("#select-request");

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  requestOutput.value = "";
  selectButton.disabled = true;
  status.classList.remove("error");
  status.textContent = "Preparing request…";

  try {
    const response = await chrome.runtime.sendMessage({
      type: "prepare-start-download",
      url: urlInput.value.trim(),
    });
    if (!response || !response.ok) {
      throw new Error(response?.error || "Could not prepare the request.");
    }

    requestOutput.value = JSON.stringify(response.request, null, 2);
    selectButton.disabled = false;
    status.textContent =
      "Prepared locally; nothing was sent. The desktop bridge is not connected.";
  } catch (error) {
    status.classList.add("error");
    status.textContent =
      error instanceof Error ? error.message : "Could not prepare the request.";
  }
});

selectButton.addEventListener("click", () => {
  requestOutput.focus();
  requestOutput.select();
  status.textContent = "JSON selected. Copy it manually with Ctrl+C or ⌘C.";
});
