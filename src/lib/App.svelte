<script lang="ts">
  import QueueScreen from "./QueueScreen.svelte";
  import { SAMPLE_ITEMS } from "./sampleData";
  import type { QueueItem } from "./queue/types";
  import { NativeQueueController } from "./queue/native-controller";
  import type { NativeState } from "./queue/native-controller";
  import { onDestroy, onMount } from "svelte";
  import { invoke } from "@tauri-apps/api/core";
  import logoUrl from "../assets/surge-logo.webp";

  // Tauri runtime starts offline/empty and never pretends to be connected.
  // The browser build (no Tauri globals) shows clearly labeled SAMPLE DATA
  // and never touches invoke.
  const isTauri: boolean =
    typeof window !== "undefined" &&
    ("__TAURI__" in window || "__TAURI_INTERNALS__" in window);

  const sampleItems: QueueItem[] = SAMPLE_ITEMS;

  // Native (Tauri-only) connection state. The bearer token lives only in the
  // password input and is cleared immediately on submit; it is passed straight
  // to the controller (which forwards it to Rust invoke) and never stored.
  let endpoint = $state("");
  let token = $state("");
  let addUrl = $state("");
  let snapshot = $state<NativeState>({ phase: "offline", items: [], error: null, refreshing: false, controllingId: null, adding: false, addOutcome: null });

  let controller: NativeQueueController | null = null;
  if (isTauri) {
    controller = new NativeQueueController(
      (command, args) => invoke(command, args ?? {}) as Promise<unknown>,
      (s) => {
        snapshot = s;
      },
      5000,
    );
  }

  // Managed auto-start: exactly once on native mount, never in browser
  // sample mode. Preserves the optional manual endpoint/token form below.
  let managedAutoStarted = $state(false);

  onMount(() => {
    if (!isTauri || !controller) return;
    if (managedAutoStarted) return;
    managedAutoStarted = true;
    void controller.startManaged();
  });

  onDestroy(() => {
    controller?.dispose();
    controller = null;
  });

  const queueMode = $derived(snapshot.phase === "connected" ? "native" : "offline");
  const isConnecting = $derived(snapshot.phase === "connecting");
  const isConnected = $derived(snapshot.phase === "connected");
  const isAdding = $derived(snapshot.adding === true);

  // One honest connection label for the branded header — no daemon is ever
  // implied beyond the four real phases. Never colour-only: the dot always
  // travels with its state word.
  const connLabel = $derived(
    snapshot.phase === "connected"
      ? snapshot.refreshing
        ? "Refreshing…"
        : "Connected"
      : snapshot.phase === "connecting"
        ? "Connecting…"
        : snapshot.phase === "error"
          ? "Connection failed"
          : "Offline",
  );
  const connTone = $derived(
    snapshot.phase === "connected"
      ? "is-live"
      : snapshot.phase === "connecting"
        ? "is-busy"
        : snapshot.phase === "error"
          ? "is-error"
          : "",
  );

  async function handleConnect(event: SubmitEvent) {
    event.preventDefault();
    if (!controller || isConnecting) return;
    addUrl = "";
    const nextEndpoint = endpoint.trim();
    const nextToken = token;
    token = "";
    await controller.connect(nextEndpoint, nextToken);
  }

  function handleRefresh() {
    void controller?.refresh();
  }

  function handleDisconnect() {
    addUrl = "";
    void controller?.disconnect();
  }

  function handleManagedRetry() {
    if (!controller || isConnecting) return;
    void controller.startManaged();
  }

  const showManagedRetry = $derived(
    snapshot.phase === "error" || (snapshot.phase === "offline" && managedAutoStarted),
  );
  const managedRetryLabel = $derived(snapshot.phase === "error" ? "Retry bundled daemon" : "Start bundled daemon");

  function handleNativeControl(action: "pause" | "resume" | "removeRecord", id: string) {
    // Native-only: always route through the controller bridge. Never mutate
    // rows locally; ack does not mean the daemon transitioned (it settles async).
    if (!isTauri) return;
    void controller?.control(action, id);
  }

  async function handleAdd(event: SubmitEvent) {
    event.preventDefault();
    if (!isTauri || !controller) return;
    if (!isConnected || isAdding) return;
    const url = addUrl.trim();
    addUrl = "";
    if (!url) return;
    void controller.add(url);
  }
</script>

<main class="app">
  {#if !isTauri}
    <QueueScreen items={sampleItems} mode="sample" />
  {:else}
    <header class="topbar">
      <span class="starfield" aria-hidden="true"></span>
      <div class="brand">
        <img class="brand-logo" src={logoUrl} alt="" width="1376" height="768" decoding="async" />
        <span class="brand-divider" aria-hidden="true"></span>
        <span class="brand-text">
          <h1 class="wordmark">Surge Desktop</h1>
          <span class="section-name">Daemon connection</span>
        </span>
      </div>
      <span class="conn-badge {connTone}" role="status" aria-label={`Connection: ${connLabel}`}>
        <span class="conn-badge-dot" aria-hidden="true"></span>{connLabel}
      </span>
    </header>
    <section class="conn-panel" aria-label="Daemon connection">
      {#if snapshot.error}
        <p class="conn-error" id="daemon-error" role="alert">{snapshot.error}</p>
      {/if}
      <div class="conn-actions">
        {#if showManagedRetry}
          <button
            type="button"
            class="btn"
            onclick={handleManagedRetry}
            disabled={isConnecting}
          >
            {managedRetryLabel}
          </button>
        {/if}
        <button
          type="button"
          class="btn"
          onclick={handleRefresh}
          disabled={!isConnected || snapshot.refreshing}
        >
          {snapshot.refreshing && isConnected ? "Refreshing…" : "Refresh"}
        </button>
        <button
          type="button"
          class="btn"
          onclick={handleDisconnect}
          disabled={snapshot.phase === "offline"}
        >
          Disconnect
        </button>
      </div>
      <details class="conn-advanced">
        <summary class="conn-advanced-summary">Advanced: connect to another daemon</summary>
        <form class="conn-form" onsubmit={handleConnect}>
          <div class="field">
            <label class="field-label" for="daemon-endpoint">Daemon endpoint URL</label>
            <input
              id="daemon-endpoint"
              class="field-input"
              type="url"
              inputmode="url"
              autocomplete="off"
              spellcheck="false"
              required
              placeholder="https://daemon.example.com"
              bind:value={endpoint}
              aria-describedby="daemon-endpoint-help"
            />
            <p class="field-help" id="daemon-endpoint-help">Remote HTTPS, or loopback HTTP via SSH tunnel. Enter the actual daemon port.</p>
          </div>
          <div class="field">
            <label class="field-label" for="daemon-token">Bearer token</label>
            <input
              id="daemon-token"
              class="field-input"
              type="password"
              autocomplete="off"
              required
              placeholder="••••••••"
              bind:value={token}
              aria-invalid={snapshot.phase === "error" ? "true" : undefined}
              aria-describedby={snapshot.error ? "daemon-error" : undefined}
            />
            <p class="field-help" aria-hidden="true">Sent to the daemon only; never stored.</p>
          </div>
          <div class="conn-actions">
            <button type="submit" class="btn" disabled={isConnecting || snapshot.refreshing && !isConnected}>
              {isConnecting ? "Connecting…" : "Connect"}
            </button>
          </div>
        </form>
      </details>
    </section>
    {#if isConnected}
      <section class="add-panel" aria-label="Add download">
        <form class="add-form" onsubmit={handleAdd}>
          <div class="field add-field">
            <label class="field-label" for="add-url">Download URL</label>
            <input
              id="add-url"
              class="field-input"
              type="url"
              required
              autocomplete="off"
              spellcheck="false"
              placeholder="https://example.com/file.iso"
              bind:value={addUrl}
              disabled={isAdding}
              aria-describedby="add-url-help"
            />
            <p class="field-help" id="add-url-help">Sent to the daemon only; cleared on submit/disconnect and not saved by this app.</p>
          </div>
          <div class="add-actions">
            <button
              type="submit"
              class="btn"
              disabled={isAdding}
              aria-busy={isAdding ? "true" : undefined}
            >
              {isAdding ? "Adding…" : "Add download"}
            </button>
          </div>
        </form>
        {#if isAdding}
          <p class="add-feedback" role="status">Adding…</p>
        {:else if snapshot.addOutcome === "queued"}
          <p class="add-feedback" role="status">Added — daemon accepted. Row may appear after refresh.</p>
        {:else if snapshot.addOutcome === "pendingApproval"}
          <p class="add-feedback" role="status">Awaiting approval in the TUI — not queued yet.</p>
        {:else if snapshot.addOutcome === "duplicate"}
          <p class="add-feedback" role="status">Duplicate — daemon already tracks this URL; not queued.</p>
        {/if}
      </section>
    {/if}
    <QueueScreen items={snapshot.items} mode={queueMode} controllingId={snapshot.controllingId ?? null} onControl={handleNativeControl} />
  {/if}
</main>

<style>
  .app {
    min-height: 100dvh;
  }

  /* ---- Branded header (native) ---- */
  /* One branded header on native screens, first in the column. Mirrors the
     QueueScreen sample topbar (same logo, mark, type); the
     only addition is the honest live connection badge. QueueScreen suppresses
     its own topbar for native/offline so this is never duplicated. */
  .topbar {
    position: relative;
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: var(--space-xs) var(--space-md);
    max-width: 72rem;
    margin-inline: auto;
    padding: var(--space-md) var(--space-md) var(--space-sm);
    border-bottom: 1px solid var(--color-rule);
    min-width: 0;
  }

  .topbar > *:not(.starfield) {
    position: relative;
    z-index: 1;
  }

  .starfield {
    position: absolute;
    inset: 0;
    pointer-events: none;
    z-index: 0;
    opacity: 0.5;
    background-image:
      radial-gradient(circle at 13% 62%, var(--color-ink-2) 0 1px, transparent 1.4px),
      radial-gradient(circle at 71% 28%, var(--color-rule-2) 0 1px, transparent 1.4px),
      radial-gradient(circle at 88% 70%, var(--color-rule-2) 0 1px, transparent 1.4px);
  }

  .brand {
    display: flex;
    align-items: center;
    gap: var(--space-sm);
    min-width: 0;
  }

  .brand-logo {
    display: block;
    height: clamp(2.75rem, 6.4vw, 4.25rem);
    width: auto;
    flex: none;
    mix-blend-mode: screen;
  }

  .brand-divider {
    width: 1px;
    align-self: stretch;
    min-height: 1.75rem;
    background: var(--color-rule-2);
    flex: none;
  }

  .brand-text {
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
    min-width: 0;
  }

  .wordmark {
    margin: 0;
    font-family: var(--font-ui);
    font-weight: 700;
    font-size: var(--text-md);
    font-style: normal;
    letter-spacing: -0.01em;
    line-height: 1.2;
    color: var(--color-ink);
    white-space: nowrap;
  }

  .section-name {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    color: var(--color-neutral);
    text-transform: uppercase;
    letter-spacing: 0.14em;
    white-space: nowrap;
  }

  .conn-badge {
    display: inline-flex;
    align-items: center;
    gap: var(--space-xs);
    margin-left: auto;
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--color-neutral);
    border: 1px solid var(--color-rule-2);
    border-radius: 0;
    padding: var(--space-2xs) var(--space-sm);
    white-space: nowrap;
  }

  .conn-badge-dot {
    width: 0.5rem;
    height: 0.5rem;
    background: var(--color-neutral);
    border-radius: 1px;
    flex: none;
  }

  .conn-badge.is-live {
    color: var(--color-ink);
    border-color: var(--color-accent);
  }

  .conn-badge.is-live .conn-badge-dot {
    background: var(--color-ok);
  }

  .conn-badge.is-busy .conn-badge-dot {
    background: var(--color-muted);
  }

  .conn-badge.is-error {
    color: var(--color-error);
    border-color: var(--color-error);
  }

  .conn-badge.is-error .conn-badge-dot {
    background: var(--color-error);
  }

  .conn-panel {
    max-width: 72rem;
    margin-inline: auto;
    padding: var(--space-md) var(--space-md) 0;
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: var(--space-sm);
  }

  .conn-advanced {
    min-width: 0;
  }

  .conn-advanced-summary {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--color-ink);
    cursor: pointer;
    min-height: var(--control-h);
    display: list-item;
    align-items: center;
  }

  .conn-advanced .conn-form {
    margin-top: var(--space-sm);
  }

  .conn-form {
    display: flex;
    flex-direction: column;
    gap: var(--space-sm);
    position: relative;
    border: 1px solid var(--color-rule);
    border-radius: 0;
    background: var(--color-paper);
    padding: var(--space-md);
    min-width: 0;
  }

  .field {
    display: flex;
    flex-direction: column;
    gap: var(--space-2xs);
    min-width: 0;
  }

  .field-label {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--color-ink);
  }

  .field-input {
    box-sizing: border-box;
    min-height: var(--control-h);
    font: inherit;
    font-size: var(--text-base);
    color: var(--color-ink);
    background: var(--color-paper);
    border: 1px solid var(--color-rule-2);
    border-radius: 0;
    outline: 2px solid transparent;
    outline-offset: 1px;
    padding: 0 var(--space-md);
    width: 100%;
    min-width: 0;
  }

  .field-input::placeholder {
    color: var(--color-neutral);
  }

  @media (hover: hover) {
    .field-input:hover {
      background: var(--color-paper-2);
    }
  }

  .field-help {
    margin: 0;
    min-height: 1lh;
    font-size: var(--text-sm);
    color: var(--color-muted);
  }

  .field-input[aria-invalid="true"] {
    border-color: var(--color-error);
  }

  .conn-error {
    margin: 0;
    font-size: var(--text-sm);
    line-height: 1.5;
    color: var(--color-error);
    border: 1px solid var(--color-error);
    border-radius: 0;
    background: var(--color-error-wash);
    padding: var(--space-sm) var(--space-md);
    overflow-wrap: anywhere;
    min-width: 0;
  }

  .conn-actions {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-xs);
  }

  .add-panel {
    max-width: 72rem;
    margin-inline: auto;
    padding: var(--space-md) var(--space-md) 0;
    min-width: 0;
  }

  .add-form {
    display: flex;
    flex-direction: column;
    gap: var(--space-sm);
    position: relative;
    border: 1px solid var(--color-rule);
    border-radius: 0;
    background: var(--color-paper);
    padding: var(--space-md);
    min-width: 0;
  }

  /* Corner-bracket ticks on the connection/add panels — echoes the
     square monochrome queue screen without a second twin frame. */
  .conn-form::before,
  .add-form::before {
    content: "";
    position: absolute;
    top: 5px;
    right: 5px;
    width: 15px;
    height: 15px;
    border-top: 1px solid var(--color-rule-2);
    border-right: 1px solid var(--color-rule-2);
    pointer-events: none;
  }

  .conn-form::after,
  .add-form::after {
    content: "";
    position: absolute;
    bottom: 5px;
    left: 5px;
    width: 15px;
    height: 15px;
    border-bottom: 1px solid var(--color-rule-2);
    border-left: 1px solid var(--color-rule-2);
    pointer-events: none;
  }

  .add-field {
    flex: 1 1 auto;
  }

  .field-input:disabled {
    opacity: 0.55;
    cursor: not-allowed;
  }

  .add-actions {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-xs);
  }

  .add-feedback {
    margin: var(--space-sm) 0 0;
    font-size: var(--text-sm);
    line-height: 1.5;
    color: var(--color-muted);
    overflow-wrap: anywhere;
    min-width: 0;
  }

  .btn[aria-busy="true"] {
    cursor: progress;
  }

  .btn {
    min-height: var(--control-h);
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font: inherit;
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    font-weight: 600;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--color-ink);
    background: var(--color-paper-2);
    border: 1px solid var(--color-rule-2);
    border-radius: 0;
    padding: 0 var(--space-md);
    white-space: nowrap;
    cursor: pointer;
  }

  .btn[type="submit"] {
    color: var(--color-accent-ink);
    background: var(--color-accent);
    border-color: var(--color-accent);
  }

  .btn:disabled {
    opacity: 0.55;
    cursor: not-allowed;
  }

  @media (hover: hover) {
    .btn:hover:not(:disabled) {
      border-color: var(--color-ink-2);
    }
  }

  .btn:active:not(:disabled) {
    transform: translateY(1px);
  }

  :focus {
    outline: none;
  }
  :focus-visible {
    outline: 2px solid var(--color-focus);
    outline-offset: 2px;
  }

  @media (min-width: 60rem) {
    .conn-form {
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
      align-items: end;
    }
    .conn-error,
    .conn-actions {
      grid-column: 1 / -1;
    }
    .add-form {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      align-items: end;
    }
  }

  @media (prefers-reduced-motion: reduce) {
    *,
    *::before,
    *::after {
      animation-duration: 150ms !important;
      transition-duration: 150ms !important;
    }
  }
</style>
