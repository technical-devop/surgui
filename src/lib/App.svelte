<script lang="ts">
  import QueueScreen from "./QueueScreen.svelte";
  import { SAMPLE_ITEMS } from "./sampleData";
  import type { QueueItem } from "./queue/types";
  import { NativeQueueController } from "./queue/native-controller";
  import type { NativeState } from "./queue/native-controller";
  import { formatRateBpsToMb, parseRateMbToBps, rateInputErrorMessage } from "./queue/queue";
  import { onDestroy, onMount } from "svelte";
  import { untrack } from "svelte";
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
  let snapshot = $state<NativeState>({ phase: "offline", items: [], error: null, refreshing: false, controllingId: null, adding: false, addOutcome: null, managed: false, rateLimitBps: null, rateLoading: false, rateSaving: false, rateError: null, rateUnconfirmed: false, locationOpeningId: null, locationError: null, locationErrorId: null });
  let rateDraft = $state("");
  let rateDirty = $state(false);
  let rateTouched = $state(false);

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
  const isManaged = $derived(snapshot.managed === true);
  const rateLimitBps = $derived(snapshot.rateLimitBps ?? null);
  const rateLoading = $derived(snapshot.rateLoading === true);
  const rateSaving = $derived(snapshot.rateSaving === true);
  const rateError = $derived(snapshot.rateError ?? null);
  const rateUnconfirmed = $derived(snapshot.rateUnconfirmed === true);
  const locationOpeningId = $derived(snapshot.locationOpeningId ?? null);
  const locationError = $derived(snapshot.locationError ?? null);
  const locationErrorId = $derived(snapshot.locationErrorId ?? null);

  // Load the managed limit once per connection epoch. The draft lives in the
  // UI so list polls never overwrite an in-progress edit. tryClaim gates the
  // auto attempt so a failed read never loops; manual Retry calls loadRate()
  // directly and reconnect (epoch bump) allows exactly one new auto attempt.
  $effect(() => {
    const phase = snapshot.phase;
    const managed = snapshot.managed === true;
    const known = snapshot.rateLimitBps;
    const loading = snapshot.rateLoading === true;
    const poisoned = snapshot.rateUnconfirmed === true;
    untrack(() => {
      if (!isTauri || !controller) return;
      if (phase !== "connected" || !managed) return;
      if (known !== null || loading || poisoned) return;
      if (rateDirty) return;
      if (!controller.tryClaimRateAutoLoad()) return;
      void controller.loadRate();
    });
  });

  // Mirror the confirmed value into the draft only while the user has no
  // unsaved edit, so background loads never clobber typing.
  $effect(() => {
    const known = snapshot.rateLimitBps;
    untrack(() => {
      if (rateDirty) return;
      if (known === null || known === undefined) return;
      rateDraft = formatRateBpsToMb(known);
      rateTouched = false;
    });
  });

  // Reset the rate draft when the connection leaves managed/connected so a
  // stale value never looks like the new daemon's limit.
  $effect(() => {
    const phase = snapshot.phase;
    const managed = snapshot.managed === true;
    untrack(() => {
      if (phase !== "connected" || !managed) {
        rateDraft = "";
        rateDirty = false;
        rateTouched = false;
      }
    });
  });

  const rateParsed = $derived(parseRateMbToBps(rateDraft));
  const rateLocalError = $derived(
    rateDirty && !rateParsed.ok ? rateInputErrorMessage(rateParsed.reason) : null,
  );
  const rateInvalid = $derived(!!(rateLocalError || rateError));
  const rateErrorVisible = $derived(!rateLoading && !!(rateLocalError || rateError));
  const rateDescribedBy = $derived(rateErrorVisible ? "global-rate-help global-rate-error" : "global-rate-help");
  const rateCanSave = $derived(
    isManaged &&
      isConnected &&
      !rateLoading &&
      !rateSaving &&
      !rateUnconfirmed &&
      rateDirty &&
      rateParsed.ok,
  );
  const rateInputDisabled = $derived(!isManaged || !isConnected || rateLoading || rateSaving || rateUnconfirmed);
  const rateCurrentLabel = $derived(
    rateLimitBps === null || rateLimitBps === undefined
      ? null
      : rateLimitBps === 0
        ? "Current: Unlimited (0)"
        : `Current: ${formatRateBpsToMb(rateLimitBps)} MB/s`,
  );

  async function handleRateSave(event: SubmitEvent) {
    event.preventDefault();
    if (!isTauri || !controller) return;
    if (!isManaged || !isConnected || rateSaving || rateLoading || rateUnconfirmed) return;
    rateTouched = true;
    const parsed = parseRateMbToBps(rateDraft);
    if (!parsed.ok) return;
    rateDirty = false;
    const ok = await controller.setRateLimit(parsed.bytes);
    if (ok) {
      untrack(() => {
        rateDraft = formatRateBpsToMb(parsed.bytes);
        rateDirty = false;
        rateTouched = false;
      });
    } else {
      untrack(() => {
        rateDirty = true;
      });
    }
  }

  function handleOpenLocation(id: string) {
    if (!isTauri) return;
    void controller?.openLocation(id);
  }

  function handleClearLocationError() {
    controller?.clearLocationError();
  }

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
    rateDraft = "";
    rateDirty = false;
    rateTouched = false;
    void controller?.disconnect();
  }

  function handleManagedRetry() {
    if (!controller || isConnecting) return;
    void controller.startManaged();
  }

  function handleRateRetry() {
    if (!controller) return;
    if (!isManaged || !isConnected || rateLoading || rateSaving || rateUnconfirmed) return;
    void controller.loadRate();
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
      <section class="add-panel" aria-label="Global download speed">
        <form class="add-form" onsubmit={handleRateSave}>
          <div class="field add-field">
            <label class="field-label" for="global-rate">Global download limit (MB/s)</label>
            <input
              id="global-rate"
              class="field-input"
              type="text"
              inputmode="decimal"
              autocomplete="off"
              spellcheck="false"
              placeholder="e.g. 2.5, 0 unlimited"
              bind:value={rateDraft}
              oninput={() => { rateDirty = true; rateTouched = true; }}
              onblur={() => { rateTouched = true; }}
              disabled={rateInputDisabled}
              aria-describedby={rateDescribedBy}
              aria-invalid={rateInvalid ? "true" : "false"}
            />
            <p class="field-help" id="global-rate-help">Decimal MB/s; 0 means unlimited. Managed daemon only; polls never overwrite your edit.</p>
          </div>
          <div class="add-actions">
            <button
              type="submit"
              class="btn"
              disabled={!rateCanSave}
              aria-busy={rateSaving ? "true" : undefined}
            >
              {rateSaving ? "Saving…" : "Save limit"}
            </button>
          </div>
        </form>
        {#if rateLoading}
          <p class="add-feedback" role="status">Loading limit…</p>
        {:else if rateUnconfirmed}
          <p class="conn-error" id="global-rate-error" role="alert">Speed limit state could not be confirmed. Speed control is disabled until the app restarts to avoid unsafe writes.</p>
        {:else if rateError}
          <p class="conn-error" id="global-rate-error" role="alert">{rateError}</p>
          <div class="add-actions">
            <button type="button" class="btn" onclick={handleRateRetry} disabled={rateLoading}>
              Retry loading limit
            </button>
          </div>
        {:else if rateLocalError}
          <p class="conn-error" id="global-rate-error" role="alert">{rateLocalError}</p>
        {:else if !isManaged}
          <p class="add-feedback" role="note">Available only on the bundled daemon connection.</p>
        {:else if rateCurrentLabel}
          <p class="add-feedback" role="status">{rateCurrentLabel}</p>
        {/if}
      </section>
    {/if}
    <QueueScreen
      items={snapshot.items}
      mode={queueMode}
      controllingId={snapshot.controllingId ?? null}
      onControl={handleNativeControl}
      isManaged={isManaged}
      openingId={locationOpeningId}
      locationError={locationError}
      locationErrorId={locationErrorId}
      onOpenLocation={handleOpenLocation}
      onClearLocationError={handleClearLocationError}
    />
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
