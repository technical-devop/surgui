<script lang="ts">
  import QueueScreen from "./QueueScreen.svelte";
  import { SAMPLE_ITEMS } from "./sampleData";
  import type { BulkAction, QueueItem } from "./queue/types";
  import { NativeQueueController } from "./queue/native-controller";
  import { projectBatchRecovery } from "./queue/native-controller";
  import type { BatchRecoveryView, NativeState } from "./queue/native-controller";
  import {
    BATCH_STATUS_LABELS,
    MAX_BATCH_LINES,
    MAX_CONCURRENT_DOWNLOADS_DEFAULT,
    MAX_CONCURRENT_DOWNLOADS_MAX,
    MAX_CONCURRENT_DOWNLOADS_MIN,
    consumeRecoveryDraft,
    formatRateBpsToMb,
    isValidMaxConcurrentValue,
    mergeRecoverableLines,
    parseRateMbToBps,
    rateInputErrorMessage,
    resolveBatchDraft,
  } from "./queue/queue";
  import { onDestroy, onMount } from "svelte";
  import { untrack } from "svelte";
  import { invoke } from "@tauri-apps/api/core";
  import { listen } from "@tauri-apps/api/event";
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
  let snapshot = $state<NativeState>({ phase: "offline", items: [], error: null, refreshing: false, controllingId: null, adding: false, addOutcome: null, batch: null, managed: false, rateLimitBps: null, rateLoading: false, rateSaving: false, rateError: null, rateUnconfirmed: false, maxConcurrent: null, maxConcurrentLoading: false, maxConcurrentSaving: false, maxConcurrentError: null, maxConcurrentUnconfirmed: false, maxConcurrentRestartPending: false, locationOpeningId: null, locationError: null, locationErrorId: null, bulkAction: null, bulkSummary: null, bulkUncertainties: [], bulkUncertaintyOverflow: 0, hashIntentWarnings: [], hashIntentWarningOverflow: 0, hashDefaultEnabled: null, hashDefaultLoading: false, hashDefaultSaving: false, hashDefaultError: null, hashes: [], hashesLoading: false, hashError: null });
  // Per-batch hash choice. Initialized from the persisted default; the user
  // may override per batch without changing the default. Snapshotted once at
  // submission so an in-flight toggle cannot retarget the running batch.
  let hashForBatch = $state(false);
  let hashBatchDirty = $state(false);
  let rateDraft = $state("");
  let rateDirty = $state(false);
  let rateTouched = $state(false);
  let maxConcurrentDraft = $state("");
  let maxConcurrentDirty = $state(false);
  let maxConcurrentTouched = $state(false);
  let addOperationSeq = 0;
  let rateSaveSeq = 0;
  let maxConcurrentSaveSeq = 0;
  let recoveredBatches = $state<BatchRecoveryView[]>([]);
  let pendingRecoveryLines = $state<string[]>([]);
  const maxConcurrentOptions = Array.from(
    { length: MAX_CONCURRENT_DOWNLOADS_MAX - MAX_CONCURRENT_DOWNLOADS_MIN + 1 },
    (_, index) => MAX_CONCURRENT_DOWNLOADS_MIN + index,
  );

  let controller: NativeQueueController | null = null;
  if (isTauri) {
    controller = new NativeQueueController(
      (command, args) => invoke(command, args ?? {}) as Promise<unknown>,
      (s) => {
        snapshot = s;
      },
      5000,
      (event, handler) => listen(event, handler),
    );
  }

  // Managed auto-start: exactly once on native mount, never in browser
  // sample mode. Preserves the optional manual endpoint/token form below.
  let managedAutoStarted = $state(false);

  onMount(() => {
    if (!isTauri || !controller) return;
    if (managedAutoStarted) return;
    managedAutoStarted = true;
    const starting = controller.startManaged();
    // captureAbandonedBatch runs synchronously before the first await, so a
    // detached batch is recoverable and editable while still connecting.
    pullRecoveries(controller);
    restoreRecoveredDraft();
    void starting.then(() => {
      pullRecoveries(controller);
      restoreRecoveredDraft();
    });
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
  const batch = $derived(snapshot.batch ?? null);
  const batchRunning = $derived(batch?.running === true);
  // Disable the whole add surface for the entire batch, not just the gaps
  // between individual add requests.
  const addBusy = $derived(isAdding || batchRunning);
  // Keep the add surface available while offline/connecting so a recovered
  // draft can be reviewed and edited; submission still requires a connection.
  const showAddPanel = $derived(
    isConnected ||
      addUrl.trim().length > 0 ||
      pendingRecoveryLines.length > 0 ||
      recoveredBatches.length > 0,
  );
  const batchHasAmbiguous = $derived(
    batch?.results.some((result) => result.status === "ambiguous") === true,
  );
  const recoveredUncertainCount = $derived(
    recoveredBatches.reduce((total, recovery) => total + recovery.uncertainCount, 0),
  );
  const recoveredUnsentCount = $derived(
    recoveredBatches.reduce((total, recovery) => total + recovery.unsentCount, 0),
  );
  const recoveredRejectedCount = $derived(
    recoveredBatches.reduce((total, recovery) => total + recovery.rejectedCount, 0),
  );
  const recoveredAcceptedCount = $derived(
    recoveredBatches.reduce((total, recovery) => total + recovery.acceptedCount, 0),
  );
  const rateLimitBps = $derived(snapshot.rateLimitBps ?? null);
  const rateLoading = $derived(snapshot.rateLoading === true);
  const rateSaving = $derived(snapshot.rateSaving === true);
  const rateError = $derived(snapshot.rateError ?? null);
  const rateUnconfirmed = $derived(snapshot.rateUnconfirmed === true);
  const locationOpeningId = $derived(snapshot.locationOpeningId ?? null);
  const locationError = $derived(snapshot.locationError ?? null);
  const locationErrorId = $derived(snapshot.locationErrorId ?? null);
  const bulkAction = $derived(snapshot.bulkAction ?? null);
  const bulkSummary = $derived(snapshot.bulkSummary ?? null);
  const bulkUncertainties = $derived(snapshot.bulkUncertainties ?? []);
  const bulkUncertaintyOverflow = $derived(snapshot.bulkUncertaintyOverflow ?? 0);
  const hashIntentWarnings = $derived(snapshot.hashIntentWarnings ?? []);
  const hashIntentWarningOverflow = $derived(snapshot.hashIntentWarningOverflow ?? 0);
  const hashDefaultEnabled = $derived(snapshot.hashDefaultEnabled ?? null);
  const hashDefaultLoading = $derived(snapshot.hashDefaultLoading === true);
  const hashDefaultSaving = $derived(snapshot.hashDefaultSaving === true);
  const hashDefaultError = $derived(snapshot.hashDefaultError ?? null);
  const hashes = $derived(snapshot.hashes ?? []);
  const hashesLoading = $derived(snapshot.hashesLoading === true);
  const hashError = $derived(snapshot.hashError ?? null);

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

  // Load the daemon concurrency setting once per connection epoch. This is
  // the saved settings value, not the running daemon's active concurrency.
  $effect(() => {
    const phase = snapshot.phase;
    const known = snapshot.maxConcurrent;
    const loading = snapshot.maxConcurrentLoading === true;
    const poisoned = snapshot.maxConcurrentUnconfirmed === true;
    untrack(() => {
      if (!isTauri || !controller) return;
      if (phase !== "connected") return;
      if (known !== null || loading || poisoned) return;
      if (maxConcurrentDirty) return;
      if (!controller.tryClaimMaxConcurrentAutoLoad()) return;
      void controller.loadMaxConcurrent();
    });
  });

  // Mirror the saved value into the select only while the user has no unsaved
  // edit, so background loads never clobber a choice.
  $effect(() => {
    const known = snapshot.maxConcurrent;
    untrack(() => {
      if (maxConcurrentDirty) return;
      if (known === null || known === undefined) return;
      maxConcurrentDraft = String(known);
      maxConcurrentTouched = false;
    });
  });

  // Clear the concurrency draft when the connection leaves connected so a
  // stale value never looks like the new daemon's setting.
  $effect(() => {
    const phase = snapshot.phase;
    untrack(() => {
      if (phase !== "connected") {
        maxConcurrentDraft = "";
        maxConcurrentDirty = false;
        maxConcurrentTouched = false;
      }
    });
  });

  // Detached batch lines wait outside the controller until no batch is running.
  // Merging appends after any newer draft, so recovery can never overwrite text
  // the user typed while reconnecting. It does not wait for a connection: the
  // editable draft is available offline and only submission needs the daemon.
  $effect(() => {
    const busy = addBusy;
    const queued = pendingRecoveryLines.length;
    untrack(() => {
      if (busy || queued === 0) return;
      addUrl = mergeRecoverableLines(addUrl, pendingRecoveryLines);
      pendingRecoveryLines = [];
    });
  });

  // Load the persisted hash default once per connection epoch. Reconciles
  // after reconnect/startup so a stale default never persists.
  $effect(() => {
    const phase = snapshot.phase;
    untrack(() => {
      if (!isTauri || !controller) return;
      if (phase !== "connected") return;
      if (!controller.tryClaimHashDefaultAutoLoad()) return;
      void controller.loadHashDefault();
    });
  });

  // The per-batch checkbox starts from the persisted default. A user override
  // is kept until the batch is submitted; an in-flight batch is never
  // retargeted by a late default load.
  $effect(() => {
    const def = snapshot.hashDefaultEnabled;
    const running = batchRunning;
    untrack(() => {
      if (hashBatchDirty) return;
      if (running) return;
      if (def === null || def === undefined) return;
      hashForBatch = def;
    });
  });

  // Load visible hashes once per managed connection; manual connections stay
  // empty so another daemon's digests can never read as the current state.
  $effect(() => {
    const phase = snapshot.phase;
    const managed = snapshot.managed === true;
    const known = snapshot.hashes;
    untrack(() => {
      if (!isTauri || !controller) return;
      if (phase !== "connected" || !managed) return;
      if (known !== undefined && known !== null && known.length > 0) return;
      if (snapshot.hashesLoading === true) return;
      void controller.loadHashes();
    });
  });

  // Clear per-batch override when leaving connected so a stale choice never
  // looks like the new daemon's default. The persisted default itself stays
  // cached and is reloaded on the next connection.
  $effect(() => {
    const phase = snapshot.phase;
    untrack(() => {
      if (phase !== "connected") {
        hashBatchDirty = false;
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

  const maxConcurrent = $derived(snapshot.maxConcurrent ?? null);
  const maxConcurrentLoading = $derived(snapshot.maxConcurrentLoading === true);
  const maxConcurrentSaving = $derived(snapshot.maxConcurrentSaving === true);
  const maxConcurrentError = $derived(snapshot.maxConcurrentError ?? null);
  const maxConcurrentUnconfirmed = $derived(snapshot.maxConcurrentUnconfirmed === true);
  const maxConcurrentRestartPending = $derived(snapshot.maxConcurrentRestartPending === true);
  const maxConcurrentParsed = $derived(
    maxConcurrentDraft === "" ? null : Number(maxConcurrentDraft),
  );
  const maxConcurrentLocalError = $derived(
    maxConcurrentTouched &&
      maxConcurrentDraft !== "" &&
      !isValidMaxConcurrentValue(maxConcurrentParsed)
      ? "Choose a whole number from 1 to 10."
      : null,
  );
  const maxConcurrentInvalid = $derived(!!(maxConcurrentLocalError || maxConcurrentError));
  const maxConcurrentErrorVisible = $derived(
    !maxConcurrentLoading && !!(maxConcurrentLocalError || maxConcurrentError),
  );
  const maxConcurrentDescribedBy = $derived(
    maxConcurrentErrorVisible
      ? "max-concurrent-help max-concurrent-error"
      : "max-concurrent-help",
  );
  const maxConcurrentCanSave = $derived(
    isManaged &&
      isConnected &&
      !maxConcurrentLoading &&
      !maxConcurrentSaving &&
      !maxConcurrentUnconfirmed &&
      maxConcurrent !== null &&
      isValidMaxConcurrentValue(maxConcurrentParsed) &&
      maxConcurrentParsed !== maxConcurrent,
  );
  const maxConcurrentInputDisabled = $derived(
    !isManaged ||
      !isConnected ||
      maxConcurrentLoading ||
      maxConcurrentSaving ||
      maxConcurrentUnconfirmed ||
      maxConcurrent === null,
  );
  const maxConcurrentSavedLabel = $derived(
    maxConcurrent === null
      ? null
      : `Saved daemon value: ${maxConcurrent}. Surge applies it only after a daemon restart; the app does not restart it.`,
  );

  async function handleMaxConcurrentSave(event: SubmitEvent) {
    event.preventDefault();
    if (!isTauri || !controller) return;
    if (!isManaged || !isConnected || maxConcurrentSaving || maxConcurrentLoading || maxConcurrentUnconfirmed) return;
    maxConcurrentTouched = true;
    if (maxConcurrent === null || !isValidMaxConcurrentValue(maxConcurrentParsed)) return;
    const activeController = controller;
    const epoch = activeController.connectionEpoch();
    const requestId = ++maxConcurrentSaveSeq;
    const before = maxConcurrent;
    const value = maxConcurrentParsed;
    maxConcurrentDirty = false;
    const ok = await activeController.setMaxConcurrent(before, value);
    // Only the connection/request that started this save may touch the draft.
    if (controller !== activeController) return;
    if (requestId !== maxConcurrentSaveSeq) return;
    if (epoch !== activeController.connectionEpoch()) return;
    if (ok) {
      untrack(() => {
        maxConcurrentDraft = String(value);
        maxConcurrentDirty = false;
        maxConcurrentTouched = false;
      });
    } else {
      untrack(() => {
        maxConcurrentDirty = true;
      });
    }
  }

  async function handleRateSave(event: SubmitEvent) {
    event.preventDefault();
    if (!isTauri || !controller) return;
    if (!isManaged || !isConnected || rateSaving || rateLoading || rateUnconfirmed) return;
    rateTouched = true;
    const parsed = parseRateMbToBps(rateDraft);
    if (!parsed.ok) return;
    const activeController = controller;
    const epoch = activeController.connectionEpoch();
    const requestId = ++rateSaveSeq;
    rateDirty = false;
    const ok = await activeController.setRateLimit(parsed.bytes);
    // Only the connection/request that started this save may touch the draft.
    if (controller !== activeController) return;
    if (requestId !== rateSaveSeq) return;
    if (epoch !== activeController.connectionEpoch()) return;
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

  // Merge only recoveries not yet delivered to the input. Consumed lines are
  // cleared so connect/disconnect cannot wipe the current draft or resurrect a
  // link the user removed, submitted or already recovered.
  function restoreRecoveredDraft(): void {
    const restored = consumeRecoveryDraft(addUrl, pendingRecoveryLines);
    addUrl = restored.draft;
    pendingRecoveryLines = restored.pending;
  }

  function dismissRecoveredBatch(id: number): void {
    // Notices are display-only projections; dismissing removes the notice and
    // there is no raw recovery left to restore it silently later.
    recoveredBatches = recoveredBatches.filter((recovery) => recovery.id !== id);
  }

  function pullRecoveries(activeController: NativeQueueController | null): void {
    if (!activeController) return;
    const drained = activeController.drainBatchRecoveries();
    if (drained.length === 0) return;
    const notices: BatchRecoveryView[] = [];
    const lines: string[] = [];
    for (const recovery of drained) {
      const projected = projectBatchRecovery(recovery);
      notices.push(projected.view);
      lines.push(...projected.lines);
      // Drop the raw signed arrays now that only redacted results and the
      // editable-draft lines remain.
      recovery.results.length = 0;
      recovery.uncertain.length = 0;
      recovery.unsent.length = 0;
      recovery.rejected.length = 0;
    }
    recoveredBatches = [...recoveredBatches, ...notices];
    pendingRecoveryLines = [...pendingRecoveryLines, ...lines];
    if (!addBusy) restoreRecoveredDraft();
  }

  async function handleConnect(event: SubmitEvent) {
    event.preventDefault();
    if (!controller || isConnecting) return;
    restoreRecoveredDraft();
    const nextEndpoint = endpoint.trim();
    const nextToken = token;
    token = "";
    const connecting = controller.connect(nextEndpoint, nextToken);
    // The controller captures the abandoned batch synchronously before its
    // first await; drain it now so recovery shows while still connecting.
    pullRecoveries(controller);
    restoreRecoveredDraft();
    await connecting;
    pullRecoveries(controller);
    restoreRecoveredDraft();
  }

  function handleRefresh() {
    void controller?.refresh();
  }

  function handleDisconnect() {
    const activeController = controller;
    rateDraft = "";
    rateDirty = false;
    rateTouched = false;
    maxConcurrentDraft = "";
    maxConcurrentDirty = false;
    maxConcurrentTouched = false;
    hashBatchDirty = false;
    void activeController?.disconnect();
    pullRecoveries(activeController);
    restoreRecoveredDraft();
  }

  async function handleManagedRetry() {
    if (!controller || isConnecting) return;
    const activeController = controller;
    const starting = activeController.startManaged();
    pullRecoveries(activeController);
    restoreRecoveredDraft();
    await starting;
    pullRecoveries(activeController);
    restoreRecoveredDraft();
  }

  function handleRateRetry() {
    if (!controller) return;
    if (!isManaged || !isConnected || rateLoading || rateSaving || rateUnconfirmed) return;
    void controller.loadRate();
  }

  function handleMaxConcurrentRetry() {
    if (!controller) return;
    if (!isConnected || maxConcurrentLoading || maxConcurrentSaving || maxConcurrentUnconfirmed) return;
    // Reloading discards the unsaved draft so the fresh daemon value can be
    // mirrored into the select (e.g. after a 409 conflict).
    maxConcurrentDirty = false;
    maxConcurrentTouched = false;
    void controller.loadMaxConcurrent();
  }

  function handleHashRetry() {
    if (!controller) return;
    if (!isConnected) return;
    void controller.loadHashDefault();
  }

  function handleHashesRefresh() {
    if (!controller) return;
    if (!isConnected || !isManaged) return;
    void controller.refreshHashes();
  }

  async function handleHashDefaultSave() {
    if (!isTauri || !controller) return;
    if (!isConnected || hashDefaultSaving || hashDefaultLoading) return;
    // Persist the current per-batch choice as the default for future batches.
    // The in-flight batch keeps its snapshotted value.
    const activeController = controller;
    const epoch = activeController.connectionEpoch();
    const ok = await activeController.setHashDefault(hashForBatch);
    if (controller !== activeController) return;
    if (epoch !== activeController.connectionEpoch()) return;
    if (ok) {
      untrack(() => {
        hashBatchDirty = false;
      });
      void activeController.loadHashes();
    }
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

  function handleBulkControl(action: BulkAction) {
    // Applies to the daemon's whole queue, not the rows currently shown.
    if (!isTauri) return;
    void controller?.bulkControl(action);
  }

  function handleDismissBulkUncertainty(id: number) {
    if (!isTauri) return;
    controller?.dismissBulkUncertainty(id);
  }

  function handleDismissBulkUncertaintyOverflow() {
    if (!isTauri) return;
    controller?.dismissBulkUncertaintyOverflow();
  }

  function handleDismissHashIntentWarning(id: number) {
    if (!isTauri) return;
    controller?.dismissHashIntentWarning(id);
  }

  function handleDismissHashIntentWarningOverflow() {
    if (!isTauri) return;
    controller?.dismissHashIntentWarningOverflow();
  }

  async function handleAdd(event: SubmitEvent) {
    event.preventDefault();
    if (!isTauri || !controller) return;
    if (!isConnected || addBusy) return;
    const input = addUrl.trim();
    if (!input) return;
    const activeController = controller;
    const operationId = ++addOperationSeq;
    const epoch = activeController.connectionEpoch();
    // Snapshot the per-batch hash choice once; the running batch keeps this
    // value even if the checkbox or default changes mid-batch.
    const hashSnapshot = hashForBatch;
    addUrl = "";
    const result = await activeController.addBatch(input, hashSnapshot);
    // A reconnect, a newer submission or a disposed controller makes this
    // result stale; the resolver returns null and leaves the input untouched.
    // Accepted, pending and duplicate links are complete. Recoverable lines
    // remain so the user can fix or resend deliberately; never auto-retry.
    const nextDraft = resolveBatchDraft({
      draft: addUrl,
      submitted: input,
      hasResults: result.results.length > 0,
      recoverable: result.recoverable,
      operationId,
      currentOperationId: addOperationSeq,
      epoch,
      currentEpoch: activeController.connectionEpoch(),
      sameController: controller === activeController,
    });
    if (nextDraft !== null) addUrl = nextDraft;
    // A stale batch is no longer allowed to touch the input through the epoch
    // resolver, but its full original lines are preserved separately. Restore
    // them only through the non-overwriting merge effect above.
    pullRecoveries(activeController);
    // The next batch starts again from the persisted default.
    untrack(() => {
      hashBatchDirty = false;
      const def = snapshot.hashDefaultEnabled;
      if (def !== null && def !== undefined) hashForBatch = def;
    });
    // A newly queued batch may carry hash intents; reconcile visible rows.
    if (isManaged) void activeController.refreshHashes();
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
    {#if recoveredBatches.length > 0}
      <section class="add-panel" aria-label="Interrupted batch recovery">
        <p class="add-feedback" role="status">
          Recovered from an interrupted batch. In-flight outcome unknown for {recoveredUncertainCount} link{recoveredUncertainCount === 1 ? "" : "s"}. The unsent tail of {recoveredUnsentCount} link{recoveredUnsentCount === 1 ? "" : "s"} was not attempted. Definitely accepted or tracked before the interruption: {recoveredAcceptedCount}.{recoveredRejectedCount > 0 ? ` Definitively rejected: ${recoveredRejectedCount}.` : ""}
        </p>
        {#each recoveredBatches as recovery (recovery.id)}
          <ol class="batch-results recovery-results" aria-label={`Recovered batch ${recovery.id} link outcomes`}>
            {#each recovery.results as result}
              <li class="batch-row is-{result.status}">
                <span class="batch-status">{BATCH_STATUS_LABELS[result.status]}</span>
                <span class="batch-url">{result.line}</span>
                <span class="batch-note">{result.message}</span>
              </li>
            {/each}
          </ol>
          <div class="add-actions">
            <button
              type="button"
              class="btn"
              onclick={() => dismissRecoveredBatch(recovery.id)}
              aria-label={`Dismiss recovered batch ${recovery.id} notice`}
            >
              Dismiss notice
            </button>
          </div>
        {/each}
        <p class="add-feedback">Each link above carries its exact outcome. Unconfirmed means the daemon may have accepted it; Not attempted means it was never sent; Invalid or Failed means it was rejected. Full original links are available only in the editable field; nothing was retried automatically.</p>
      </section>
    {/if}
    {#if showAddPanel}
      <section class="add-panel" aria-label="Add downloads">
        <form class="add-form" onsubmit={handleAdd}>
          <div class="field add-field">
            <label class="field-label" for="add-url">Download URLs</label>
            <textarea
              id="add-url"
              class="field-input url-lines"
              rows="3"
              required
              autocomplete="off"
              spellcheck="false"
              placeholder="https://example.com/file.iso"
              bind:value={addUrl}
              disabled={addBusy}
              aria-describedby="add-url-help"
            ></textarea>
            <p class="field-help" id="add-url-help">One HTTP(S) URL per line. Up to {MAX_BATCH_LINES} per batch, sent one at a time through the normal add path. Repeated exact links are sent once per batch; daemon duplicates are reported. Failed, unconfirmed, invalid and unattempted links stay here. The field locks while a batch runs. You can edit offline; submitting needs a connection.</p>
          </div>
          <div class="field hash-field">
            <label class="hash-check" for="hash-batch">
              <input
                id="hash-batch"
                type="checkbox"
                class="hash-input"
                bind:checked={hashForBatch}
                onchange={() => { hashBatchDirty = true; }}
                disabled={!isManaged || !isConnected || addBusy}
              />
              <span class="hash-check-text">Compute MD5 + SHA-256 for this batch</span>
            </label>
            {#if !isManaged}
              <p class="field-help" id="hash-batch-help">Available only on the bundled daemon connection. Digests identify file content, not trust or safety.</p>
            {:else}
              <p class="field-help" id="hash-batch-help">Hashes each queued download after it finishes. Digests identify file content, not trust or safety. {#if hashDefaultEnabled !== null}Default for future batches: {hashDefaultEnabled ? "on" : "off"}.{/if}</p>
            {/if}
            {#if hashDefaultLoading}
              <p class="add-feedback" role="status">Loading hashing default…</p>
            {:else if hashDefaultError}
              <p class="conn-error" role="alert">{hashDefaultError}</p>
              <div class="add-actions">
                <button type="button" class="btn" onclick={handleHashRetry}>
                  Retry loading default
                </button>
              </div>
            {:else if isManaged && isConnected}
              <div class="add-actions">
                <button
                  type="button"
                  class="btn"
                  onclick={handleHashDefaultSave}
                  disabled={hashDefaultSaving || addBusy}
                  aria-busy={hashDefaultSaving ? "true" : undefined}
                >
                  {hashDefaultSaving ? "Saving…" : "Save choice as default"}
                </button>
              </div>
            {/if}
          </div>
          <div class="add-actions">
            <button
              type="submit"
              class="btn"
              disabled={addBusy || addUrl.trim().length === 0 || !isConnected}
              aria-busy={addBusy ? "true" : undefined}
            >
              {!isConnected ? "Connect to add" : batchRunning ? "Adding links…" : isAdding ? "Adding…" : "Add downloads"}
            </button>
          </div>
        </form>
        {#if !isConnected}
          <p class="add-feedback" role="note">Offline or connecting — this draft is preserved and editable. Reconnect before submitting.</p>
        {/if}
        {#if batch}
          <div class="batch-summary">
            {#if batch.running}
              <p class="add-feedback" role="status">Adding {Math.min(batch.completed + 1, batch.total)} of {batch.total}…</p>
            {:else if batch.stopReason}
              <p class="add-feedback" role="status">{batch.stopReason}</p>
            {/if}
            {#if batch.results.length > 0}
              <ol class="batch-results" aria-label="Add outcomes">
                {#each batch.results as result}
                  <li class="batch-row is-{result.status}">
                    <span class="batch-status">{BATCH_STATUS_LABELS[result.status]}</span>
                    <span class="batch-url">{result.line}</span>
                    <span class="batch-note">{result.message}</span>
                  </li>
                {/each}
              </ol>
            {/if}
            {#if !batch.running && batchHasAmbiguous}
              <p class="add-feedback">Unconfirmed links stay above and are not retried. Check the daemon queue before resending.</p>
            {/if}
          </div>
        {:else if isAdding}
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
    {#if isConnected}
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
      <section class="add-panel" aria-label="Daemon max simultaneous downloads">
        <form class="add-form" onsubmit={handleMaxConcurrentSave}>
          <div class="field add-field">
            <label class="field-label" for="max-concurrent">Max simultaneous downloads</label>
            <select
              id="max-concurrent"
              class="field-input select-input"
              bind:value={maxConcurrentDraft}
              onchange={() => { maxConcurrentDirty = true; maxConcurrentTouched = true; }}
              disabled={maxConcurrentInputDisabled}
              aria-describedby={maxConcurrentDescribedBy}
              aria-invalid={maxConcurrentInvalid ? "true" : "false"}
            >
              <option value="" disabled>Select 1–10</option>
              {#each maxConcurrentOptions as option}
                <option value={String(option)}>{option}</option>
              {/each}
            </select>
            <p class="field-help" id="max-concurrent-help">Daemon setting (1–10; default {MAX_CONCURRENT_DOWNLOADS_DEFAULT}). Saved changes apply only after the daemon restarts. The app never restarts the daemon automatically; active transfers are unchanged.</p>
          </div>
          <div class="add-actions">
            <button
              type="submit"
              class="btn"
              disabled={!maxConcurrentCanSave}
              aria-busy={maxConcurrentSaving ? "true" : undefined}
            >
              {maxConcurrentSaving ? "Saving…" : "Save setting"}
            </button>
          </div>
        </form>
        {#if maxConcurrentLoading}
          <p class="add-feedback" role="status">Loading daemon setting…</p>
        {:else if maxConcurrentUnconfirmed}
          <p class="conn-error" id="max-concurrent-error" role="alert">Max simultaneous downloads could not be confirmed. This control is disabled until the app restarts to avoid unsafe writes; the daemon may have saved a value. Restart the daemon if you intended the change.</p>
        {:else if maxConcurrentError}
          <p class="conn-error" id="max-concurrent-error" role="alert">{maxConcurrentError}</p>
          <div class="add-actions">
            <button type="button" class="btn" onclick={handleMaxConcurrentRetry} disabled={maxConcurrentLoading}>
              Retry loading setting
            </button>
          </div>
        {:else if maxConcurrentLocalError}
          <p class="conn-error" id="max-concurrent-error" role="alert">{maxConcurrentLocalError}</p>
        {:else if !isManaged}
          <p class="add-feedback" role="note">Read-only on manual connections. Saving requires the bundled daemon.</p>
        {:else if maxConcurrentRestartPending}
          <p class="add-feedback" role="status">Saved {maxConcurrent}. Restart the daemon to apply; active transfers are unchanged.</p>
        {:else if maxConcurrentSavedLabel}
          <p class="add-feedback" role="status">{maxConcurrentSavedLabel}</p>
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
      bulkAction={bulkAction}
      bulkSummary={bulkSummary}
      onBulkControl={handleBulkControl}
      bulkUncertainties={bulkUncertainties}
      bulkUncertaintyOverflow={bulkUncertaintyOverflow}
      onDismissBulkUncertainty={handleDismissBulkUncertainty}
      onDismissBulkUncertaintyOverflow={handleDismissBulkUncertaintyOverflow}
      hashIntentWarnings={hashIntentWarnings}
      hashIntentWarningOverflow={hashIntentWarningOverflow}
      onDismissHashIntentWarning={handleDismissHashIntentWarning}
      onDismissHashIntentWarningOverflow={handleDismissHashIntentWarningOverflow}
      hashes={hashes}
      hashesLoading={hashesLoading}
      hashError={hashError}
      onRefreshHashes={handleHashesRefresh}
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

  .hash-field {
    border-top: 1px solid var(--color-rule);
    padding-top: var(--space-sm);
  }

  .hash-check {
    display: flex;
    align-items: flex-start;
    gap: var(--space-xs);
    min-height: var(--control-h);
    cursor: pointer;
    min-width: 0;
  }

  .hash-check-text {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--color-ink);
    overflow-wrap: anywhere;
    min-width: 0;
  }

  .hash-input {
    width: 1.125rem;
    height: 1.125rem;
    flex: none;
    margin-top: 0.125rem;
    accent-color: var(--color-accent);
  }

  .hash-input:disabled {
    cursor: not-allowed;
    opacity: 0.55;
  }

  .hash-check:has(.hash-input:disabled) {
    cursor: not-allowed;
  }

  .hash-check:has(.hash-input:disabled) .hash-check-text {
    color: var(--color-neutral);
  }

  .url-lines {
    min-height: calc(var(--control-h) * 2);
    padding-block: var(--space-sm);
    font-family: var(--font-mono);
    font-size: var(--text-sm);
    line-height: 1.5;
    resize: vertical;
  }

  .select-input {
    cursor: pointer;
  }

  .select-input:disabled {
    cursor: not-allowed;
  }

  .batch-summary {
    margin-top: var(--space-sm);
    min-width: 0;
  }

  .batch-results {
    list-style: none;
    margin: var(--space-xs) 0 0;
    padding: 0;
    border-top: 1px solid var(--color-rule);
    min-width: 0;
  }

  .batch-row {
    display: grid;
    grid-template-columns: minmax(0, max-content) minmax(0, 1fr);
    column-gap: var(--space-sm);
    row-gap: var(--space-2xs);
    padding-block: var(--space-xs);
    border-bottom: 1px solid var(--color-rule);
    min-width: 0;
  }

  .batch-status {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--color-muted);
    white-space: nowrap;
  }

  .batch-url {
    font-family: var(--font-mono);
    font-size: var(--text-sm);
    color: var(--color-ink);
    overflow-wrap: anywhere;
    min-width: 0;
  }

  .batch-note {
    grid-column: 1 / -1;
    font-size: var(--text-sm);
    color: var(--color-muted);
    overflow-wrap: anywhere;
    min-width: 0;
  }

  .batch-row.is-queued .batch-status {
    color: var(--color-ok);
  }

  .batch-row.is-invalid .batch-status,
  .batch-row.is-failed .batch-status,
  .batch-row.is-ambiguous .batch-status {
    color: var(--color-error);
  }

  @media (max-width: 40rem) {
    .batch-row {
      grid-template-columns: minmax(0, 1fr);
    }
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
