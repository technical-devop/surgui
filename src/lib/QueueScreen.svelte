<script lang="ts">
  import { filterQueue, formatEta, formatSizePair, formatSpeed, groupByStatus, progressRatio } from "./queue/queue";
  import type { BulkAction, BulkSummary, BulkUncertainty, QueueItem, StatusFilter } from "./queue/types";
  import { STATUS_ORDER } from "./queue/types";
  import { untrack } from "svelte";
  import logoUrl from "../assets/surge-logo.webp";

  let {
    items,
    mode,
    controllingId = null,
    onControl = undefined,
    isManaged = false,
    openingId = null,
    locationError = null,
    locationErrorId = null,
    onOpenLocation = undefined,
    onClearLocationError = undefined,
    bulkAction = null,
    bulkSummary = null,
    onBulkControl = undefined,
    bulkUncertainties = [],
    bulkUncertaintyOverflow = 0,
    onDismissBulkUncertainty = undefined,
    onDismissBulkUncertaintyOverflow = undefined,
  }: {
    items: QueueItem[];
    mode: "offline" | "sample" | "native";
    controllingId?: string | null;
    onControl?: (action: "pause" | "resume" | "removeRecord", id: string) => void;
    isManaged?: boolean;
    openingId?: string | null;
    locationError?: string | null;
    locationErrorId?: string | null;
    onOpenLocation?: (id: string) => void;
    onClearLocationError?: () => void;
    bulkAction?: BulkAction | null;
    bulkSummary?: BulkSummary | null;
    onBulkControl?: (action: BulkAction) => void;
    bulkUncertainties?: BulkUncertainty[];
    bulkUncertaintyOverflow?: number;
    onDismissBulkUncertainty?: (id: number) => void;
    onDismissBulkUncertaintyOverflow?: () => void;
  } = $props();

  let rows = $state<QueueItem[]>(items.map((i) => ({ ...i })));
  let query = $state("");
  let statusFilter = $state<StatusFilter>("all");
  let selectedId = $state<string | null>(rows.length > 0 ? rows[0].id : null);
  let pendingRemoveId = $state<string | null>(null);

  // Sync from items only: track `items`, write rows/selection untracked so
  // local row actions (pause/remove/select) never retrigger this effect.
  $effect(() => {
    const next = items.map((i) => ({ ...i }));
    untrack(() => {
      rows = next;
      const current = selectedId;
      if (next.length > 0 && (current === null || !next.some((r) => r.id === current))) {
        selectedId = next[0].id;
      }
      if (next.length === 0) selectedId = null;
    });
  });

  // Pending two-click removal is selection- and connection-scoped: it survives
  // same-selected-row refresh/poll, and backs out only on actual selection
  // change, removal of the pending row, leaving native mode, or explicit Cancel.
  $effect(() => {
    const _sel = selectedId;
    const _mode = mode;
    const _idsKey = items.map((i) => i.id).join("\0");
    untrack(() => {
      const pending = pendingRemoveId;
      if (pending === null) return;
      if (_mode !== "native") {
        pendingRemoveId = null;
        return;
      }
      if (_sel !== pending) {
        pendingRemoveId = null;
        return;
      }
      const ids = _idsKey === "" ? [] : _idsKey.split("\0");
      if (!ids.includes(pending)) {
        pendingRemoveId = null;
      }
    });
  });

  // A location error belongs to its origin id; it renders only when the
  // current selection equals that origin, so a stale message never reads as
  // the new row's state. Moving away from the origin clears the stale error;
  // returning to the origin preserves it for display. No paths ever enter.
  $effect(() => {
    const _sel = selectedId;
    untrack(() => {
      if (_sel === undefined || _sel === null) return;
      const origin = locationErrorId ?? null;
      const msg = locationError ?? null;
      if (msg === null || origin === null) {
        // Generic (non-row) errors clear on any move to avoid stale text.
        if (msg !== null) onClearLocationError?.();
        return;
      }
      if (origin !== _sel) onClearLocationError?.();
    });
  });

  const groups = $derived(groupByStatus(rows));
  const filtered = $derived(filterQueue(rows, query, statusFilter));
  const selected = $derived(rows.find((r) => r.id === selectedId) ?? null);
  const activeSpeed = $derived(rows.filter((r) => r.status === "active").reduce((n, r) => n + r.speedBps, 0));
  const bulkBusy = $derived(bulkAction !== null);
  // One shared exclusion for row and bulk actions: exactly one action from
  // either surface may be in flight at a time.
  const controlsBusy = $derived((controllingId !== null && controllingId !== undefined) || bulkBusy);

  function setStatus(id: string, status: QueueItem["status"]) {
    rows = rows.map((r) => (r.id === id ? { ...r, status, speedBps: status === "active" ? r.speedBps : 0 } : r));
  }

  function removeRow(id: string) {
    rows = rows.filter((r) => r.id !== id);
    if (selectedId === id) selectedId = rows.length > 0 ? rows[0].id : null;
  }

  function actionsFor(item: QueueItem): { label: string; run: () => void }[] {
    // Sample preview keeps its local demo actions (offline/native show none
    // here — native Pause/Resume/Remove route via onControl in detail pane).
    // Live native rows are never mutated locally.
    if (mode !== "sample") return [];
    switch (item.status) {
      case "active":
        return [{ label: "Pause", run: () => setStatus(item.id, "paused") }];
      case "paused":
        return [{ label: "Resume", run: () => setStatus(item.id, "active") }];
      case "pausing":
      case "unknown":
        return [];
      case "failed":
        return [{ label: "Retry", run: () => setStatus(item.id, "queued") }];
      case "queued":
        return [{ label: "Cancel", run: () => removeRow(item.id) }];
      case "completed":
        return [{ label: "Clear", run: () => removeRow(item.id) }];
    }
  }

  const filterOptions: StatusFilter[] = ["all", ...STATUS_ORDER];
  const filterLabel: Record<StatusFilter, string> = {
    all: "All",
    active: "Active",
    queued: "Queued",
    pausing: "Pausing",
    paused: "Paused",
    failed: "Failed",
    completed: "Completed",
    unknown: "Unknown",
  };

  function countFor(f: StatusFilter): number {
    return f === "all" ? rows.length : groups[f].length;
  }
</script>

<div class="workbench">
  {#if mode === "sample"}
    <header class="topbar">
      <span class="starfield" aria-hidden="true"></span>
      <div class="brand">
        <img class="brand-logo" src={logoUrl} alt="" width="1376" height="768" decoding="async" />
        <span class="brand-divider" aria-hidden="true"></span>
        <span class="brand-text">
          <span class="wordmark">Surge Desktop</span>
          <span class="section-name">Queue</span>
        </span>
      </div>
      <span class="conn conn-sample" role="status" aria-label="Connection: sample data preview">
        <span class="conn-dot" aria-hidden="true"></span>Sample
      </span>
    </header>
  {/if}

  {#if mode === "sample"}
    <p class="notice notice-sample" role="note">
      <strong>Sample data — browser preview only.</strong> No daemon connection, no Surge token stored or sent.
    </p>
  {:else if mode === "native"}
    <p class="notice notice-live" role="note">
      <strong>Live daemon queue.</strong> Pause, Resume, and Remove record are available on the selected download. Daemon state can settle asynchronously after an action.
    </p>
  {:else}
    <p class="notice" role="note">
      <strong>Offline — daemon not connected.</strong> The queue starts empty. Start the bundled daemon or connect to another daemon to load downloads.
    </p>
  {/if}

  <section class="toolbar" aria-label="Queue controls">
    <div class="search-wrap">
      <label class="search-label" for="queue-search">Search downloads</label>
      <input
        id="queue-search"
        class="search"
        type="search"
        autocomplete="off"
        placeholder="Filter by file name…"
        bind:value={query}
      />
    </div>
    <div class="filters" role="group" aria-label="Filter by status">
      {#each filterOptions as f (f)}
        <button
          type="button"
          class="chip"
          aria-pressed={statusFilter === f}
          aria-label={`Show ${f} downloads, ${countFor(f)} items`}
          onclick={() => { statusFilter = f; }}
        >
          {filterLabel[f]} · {countFor(f)}
        </button>
      {/each}
    </div>
  </section>

  {#if mode === "native"}
    <section class="bulk-bar" aria-label="Bulk queue actions">
      <div class="bulk-actions" role="group" aria-label="Apply to the whole daemon queue">
        <button
          type="button"
          class="btn"
          disabled={controlsBusy || onBulkControl === undefined}
          aria-busy={bulkAction === "pauseAll" ? "true" : undefined}
          aria-label="Pause all downloads on the daemon"
          aria-describedby="bulk-actions-help"
          onclick={() => {
            if (controlsBusy || onBulkControl === undefined) return;
            onBulkControl("pauseAll");
          }}
        >
          {bulkAction === "pauseAll" ? "Pausing all…" : "Pause all"}
        </button>
        <button
          type="button"
          class="btn"
          disabled={controlsBusy || onBulkControl === undefined}
          aria-busy={bulkAction === "resumeAll" ? "true" : undefined}
          aria-label="Resume all paused downloads on the daemon"
          aria-describedby="bulk-actions-help"
          onclick={() => {
            if (controlsBusy || onBulkControl === undefined) return;
            onBulkControl("resumeAll");
          }}
        >
          {bulkAction === "resumeAll" ? "Resuming all…" : "Resume all"}
        </button>
      </div>
      <p class="bulk-hint" id="bulk-actions-help">
        Applies to every download on the daemon, not just the filtered rows. Pause targets
        everything except completed, paused, and error rows (queued and unknown included); Resume
        targets every paused row, including individually paused ones. Surge acknowledges the
        request; row changes are applied asynchronously and the queue keeps refreshing
        periodically.
      </p>
      {#if bulkSummary}
        <p class="bulk-feedback" role="status" aria-live="polite">{bulkSummary.message}</p>
        {#if bulkSummary.status === "done" && (bulkSummary.failed ?? 0) > 0}
          <details class="bulk-details">
            <summary>Show failed items ({bulkSummary.failed})</summary>
            <ul class="bulk-failures">
              {#each bulkSummary.failures.slice(0, 20) as failure (failure.id)}
                <li>
                  <span class="bulk-failure-id">{rows.find((r) => r.id === failure.id)?.name ?? "Unknown download"}</span>
                  <span class="bulk-failure-note">{failure.error ?? "Daemon reported an error."}</span>
                </li>
              {/each}
            </ul>
            {#if bulkSummary.failures.length > 20}
              <p class="bulk-hint">Showing 20 of {bulkSummary.failures.length} failed items.</p>
            {/if}
          </details>
        {/if}
      {/if}
    </section>
  {/if}

  {#if bulkUncertainties.length > 0 || bulkUncertaintyOverflow > 0}
    <section class="bulk-uncertainty-region" aria-label="Unresolved bulk actions">
      {#each bulkUncertainties as notice (notice.id)}
        <div class="bulk-uncertain">
          <p class="bulk-uncertain-text" role="status" aria-live="polite">{notice.message}</p>
          {#if onDismissBulkUncertainty}
            <button
              type="button"
              class="btn"
              aria-label={`Dismiss ${notice.action === "pauseAll" ? "Pause all" : "Resume all"} warning`}
              onclick={() => onDismissBulkUncertainty(notice.id)}
            >
              Dismiss
            </button>
          {/if}
        </div>
      {/each}
      {#if bulkUncertaintyOverflow > 0}
        <div class="bulk-uncertain-overflow-row">
          <p class="bulk-uncertain-overflow" role="status">
            {bulkUncertaintyOverflow} additional unresolved bulk {bulkUncertaintyOverflow === 1 ? "action is" : "actions are"}
            not listed here. They may still apply; none were retried automatically.
          </p>
          {#if onDismissBulkUncertaintyOverflow}
            <button
              type="button"
              class="btn"
              aria-label="Dismiss overflowed bulk warnings"
              onclick={() => onDismissBulkUncertaintyOverflow()}
            >
              Dismiss
            </button>
          {/if}
        </div>
      {/if}
    </section>
  {/if}

  <div class="split">
    <section class="pane list-pane" aria-label="Downloads">
      <span class="pane-ticks" aria-hidden="true"></span>
      {#if filtered.length === 0}
        <div class="empty" role="status">
          {#if rows.length === 0}
            <p class="empty-title">Queue is empty.</p>
            <p class="empty-body">
              {#if mode === "offline"}
                Nothing is tracked yet. Connect the daemon to fill this list.
              {:else if mode === "native"}
                Connected — no downloads are tracked right now.
              {:else}
                All sample rows were cleared. Reload the preview to restore them.
              {/if}
            </p>
          {:else}
            <p class="empty-title">No downloads match.</p>
            <p class="empty-body">No {statusFilter === "all" ? "" : statusFilter + " "}downloads named “{query.trim()}”. Try a shorter search.</p>
            <button
              type="button"
              class="btn"
              onclick={() => { query = ""; statusFilter = "all"; }}
            >Clear search</button>
          {/if}
        </div>
      {:else}
        <ul class="rows">
          {#each filtered as item (item.id)}
            {@const ratio = progressRatio(item)}
            {@const pct = Math.round(ratio * 100)}
            <li class="row" data-status={item.status} data-selected={selectedId === item.id}>
              <button
                type="button"
                class="row-main"
                aria-label={`Inspect ${item.name}, status ${item.status}, ${pct} percent`}
                aria-current={selectedId === item.id ? "true" : undefined}
                onclick={() => { selectedId = item.id; }}
              >
                <span class="row-top">
                  <span class="status" data-status={item.status}>
                    <span class="status-dot" aria-hidden="true"></span>{item.status}
                  </span>
                  <span class="row-name">{item.name}</span>
                </span>
                <span
                  class="progress"
                  role="progressbar"
                  aria-label={`${item.name} progress`}
                  aria-valuemin={0}
                  aria-valuemax={100}
                  aria-valuenow={pct}
                >
                  <span class="progress-fill" data-status={item.status} style={`width: ${ratio * 100}%`}></span>
                </span>
                <span class="row-meta">
                  <span>{formatSpeed(item.speedBps)}</span>
                  <span aria-hidden="true">·</span>
                  <span>{formatEta(item)}</span>
                  <span aria-hidden="true">·</span>
                  <span>{pct}%</span>
                </span>
              </button>
              {#if mode === "sample"}
                {@const rowActions = actionsFor(item)}
                {#if rowActions.length > 0}
                  <span class="row-actions">
                    {#each rowActions as a (a.label)}
                      <button type="button" class="btn btn-row" aria-label={`${a.label} ${item.name}`} onclick={a.run}>
                        {a.label}
                      </button>
                    {/each}
                  </span>
                {/if}
              {/if}
            </li>
          {/each}
        </ul>
      {/if}
    </section>

    <section class="pane detail-pane" aria-label="Download details" aria-live="polite">
      <span class="lz-frame" aria-hidden="true">
        <svg class="lz-frame__svg" viewBox="0 0 1000 1000" preserveAspectRatio="none" focusable="false" aria-hidden="true">
          <path
            class="lz-frame__rail lz-frame__rail--outer"
            d="M 40 8 L 960 8 L 960 32 L 984 32 L 984 968 L 960 968 L 960 992 L 40 992 L 40 968 L 16 968 L 16 32 L 40 32 Z"
          />
          <path
            class="lz-frame__rail lz-frame__rail--inner"
            d="M 54 20 L 946 20 L 946 44 L 970 44 L 970 956 L 946 956 L 946 980 L 54 980 L 54 956 L 30 956 L 30 44 L 54 44 Z"
          />
        </svg>
        <span class="lz-checker-foot lz-checker-foot--l"></span>
        <span class="lz-checker-foot lz-checker-foot--r"></span>
      </span>
      {#if selected === null}
        <div class="empty">
          <p class="empty-title">Nothing selected.</p>
          <p class="empty-body">Select a download to inspect progress, speed and remaining time.</p>
        </div>
      {:else}
        {@const sel = selected}
        {@const ratio = progressRatio(sel)}
        {@const pct = Math.round(ratio * 100)}
        <h2 class="detail-name">{sel.name}</h2>
        <p class="detail-status">
          <span class="status" data-status={sel.status}>
            <span class="status-dot" aria-hidden="true"></span>{sel.status}
          </span>
        </p>
        <span
          class="progress progress-large"
          role="progressbar"
          aria-label={`${sel.name} progress`}
          aria-valuemin={0}
          aria-valuemax={100}
          aria-valuenow={pct}
        >
          <span class="progress-fill" data-status={sel.status} style={`width: ${ratio * 100}%`}></span>
        </span>
        <dl class="facts">
          <div><dt>Progress</dt><dd>{pct}%</dd></div>
          <div><dt>Speed</dt><dd>{formatSpeed(sel.speedBps)}</dd></div>
          <div><dt>Remaining</dt><dd>{formatEta(sel)}</dd></div>
          <div><dt>Size</dt><dd>{formatSizePair(sel)}</dd></div>
          {#if sel.error}
            <div class="fact-error"><dt>Error</dt><dd>{sel.error}</dd></div>
          {/if}
        </dl>
        <div class="detail-actions">
          {#if mode === "sample"}
            {#each actionsFor(sel) as a (a.label)}
              <button type="button" class="btn" aria-label={`${a.label} ${sel.name}`} onclick={a.run}>
                {a.label}
              </button>
            {/each}
          {:else if mode === "native"}
            {@const nativePrimary = sel.status === "active" ? "pause" : sel.status === "paused" ? "resume" : null}
            {@const isBusy = controlsBusy}
            {@const isSelfBusy = controllingId === sel.id}
            {@const isOpening = openingId !== null && openingId !== undefined}
            {@const isSelfOpening = openingId === sel.id}
            {@const locationBlocked = !isManaged || isBusy || isOpening}
            {#if nativePrimary !== null}
              <button
                type="button"
                class="btn"
                disabled={isBusy}
                aria-busy={isSelfBusy ? "true" : undefined}
                aria-label={`${nativePrimary === "pause" ? "Pause" : "Resume"} ${sel.name}`}
                onclick={() => {
                  if (isBusy || onControl === undefined) return;
                  pendingRemoveId = null;
                  onControl(nativePrimary, sel.id);
                }}
              >
                {isSelfBusy ? (nativePrimary === "pause" ? "Pausing…" : "Resuming…") : nativePrimary === "pause" ? "Pause" : "Resume"}
              </button>
            {/if}
            {#if sel.status === "completed"}
              <button
                type="button"
                class="btn"
                disabled={locationBlocked}
                aria-busy={isSelfOpening ? "true" : undefined}
                aria-label={`Open download location for ${sel.name}`}
                onclick={() => {
                  if (locationBlocked || onOpenLocation === undefined) return;
                  onOpenLocation(sel.id);
                }}
              >
                {isSelfOpening ? "Opening…" : "Open download location"}
              </button>
              {#if !isManaged}
                <p class="control-pending" role="note">Open location needs the bundled daemon connection.</p>
              {:else if isOpening && !isSelfOpening}
                <p class="control-pending" role="status">Opening another download… try again shortly.</p>
              {/if}
              {#if locationError && (locationErrorId == null || locationErrorId === sel.id)}
                <p class="control-pending" role="alert">{locationError}</p>
              {/if}
            {/if}
            {#if pendingRemoveId === sel.id}
              <div class="remove-confirm" role="group" aria-label={`Confirm removal of ${sel.name}`}>
                <p class="remove-explain">Removing stops the active transfer. A completed final file remains. Partial bytes may change.</p>
                <div class="remove-confirm-actions">
                  <button
                    type="button"
                    id={`native-remove-${sel.id}-confirm`}
                    class="btn btn-danger"
                    disabled={isBusy}
                    aria-busy={isSelfBusy ? "true" : undefined}
                    onclick={() => {
                      if (isBusy || onControl === undefined) return;
                      pendingRemoveId = null;
                      onControl("removeRecord", sel.id);
                    }}
                  >
                    {isSelfBusy ? "Removing…" : "Confirm remove"}
                  </button>
                  <button
                    type="button"
                    class="btn"
                    disabled={isBusy}
                    onclick={() => {
                      pendingRemoveId = null;
                      queueMicrotask(() => document.getElementById(`native-remove-${sel.id}`)?.focus());
                    }}
                  >
                    Cancel
                  </button>
                </div>
              </div>
            {:else}
              <button
                type="button"
                id={`native-remove-${sel.id}`}
                class="btn"
                disabled={isBusy}
                aria-expanded="false"
                aria-label={`Remove ${sel.name} from queue`}
                onclick={() => {
                  if (isBusy) return;
                  pendingRemoveId = sel.id;
                  queueMicrotask(() => document.getElementById(`native-remove-${sel.id}-confirm`)?.focus());
                }}
              >
                Remove from queue
              </button>
            {/if}
            {#if isBusy}
              <p class="control-pending" role="status">Sending to daemon… state settles asynchronously.</p>
            {/if}
          {/if}
        </div>
      {/if}
    </section>
  </div>

  <footer class="statusbar" aria-live="polite">
    <span>{rows.length} tracked</span>
    <span aria-hidden="true">·</span>
    <span>{groups.active.length} active</span>
    <span aria-hidden="true">·</span>
    <span>{formatSpeed(activeSpeed)} overall</span>
  </footer>
</div>

<style>
  /* Queue screen: near-black field with stepped twin rails, checker feet,
   * Times serif copy and mono telemetry. Visual reference: Lunarz.
   * Type: Times New Roman serif (headings/copy) + system mono (telemetry only). Dark-only.
   * One twin-framed region (detail pane); list pane carries corner-bracket ticks only, per
   *   design.md Stepped twin frame (at most one twin-framed region visible at a time).
   * Logo: full source mark, aria-decorative; screen blend drops its black field onto paper.
   * Contrast: focus ring uses --color-focus at high lightness against every
   *   surface; status labels read as text plus a dot glyph, never colour alone.
   * Motion-cut: no transitions — state changes and progress percentage updates apply
   *   instantly (progress widths snap, no width animation). Reduced motion honoured:
   *   durations clamped to 150ms and the :active nudge is disabled under the media query.
   */
  .workbench {
    /* Local aliases — every value traces to a tokens.css token (no inline colour). */
    --lz-line: var(--color-ink-2);
    --lz-line-dim: var(--color-rule-2);
    --lz-panel: var(--color-paper);
    --lz-panel-2: var(--color-paper-2);
    --lz-checker: color-mix(in oklab, var(--color-ink) 82%, transparent);
    --lz-checker-soft: color-mix(in oklab, var(--color-ink) 44%, transparent);
    color-scheme: dark;
    display: flex;
    flex-direction: column;
    gap: var(--space-sm);
    max-width: 72rem;
    margin-inline: auto;
    padding: var(--space-md);
    min-width: 0;
  }

  /* ---- Brand header ---- */
  .topbar {
    position: relative;
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: var(--space-xs) var(--space-md);
    border-bottom: 1px solid var(--color-rule);
    padding-bottom: var(--space-sm);
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
      radial-gradient(circle at 13% 62%, var(--lz-checker) 0 1px, transparent 1.4px),
      radial-gradient(circle at 71% 28%, var(--lz-checker-soft) 0 1px, transparent 1.4px),
      radial-gradient(circle at 88% 70%, var(--lz-checker-soft) 0 1px, transparent 1.4px),
      radial-gradient(circle at 52% 18%, var(--lz-checker-soft) 0 1px, transparent 1.4px);
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
    /* Full source mark on near-black paper: screen blend lifts the mark and
       dissolves the image's own black field. No recolour, glow, or redraw. */
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
    font-family: var(--font-ui);
    font-weight: 700;
    font-size: var(--text-md);
    font-style: normal;
    letter-spacing: -0.01em;
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

  .conn {
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

  .conn-dot {
    width: 0.5rem;
    height: 0.5rem;
    background: var(--color-neutral);
    border-radius: 1px;
    flex: none;
  }

  .conn-sample .conn-dot {
    background: var(--color-accent);
  }

  /* ---- Notices ---- */
  .notice {
    margin: 0;
    font-size: var(--text-sm);
    line-height: 1.5;
    color: var(--color-muted);
    border: 1px solid var(--color-rule);
    border-radius: 0;
    background: var(--color-paper-2);
    padding: var(--space-sm) var(--space-md);
    overflow-wrap: anywhere;
    min-width: 0;
  }

  .notice strong {
    color: var(--color-ink);
  }

  .notice-sample {
    border-color: var(--color-rule-2);
    background: var(--color-paper-2);
    color: var(--color-ink);
  }

  .notice-live {
    border-color: var(--color-rule-2);
    background: var(--color-paper);
    color: var(--color-ink);
  }

  /* ---- Toolbar ---- */
  .toolbar {
    display: flex;
    flex-direction: column;
    gap: var(--space-sm);
    min-width: 0;
  }

  .search-wrap {
    display: flex;
    flex-direction: column;
    gap: var(--space-2xs);
    min-width: 0;
  }

  .search-label {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--color-neutral);
  }

  .search {
    box-sizing: border-box;
    min-height: var(--control-h);
    font-family: var(--font-ui);
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

  .search::placeholder {
    color: var(--color-neutral);
  }

  @media (hover: hover) {
    .search:hover {
      background: var(--color-paper-2);
    }
  }

  .filters {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-xs);
    min-width: 0;
  }

  .chip {
    min-height: var(--control-h);
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    font-variant-numeric: tabular-nums;
    letter-spacing: 0.04em;
    color: var(--color-muted);
    background: transparent;
    border: 1px solid var(--color-rule-2);
    border-radius: 0;
    padding: 0 var(--space-sm);
    white-space: nowrap;
    cursor: pointer;
  }

  .chip[aria-pressed="true"] {
    color: var(--color-accent-ink);
    background: var(--color-ink);
    border-color: var(--color-ink);
  }

  /* ---- Bulk actions (daemon-wide, not filtered) ---- */
  .bulk-bar {
    display: flex;
    flex-direction: column;
    gap: var(--space-xs);
    border: 1px solid var(--color-rule);
    background: var(--color-paper-2);
    padding: var(--space-sm) var(--space-md);
    min-width: 0;
  }

  .bulk-actions {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-xs);
    min-width: 0;
  }

  .bulk-hint {
    margin: 0;
    font-size: var(--text-sm);
    line-height: 1.5;
    color: var(--color-muted);
    overflow-wrap: anywhere;
    min-width: 0;
  }

  .bulk-uncertainty-region {
    display: flex;
    flex-direction: column;
    gap: var(--space-xs);
    padding: var(--space-sm) var(--space-md);
    min-width: 0;
  }

  .bulk-uncertain-overflow {
    margin: 0;
    font-size: var(--text-sm);
    line-height: 1.5;
    color: var(--color-muted);
    overflow-wrap: anywhere;
    min-width: 0;
  }

  .bulk-uncertain-overflow-row {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: var(--space-xs);
    min-width: 0;
  }

  .bulk-uncertain {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: var(--space-xs);
    border: 1px solid var(--color-error);
    background: var(--color-error-wash);
    padding: var(--space-xs) var(--space-sm);
    min-width: 0;
  }

  .bulk-uncertain-text {
    flex: 1 1 18rem;
    margin: 0;
    font-size: var(--text-sm);
    line-height: 1.5;
    color: var(--color-ink);
    overflow-wrap: anywhere;
    min-width: 0;
  }

  .bulk-feedback {
    margin: 0;
    font-size: var(--text-sm);
    line-height: 1.5;
    color: var(--color-ink);
    overflow-wrap: anywhere;
    min-width: 0;
  }

  .bulk-details {
    min-width: 0;
    font-size: var(--text-sm);
    color: var(--color-muted);
  }

  .bulk-details summary {
    cursor: pointer;
    color: var(--color-ink);
    min-height: var(--control-h);
    display: flex;
    align-items: center;
  }

  .bulk-failures {
    list-style: none;
    margin: var(--space-xs) 0 0;
    padding: 0;
    border-top: 1px solid var(--color-rule);
    min-width: 0;
  }

  .bulk-failures li {
    display: flex;
    flex-direction: column;
    gap: var(--space-2xs);
    padding-block: var(--space-xs);
    border-bottom: 1px solid var(--color-rule);
    min-width: 0;
  }

  .bulk-failure-id {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    color: var(--color-ink);
    overflow-wrap: anywhere;
    min-width: 0;
  }

  .bulk-failure-note {
    font-size: var(--text-sm);
    color: var(--color-muted);
    overflow-wrap: anywhere;
    min-width: 0;
  }

  /* ---- Split ---- */
  .split {
    display: grid;
    grid-template-columns: minmax(0, 1fr);
    gap: var(--space-md);
    align-items: start;
    min-width: 0;
  }

  .pane {
    position: relative;
    background: var(--color-paper);
    border-radius: 0;
    min-width: 0;
  }

  .list-pane {
    padding: var(--space-sm);
    border: 1px solid var(--color-rule);
  }

  /* Corner-bracket ticks — echo the stepped-rail family without a second twin frame. */
  .pane-ticks {
    position: absolute;
    inset: 5px;
    pointer-events: none;
    z-index: 0;
  }

  .pane-ticks::before,
  .pane-ticks::after {
    content: "";
    position: absolute;
    width: 15px;
    height: 15px;
  }

  .pane-ticks::before {
    top: 0;
    right: 0;
    border-top: 1px solid var(--lz-line-dim);
    border-right: 1px solid var(--lz-line-dim);
  }

  .pane-ticks::after {
    bottom: 0;
    left: 0;
    border-bottom: 1px solid var(--lz-line-dim);
    border-left: 1px solid var(--lz-line-dim);
  }

  .detail-pane {
    padding: var(--space-md) var(--space-xl);
    border: none;
    overflow: visible;
  }

  .detail-pane > *:not(.lz-frame) {
    position: relative;
    z-index: 1;
  }

  /* Stepped twin-rail decorative frame (the one framed region). */
  .lz-frame {
    position: absolute;
    inset: 0;
    pointer-events: none;
    z-index: 0;
    overflow: visible;
  }

  .lz-frame__svg {
    position: absolute;
    inset: 0;
    width: 100%;
    height: 100%;
    display: block;
    fill: none;
    overflow: visible;
  }

  .lz-frame__rail {
    fill: none;
    vector-effect: non-scaling-stroke;
    stroke-linejoin: miter;
    stroke-linecap: butt;
  }

  .lz-frame__rail--outer {
    stroke: var(--lz-line);
    stroke-width: 1;
  }

  .lz-frame__rail--inner {
    stroke: var(--lz-line-dim);
    stroke-width: 1;
  }

  /* Checker foot detail: two bands capping the framed region's base. */
  .lz-checker-foot {
    position: absolute;
    bottom: 2px;
    width: 30%;
    max-width: 200px;
    height: 9px;
    pointer-events: none;
    z-index: 0;
    background-color: var(--color-rule-2);
    background-image:
      linear-gradient(
        to bottom,
        color-mix(in oklab, var(--color-ink) 32%, transparent),
        transparent 42%,
        color-mix(in oklab, var(--color-paper) 62%, transparent)
      ),
      repeating-conic-gradient(var(--lz-checker) 0% 25%, transparent 0% 50%),
      repeating-conic-gradient(var(--lz-checker-soft) 0% 25%, transparent 0% 50%);
    background-size: 100% 100%, 7px 7px, 3px 3px;
    background-repeat: no-repeat, repeat, repeat;
    background-position: 0 0, 0 0, 1px 1px;
    opacity: 0.85;
  }

  .lz-checker-foot--l {
    left: 16%;
  }

  .lz-checker-foot--r {
    right: 16%;
  }

  /* ---- Rows ---- */
  .rows {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: var(--space-2xs);
  }

  .row {
    display: flex;
    flex-direction: column;
    gap: var(--space-2xs);
    border: 1px solid transparent;
    border-radius: 0;
    padding: var(--space-xs) var(--space-sm);
    min-width: 0;
  }

  .row[data-selected="true"] {
    border-color: var(--color-accent);
    background: var(--color-accent-wash);
  }

  .row-main {
    display: flex;
    flex-direction: column;
    gap: var(--space-xs);
    background: none;
    border: 0;
    padding: var(--space-2xs);
    text-align: start;
    font: inherit;
    color: var(--color-ink);
    cursor: pointer;
    min-width: 0;
  }

  .row-top {
    display: flex;
    align-items: baseline;
    gap: var(--space-sm);
    min-width: 0;
  }

  .row-name {
    font-family: var(--font-ui);
    font-weight: 600;
    font-size: var(--text-base);
    letter-spacing: -0.005em;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
  }

  .status {
    display: inline-flex;
    align-items: center;
    gap: var(--space-2xs);
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--color-neutral);
    white-space: nowrap;
    flex: none;
  }

  /* Shape carries the class, the text label carries the truth — never colour alone. */
  .status-dot {
    width: 0.5rem;
    height: 0.5rem;
    background: var(--color-rule-2);
    border-radius: 50%;
    flex: none;
  }

  .status[data-status="active"] {
    color: var(--color-accent);
  }
  .status[data-status="active"] .status-dot {
    background: var(--color-accent);
  }

  .status[data-status="failed"] {
    color: var(--color-error);
  }
  .status[data-status="failed"] .status-dot {
    background: var(--color-error);
    border-radius: 1px;
    transform: rotate(45deg);
  }

  /* Pausing is transitional: accent-tinted but text stays "pausing" — never "paused". */
  .status[data-status="pausing"] {
    color: var(--color-accent);
  }
  .status[data-status="pausing"] .status-dot {
    background: transparent;
    border: 1px solid var(--color-accent);
  }

  /* Paused gets a squared marker so it reads distinct from active/queued at a glance. */
  .status[data-status="paused"] .status-dot {
    border-radius: 1px;
  }

  /* Unknown keeps neutral ink with a hollow marker so it reads as unclassified. */
  .status[data-status="unknown"] {
    color: var(--color-neutral);
  }
  .status[data-status="unknown"] .status-dot {
    background: transparent;
    border: 1px solid var(--color-rule-2);
  }

  .status[data-status="completed"] {
    color: var(--color-ok);
  }
  .status[data-status="completed"] .status-dot {
    background: var(--color-ok);
  }

  .progress {
    display: block;
    height: 0.375rem;
    border-radius: 0;
    background-color: var(--color-paper-3);
    background-image: repeating-linear-gradient(
      to right,
      transparent 0 6px,
      color-mix(in oklab, var(--color-ink) 7%, transparent) 6px 7px
    );
    overflow: hidden;
  }

  .progress-large {
    height: 0.625rem;
    margin-top: var(--space-sm);
  }

  .progress-fill {
    display: block;
    height: 100%;
    background: var(--color-neutral);
  }

  .progress-fill[data-status="active"] {
    background: var(--color-accent);
  }
  .progress-fill[data-status="failed"] {
    background: var(--color-error);
  }
  .progress-fill[data-status="completed"] {
    background: var(--color-ok);
  }

  .row-meta {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-xs);
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    font-variant-numeric: tabular-nums;
    color: var(--color-neutral);
  }

  .row-actions {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-xs);
    padding-inline: var(--space-2xs);
  }

  /* ---- Buttons ---- */
  .btn {
    min-height: var(--control-h);
    display: inline-flex;
    align-items: center;
    justify-content: center;
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

  .btn-row {
    min-height: var(--control-h);
  }

  @media (hover: hover) {
    .btn:hover:not(:disabled),
    .chip:hover {
      border-color: var(--color-ink-2);
    }
    .chip[aria-pressed="true"]:hover {
      border-color: var(--color-ink);
    }
  }

  .btn:active:not(:disabled),
  .chip:active {
    transform: translateY(1px);
  }

  :focus {
    outline: none;
  }
  :focus-visible {
    outline: 2px solid var(--color-focus);
    outline-offset: 2px;
  }

  /* ---- Empty ---- */
  .empty {
    padding: var(--space-lg) var(--space-md);
    text-align: start;
    min-width: 0;
  }

  .empty-title {
    margin: 0 0 var(--space-2xs);
    font-family: var(--font-ui);
    font-weight: 700;
    font-style: normal;
    color: var(--color-ink);
  }

  .empty-body {
    margin: 0 0 var(--space-md);
    font-size: var(--text-sm);
    line-height: 1.55;
    color: var(--color-muted);
    overflow-wrap: anywhere;
  }

  /* ---- Detail ---- */
  .detail-name {
    margin: 0;
    font-family: var(--font-ui);
    font-weight: 700;
    font-style: normal;
    font-size: var(--text-md);
    letter-spacing: -0.01em;
    overflow-wrap: anywhere;
    min-width: 0;
  }

  .detail-status {
    margin: var(--space-xs) 0 0;
  }

  .facts {
    margin: var(--space-md) 0 0;
    display: grid;
    gap: 0;
    min-width: 0;
  }

  .facts > div {
    display: grid;
    grid-template-columns: minmax(5rem, 6.5rem) minmax(0, 1fr);
    gap: var(--space-sm);
    border-top: 1px solid var(--color-rule);
    padding: var(--space-xs) 0;
    font-size: var(--text-sm);
    min-width: 0;
  }

  .facts dt {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--color-neutral);
    margin: 0;
  }

  .facts dd {
    margin: 0;
    color: var(--color-ink);
    font-family: var(--font-mono);
    font-variant-numeric: tabular-nums;
    overflow-wrap: anywhere;
    min-width: 0;
  }

  .fact-error dt,
  .fact-error dd {
    color: var(--color-error);
  }

  .fact-error dt::before {
    content: "";
    display: inline-block;
    width: 0.45rem;
    height: 0.45rem;
    margin-right: var(--space-2xs);
    background: var(--color-error);
    transform: rotate(45deg) translateY(-1px);
    vertical-align: middle;
  }

  .detail-actions {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-xs);
    margin-top: var(--space-md);
    min-width: 0;
  }

  .btn:disabled {
    opacity: 0.55;
    cursor: not-allowed;
  }

  .btn[aria-busy="true"] {
    cursor: progress;
  }

  .btn-danger {
    border-color: var(--color-error);
    color: var(--color-error);
    background: var(--color-error-wash);
  }

  .remove-confirm {
    flex: 1 1 100%;
    min-width: 0;
    border: 1px solid var(--color-rule-2);
    border-radius: 0;
    background: var(--color-paper-2);
    padding: var(--space-sm) var(--space-md);
  }

  .remove-explain {
    margin: 0 0 var(--space-sm);
    font-size: var(--text-sm);
    line-height: 1.5;
    color: var(--color-ink);
    overflow-wrap: anywhere;
    min-width: 0;
  }

  .remove-confirm-actions {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-xs);
    min-width: 0;
  }

  .control-pending {
    flex: 1 1 100%;
    margin: var(--space-2xs) 0 0;
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    letter-spacing: 0.04em;
    color: var(--color-neutral);
    overflow-wrap: anywhere;
    min-width: 0;
  }

  /* ---- Status bar (dense mono telemetry) ---- */
  .statusbar {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-xs);
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    font-variant-numeric: tabular-nums;
    letter-spacing: 0.04em;
    color: var(--color-neutral);
    border-top: 1px solid var(--color-rule);
    padding-top: var(--space-sm);
  }

  /* ---- Responsive ---- */
  @media (min-width: 40rem) {
    .toolbar {
      flex-direction: row;
      align-items: end;
      justify-content: space-between;
    }
    .search-wrap {
      flex: 0 1 22rem;
    }
    .brand-divider {
      min-height: 2rem;
    }
    .bulk-bar {
      flex-direction: row;
      flex-wrap: wrap;
      align-items: center;
    }
    .bulk-hint,
    .bulk-feedback,
    .bulk-details,
    .bulk-uncertain {
      flex: 1 1 100%;
    }
  }

  @media (min-width: 60rem) {
    .split {
      grid-template-columns: minmax(0, 25rem) minmax(0, 1fr);
    }
  }

  @media (max-width: 30rem) {
    .brand-logo {
      height: 2.75rem;
    }
    .detail-pane {
      padding-inline: var(--space-lg);
    }
    .facts > div {
      grid-template-columns: minmax(4.5rem, 5.5rem) minmax(0, 1fr);
    }
  }

  @media (prefers-reduced-motion: reduce) {
    *,
    *::before,
    *::after {
      animation-duration: 150ms !important;
      transition-duration: 150ms !important;
    }
    .btn:active:not(:disabled),
    .chip:active {
      transform: none;
    }
  }
</style>
