import type {
  BulkAction,
  BulkItemFailure,
  BulkSummary,
  BulkUncertainty,
  DownloadStatus,
  HashIntentWarning,
  QueueItem,
} from "./types.ts";
import {
  batchStatusMessage,
  isValidMaxConcurrentValue,
  planUrlLines,
  redactUrlForDisplay,
  type BatchLinkStatus,
  type BatchPlanEntry,
  type UrlLineError,
} from "./queue.ts";
import {
  applyProgressUpdates,
  mergeSnapshotWithProgress,
  sanitizeLifecycleNotice,
  sanitizeProgressBatch,
  sanitizeSnapshotPayload,
  sanitizeStreamState,
} from "./progress.ts";
import { sanitizeDownloadHashes, type DownloadHash } from "./hashes.ts";

export type NativePhase = "offline" | "connecting" | "connected" | "error";

export type AddOutcome = "queued" | "pendingApproval" | "duplicate";

export interface BatchLinkResult {
  index: number;
  /** Redacted display label; the full line stays only in the editable input. */
  line: string;
  status: BatchLinkStatus;
  message: string;
  reason?: UrlLineError;
}

export interface BatchState {
  running: boolean;
  total: number;
  completed: number;
  results: BatchLinkResult[];
  stopped: boolean;
  stopReason: string | null;
}

export interface BatchResult {
  results: BatchLinkResult[];
  /** Invalid, failed, unconfirmed and unattempted lines to keep in the input. */
  recoverable: string[];
}

/**
 * A batch detached by a reconnect/disconnect. Full original lines stay only
 * in this in-memory recovery record (never in emitted state); `results` is the
 * redacted, display-safe outcome list. Links are restored to the editable
 * input by the UI and are never retried automatically.
 */
export interface BatchRecovery {
  id: number;
  /** Redacted per-link outcomes, including the in-flight and unsent tail. */
  results: BatchLinkResult[];
  /** Definitely settled by the daemon (queued, pending approval or duplicate). */
  acceptedCount: number;
  /** Full original in-flight lines whose outcome is unknown. Never display. */
  uncertain: string[];
  /** Full original tail lines that were never attempted. Never display. */
  unsent: string[];
  /** Full original invalid/definitively failed lines. Never display. */
  rejected: string[];
}

/**
 * Display-safe projection of a detached batch. The view keeps only redacted
 * results and counts; the full lines are returned once so the caller can move
 * them into the editable draft and then drop the recovery object.
 */
export interface BatchRecoveryView {
  id: number;
  results: BatchLinkResult[];
  acceptedCount: number;
  uncertainCount: number;
  unsentCount: number;
  rejectedCount: number;
}

export function projectBatchRecovery(recovery: BatchRecovery): {
  view: BatchRecoveryView;
  lines: string[];
} {
  return {
    view: {
      id: recovery.id,
      results: recovery.results.map((result) => ({ ...result })),
      acceptedCount: recovery.acceptedCount,
      uncertainCount: recovery.uncertain.length,
      unsentCount: recovery.unsent.length,
      rejectedCount: recovery.rejected.length,
    },
    lines: [...recovery.uncertain, ...recovery.unsent, ...recovery.rejected],
  };
}

export interface NativeState {
  phase: NativePhase;
  items: QueueItem[];
  error: string | null;
  refreshing: boolean;
  controllingId?: string | null;
  adding?: boolean;
  addOutcome?: AddOutcome | null;
  batch?: BatchState | null;
  /** True only when the active connection is the owned bundled (managed) daemon. */
  managed?: boolean;
  /** Confirmed global limit in bytes/s; 0 = unlimited; null = unknown/not loaded. */
  rateLimitBps?: number | null;
  rateLoading?: boolean;
  rateSaving?: boolean;
  rateError?: string | null;
  /** True after RateUnconfirmed: writer poisoned until the app restarts. */
  rateUnconfirmed?: boolean;
  /** Saved `network.max_concurrent_downloads`; null = unknown/not loaded. */
  maxConcurrent?: number | null;
  maxConcurrentLoading?: boolean;
  maxConcurrentSaving?: boolean;
  maxConcurrentError?: string | null;
  /** True after SettingsUnconfirmed: settings writes disabled until app restart. */
  maxConcurrentUnconfirmed?: boolean;
  /** True only after this controller confirmed a save in this session. */
  maxConcurrentRestartPending?: boolean;
  locationOpeningId?: string | null;
  locationError?: string | null;
  locationErrorId?: string | null;
  /** Bulk action currently in flight; mutually exclusive with `controllingId`. */
  bulkAction?: BulkAction | null;
  /** Display-safe result of the last bulk action on this connection. */
  bulkSummary?: BulkSummary | null;
  /**
   * Persistent warnings for bulk requests whose outcome was never confirmed.
   * Each notice is independent, survives reconnect, and clears only on its own
   * explicit dismissal. The current connection may be the same daemon.
   */
  bulkUncertainties?: BulkUncertainty[];
  /**
   * Count of unresolved notices omitted from the bounded list above. Treated
   * conservatively: they may still have applied and were not retried.
   */
  bulkUncertaintyOverflow?: number;
  /** Near-real-time stream status; polling always continues regardless. */
  streamState?: "off" | "starting" | "live" | "fallback";
  /**
   * Persisted automatic-hash default; null until loaded. False on first run.
   * Also null after an ambiguous save: no read can prove a timed-out write will
   * not commit later, so the value stays unknown for this controller and every
   * controller sharing this module store. An explicit per-batch override
   * remains available.
   */
  hashDefaultEnabled?: boolean | null;
  hashDefaultLoading?: boolean;
  hashDefaultSaving?: boolean;
  hashDefaultError?: string | null;
  /**
   * True after any ambiguous default write. The control is disabled for this
   * controller and every controller sharing this module store across every
   * connection epoch; no reload clears it.
   */
  hashDefaultUnconfirmed?: boolean;
  /**
   * Bounded persistent warnings for accepted queued downloads whose requested
   * hash intent was not confirmed as saved. Each survives reconnect and is
   * dismissed individually; the accepted download is never retried.
   */
  hashIntentWarnings?: HashIntentWarning[];
  /** Count of unresolved hash-intent warnings omitted from the bounded list. */
  hashIntentWarningOverflow?: number;
  /** Sanitized hash rows for the current managed connection; empty on manual. */
  hashes?: DownloadHash[];
  hashesLoading?: boolean;
  hashError?: string | null;
}

export type InvokeFn = (command: string, args?: Record<string, unknown>) => Promise<unknown>;
export type OnChangeFn = (snapshot: NativeState) => void;
/** Minimal Tauri event-listener seam; injection keeps this module testable. */
export type ListenFn = (
  event: string,
  handler: (event: { payload: unknown }) => void,
) => Promise<() => void>;

const PROGRESS_EVENT = "surge://progress";
const LIFECYCLE_EVENT = "surge://lifecycle";
const STREAM_STATE_EVENT = "surge://stream";

/** Bounded reconcile retry delays after a transient snapshot failure. */
const RECONCILE_RETRY_MS = [300, 750, 1500, 3000];
/** Bounded time allowed for event-listener registration before connecting. */
const DEFAULT_LISTENER_TIMEOUT_MS = 2000;
/**
 * Single-timer propagation cadence while an active/completed download still
 * has a pending hash row. This never starts hashing: it only reloads rows
 * through the verified serve path, with bounded backoff. A completion observed
 * in an authoritative queue read may additionally ask the existing worker for
 * one pass.
 */
const DEFAULT_HASH_REFRESH_MS = 2000;
/** Bound automatic propagation so a paused/stalled row cannot poll forever. */
const DEFAULT_HASH_AUTO_MAX_ATTEMPTS = 12;
const MAX_HASH_REFRESH_DELAY_MS = 30_000;
const HASH_AUTO_STOPPED_MESSAGE =
  "Automatic hash refresh paused after repeated checks. Refresh hashes to try again.";
/**
 * HashUnconfirmed means the daemon's persisted hash state is quarantined, so
 * the reply is authoritative only about uncertainty. The message is a constant:
 * it never echoes the backend payload, never names a marker file and never
 * suggests deleting anything or retrying blindly. Downloads are untouched by
 * hash-state quarantine, and the rows last confirmed by this controller stay.
 */
const HASH_UNCONFIRMED_MESSAGE =
  "Hash state could not be confirmed. Existing downloads are unaffected.";
/**
 * A write may have committed even though its reply was lost. The default stays
 * unavailable for automatic batch use until a read proves the saved value;
 * an explicit per-batch choice remains the availability escape hatch.
 */
const HASH_DEFAULT_UNCERTAIN_MESSAGE =
  "The saved hashing default could not be confirmed. New batches will not use it until a read proves the saved value; a per-batch choice is still available.";
const HASH_DEFAULT_UNCERTAIN_MANUAL_MESSAGE =
  "The saved hashing default could not be confirmed. New batches will not use it until a read proves the saved value; per-batch hashing is available only on the bundled daemon connection.";
/**
 * A timed-out or otherwise ambiguous write may still commit after any read,
 * even one returning the attempted value. There is no backend completion fence
 * exposed here, so the uncertainty lasts for the life of this module's shared
 * store.
 */
const HASH_DEFAULT_UNCONFIRMED_MESSAGE =
  "The saved hashing default could not be confirmed after a save. Changing it is disabled until the app restarts to avoid unsafe writes; a per-batch choice is still available.";
const HASH_DEFAULT_UNCONFIRMED_MANUAL_MESSAGE =
  "The saved hashing default could not be confirmed after a save. Changing it is disabled until the app restarts to avoid unsafe writes; per-batch hashing is available only on the bundled daemon connection.";

function hashDefaultUncertainMessage(managed: boolean): string {
  return managed ? HASH_DEFAULT_UNCERTAIN_MESSAGE : HASH_DEFAULT_UNCERTAIN_MANUAL_MESSAGE;
}

function hashDefaultUnconfirmedMessage(managed: boolean): string {
  return managed ? HASH_DEFAULT_UNCONFIRMED_MESSAGE : HASH_DEFAULT_UNCONFIRMED_MANUAL_MESSAGE;
}
const TRANSIENT_REFRESH_CODES: ReadonlySet<string> = new Set([
  "network",
  "timeout",
  "state",
]);

const KNOWN_STATUSES: ReadonlySet<string> = new Set([
  "active",
  "queued",
  "pausing",
  "paused",
  "failed",
  "completed",
  "unknown",
]);

const JS_MAX_SAFE = 9_007_199_254_740_991;

function toSafeNumber(value: unknown): number {
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n) || n <= 0) return 0;
  return Math.min(JS_MAX_SAFE, Math.floor(n));
}

function toSafeSpeed(value: unknown): number {
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n) || n <= 0) return 0;
  return Math.min(JS_MAX_SAFE, n);
}

const KNOWN_PHASES: ReadonlySet<string> = new Set([
  "queued",
  "retrying",
  "downloading",
  "verifying",
  "finalizing",
  "unknown",
]);

function sanitizePhase(value: unknown): QueueItem["phase"] {
  if (typeof value !== "string") return undefined;
  const phase = value.trim();
  if (phase.length === 0) return undefined;
  return KNOWN_PHASES.has(phase) ? (phase as NonNullable<QueueItem["phase"]>) : "unknown";
}

function sanitizeItem(raw: unknown): QueueItem {
  const r = (raw ?? {}) as Record<string, unknown>;
  const statusRaw = typeof r.status === "string" ? r.status : "unknown";
  const status: DownloadStatus = KNOWN_STATUSES.has(statusRaw) ? (statusRaw as DownloadStatus) : "unknown";
  const bytesTotal = toSafeNumber(r.bytesTotal);
  let bytesDone = toSafeNumber(r.bytesDone);
  if (bytesTotal > 0 && bytesDone > bytesTotal) bytesDone = bytesTotal;
  // v0.13 lifecycle bits: absent on older daemons, so default to idle.
  // Only the hookFailed bit survives; hook_error/hookError text, commands and
  // secrets are never read into state.
  const hookFailed = r.hookFailed === true || r.hook_failed === true;
  return {
    id: String(r.id ?? ""),
    name: String(r.name ?? "download"),
    status,
    bytesDone,
    bytesTotal,
    speedBps: toSafeSpeed(r.speedBps),
    addedAt: toSafeNumber(r.addedAt),
    phase: sanitizePhase(r.phase),
    processing: r.processing === true,
    processed: r.processed === true,
    hookFailed,
  };
}

function sanitizeList(raw: unknown): QueueItem[] {
  if (!Array.isArray(raw)) throw "invalidResponse";
  return raw.map(sanitizeItem);
}

function extractCodeText(err: unknown): string {
  if (typeof err === "string") return err;
  if (err !== null && typeof err === "object") {
    const o = err as Record<string, unknown>;
    if (typeof o.code === "string") return o.code;
    if (typeof o.message === "string") return o.message;
    try {
      return JSON.stringify(o);
    } catch {
      return "";
    }
  }
  try {
    return String(err);
  } catch {
    return "";
  }
}

function mapToSafeMessage(err: unknown, fallback: string): string | null {
  const text = extractCodeText(err).toLowerCase();
  if (text.includes("invalidendpoint")) return "Check the daemon address and try again.";
  if (text.includes("emptytoken")) return "Enter a bearer token to connect.";
  if (text.includes("unauthorized")) return "Connection refused — check the bearer token.";
  if (text.includes("bodytoolarge")) return "Daemon response was too large. Try again.";
  if (text.includes("invalidresponse")) return "Daemon returned an unexpected response.";
  if (text.includes("notconnected")) return "Not connected to a daemon.";
  if (text.includes("timeout")) return "Connection timed out. Try again.";
  if (text.includes("network")) return "Cannot reach the daemon. Check the address and try again.";
  if (text.includes("superseded")) return "Connection was replaced. Try again.";
  if (/\bstate\b/.test(text)) return "Connection state unavailable. Try again.";
  return fallback;
}

const CONNECT_FALLBACK = "Could not connect. Check the address and try again.";
const REFRESH_FALLBACK = "Refresh failed. Check the connection and retry.";
const ADD_FALLBACK = "Could not add download. Check the URL and try again.";
const ADD_AMBIGUOUS_MESSAGE =
  "Add outcome could not be confirmed. The daemon may have accepted this URL; it was not retried.";
const MANAGED_FALLBACK = "Could not start the bundled daemon. Try again.";
const RATE_FALLBACK = "Could not load the download speed limit. Try again.";
const RATE_SAVE_FALLBACK = "Could not save the download speed limit. Try again.";
const MAX_CONCURRENT_FALLBACK = "Could not load the daemon concurrency setting. Try again.";
const MAX_CONCURRENT_SAVE_FALLBACK = "Could not save the daemon concurrency setting. Try again.";
const MAX_CONCURRENT_UNCONFIRMED_MESSAGE =
  "Max simultaneous downloads could not be confirmed. This control is disabled until the app restarts to avoid unsafe writes.";
const LOCATION_FALLBACK = "Download location could not be opened.";
const RATE_UNCONFIRMED_MESSAGE =
  "Speed limit state could not be confirmed. Speed control is disabled until the app restarts to avoid unsafe writes.";

function isRateUnconfirmed(err: unknown): boolean {
  return extractCodeText(err).toLowerCase().includes("rateunconfirmed");
}

function isSuperseded(err: unknown): boolean {
  return extractCodeText(err).toLowerCase().includes("superseded");
}

type AddAttempt =
  | { kind: "ok"; outcome: AddOutcome }
  | { kind: "busy" }
  | { kind: "stopped"; code: string }
  | { kind: "failed"; code: string }
  | { kind: "ambiguous"; code: string };

/**
 * Classify an add error without retrying anything.
 * `notconnected` is definitively pre-request; `superseded` means a request
 * may have crossed an epoch boundary. Network/timeout/body/protocol errors
 * happen after the POST may have left the app, so they stay ambiguous.
 */
function classifyAddError(err: unknown): "stopped" | "failed" | "ambiguous" {
  const text = extractCodeText(err).toLowerCase();
  if (text.includes("notconnected")) return "stopped";
  if (
    text.includes("unauthorized") ||
    text.includes("invalidendpoint") ||
    text.includes("emptytoken")
  ) {
    return "failed";
  }
  return "ambiguous";
}

function isSettingsUnconfirmed(err: unknown): boolean {
  return extractCodeText(err).toLowerCase().includes("settingsunconfirmed");
}

function isHashUnconfirmed(err: unknown): boolean {
  return extractCodeText(err).toLowerCase().includes("hashunconfirmed");
}

const KNOWN_BULK_ACTIONS: ReadonlySet<string> = new Set(["pauseAll", "resumeAll"]);
const MAX_BULK_RESULTS = 100_000;
const MAX_BULK_ID_LENGTH = 1024;
/** Bounded number of independent unresolved bulk notices retained for display. */
const MAX_BULK_UNCERTAINTIES = 5;
/** Bounded number of independent unresolved hash-intent warnings retained. */
const MAX_HASH_INTENT_WARNINGS = 5;

const HASH_INTENT_UNSAVED_MESSAGE =
  "The hash request was not saved for an accepted queued download. The download was accepted; hashing may not run automatically. It was not retried automatically.";

const BULK_FALLBACK =
  "Bulk action could not be completed. Downloads may have changed; the queue keeps refreshing periodically. Not retried automatically.";
const BULK_UNCONFIRMED_MESSAGE =
  "Bulk action outcome could not be confirmed. Downloads may have changed; the queue keeps refreshing periodically. Not retried automatically.";
const BULK_NOT_CONNECTED_MESSAGE = "Not connected. No bulk action was sent.";
const BULK_ITEM_ERROR_MESSAGE = "Daemon reported an error for this item.";

function isBulkAction(value: unknown): value is BulkAction {
  return typeof value === "string" && KNOWN_BULK_ACTIONS.has(value);
}

function bulkLabel(action: BulkAction): string {
  return action === "pauseAll" ? "Pause all" : "Resume all";
}

function bulkUncertaintyMessage(action: BulkAction, sameConnection: boolean): string {
  const scope = sameConnection
    ? "was sent on this connection, but its outcome was not confirmed. It may still apply here."
    : "was sent on a previous connection, but its outcome was never confirmed. The current connection may be the same daemon, so the action may still apply here.";
  return `${bulkLabel(action)} ${scope} It was not retried automatically; the queue keeps refreshing periodically.`;
}

/**
 * Validate the error field without ever forwarding its text. The daemon's
 * per-item error can carry a bearer token, a signed URL, or a local path, so
 * only the validated item id and the ok/failed bit survive into state.
 */
function sanitizeBulkError(raw: unknown): string | undefined {
  if (raw === undefined || raw === null) return undefined;
  if (typeof raw !== "string") throw "bulkUnconfirmed";
  return BULK_ITEM_ERROR_MESSAGE;
}

function unconfirmedBulkSummary(action: BulkAction, message: string): BulkSummary {
  return {
    action,
    status: "unconfirmed",
    accepted: null,
    failed: null,
    total: null,
    failures: [],
    message,
  };
}

function failedBulkSummary(action: BulkAction, message: string): BulkSummary {
  return {
    action,
    status: "failed",
    accepted: 0,
    failed: 0,
    total: 0,
    failures: [],
    message,
  };
}

/**
 * Validate the Rust bulk outcome a second time at the IPC boundary. The
 * backend already enforces this, but a malformed/oversized payload is treated
 * as unconfirmed rather than rendered as a clean result. A count that does not
 * equal the per-item successes is rejected.
 */
function sanitizeBulkOutcome(raw: unknown, action: BulkAction): BulkSummary {
  const r = (raw ?? {}) as Record<string, unknown>;
  if (r.status !== "done") throw "bulkUnconfirmed";
  const count = r.count;
  if (
    typeof count !== "number" ||
    !Number.isInteger(count) ||
    count < 0 ||
    count > Number.MAX_SAFE_INTEGER
  ) {
    throw "bulkUnconfirmed";
  }
  const results = r.results;
  if (!Array.isArray(results) || results.length > MAX_BULK_RESULTS) throw "bulkUnconfirmed";
  const seen = new Set<string>();
  let accepted = 0;
  const failures: BulkItemFailure[] = [];
  for (const entry of results) {
    if (entry === null || typeof entry !== "object") throw "bulkUnconfirmed";
    const item = entry as Record<string, unknown>;
    const id = item.id;
    if (typeof id !== "string" || id.length === 0 || id.length > MAX_BULK_ID_LENGTH) {
      throw "bulkUnconfirmed";
    }
    if (seen.has(id)) throw "bulkUnconfirmed";
    seen.add(id);
    const ok = item.ok;
    if (typeof ok !== "boolean") throw "bulkUnconfirmed";
    const error = sanitizeBulkError(item.error);
    if (ok) {
      if (error !== undefined) throw "bulkUnconfirmed";
      accepted += 1;
    } else {
      if (error === undefined) throw "bulkUnconfirmed";
      failures.push({ id, error });
    }
  }
  if (accepted !== count) throw "bulkUnconfirmed";
  const failed = results.length - accepted;
  const label = bulkLabel(action);
  return {
    action,
    status: "done",
    accepted,
    failed,
    total: results.length,
    failures,
    message:
      results.length === 0
        ? `${label}: no downloads were eligible.`
        : `${label} acknowledged: ${accepted} accepted, ${failed} failed. The daemon applies transitions asynchronously; the queue keeps refreshing periodically.`,
  };
}

function classifyBulkError(err: unknown): BulkSummary["status"] | "notconnected" {
  const text = extractCodeText(err).toLowerCase();
  if (text.includes("notconnected")) return "notconnected";
  if (
    text.includes("unauthorized") ||
    text.includes("invalidresponse") ||
    text.includes("bulkunconfirmed")
  ) {
    return text.includes("bulkunconfirmed") ? "unconfirmed" : "failed";
  }
  return "unconfirmed";
}

function mapMaxConcurrentMessage(err: unknown, fallback: string): string {
  const text = extractCodeText(err).toLowerCase();
  if (text.includes("settingsunconfirmed")) return MAX_CONCURRENT_UNCONFIRMED_MESSAGE;
  if (text.includes("settingsconflict")) {
    return "The daemon setting changed since it was loaded. Reload it and try again.";
  }
  if (text.includes("managedconnectionrequired")) {
    return "Max simultaneous downloads needs the bundled daemon connection.";
  }
  if (text.includes("notconnected")) return "Not connected to a daemon.";
  if (text.includes("invalidresponse")) return "Daemon returned an unexpected response.";
  if (text.includes("timeout")) return "Connection timed out. Try again.";
  if (text.includes("network")) return "Cannot reach the daemon. Check the connection and try again.";
  if (text.includes("superseded")) return "Connection was replaced. Reload the setting and try again.";
  if (/\bstate\b/.test(text)) return "Connection state unavailable. Try again.";
  return fallback;
}

function sanitizeRateValue(raw: unknown): number {
  const n = typeof raw === "number" ? raw : Number(raw);
  if (!Number.isFinite(n) || !Number.isInteger(n) || n < 0 || n > JS_MAX_SAFE) throw "invalidResponse";
  return n;
}

function mapRateMessage(err: unknown, fallback: string): string {
  const text = extractCodeText(err).toLowerCase();
  if (text.includes("rateunconfirmed")) return RATE_UNCONFIRMED_MESSAGE;
  if (text.includes("managedconnectionrequired")) return "Speed control needs the bundled daemon connection.";
  if (text.includes("notconnected")) return "Not connected to a daemon.";
  if (text.includes("invalidresponse")) return "Daemon returned an unexpected response.";
  if (text.includes("timeout")) return "Connection timed out. Try again.";
  if (text.includes("network")) return "Cannot reach the daemon. Check the connection and try again.";
  if (/\bstate\b/.test(text)) return "Connection state unavailable. Try again.";
  return fallback;
}

function mapLocationMessage(err: unknown): string {
  const text = extractCodeText(err).toLowerCase();
  if (text.includes("managedconnectionrequired")) return "Open location needs the bundled daemon connection.";
  if (text.includes("notconnected")) return "Not connected to a daemon.";
  if (text.includes("invaliddownloadid") || text.includes("invalidresponse")) return "This download cannot be opened.";
  if (text.includes("downloadnotfound")) return "Download not found. It may have been removed.";
  if (text.includes("downloadambiguous")) return "Download location is ambiguous. Try again.";
  if (text.includes("downloadnotcompleted")) return "Only completed downloads can be opened.";
  if (text.includes("downloadmoving")) return "Download is still moving into place. Try again shortly.";
  if (text.includes("downloadfilemissing")) return "Download file is missing.";
  if (text.includes("locationunavailable")) return "Download location is unavailable.";
  if (text.includes("locationunsupported")) return "Opening the download location is not supported here.";
  if (text.includes("locationlaunchfailed")) return "Download location could not be opened.";
  if (text.includes("timeout")) return "Connection timed out. Try again.";
  if (text.includes("network")) return "Cannot reach the daemon. Check the connection and try again.";
  if (/\bstate\b/.test(text)) return "Connection state unavailable. Try again.";
  return LOCATION_FALLBACK;
}

const MANAGED_START_CODES: ReadonlySet<string> = new Set([
  "resource",
  "paths",
  "identity",
  "process",
  "readiness",
  "invalidInput",
  "unsupportedPlatform",
]);

function mapManagedStartError(err: unknown): string {
  if (typeof err === "string" && MANAGED_START_CODES.has(err)) {
    return `Could not start the bundled daemon. Code: ${err}.`;
  }
  return MANAGED_FALLBACK;
}

const KNOWN_ADD_OUTCOMES: ReadonlySet<string> = new Set(["queued", "pendingApproval", "duplicate"]);

interface RefreshResult {
  ok: boolean;
  code: string | null;
}

/**
 * One hash-intent write while it is pending. Reads capture these records at
 * issue time so a read that overlapped a write is never treated as more
 * authoritative than the write's committed response. The record itself is
 * removed from the map when the write settles; any read still holding it can
 * then tell that its snapshot may predate the commit.
 */
interface HashWriteRecord {
  order: number;
  settled: boolean;
  followUpCommand: string | null;
  /** Issue-order wave shared by intent writes that overlap in time. */
  waveId: number;
}

/**
 * One bounded settlement wave for concurrent intent writes. A post-write read
 * is issued only after the final write in the wave settles, so a read cannot
 * supersede a still-pending write's committed response.
 */
interface HashWriteWave {
  id: number;
  writes: number;
  settled: number;
  overlapped: boolean;
  followUpCommand: string | null;
  /** A write in this wave failed, so its committed state is unconfirmed. */
  failed: boolean;
}

interface HashWaveSettlement {
  final: boolean;
  needsRead: boolean;
  followUpCommand: string | null;
  /** Any write in this wave failed; the final read must not hide its error. */
  hadFailure: boolean;
}

/**
 * One automatic-hash default mutation while it is pending. The persisted
 * default is app-wide, so there is at most one write in flight across every
 * connection epoch and mode; a late settlement is treated as sharing the same
 * uncertain store.
 */
interface HashDefaultWriteRecord {
  order: number;
  epoch: number;
  target: boolean;
  settled: boolean;
  /** Controller that issued the write; used to keep its own UI authoritative. */
  owner: object;
}

interface HashDefaultStoreEvent {
  kind: "started" | "settled";
  record: HashDefaultWriteRecord;
}

type HashDefaultStoreListener = (event: HashDefaultStoreEvent) => void;

/**
 * The persisted hash default belongs to the backend, not to one controller.
 * Every controller in this JS module shares this lock and poison so a second
 * instance cannot write while another write is pending or after uncertainty.
 * Live controllers are also notified so their cached default is invalidation,
 * not reused to drive an implicit batch. This does not coordinate separate
 * webviews, processes or naive clients; the backend remains the single writer
 * and must enforce one owner.
 */
export interface HashDefaultStoreCoordinator {
  pendingWrites: Map<number, HashDefaultWriteRecord>;
  writeSeq: number;
  generation: number;
  uncertain: boolean;
  listeners: Set<HashDefaultStoreListener>;
}

function createHashDefaultStoreCoordinator(): HashDefaultStoreCoordinator {
  return {
    pendingWrites: new Map(),
    writeSeq: 0,
    generation: 0,
    uncertain: false,
    listeners: new Set(),
  };
}

const sharedHashDefaultStoreCoordinator = createHashDefaultStoreCoordinator();

export function resetSharedHashDefaultStoreForTests(): void {
  const store = sharedHashDefaultStoreCoordinator;
  store.pendingWrites.clear();
  store.writeSeq = 0;
  store.generation = 0;
  store.uncertain = false;
}

interface ActiveBatchContext {
  epoch: number;
  identity: number;
  plan: BatchPlanEntry[];
  results: BatchLinkResult[];
  nextIndex: number;
  inFlight: boolean;
  recoveryRecorded: boolean;
  sealed: boolean;
}

/**
 * Native daemon queue bridge.
 *
 * - `invoke` is the only path the bearer token ever travels; it is passed
 *   straight to `connect_surge` and never stored on this object, in state,
 *   in browser storage, or in logs.
 * - `epoch` fences late async results: every connect/disconnect/dispose bumps
 *   it, and a settled call whose epoch is stale is discarded without emitting.
 * - Polling is bounded single-flight on `pollMs` (default 5000); the timer is
 *   cancelled on disconnect/dispose and never overlaps a refresh.
 */
export class NativeQueueController {
  readonly state: NativeState = {
    phase: "offline",
    items: [],
    error: null,
    refreshing: false,
    controllingId: null,
    adding: false,
    addOutcome: null,
    batch: null,
    managed: false,
    rateLimitBps: null,
    rateLoading: false,
    rateSaving: false,
    rateError: null,
    rateUnconfirmed: false,
    maxConcurrent: null,
    maxConcurrentLoading: false,
    maxConcurrentSaving: false,
    maxConcurrentError: null,
    maxConcurrentUnconfirmed: false,
    maxConcurrentRestartPending: false,
    locationOpeningId: null,
    locationError: null,
    locationErrorId: null,
    bulkAction: null,
    bulkSummary: null,
    bulkUncertainties: [],
    bulkUncertaintyOverflow: 0,
    hashIntentWarnings: [],
    hashIntentWarningOverflow: 0,
    streamState: "off",
    hashDefaultEnabled: null,
    hashDefaultLoading: false,
    hashDefaultSaving: false,
    hashDefaultError: null,
    hashDefaultUnconfirmed: false,
    hashes: [],
    hashesLoading: false,
    hashError: null,
  };

  private epoch = 0;
  private timer: ReturnType<typeof setInterval> | null = null;
  private disposed = false;
  private streamListen: ListenFn | null;
  private listenerPromise: Promise<void> | null = null;
  private listenerAttempt: object | null = null;
  private listenerReady = false;
  private listenerTimeoutMs: number;
  private unlisteners: Array<() => void> = [];
  private streamSubscription: number | null = null;
  private snapshotCursor = 0;
  private lastLifecycleSeq = 0;
  private lastStateSeq = 0;
  private progressSeqById = new Map<string, number>();
  /**
   * Ids whose daemon reset was observed in a snapshot, mapped to the generation
   * (`addedAt`) that reset. Live events for that generation are ignored because
   * a delayed old sample cannot be distinguished without a daemon generation
   * id; snapshots alone update the row until `addedAt` changes.
   */
  private progressQuarantined = new Map<string, number>();
  /**
   * Ids whose lower snapshot was deferred because a newer in-flight sample may
   * explain it. A fresh authoritative read confirms a real reset or clears the
   * suspicion; while pending, the live value is kept rather than rewound.
   */
  private pendingSnapshotConfirmation = new Set<string>();
  private reconcileQueued = false;
  private reconcileRunning = false;
  private reconcileRetryAttempt = 0;
  private reconcileRetryTimer: ReturnType<typeof setTimeout> | null = null;
  private reconcileRetryMs: number[];
  private controlEpoch: number | null = null;
  private bulkEpoch: number | null = null;
  private bulkSeq = 0;
  private bulkUncertaintySeq = 0;
  private hashIntentWarningSeq = 0;
  /**
   * Connection epoch that produced the currently displayed bulk summary. Used
   * to describe an unresolved outcome without blaming a previous connection
   * when the request was made on the connection that is still current.
   */
  private bulkSummaryEpoch: number | null = null;
  private bulkAwaitingReply = false;
  private bulkReconcilePending = false;
  private addEpoch: number | null = null;
  private managedEpoch: number | null = null;
  /** Epoch whose connection targets the persisted managed default store. */
  private managedStoreEpoch: number | null = null;
  private refreshPromise: Promise<RefreshResult> | null = null;
  private refreshEpoch: number | null = null;
  private refreshSeq = 0;
  private rateSeq = 0;
  private rateWriteEpoch: number | null = null;
  private ratePoisoned = false;
  private rateAutoLoadEpoch: number | null = null;
  private rateLoadPromise: Promise<void> | null = null;
  private rateLoadEpoch: number | null = null;
  private maxConcurrentSeq = 0;
  private maxConcurrentWriteEpoch: number | null = null;
  private maxConcurrentPoisoned = false;
  private maxConcurrentAutoLoadEpoch: number | null = null;
  private maxConcurrentLoadPromise: Promise<void> | null = null;
  private maxConcurrentLoadEpoch: number | null = null;
  private batchEpoch: number | null = null;
  private batchIdentity: number | null = null;
  private batchSeq = 0;
  private activeBatch: ActiveBatchContext | null = null;
  private batchRecoveries: BatchRecovery[] = [];
  private batchRecoverySeq = 0;
  private locationEpoch: number | null = null;
  private locationSeq = 0;
  private hashDefaultSeq = 0;
  private hashDefaultLoadPromise: Promise<void> | null = null;
  private hashDefaultLoadEpoch: number | null = null;
  /**
   * Shared across every controller in this module. Reads capture it at issue
   * time; every mutation by any controller starts and settles into a new
   * generation, so a read that began before a write settled cannot overwrite
   * that write outcome even across controller instances.
   */
  private hashDefaultStore: HashDefaultStoreCoordinator;
  private readonly hashDefaultOwner = {};
  private hashDefaultStoreListener: HashDefaultStoreListener;
  private hashDefaultLoadGeneration = 0;
  private hashDefaultAutoLoadEpoch: number | null = null;
  private hashAutoLoadEpoch: number | null = null;
  private hashLoadSeq = 0;
  private hashLoadPromise: Promise<void> | null = null;
  private hashLoadEpoch: number | null = null;
  private hashWaveSeq = 0;
  private hashActiveWave: HashWriteWave | null = null;
  private hashAutoTimer: ReturnType<typeof setTimeout> | null = null;
  private hashRefreshMs: number;
  private hashAutoMaxAttempts: number;
  private hashAutoAttempts = 0;
  /** Per-epoch hash command counter; auto propagation never overlaps a read. */
  private hashCommandsInFlight = new Map<number, number>();
  /** Id+generation completions already given one worker pass this connection. */
  private hashCompletionRequested = new Set<string>();
  /** Completions whose worker pass has not been issued yet (kept on deferral). */
  private hashCompletionCandidates = new Set<string>();
  /** Shared issue order for reads and intent writes; stale applies are fenced. */
  private hashOrderSeq = 0;
  private hashAppliedSeq = 0;
  /** Highest issue order whose applied rows are authoritative post-write state. */
  private hashAuthoritativeSeq = 0;
  /** Pending intent writes by issue order for the current connection epoch. */
  private hashPendingWrites = new Map<number, HashWriteRecord>();
  private invoke: InvokeFn;
  private onChange: OnChangeFn;
  private pollMs: number;

  constructor(
    invoke: InvokeFn,
    onChange: OnChangeFn,
    pollMs = 5000,
    listen?: ListenFn,
    options?: {
      reconcileRetryMs?: number[];
      listenerTimeoutMs?: number;
      hashRefreshMs?: number;
      hashAutoMaxAttempts?: number;
      hashDefaultStore?: HashDefaultStoreCoordinator;
    },
  ) {
    this.invoke = invoke;
    this.onChange = onChange;
    this.pollMs = pollMs;
    this.streamListen = typeof listen === "function" ? listen : null;
    this.reconcileRetryMs = options?.reconcileRetryMs ?? RECONCILE_RETRY_MS;
    const listenerTimeout = options?.listenerTimeoutMs;
    this.listenerTimeoutMs =
      typeof listenerTimeout === "number" && Number.isFinite(listenerTimeout) && listenerTimeout > 0
        ? listenerTimeout
        : DEFAULT_LISTENER_TIMEOUT_MS;
    const hashRefreshMs = options?.hashRefreshMs;
    this.hashRefreshMs =
      typeof hashRefreshMs === "number" && Number.isFinite(hashRefreshMs) && hashRefreshMs > 0
        ? hashRefreshMs
        : DEFAULT_HASH_REFRESH_MS;
    const hashAutoMaxAttempts = options?.hashAutoMaxAttempts;
    this.hashAutoMaxAttempts =
      typeof hashAutoMaxAttempts === "number" &&
      Number.isFinite(hashAutoMaxAttempts) &&
      hashAutoMaxAttempts > 0
        ? Math.floor(hashAutoMaxAttempts)
        : DEFAULT_HASH_AUTO_MAX_ATTEMPTS;
    this.hashDefaultStore = options?.hashDefaultStore ?? sharedHashDefaultStoreCoordinator;
    this.hashDefaultStoreListener = (event) => this.onSharedHashDefaultEvent(event);
    this.hashDefaultStore.listeners.add(this.hashDefaultStoreListener);
  }

  /** Monotonic connection epoch; changes on every connect/managed start/disconnect. */
  connectionEpoch(): number {
    return this.epoch;
  }

  /**
   * Take the recoveries left by batches that were detached by a reconnect,
   * disconnect or newer batch. The full original lines are handed to the UI
   * once, for merging into the editable input; nothing is retried here.
   */
  drainBatchRecoveries(): BatchRecovery[] {
    const drained = this.batchRecoveries;
    this.batchRecoveries = [];
    return drained.map((recovery) => ({
      ...recovery,
      results: recovery.results.map((result) => ({ ...result })),
      uncertain: [...recovery.uncertain],
      unsent: [...recovery.unsent],
      rejected: [...recovery.rejected],
    }));
  }

  private recordBatchRecovery(
    results: BatchLinkResult[],
    uncertain: string[],
    unsent: string[],
    rejected: string[],
  ): void {
    if (uncertain.length === 0 && unsent.length === 0 && rejected.length === 0) return;
    const acceptedCount = results.filter(
      (result) =>
        result.status === "queued" ||
        result.status === "pendingApproval" ||
        result.status === "duplicate",
    ).length;
    this.batchRecoveries.push({
      id: ++this.batchRecoverySeq,
      acceptedCount,
      results: results.map((result) => ({ ...result })),
      uncertain,
      unsent,
      rejected,
    });
  }

  /**
   * Snapshot the active batch the moment its connection epoch is abandoned.
   * This makes the in-flight link and unsent tail recoverable even if the
   * deferred request never settles. Once captured, the later settlement of the
   * stale batch must not record the same work twice.
   */
  private captureAbandonedBatch(): void {
    const context = this.activeBatch;
    if (!context || context.sealed || context.recoveryRecorded) return;
    if (context.epoch === this.epoch) return;

    const displayResults = context.results.map((result) => ({ ...result }));
    const uncertain: string[] = [];
    const unsent: string[] = [];
    const rejected: string[] = [];

    for (const result of context.results) {
      const line = context.plan[result.index]?.line ?? result.line;
      if (result.status === "ambiguous") uncertain.push(line);
      else if (result.status === "unattempted") unsent.push(line);
      else if (result.status === "invalid" || result.status === "failed") rejected.push(line);
    }

    let next = context.nextIndex;
    if (context.inFlight && next < context.plan.length) {
      const entry = context.plan[next];
      uncertain.push(entry.line);
      displayResults.push(
        this.batchResult(
          entry,
          "ambiguous",
          undefined,
          "In-flight outcome unknown after the connection changed.",
        ),
      );
      next += 1;
    }
    for (let index = next; index < context.plan.length; index++) {
      const entry = context.plan[index];
      if (entry.kind === "invalid") {
        rejected.push(entry.line);
        displayResults.push(this.batchResult(entry, "invalid", entry.reason));
      } else {
        unsent.push(entry.line);
        displayResults.push(
          this.batchResult(
            entry,
            "unattempted",
            undefined,
            "Unsent tail was not attempted.",
          ),
        );
      }
    }

    this.recordBatchRecovery(displayResults, uncertain, unsent, rejected);
    context.recoveryRecorded = true;
  }

  private emit(): void {
    if (this.disposed) return;
    this.onChange({
      phase: this.state.phase,
      items: this.state.items.map((i) => ({ ...i })),
      error: this.state.error,
      refreshing: this.state.refreshing,
      controllingId: this.state.controllingId,
      adding: this.state.adding ?? false,
      addOutcome: this.state.addOutcome ?? null,
      batch: this.state.batch
        ? {
            ...this.state.batch,
            results: this.state.batch.results.map((result) => ({ ...result })),
          }
        : null,
      managed: this.state.managed ?? false,
      rateLimitBps: this.state.rateLimitBps ?? null,
      rateLoading: this.state.rateLoading ?? false,
      rateSaving: this.state.rateSaving ?? false,
      rateError: this.state.rateError ?? null,
      rateUnconfirmed: this.state.rateUnconfirmed ?? false,
      maxConcurrent: this.state.maxConcurrent ?? null,
      maxConcurrentLoading: this.state.maxConcurrentLoading ?? false,
      maxConcurrentSaving: this.state.maxConcurrentSaving ?? false,
      maxConcurrentError: this.state.maxConcurrentError ?? null,
      maxConcurrentUnconfirmed: this.state.maxConcurrentUnconfirmed ?? false,
      maxConcurrentRestartPending: this.state.maxConcurrentRestartPending ?? false,
      locationOpeningId: this.state.locationOpeningId ?? null,
      locationError: this.state.locationError ?? null,
      locationErrorId: this.state.locationErrorId ?? null,
      bulkAction: this.state.bulkAction ?? null,
      bulkSummary: this.state.bulkSummary
        ? {
            ...this.state.bulkSummary,
            failures: this.state.bulkSummary.failures.map((failure) => ({ ...failure })),
          }
        : null,
      bulkUncertainties: (this.state.bulkUncertainties ?? []).map((notice) => ({
        ...notice,
      })),
      bulkUncertaintyOverflow: this.state.bulkUncertaintyOverflow ?? 0,
      hashIntentWarnings: (this.state.hashIntentWarnings ?? []).map((notice) => ({
        ...notice,
      })),
      hashIntentWarningOverflow: this.state.hashIntentWarningOverflow ?? 0,
      streamState: this.state.streamState ?? "off",
      hashDefaultEnabled: this.state.hashDefaultEnabled ?? null,
      hashDefaultLoading: this.state.hashDefaultLoading ?? false,
      hashDefaultSaving: this.state.hashDefaultSaving ?? false,
      hashDefaultError: this.state.hashDefaultError ?? null,
      hashDefaultUnconfirmed: this.state.hashDefaultUnconfirmed ?? false,
      hashes: (this.state.hashes ?? []).map((row) => ({ ...row })),
      hashesLoading: this.state.hashesLoading ?? false,
      hashError: this.state.hashError ?? null,
    });
  }

  /**
   * Convert an unconfirmed bulk outcome into an independent persistent notice
   * before the per-connection summary is cleared. Notices survive reconnect,
   * are never attributed to the new daemon, and are only removed one at a time.
   */
  private noteBulkUncertainty(): void {
    const summary = this.state.bulkSummary;
    const unconfirmed = summary?.status === "unconfirmed";
    const action = this.state.bulkAction ?? summary?.action ?? null;
    if (!this.bulkAwaitingReply && !unconfirmed) return;
    if (!isBulkAction(action)) return;
    this.recordBulkUncertainty(action, this.bulkSummaryEpoch === this.epoch);
  }

  private recordBulkUncertainty(action: BulkAction, sameConnection: boolean): void {
    const notices = this.state.bulkUncertainties ?? [];
    notices.push({
      id: ++this.bulkUncertaintySeq,
      action,
      message: bulkUncertaintyMessage(action, sameConnection),
    });
    if (notices.length > MAX_BULK_UNCERTAINTIES) {
      const omitted = notices.length - MAX_BULK_UNCERTAINTIES;
      notices.splice(0, omitted);
      this.state.bulkUncertaintyOverflow = (this.state.bulkUncertaintyOverflow ?? 0) + omitted;
    }
    this.state.bulkUncertainties = notices;
  }

  /** Dismiss one persistent bulk-uncertainty notice explicitly. */
  dismissBulkUncertainty(id: number): void {
    if (this.disposed) return;
    const notices = this.state.bulkUncertainties ?? [];
    const next = notices.filter((notice) => notice.id !== id);
    if (next.length === notices.length) return;
    this.state.bulkUncertainties = next;
    this.emit();
  }

  /**
   * Acknowledge the overflow summary on its own. Individual retained notices
   * are independent and are never removed by this action.
   */
  dismissBulkUncertaintyOverflow(): void {
    if (this.disposed) return;
    if ((this.state.bulkUncertaintyOverflow ?? 0) === 0) return;
    this.state.bulkUncertaintyOverflow = 0;
    this.emit();
  }

  /**
   * Record one bounded, redacted warning for an accepted queued download whose
   * requested hash intent was not confirmed. Warnings are independent, survive
   * reconnect, and are removed only by explicit dismissal — never by a retry.
   */
  private recordHashIntentWarning(): void {
    const notices = this.state.hashIntentWarnings ?? [];
    notices.push({
      id: ++this.hashIntentWarningSeq,
      message: HASH_INTENT_UNSAVED_MESSAGE,
    });
    if (notices.length > MAX_HASH_INTENT_WARNINGS) {
      const omitted = notices.length - MAX_HASH_INTENT_WARNINGS;
      notices.splice(0, omitted);
      this.state.hashIntentWarningOverflow =
        (this.state.hashIntentWarningOverflow ?? 0) + omitted;
    }
    this.state.hashIntentWarnings = notices;
    this.emit();
  }

  /** Dismiss one persistent hash-intent warning explicitly. */
  dismissHashIntentWarning(id: number): void {
    if (this.disposed) return;
    const notices = this.state.hashIntentWarnings ?? [];
    const next = notices.filter((notice) => notice.id !== id);
    if (next.length === notices.length) return;
    this.state.hashIntentWarnings = next;
    this.emit();
  }

  /**
   * Acknowledge the hash-intent overflow summary on its own. Retained notices
   * are independent and are never removed by this action.
   */
  dismissHashIntentWarningOverflow(): void {
    if (this.disposed) return;
    if ((this.state.hashIntentWarningOverflow ?? 0) === 0) return;
    this.state.hashIntentWarningOverflow = 0;
    this.emit();
  }

  private resetBulkState(): void {
    this.noteBulkUncertainty();
    this.bulkEpoch = null;
    this.bulkSeq += 1;
    this.bulkAwaitingReply = false;
    this.bulkReconcilePending = false;
    this.bulkSummaryEpoch = null;
    this.state.bulkAction = null;
    this.state.bulkSummary = null;
  }

  /**
   * If a possibly applied action settled while a connect was still in flight,
   * its `/list` may predate the action. Run one more fresh read once the new
   * connection is ready.
   */
  private flushBulkReconcile(): void {
    if (!this.bulkReconcilePending) return;
    this.bulkReconcilePending = false;
    if (this.disposed || this.state.phase !== "connected") return;
    void this.startRefresh();
  }

  private resetMaxConcurrentState(): void {
    this.state.maxConcurrent = null;
    this.state.maxConcurrentLoading = false;
    this.state.maxConcurrentSaving = false;
    this.state.maxConcurrentRestartPending = false;
    this.state.maxConcurrentUnconfirmed = this.maxConcurrentPoisoned;
    this.state.maxConcurrentError = this.maxConcurrentPoisoned
      ? MAX_CONCURRENT_UNCONFIRMED_MESSAGE
      : null;
  }

  private hashDefaultPoisoned(): boolean {
    return this.hashDefaultStore.uncertain;
  }

  private activeHashDefaultPoisonMessage(): string {
    return hashDefaultUnconfirmedMessage(this.state.managed === true);
  }

  private activeHashDefaultUncertainMessage(): string {
    return hashDefaultUncertainMessage(this.state.managed === true);
  }

  private hashDefaultGenerationFor(): number {
    return this.hashDefaultStore.generation;
  }

  private bumpHashDefaultGeneration(): void {
    this.hashDefaultStore.generation += 1;
  }

  /**
   * The persisted default is app-wide, so a pending write on any connection
   * also blocks reads and writes on every other connection.
   */
  private hasPendingHashDefaultWrite(): boolean {
    return this.hashDefaultStore.pendingWrites.size > 0;
  }

  /**
   * Notify every live controller in this module about a shared default mutation.
   * The writer keeps its own state authoritative; everyone else invalidates the
   * cached baseline so it can never drive an implicit batch. Disposed listeners
   * are removed in dispose(), so this set holds no stale controller references.
   */
  private publishHashDefaultStoreEvent(event: HashDefaultStoreEvent): void {
    for (const listener of [...this.hashDefaultStore.listeners]) {
      try {
        listener(event);
      } catch {
        // A listener must never break the controller that owns the write.
      }
    }
  }

  private onSharedHashDefaultEvent(event: HashDefaultStoreEvent): void {
    if (this.disposed) return;
    // The initiating controller updates its own visible state and return value.
    if (event.record.owner === this.hashDefaultOwner) return;
    if (event.kind === "started") {
      if (this.state.hashDefaultSaving === true) return;
      this.state.hashDefaultSaving = true;
      this.emit();
      return;
    }
    let changed = false;
    const pending = this.hasPendingHashDefaultWrite();
    if (this.state.hashDefaultSaving !== pending) {
      this.state.hashDefaultSaving = pending;
      changed = true;
    }
    // A shared write settled, so this controller's cached default may predate
    // its commit even when the reply acknowledged success. Require a fresh read
    // before the value can drive an implicit batch again.
    if (this.state.hashDefaultEnabled !== null) {
      this.state.hashDefaultEnabled = null;
      changed = true;
    }
    if (this.hashDefaultPoisoned()) {
      if (this.state.hashDefaultUnconfirmed !== true) {
        this.state.hashDefaultUnconfirmed = true;
        changed = true;
      }
      const message = this.activeHashDefaultPoisonMessage();
      if (this.state.hashDefaultError !== message) {
        this.state.hashDefaultError = message;
        changed = true;
      }
    } else {
      if (this.state.hashDefaultUnconfirmed !== false) {
        this.state.hashDefaultUnconfirmed = false;
        changed = true;
      }
      if (this.state.hashDefaultError !== null) {
        this.state.hashDefaultError = null;
        changed = true;
      }
    }
    if (changed) this.emit();
  }

  private clearHashDefaultLoadingForLatest(seq: number): void {
    if (seq !== this.hashDefaultSeq) return;
    this.state.hashDefaultLoading = false;
    this.emit();
  }

  private refreshHashDefaultSaving(): void {
    this.state.hashDefaultSaving = this.hashDefaultStore.pendingWrites.size > 0;
  }

  private settleHashDefaultWrite(
    record: HashDefaultWriteRecord,
    ambiguous = false,
  ): void {
    if (record.settled) return;
    record.settled = true;
    if (this.hashDefaultStore.pendingWrites.get(record.order) === record) {
      this.hashDefaultStore.pendingWrites.delete(record.order);
    }
    // Uncertainty is a property of the shared store, not of this controller's
    // lifetime: an error that arrives after dispose must still block every
    // other controller before their next IPC.
    if (ambiguous) this.hashDefaultStore.uncertain = true;
    // Any read that began before this settlement is now stale, even when it
    // is still in flight and the epoch itself has not changed.
    this.bumpHashDefaultGeneration();
    this.publishHashDefaultStoreEvent({ kind: "settled", record });
  }

  private invalidateHashDefaultBaseline(message: string): void {
    this.state.hashDefaultEnabled = null;
    this.state.hashDefaultError = message;
  }

  private resetHashState(): void {
    // Epoch-fenced visibility: a new connection starts with no rows so a
    // previous daemon's digests can never read as the new daemon's state.
    // The persisted default is also unconfirmed in every new epoch: keeping a
    // cached value would let a batch be submitted from a default that changed
    // externally while this app was disconnected. It must be re-read on the
    // current epoch before it can resolve a batch (an explicit per-batch
    // override is still allowed). An ambiguous save stays unknown here too.
    this.stopHashAutoRefresh();
    this.hashCompletionRequested.clear();
    this.hashCompletionCandidates.clear();
    this.hashAutoAttempts = 0;
    this.hashPendingWrites.clear();
    this.hashActiveWave = null;
    this.state.hashes = [];
    this.state.hashesLoading = false;
    this.state.hashError = null;
    this.state.hashDefaultEnabled = null;
    this.state.hashDefaultLoading = false;
    this.state.hashDefaultSaving = this.hasPendingHashDefaultWrite();
    this.state.hashDefaultUnconfirmed = this.hashDefaultPoisoned();
    this.state.hashDefaultError = this.hashDefaultPoisoned()
      ? this.activeHashDefaultPoisonMessage()
      : null;
    this.hashDefaultLoadPromise = null;
    this.hashDefaultLoadEpoch = null;
    this.hashLoadPromise = null;
    this.hashLoadEpoch = null;
  }

  private stopHashAutoRefresh(): void {
    if (this.hashAutoTimer !== null) {
      clearTimeout(this.hashAutoTimer);
      this.hashAutoTimer = null;
    }
  }

  private beginHashCommand(epoch: number): void {
    this.hashCommandsInFlight.set(epoch, (this.hashCommandsInFlight.get(epoch) ?? 0) + 1);
  }

  private endHashCommand(epoch: number): void {
    const next = (this.hashCommandsInFlight.get(epoch) ?? 1) - 1;
    if (next <= 0) this.hashCommandsInFlight.delete(epoch);
    else this.hashCommandsInFlight.set(epoch, next);
  }

  private hashCommandInFlight(epoch: number): boolean {
    return (this.hashCommandsInFlight.get(epoch) ?? 0) > 0;
  }

  private captureHashWriteOverlap(): HashWriteRecord[] {
    return [...this.hashPendingWrites.values()];
  }

  private noteHashReadOverlap(overlaps: HashWriteRecord[], command: string): void {
    for (const record of overlaps) record.followUpCommand = command;
    const wave = this.hashActiveWave;
    if (wave !== null && overlaps.length > 0) {
      wave.overlapped = true;
      wave.followUpCommand = command;
    }
  }

  private hashReadOverlappedSettledWrite(overlaps: HashWriteRecord[]): boolean {
    return overlaps.some((record) => record.settled);
  }

  private settleHashWrite(record: HashWriteRecord, failed = false): HashWaveSettlement {
    record.settled = true;
    if (this.hashPendingWrites.get(record.order) === record) {
      this.hashPendingWrites.delete(record.order);
    }
    const wave = this.hashActiveWave;
    if (wave === null || wave.id !== record.waveId) {
      return { final: false, needsRead: false, followUpCommand: null, hadFailure: failed };
    }
    if (failed) wave.failed = true;
    wave.settled += 1;
    if (wave.settled < wave.writes) {
      return { final: false, needsRead: false, followUpCommand: null, hadFailure: wave.failed };
    }
    // The final write in the wave closes it. Concurrent writes, any read that
    // overlapped the wave, and any failed write (whose committed state is
    // uncertain) require one authoritative post-write read.
    this.hashActiveWave = null;
    return {
      final: true,
      needsRead: wave.writes > 1 || wave.overlapped || wave.failed,
      followUpCommand: wave.followUpCommand,
      hadFailure: wave.failed,
    };
  }

  /**
   * Only a pending row whose download can actually advance is propagated.
   * A paused/pausing/queued download would otherwise poll forever without the
   * backend ever being able to finish the hash.
   */
  private hasActivePendingHashes(): boolean {
    const pendingIds = new Set(
      (this.state.hashes ?? []).filter((row) => row.state === "pending").map((row) => row.id),
    );
    if (pendingIds.size === 0) return false;
    for (const item of this.state.items) {
      if (!pendingIds.has(item.id)) continue;
      if (item.status === "active" || item.status === "completed") return true;
    }
    return false;
  }

  private hashRefreshDelay(): number {
    const factor = 2 ** Math.min(this.hashAutoAttempts, 5);
    return Math.min(this.hashRefreshMs * factor, MAX_HASH_REFRESH_DELAY_MS);
  }

  private resetHashAutoBudget(): void {
    this.hashAutoAttempts = 0;
  }

  private stopHashAutoWithRecovery(): void {
    this.stopHashAutoRefresh();
    if (this.state.hashError !== null) return;
    this.state.hashesLoading = false;
    this.state.hashError = HASH_AUTO_STOPPED_MESSAGE;
    this.emit();
  }

  /**
   * Keep at most one propagation timer for the current managed connection.
   * It exists only while an active download or completed row still has a
   * pending hash. Attempts are bounded with backoff; exhaustion stops the
   * timer but surfaces an explicit manual-refresh recovery state.
   */
  private scheduleHashAutoRefresh(epoch = this.epoch): void {
    if (this.disposed || epoch !== this.epoch) return;
    if (this.state.phase !== "connected" || this.state.managed !== true) {
      this.stopHashAutoRefresh();
      return;
    }
    if (!this.hasActivePendingHashes()) {
      this.stopHashAutoRefresh();
      this.resetHashAutoBudget();
      return;
    }
    if (this.state.hashError !== null) {
      this.stopHashAutoRefresh();
      return;
    }
    if (this.hashAutoAttempts >= this.hashAutoMaxAttempts) {
      this.stopHashAutoWithRecovery();
      return;
    }
    if (this.hashAutoTimer !== null) return;
    this.hashAutoTimer = setTimeout(() => {
      this.hashAutoTimer = null;
      if (this.disposed || epoch !== this.epoch) return;
      if (this.state.phase !== "connected" || this.state.managed !== true) return;
      if (this.state.hashError !== null || !this.hasActivePendingHashes()) {
        this.scheduleHashAutoRefresh(epoch);
        return;
      }
      void this.runHashAutoRefresh(epoch);
    }, this.hashRefreshDelay());
  }

  private async runHashAutoRefresh(epoch: number): Promise<void> {
    if (this.disposed || epoch !== this.epoch) return;
    if (this.hashCommandInFlight(epoch)) {
      this.scheduleHashAutoRefresh(epoch);
      return;
    }
    const pending = this.hashLoadPromise;
    if (pending !== null && this.hashLoadEpoch === epoch) {
      await pending.then(
        () => {},
        () => {},
      );
      if (this.disposed || epoch !== this.epoch) return;
      this.scheduleHashAutoRefresh(epoch);
      return;
    }
    // Reload only. Computation is the backend worker's job; this path never
    // infers digests or starts/retries downloads.
    this.hashAutoAttempts += 1;
    await this.loadHashes();
    if (this.disposed || epoch !== this.epoch) return;
    this.scheduleHashAutoRefresh(epoch);
  }

  private static completionKey(id: string, addedAt: number): string {
    return `${id}\u0000${addedAt}`;
  }

  /**
   * Remember newly authoritative completions even when the hash row is not
   * visible yet (a read may be in flight, or the pending row may arrive later).
   * The candidate survives deferral so the worker pass is never silently lost.
   */
  private noteHashCompletions(previous: QueueItem[], next: QueueItem[]): void {
    if (this.state.managed !== true) {
      this.hashCompletionCandidates.clear();
      return;
    }
    const before = new Map(previous.map((item) => [item.id, item]));
    const completedKeys = new Set<string>();
    for (const item of next) {
      if (item.status !== "completed") continue;
      const key = NativeQueueController.completionKey(item.id, item.addedAt);
      completedKeys.add(key);
      if (this.hashCompletionRequested.has(key)) continue;
      const prior = before.get(item.id);
      if (
        prior !== undefined &&
        prior.status === "completed" &&
        prior.addedAt === item.addedAt
      ) {
        continue;
      }
      this.hashCompletionCandidates.add(key);
    }
    // Drop candidates whose completed row is gone from the authoritative queue.
    for (const key of this.hashCompletionCandidates) {
      if (!completedKeys.has(key)) this.hashCompletionCandidates.delete(key);
    }
  }

  private pendingCompletionKeys(): string[] {
    const pendingIds = new Set(
      (this.state.hashes ?? []).filter((row) => row.state === "pending").map((row) => row.id),
    );
    if (pendingIds.size === 0) return [];
    const keys: string[] = [];
    for (const key of this.hashCompletionCandidates) {
      if (this.hashCompletionRequested.has(key)) continue;
      const id = key.slice(0, key.indexOf("\u0000"));
      if (pendingIds.has(id)) keys.push(key);
    }
    return keys;
  }

  /**
   * A completed row with a pending hash intent is the one moment a worker
   * pass is useful. The trigger is only marked once it is actually issued;
   * while another hash IPC is in flight the candidate is kept for the next
   * application instead of being dropped.
   */
  private async maybeTriggerHashWorker(epoch: number): Promise<boolean> {
    if (this.disposed || epoch !== this.epoch) return false;
    if (this.state.phase !== "connected" || this.state.managed !== true) return false;
    // A transport error stays a manual-recovery state. An exhausted automatic
    // budget is not a transport failure: an authoritative completion is new
    // activity and may restart propagation exactly once.
    if (this.state.hashError !== null && this.state.hashError !== HASH_AUTO_STOPPED_MESSAGE) {
      return false;
    }
    const keys = this.pendingCompletionKeys();
    if (keys.length === 0) return false;
    if (this.hashCommandInFlight(epoch)) return false;
    for (const key of keys) this.hashCompletionRequested.add(key);
    this.resetHashAutoBudget();
    await this.refreshHashes();
    return true;
  }

  /**
   * Every hash read/write/default path shares this mapping so a quarantined
   * hash state always reads as an explicit unconfirmed state instead of a
   * generic transport note, while ordinary transport codes keep their meaning.
   * The returned text is always one of the constants below or a fallback:
   * backend payload text is never forwarded.
   */
  private hashErrorMessage(err: unknown, fallback: string): string {
    if (isHashUnconfirmed(err)) return HASH_UNCONFIRMED_MESSAGE;
    const text = extractCodeText(err).toLowerCase();
    if (text.includes("managedconnectionrequired")) return "Hashing needs the bundled daemon connection.";
    if (text.includes("notconnected")) return "Not connected to a daemon.";
    if (text.includes("invalidresponse")) return "Daemon returned an unexpected hash response.";
    if (text.includes("timeout")) return "Hash state timed out. Try again.";
    if (text.includes("network")) return "Cannot reach the daemon for hash state.";
    if (/\bstate\b/.test(text)) return "Hash state unavailable. Try again.";
    return fallback;
  }

  /**
   * Keep a failed post-write read from hiding the mutation uncertainty that
   * triggered it. Both messages are already sanitized; combining them keeps
   * the original warning visible while still reporting that the reconciliation
   * read failed.
   */
  private applyHashReadFailure(message: string, preserveError: boolean): void {
    const current = this.state.hashError ?? null;
    if (!preserveError || current === null) {
      this.state.hashError = message;
      return;
    }
    if (current.includes(message)) return;
    this.state.hashError = `${current} ${message}`;
  }

  private sanitizeHashDefault(raw: unknown): boolean {
    const r = (raw ?? {}) as Record<string, unknown>;
    // Backend serializes as { defaultEnabled }; accept camelCase only.
    if (typeof r.defaultEnabled === "boolean") return r.defaultEnabled;
    throw "invalidResponse";
  }

  tryClaimHashDefaultAutoLoad(): boolean {
    if (this.disposed) return false;
    if (this.state.phase !== "connected") return false;
    if (this.hashDefaultAutoLoadEpoch === this.epoch) return false;
    this.hashDefaultAutoLoadEpoch = this.epoch;
    return true;
  }

  /**
   * One-shot gate for the UI hash-row auto-load. An empty or failed result
   * must never re-trigger from an effect rerun; explicit refresh/retry paths
   * call loadHashes()/refreshHashes() directly. Reconnect (epoch bump) allows
   * exactly one new auto attempt, and the controller's bounded pending-row
   * polling/completion triggers take over from there.
   */
  tryClaimHashesAutoLoad(): boolean {
    if (this.disposed) return false;
    if (this.state.phase !== "connected") return false;
    if (!this.state.managed) return false;
    if (this.hashAutoLoadEpoch === this.epoch) return false;
    this.hashAutoLoadEpoch = this.epoch;
    return true;
  }

  async loadHashDefault(): Promise<void> {
    if (this.disposed) return;
    if (this.state.phase !== "connected") return;
    const epoch = this.epoch;
    const generation = this.hashDefaultGenerationFor();
    if (
      this.hashDefaultLoadPromise !== null &&
      this.hashDefaultLoadEpoch === epoch &&
      this.hashDefaultLoadGeneration === generation
    ) {
      return this.hashDefaultLoadPromise;
    }
    const seq = (this.hashDefaultSeq += 1);
    const task = this.executeLoadHashDefault(epoch, seq, generation);
    this.hashDefaultLoadPromise = task;
    this.hashDefaultLoadEpoch = epoch;
    this.hashDefaultLoadGeneration = generation;
    return task.then(
      () => {
        if (this.hashDefaultLoadPromise === task) {
          this.hashDefaultLoadPromise = null;
          this.hashDefaultLoadEpoch = null;
        }
      },
      () => {
        if (this.hashDefaultLoadPromise === task) {
          this.hashDefaultLoadPromise = null;
          this.hashDefaultLoadEpoch = null;
        }
      },
    );
  }

  private async executeLoadHashDefault(
    epoch: number,
    seq: number,
    generation: number,
  ): Promise<void> {
    if (this.disposed) return;
    this.state.hashDefaultLoading = true;
    // A poisoned value stays unavailable while a reload is in flight. The read
    // cannot prove a timed-out write did not commit later, so it never clears
    // that uncertainty below.
    if (!this.hashDefaultPoisoned()) this.state.hashDefaultError = null;
    this.emit();
    let raw: unknown;
    try {
      raw = await this.invoke("get_automatic_hash_settings");
    } catch (err) {
      if (epoch !== this.epoch || this.disposed) return;
      if (seq !== this.hashDefaultSeq) return;
      if (
        generation !== this.hashDefaultGenerationFor() ||
        this.hasPendingHashDefaultWrite()
      ) {
        this.clearHashDefaultLoadingForLatest(seq);
        return;
      }
      if (isSuperseded(err)) {
        this.clearHashDefaultLoadingForLatest(seq);
        return;
      }
      // Never replace a lifetime uncertainty with a transport note that could
      // imply a retry will make the saved value safe again.
      if (this.hashDefaultPoisoned()) {
        this.state.hashDefaultEnabled = null;
        this.state.hashDefaultUnconfirmed = true;
        this.state.hashDefaultError = this.activeHashDefaultPoisonMessage();
        this.state.hashDefaultLoading = false;
        this.emit();
        return;
      }
      this.state.hashDefaultError = this.hashErrorMessage(err, "Could not load the hashing default.");
      this.state.hashDefaultLoading = false;
      this.emit();
      return;
    }
    if (epoch !== this.epoch || this.disposed) return;
    if (seq !== this.hashDefaultSeq) return;
    let value: boolean;
    try {
      value = this.sanitizeHashDefault(raw);
    } catch (err) {
      if (epoch !== this.epoch || this.disposed) return;
      if (seq !== this.hashDefaultSeq) return;
      if (
        generation !== this.hashDefaultGenerationFor() ||
        this.hasPendingHashDefaultWrite()
      ) {
        this.clearHashDefaultLoadingForLatest(seq);
        return;
      }
      if (this.hashDefaultPoisoned()) {
        this.state.hashDefaultEnabled = null;
        this.state.hashDefaultUnconfirmed = true;
        this.state.hashDefaultError = this.activeHashDefaultPoisonMessage();
        this.state.hashDefaultLoading = false;
        this.emit();
        return;
      }
      this.state.hashDefaultError = this.hashErrorMessage(err, "Could not load the hashing default.");
      this.state.hashDefaultLoading = false;
      this.emit();
      return;
    }
    if (epoch !== this.epoch || this.disposed) return;
    if (seq !== this.hashDefaultSeq) return;
    // A write settled after this read began, or another write is still
    // outstanding. This snapshot cannot prove the persisted default.
    if (
      generation !== this.hashDefaultGenerationFor() ||
      this.hasPendingHashDefaultWrite()
    ) {
      this.clearHashDefaultLoadingForLatest(seq);
      return;
    }
    // Any ambiguous default write remains uncertain for this controller and
    // every controller sharing this module store. A read that happens to match
    // the attempted target is not a completion fence: the timed-out write may
    // still commit later and change the stored value.
    if (this.hashDefaultPoisoned()) {
      this.state.hashDefaultEnabled = null;
      this.state.hashDefaultUnconfirmed = true;
      this.state.hashDefaultError = this.activeHashDefaultPoisonMessage();
      this.state.hashDefaultLoading = false;
      this.emit();
      return;
    }
    this.state.hashDefaultEnabled = value;
    this.state.hashDefaultError = null;
    this.state.hashDefaultLoading = false;
    this.emit();
  }

  async setHashDefault(enabled: boolean): Promise<boolean> {
    if (this.disposed) return false;
    if (this.state.phase !== "connected") return false;
    if (typeof enabled !== "boolean") return false;
    // Once any write outcome is ambiguous, the persisted value is unknowable
    // for this controller and every controller sharing this module store.
    // Reject before IPC; a reload cannot make it safe.
    if (this.hashDefaultPoisoned()) return false;
    // The default is app-wide, so only one mutation may be in flight globally,
    // across managed and manual connections and across epochs.
    if (this.hasPendingHashDefaultWrite()) return false;
    const epoch = this.epoch;
    const order = (this.hashDefaultStore.writeSeq += 1);
    const record: HashDefaultWriteRecord = {
      order,
      epoch,
      target: enabled,
      settled: false,
      owner: this.hashDefaultOwner,
    };
    this.hashDefaultStore.pendingWrites.set(order, record);
    // Invalidate every read that started before this mutation, then again at
    // settlement for reads that started while it was in flight.
    this.bumpHashDefaultGeneration();
    this.state.hashDefaultSaving = true;
    this.state.hashDefaultError = null;
    this.emit();
    // Let every other live controller observe the shared in-flight mutation.
    // This happens before the IPC await so an implicit batch cannot use their
    // stale baseline while this write is unresolved.
    this.publishHashDefaultStoreEvent({ kind: "started", record });
    try {
      const raw = await this.invoke("set_automatic_hash_default", { enabled });
      const value = this.sanitizeHashDefault(raw);
      this.settleHashDefaultWrite(record);
      if (this.disposed) return false;
      if (record.epoch !== this.epoch) {
        // The acknowledgement is a definite completion of this write, but the
        // current epoch's baseline may predate that commit. Invalidate it and
        // require a fresh read; this is not an ambiguous outcome.
        this.invalidateHashDefaultBaseline(
          this.hashDefaultPoisoned()
            ? this.activeHashDefaultPoisonMessage()
            : this.activeHashDefaultUncertainMessage(),
        );
        this.state.hashDefaultUnconfirmed = this.hashDefaultPoisoned();
        this.refreshHashDefaultSaving();
        this.emit();
        return false;
      }
      // A successful acknowledgement of this exact write is the current value.
      // It never clears uncertainty from an earlier ambiguous write; such a
      // write was rejected before IPC above.
      this.state.hashDefaultEnabled = value;
      this.state.hashDefaultError = null;
      this.state.hashDefaultUnconfirmed = this.hashDefaultPoisoned();
      this.refreshHashDefaultSaving();
      this.emit();
      return true;
    } catch (err) {
      // The save may have applied before this error crossed IPC (transport,
      // malformed reply, timeout or supersession), so the prior value is no
      // longer trustworthy. Any such outcome poisons all modes and epochs for
      // this controller and every controller sharing this module store: a later
      // read matching the target is not proof the write completed, and the
      // write may still commit later. Marking happens inside settlement, before
      // the disposed guard, so a response arriving after disposal still blocks
      // every other controller.
      this.settleHashDefaultWrite(record, true);
      if (this.disposed) return false;
      this.invalidateHashDefaultBaseline(this.activeHashDefaultPoisonMessage());
      this.state.hashDefaultUnconfirmed = true;
      this.refreshHashDefaultSaving();
      this.emit();
      return false;
    }
  }

  async loadHashes(): Promise<void> {
    if (this.disposed) return;
    if (this.state.phase !== "connected") return;
    // Manual connections never surface another daemon's digests.
    if (!this.state.managed) {
      this.stopHashAutoRefresh();
      this.state.hashes = [];
      this.state.hashError = null;
      this.state.hashesLoading = false;
      this.emit();
      return;
    }
    if (this.hashLoadPromise !== null && this.hashLoadEpoch === this.epoch) {
      return this.hashLoadPromise;
    }
    const epoch = this.epoch;
    const seq = (this.hashLoadSeq += 1);
    const order = (this.hashOrderSeq += 1);
    const overlap = this.captureHashWriteOverlap();
    this.noteHashReadOverlap(overlap, "get_download_hashes");
    const task = this.executeLoadHashes(epoch, seq, order, "get_download_hashes", overlap);
    this.hashLoadPromise = task;
    this.hashLoadEpoch = epoch;
    return task.then(
      () => {
        if (this.hashLoadPromise === task) {
          this.hashLoadPromise = null;
          this.hashLoadEpoch = null;
        }
      },
      () => {
        if (this.hashLoadPromise === task) {
          this.hashLoadPromise = null;
          this.hashLoadEpoch = null;
        }
      },
    );
  }

  async refreshHashes(): Promise<void> {
    if (this.disposed) return;
    if (this.state.phase !== "connected") return;
    if (!this.state.managed) {
      this.stopHashAutoRefresh();
      this.state.hashes = [];
      this.state.hashError = null;
      this.state.hashesLoading = false;
      this.emit();
      return;
    }
    const epoch = this.epoch;
    const seq = (this.hashLoadSeq += 1);
    const order = (this.hashOrderSeq += 1);
    const overlap = this.captureHashWriteOverlap();
    this.noteHashReadOverlap(overlap, "refresh_download_hashes");
    this.resetHashAutoBudget();
    await this.executeLoadHashes(
      epoch,
      seq,
      order,
      "refresh_download_hashes",
      overlap,
    );
  }

  private async executeLoadHashes(
    epoch: number,
    seq: number,
    order: number,
    command: string,
    overlaps: HashWriteRecord[],
    preserveError = false,
  ): Promise<void> {
    if (this.disposed) return;
    this.state.hashesLoading = true;
    this.emit();
    let raw: unknown;
    this.beginHashCommand(epoch);
    try {
      raw = await this.invoke(command);
    } catch (err) {
      if (epoch !== this.epoch || this.disposed) return;
      if (seq !== this.hashLoadSeq) return;
      if (order < this.hashAppliedSeq) {
        this.state.hashesLoading = false;
        this.emit();
        return;
      }
      if (this.hashReadOverlappedSettledWrite(overlaps)) {
        this.state.hashesLoading = false;
        this.emit();
        return;
      }
      if (isSuperseded(err)) return;
      this.applyHashReadFailure(
        this.hashErrorMessage(err, "Could not load hashes."),
        preserveError,
      );
      this.state.hashesLoading = false;
      // A failed automatic read must not form a retry loop; the warning stays
      // visible until the user retries or a later explicit action succeeds.
      this.stopHashAutoRefresh();
      this.emit();
      return;
    } finally {
      this.endHashCommand(epoch);
    }
    if (epoch !== this.epoch || this.disposed) return;
    if (seq !== this.hashLoadSeq) return;
    if (order < this.hashAppliedSeq) {
      // A newer intent write/read already painted; never regress to this row set.
      this.state.hashesLoading = false;
      this.emit();
      return;
    }
    if (this.hashReadOverlappedSettledWrite(overlaps)) {
      // This read was issued while a write was pending and settled first. Its
      // snapshot may predate the commit; the write's response and follow-up
      // read are authoritative.
      this.state.hashesLoading = false;
      this.emit();
      return;
    }
    let rows: DownloadHash[];
    try {
      rows = sanitizeDownloadHashes(raw);
    } catch (err) {
      // Reject a malformed top-level payload before advancing either sequence
      // or replacing the already validated rows. A legitimate empty array is
      // still a valid response and is handled normally below.
      if (epoch !== this.epoch || this.disposed) return;
      if (seq !== this.hashLoadSeq) return;
      if (isSuperseded(err)) return;
      this.applyHashReadFailure(
        this.hashErrorMessage(err, "Could not load hashes."),
        preserveError,
      );
      this.state.hashesLoading = false;
      this.stopHashAutoRefresh();
      this.emit();
      return;
    }
    this.hashAppliedSeq = order;
    if (overlaps.length === 0) this.hashAuthoritativeSeq = order;
    // Manual connections never retain digests, even if a late managed
    // response arrives after a connection switch.
    if (!this.state.managed) {
      this.stopHashAutoRefresh();
      this.state.hashes = [];
      this.state.hashError = null;
      this.state.hashesLoading = false;
      this.emit();
      return;
    }
    this.state.hashes = rows;
    if (!preserveError) this.state.hashError = null;
    this.state.hashesLoading = false;
    this.emit();
    // A failed write keeps its error visible as a manual-recovery state; the
    // reconciliation read only refreshes the authoritative rows.
    if (preserveError) return;
    // A pending row that arrives after the completed queue row still gets its
    // one worker pass; otherwise keep the active propagation timer.
    const triggered = await this.maybeTriggerHashWorker(epoch);
    if (this.disposed || epoch !== this.epoch) return;
    if (!triggered) this.scheduleHashAutoRefresh(epoch);
  }

  private async followUpAfterHashWrite(
    epoch: number,
    command: string,
    preserveError = false,
  ): Promise<void> {
    if (this.disposed || epoch !== this.epoch) return;
    if (this.state.phase !== "connected" || this.state.managed !== true) return;
    const seq = (this.hashLoadSeq += 1);
    const order = (this.hashOrderSeq += 1);
    const task = this.executeLoadHashes(epoch, seq, order, command, [], preserveError);
    this.hashLoadPromise = task;
    this.hashLoadEpoch = epoch;
    await task.then(
      () => {
        if (this.hashLoadPromise === task) {
          this.hashLoadPromise = null;
          this.hashLoadEpoch = null;
        }
      },
      () => {
        if (this.hashLoadPromise === task) {
          this.hashLoadPromise = null;
          this.hashLoadEpoch = null;
        }
      },
    );
  }

  async setHashIntents(intents: Array<{ id: string; enabled: boolean }>): Promise<boolean> {
    if (this.disposed) return false;
    if (this.state.phase !== "connected") return false;
    if (!Array.isArray(intents) || intents.length > 100) return false;
    for (const intent of intents) {
      if (intent === null || typeof intent !== "object") return false;
      const row = intent as Record<string, unknown>;
      if (typeof row.id !== "string" || row.id.length === 0) return false;
      if (typeof row.enabled !== "boolean") return false;
    }
    const epoch = this.epoch;
    const order = (this.hashOrderSeq += 1);
    if (this.hashActiveWave === null) {
      this.hashActiveWave = {
        id: (this.hashWaveSeq += 1),
        writes: 0,
        settled: 0,
        overlapped: false,
        followUpCommand: null,
        failed: false,
      };
    }
    const wave = this.hashActiveWave;
    wave.writes += 1;
    const writeRecord: HashWriteRecord = {
      order,
      settled: false,
      followUpCommand: null,
      waveId: wave.id,
    };
    this.hashPendingWrites.set(order, writeRecord);
    let raw: unknown;
    this.beginHashCommand(epoch);
    try {
      raw = await this.invoke("set_download_hash_intents", {
        intents: intents.map((intent) => ({ id: intent.id, enabled: intent.enabled })),
      });
    } catch (err) {
      // A failed write is an uncertain outcome, not a no-op: the daemon may
      // have committed it before the transport failed. Settle the wave before
      // returning so a final failure still issues exactly one authoritative
      // reconciliation read (never a retried mutation). The read is required
      // even when this older request settles after a newer success: request
      // order does not make an unconfirmed mutation disappear.
      const settlement = this.settleHashWrite(writeRecord, true);
      if (epoch !== this.epoch || this.disposed) return false;
      if (isSuperseded(err)) return false;
      this.state.hashError = this.hashErrorMessage(err, "Could not update hash choices.");
      this.stopHashAutoRefresh();
      this.emit();
      if (settlement.final) {
        await this.followUpAfterHashWrite(
          epoch,
          settlement.followUpCommand ?? "get_download_hashes",
          true,
        );
      }
      return false;
    } finally {
      this.endHashCommand(epoch);
    }
    let rows: DownloadHash[] | null = null;
    let malformed = false;
    let malformedError: unknown = null;
    try {
      rows = sanitizeDownloadHashes(raw);
    } catch (err) {
      malformed = true;
      malformedError = err;
    }
    // A malformed response is an unconfirmed mutation, so it must fail the
    // wave before settlement; otherwise a later success would clear it.
    const settlement = this.settleHashWrite(writeRecord, malformed);
    if (epoch !== this.epoch || this.disposed) return false;
    let applied = false;
    if (malformed) {
      // The write may have committed, but this top-level response cannot be
      // trusted as row state. Keep the existing rows and sequences, surface
      // the safe protocol error, and let the bounded follow-up read decide.
      this.applyHashReadFailure(
        this.hashErrorMessage(malformedError, "Could not update hash choices."),
        settlement.hadFailure,
      );
      this.stopHashAutoRefresh();
      this.emit();
    } else if (rows !== null && order >= this.hashAuthoritativeSeq) {
      applied = true;
      this.hashAppliedSeq = Math.max(this.hashAppliedSeq, order);
      this.hashAuthoritativeSeq = order;
      if (!this.state.managed) {
        this.stopHashAutoRefresh();
        this.state.hashes = [];
        this.emit();
        return true;
      }
      this.state.hashes = rows;
      // A later success in the same wave does not erase an earlier write
      // failure: the wave's authoritative follow-up read reconciles the rows
      // while the sanitized error stays visible for manual recovery.
      if (!settlement.hadFailure) this.state.hashError = null;
      this.emit();
      this.resetHashAutoBudget();
    }
    // An earlier write in a concurrent wave must not close the wave or regress
    // the newer response. The last write to settle performs exactly one read,
    // so the committed rows from every write are visible.
    // A malformed/unknown mutation response never counts as applied, even
    // when a later write in an overlapping wave reconciles successfully.
    if (!settlement.final) return !malformed;
    if (settlement.needsRead || malformed) {
      await this.followUpAfterHashWrite(
        epoch,
        settlement.followUpCommand ?? "get_download_hashes",
        settlement.hadFailure || malformed,
      );
      return !malformed;
    }
    // A single write still seeds provider triggers from its own response, but
    // a response fenced behind a newer read must not trigger anything.
    if (!applied) return true;
    const triggered = await this.maybeTriggerHashWorker(epoch);
    if (this.disposed || epoch !== this.epoch) return true;
    if (!triggered) this.scheduleHashAutoRefresh(epoch);
    return true;
  }

  private stopTimer(): void {
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private startTimer(): void {
    this.stopTimer();
    if (this.disposed) return;
    const ms = Number.isFinite(this.pollMs) && this.pollMs > 0 ? this.pollMs : 5000;
    this.timer = setInterval(() => {
      void this.refresh();
    }, ms);
  }

  /**
   * Register the three stream listeners once, before the connection command is
   * sent so no ready/progress frame can arrive before the renderer is
   * listening. Registration is bounded: a hung listen never blocks the queue,
   * a partial/timed-out attempt is unlistened (including late resolutions),
   * and a failure is retried on the next reconnect. Disposal unlistens every
   * registered callback.
   */
  private ensureStreamListener(): Promise<void> {
    if (this.disposed || this.streamListen === null) return Promise.resolve();
    if (this.listenerReady) return Promise.resolve();
    if (this.listenerPromise !== null) return this.listenerPromise;
    const listen = this.streamListen;
    const registered: Array<() => void> = [];
    const deadline = Date.now() + this.listenerTimeoutMs;
    const attempt = {};
    this.listenerAttempt = attempt;
    const run = (async () => {
      const registrations: Array<[string, (event: { payload: unknown }) => void]> = [
        [PROGRESS_EVENT, (event) => this.onProgressPayload(event.payload)],
        [LIFECYCLE_EVENT, (event) => this.onLifecyclePayload(event.payload)],
        [STREAM_STATE_EVENT, (event) => this.onStreamStatePayload(event.payload)],
      ];
      try {
        for (const [event, handler] of registrations) {
          if (this.disposed) return;
          const remaining = deadline - Date.now();
          if (remaining <= 0) throw new Error("listener timeout");
          const unlisten = await this.awaitListener(listen(event, handler), remaining);
          if (this.disposed) {
            try {
              unlisten();
            } catch {
              // Best-effort cleanup.
            }
            return;
          }
          registered.push(unlisten);
        }
        this.unlisteners.push(...registered.splice(0));
        this.listenerReady = true;
      } catch {
        // Streaming is an enhancement; the existing polling fallback stays and
        // the next reconnect may retry the whole registration.
      } finally {
        for (const unlisten of registered.splice(0)) {
          try {
            unlisten();
          } catch {
            // Best-effort cleanup.
          }
        }
        if (this.listenerAttempt === attempt) {
          this.listenerAttempt = null;
          this.listenerPromise = null;
        }
      }
    })();
    if (this.listenerAttempt === attempt) {
      this.listenerPromise = run;
    }
    return run;
  }

  /**
   * Resolve a listener registration within `remaining` ms. A registration that
   * resolves after the timeout is unlistened immediately so a timed-out
   * attempt cannot leak a listener into the active set.
   */
  private awaitListener(
    pending: Promise<() => void>,
    remaining: number,
  ): Promise<() => void> {
    return new Promise((resolve, reject) => {
      let settled = false;
      const timer = setTimeout(() => {
        if (settled) return;
        settled = true;
        reject(new Error("listener timeout"));
      }, Math.max(1, remaining));
      pending.then(
        (unlisten) => {
          if (settled) {
            try {
              unlisten();
            } catch {
              // Best-effort cleanup for a late registration.
            }
            return;
          }
          settled = true;
          clearTimeout(timer);
          resolve(unlisten);
        },
        (error) => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          reject(error);
        },
      );
    });
  }

  private clearStreamState(): void {
    this.streamSubscription = null;
    this.snapshotCursor = 0;
    this.lastLifecycleSeq = 0;
    this.lastStateSeq = 0;
    this.progressSeqById.clear();
    this.progressQuarantined.clear();
    this.pendingSnapshotConfirmation.clear();
    this.reconcileQueued = false;
    this.reconcileRetryAttempt = 0;
    if (this.reconcileRetryTimer !== null) {
      clearTimeout(this.reconcileRetryTimer);
      this.reconcileRetryTimer = null;
    }
    this.state.streamState = "off";
  }

  private async startProgressStream(epoch: number): Promise<void> {
    if (this.disposed || this.streamListen === null || epoch !== this.epoch) return;
    // Never start the native stream before all three listeners are registered.
    // Otherwise ready/progress frames can be lost while the state is stuck on
    // "starting"; the polling fallback stays active and the next reconnect
    // retries registration.
    if (!this.listenerReady) {
      this.state.streamState = "fallback";
      this.emit();
      return;
    }
    this.streamSubscription = epoch;
    this.snapshotCursor = 0;
    this.lastLifecycleSeq = 0;
    this.lastStateSeq = 0;
    this.progressSeqById.clear();
    this.progressQuarantined.clear();
    this.pendingSnapshotConfirmation.clear();
    this.state.streamState = "starting";
    this.emit();
    try {
      await this.invoke("start_progress_stream", { subscription: epoch });
    } catch {
      if (this.disposed || epoch !== this.epoch) return;
      this.state.streamState = "fallback";
      this.emit();
    }
  }

  private onProgressPayload(payload: unknown): void {
    if (this.disposed || this.state.phase !== "connected") return;
    const batch = sanitizeProgressBatch(payload);
    if (batch === null || batch.subscription !== this.streamSubscription) return;
    if (batch.subscription !== this.epoch) return;

    const byId = new Map(this.state.items.map((item) => [item.id, item]));
    const applicable = batch.updates.filter((update) => {
      const item = byId.get(update.id);
      if (item === undefined || item.status !== "active") return false;
      // After an observed reset, a live sample may belong to the previous
      // generation but still carry a newer receipt sequence. Without a daemon
      // generation id it cannot be proven safe, so snapshots alone update this
      // row for the quarantined generation.
      const quarantinedAt = this.progressQuarantined.get(update.id);
      if (quarantinedAt !== undefined && quarantinedAt === item.addedAt) return false;
      // The snapshot cursor is captured before the list request. An update at
      // or below it is already covered by the authoritative snapshot. Do not
      // let a replayed/queued update roll newer bytes back.
      if (update.seq <= this.snapshotCursor) return false;
      const applied = this.progressSeqById.get(update.id) ?? -1;
      if (update.seq <= applied) return false;
      return update.bytesDone >= item.bytesDone;
    });
    if (applicable.length === 0) return;

    for (const update of applicable) {
      this.progressSeqById.set(update.id, update.seq);
    }
    const next = applyProgressUpdates(this.state.items, applicable);
    if (next === this.state.items) return;
    this.state.items = next;
    this.emit();
  }

  private onLifecyclePayload(payload: unknown): void {
    if (this.disposed || this.state.phase !== "connected") return;
    const notice = sanitizeLifecycleNotice(payload);
    if (notice === null || notice.subscription !== this.streamSubscription) return;
    if (notice.subscription !== this.epoch || notice.seq <= this.lastLifecycleSeq) return;
    this.lastLifecycleSeq = notice.seq;
    this.requestReconcile();
  }

  private onStreamStatePayload(payload: unknown): void {
    if (this.disposed || this.state.phase !== "connected") return;
    const notice = sanitizeStreamState(payload);
    if (notice === null || notice.subscription !== this.streamSubscription) return;
    if (notice.subscription !== this.epoch) return;
    // A delayed unavailable notice must not override a newer ready notice.
    if (notice.seq <= this.lastStateSeq) return;
    this.lastStateSeq = notice.seq;
    if (notice.state === "unavailable") {
      this.state.streamState = "fallback";
      this.cancelReconcileRetry();
      this.emit();
      return;
    }
    this.state.streamState = "live";
    this.emit();
    this.requestReconcile();
  }

  /**
   * Coalesce lifecycle/resync reconciliation. At most one snapshot is in
   * flight; a transition during the read queues exactly one follow-up read.
   * Transient snapshot failures retry with bounded backoff, then fall back to
   * the continuing 5s poll.
   */
  private requestReconcile(): void {
    if (this.disposed || this.state.phase !== "connected") return;
    if (this.reconcileRetryTimer !== null) {
      clearTimeout(this.reconcileRetryTimer);
      this.reconcileRetryTimer = null;
    }
    this.reconcileRetryAttempt = 0;
    this.reconcileQueued = true;
    void this.drainReconcile();
  }

  private async drainReconcile(): Promise<void> {
    if (this.reconcileRunning || this.disposed) return;
    this.reconcileRunning = true;
    try {
      while (!this.disposed && this.reconcileQueued) {
        const epoch = this.epoch;
        if (this.state.phase !== "connected") break;
        this.reconcileQueued = false;
        // A read may already be in flight (poll, control, add). Wait for it so
        // the queued lifecycle transition still gets one fresh authoritative
        // read on the current epoch; never bypass the single-flight guard.
        const pending = this.refreshPromise;
        if (pending !== null) {
          await pending.then(
            () => {},
            () => {},
          );
        }
        if (this.disposed || epoch !== this.epoch) break;
        const result = await this.startRefresh();
        if (this.disposed || epoch !== this.epoch) break;
        if (result.ok) {
          this.reconcileRetryAttempt = 0;
          continue;
        }
        const code = (result.code ?? "").toLowerCase();
        if (!TRANSIENT_REFRESH_CODES.has(code)) break;
        const delay = this.reconcileRetryMs[this.reconcileRetryAttempt];
        if (delay === undefined) break;
        this.reconcileRetryAttempt += 1;
        this.reconcileRetryTimer = setTimeout(() => {
          this.reconcileRetryTimer = null;
          if (
            this.disposed ||
            epoch !== this.epoch ||
            this.state.phase !== "connected"
          ) {
            return;
          }
          this.reconcileQueued = true;
          void this.drainReconcile();
        }, delay);
        break;
      }
    } finally {
      this.reconcileRunning = false;
      // The drain may have started on an epoch that was replaced while its
      // read was blocked. Queued work for the new epoch must still run once
      // the old drain releases its single-flight flag.
      if (
        !this.disposed &&
        this.reconcileQueued &&
        this.state.phase === "connected"
      ) {
        void this.drainReconcile();
      }
    }
  }

  private cancelReconcileRetry(): void {
    this.reconcileRetryAttempt = 0;
    this.reconcileQueued = false;
    if (this.reconcileRetryTimer !== null) {
      clearTimeout(this.reconcileRetryTimer);
      this.reconcileRetryTimer = null;
    }
  }

  async connect(endpoint: string, token: string): Promise<void> {
    if (this.disposed) return;
    this.epoch += 1;
    this.captureAbandonedBatch();
    const epoch = this.epoch;
    this.clearStreamState();
    this.resetBulkState();
    this.stopTimer();
    this.addEpoch = null;
    this.managedEpoch = null;
    this.managedStoreEpoch = null;
    this.rateWriteEpoch = null;
    this.rateLoadPromise = null;
    this.rateLoadEpoch = null;
    this.maxConcurrentWriteEpoch = null;
    this.maxConcurrentLoadPromise = null;
    this.maxConcurrentLoadEpoch = null;
    this.batchEpoch = null;
    this.batchIdentity = null;
    this.state.batch = null;
    this.locationEpoch = null;
    this.state.phase = "connecting";
    this.state.items = [];
    this.state.refreshing = true;
    this.state.error = null;
    this.state.controllingId = null;
    this.state.adding = false;
    this.state.addOutcome = null;
    this.state.batch = null;
    this.resetMaxConcurrentState();
    this.resetHashState();
    this.state.managed = false;
    this.state.rateLimitBps = null;
    this.state.rateLoading = false;
    this.state.rateSaving = false;
    if (!this.ratePoisoned) this.state.rateError = null;
    else {
      this.state.rateUnconfirmed = true;
      this.state.rateError = RATE_UNCONFIRMED_MESSAGE;
    }
    // rateUnconfirmed/poison persists until the app restarts.
    this.state.locationOpeningId = null;
    this.state.locationError = null;
    this.state.locationErrorId = null;
    this.emit();
    if (this.streamListen !== null) {
      await this.ensureStreamListener();
      if (epoch !== this.epoch || this.disposed) return;
    }
    let raw: unknown;
    try {
      raw = await this.invoke("connect_surge", { endpoint, token });
    } catch (err) {
      if (epoch !== this.epoch || this.disposed) return;
      this.state.phase = "error";
      this.state.items = [];
      this.state.error = mapToSafeMessage(err, CONNECT_FALLBACK) ?? CONNECT_FALLBACK;
      this.state.refreshing = false;
      this.state.controllingId = null;
      this.state.adding = false;
      this.state.addOutcome = null;
      this.state.batch = null;
      this.resetMaxConcurrentState();
      this.state.managed = false;
      this.state.rateLimitBps = null;
      this.state.rateLoading = false;
      this.state.rateSaving = false;
      if (!this.ratePoisoned) this.state.rateError = null;
      else {
        this.state.rateUnconfirmed = true;
        this.state.rateError = RATE_UNCONFIRMED_MESSAGE;
      }
      this.state.locationOpeningId = null;
      this.state.locationError = null;
      this.state.locationErrorId = null;
      this.emit();
      return;
    }
    if (epoch !== this.epoch || this.disposed) return;
    let items: QueueItem[];
    try {
      items = sanitizeList(raw);
    } catch (err) {
      this.state.phase = "error";
      this.state.items = [];
      this.state.error = mapToSafeMessage(err, CONNECT_FALLBACK) ?? CONNECT_FALLBACK;
      this.state.refreshing = false;
      this.state.controllingId = null;
      this.state.adding = false;
      this.state.addOutcome = null;
      this.state.batch = null;
      this.resetMaxConcurrentState();
      this.state.managed = false;
      this.state.rateLimitBps = null;
      this.state.rateLoading = false;
      this.state.rateSaving = false;
      if (!this.ratePoisoned) this.state.rateError = null;
      else {
        this.state.rateUnconfirmed = true;
        this.state.rateError = RATE_UNCONFIRMED_MESSAGE;
      }
      this.state.locationOpeningId = null;
      this.state.locationError = null;
      this.state.locationErrorId = null;
      this.emit();
      return;
    }
    this.state.phase = "connected";
    this.state.items = items;
    this.state.error = null;
    this.state.refreshing = false;
    this.state.controllingId = null;
    this.state.adding = false;
    this.state.addOutcome = null;
    this.state.batch = null;
    this.resetMaxConcurrentState();
    this.resetHashState();
    this.state.managed = false;
    this.state.rateLimitBps = null;
    this.state.rateLoading = false;
    this.state.rateSaving = false;
    if (!this.ratePoisoned) this.state.rateError = null;
    else {
      this.state.rateUnconfirmed = true;
      this.state.rateError = RATE_UNCONFIRMED_MESSAGE;
    }
    this.state.locationOpeningId = null;
    this.state.locationError = null;
    this.state.locationErrorId = null;
    this.emit();
    this.flushBulkReconcile();
    this.startTimer();
    await this.startProgressStream(epoch);
  }

  /**
   * Start the single bundled managed daemon.
   *
   * - Frontend supplies no args: `start_managed_surge` derives all paths
   *   from Tauri resolvers and returns the same sanitized queue DTO.
   * - Same epoch fence as manual `connect`: bumps `epoch`, clears any
   *   in-flight manual rows, and discards late results after
   *   `connect`/`disconnect`/`dispose`.
   * - Same-epoch duplicate starts issue exactly one IPC and report `false`;
   *   disposal/supersession never emit stale callbacks.
   * - Failures map to the sanitized `MANAGED_FALLBACK`; no endpoint, token,
   *   URL, path, or raw transport detail ever reaches state or logs.
   */
  async startManaged(): Promise<boolean> {
    if (this.disposed) return false;
    if (this.managedEpoch === this.epoch) return false;
    this.epoch += 1;
    this.captureAbandonedBatch();
    const epoch = this.epoch;
    this.managedEpoch = epoch;
    this.managedStoreEpoch = epoch;
    this.clearStreamState();
    this.resetBulkState();
    this.stopTimer();
    this.addEpoch = null;
    this.rateWriteEpoch = null;
    this.rateLoadPromise = null;
    this.rateLoadEpoch = null;
    this.maxConcurrentWriteEpoch = null;
    this.maxConcurrentLoadPromise = null;
    this.maxConcurrentLoadEpoch = null;
    this.batchEpoch = null;
    this.batchIdentity = null;
    this.state.batch = null;
    this.locationEpoch = null;
    this.state.phase = "connecting";
    this.state.items = [];
    this.state.refreshing = true;
    this.state.error = null;
    this.state.controllingId = null;
    this.state.adding = false;
    this.state.addOutcome = null;
    this.state.batch = null;
    this.resetMaxConcurrentState();
    this.resetHashState();
    this.state.managed = false;
    this.state.rateLimitBps = null;
    this.state.rateLoading = false;
    this.state.rateSaving = false;
    if (!this.ratePoisoned) this.state.rateError = null;
    else {
      this.state.rateUnconfirmed = true;
      this.state.rateError = RATE_UNCONFIRMED_MESSAGE;
    }
    this.state.locationOpeningId = null;
    this.state.locationError = null;
    this.state.locationErrorId = null;
    this.emit();
    if (this.streamListen !== null) {
      await this.ensureStreamListener();
      if (epoch !== this.epoch || this.disposed) return false;
    }
    try {
      let raw: unknown;
      try {
        raw = await this.invoke("start_managed_surge");
      } catch (err) {
        if (epoch !== this.epoch || this.disposed) return false;
        if (this.managedEpoch !== epoch) return false;
        this.state.phase = "error";
        this.state.items = [];
        this.state.error = mapManagedStartError(err);
        this.state.refreshing = false;
        this.state.controllingId = null;
        this.state.adding = false;
        this.state.addOutcome = null;
        this.state.batch = null;
        this.resetMaxConcurrentState();
        this.state.managed = false;
        this.state.rateLimitBps = null;
        this.state.rateLoading = false;
        this.state.rateSaving = false;
        if (!this.ratePoisoned) this.state.rateError = null;
        else {
          this.state.rateUnconfirmed = true;
          this.state.rateError = RATE_UNCONFIRMED_MESSAGE;
        }
        this.state.locationOpeningId = null;
        this.state.locationError = null;
        this.state.locationErrorId = null;
        this.emit();
        return false;
      }
      if (epoch !== this.epoch || this.disposed) return false;
      if (this.managedEpoch !== epoch) return false;
      let items: QueueItem[];
      try {
        items = sanitizeList(raw);
      } catch (err) {
        if (epoch !== this.epoch || this.disposed) return false;
        if (this.managedEpoch !== epoch) return false;
        this.state.phase = "error";
        this.state.items = [];
        this.state.error = mapManagedStartError(err);
        this.state.refreshing = false;
        this.state.controllingId = null;
        this.state.adding = false;
        this.state.addOutcome = null;
        this.state.batch = null;
        this.resetMaxConcurrentState();
        this.state.managed = false;
        this.state.rateLimitBps = null;
        this.state.rateLoading = false;
        this.state.rateSaving = false;
        if (!this.ratePoisoned) this.state.rateError = null;
        else {
          this.state.rateUnconfirmed = true;
          this.state.rateError = RATE_UNCONFIRMED_MESSAGE;
        }
        this.state.locationOpeningId = null;
        this.state.locationError = null;
        this.state.locationErrorId = null;
        this.emit();
        return false;
      }
      if (epoch !== this.epoch || this.disposed) return false;
      if (this.managedEpoch !== epoch) return false;
      this.state.phase = "connected";
      this.state.items = items;
      this.state.error = null;
      this.state.refreshing = false;
      this.state.controllingId = null;
      this.state.adding = false;
      this.state.addOutcome = null;
      this.state.batch = null;
      this.resetMaxConcurrentState();
      this.resetHashState();
      this.state.managed = true;
      // The initial snapshot can already contain completed rows; remember them
      // so a hash row that arrives later still triggers one worker pass.
      this.noteHashCompletions([], items);
      this.state.rateLimitBps = null;
      this.state.rateLoading = false;
      this.state.rateSaving = false;
      if (!this.ratePoisoned) this.state.rateError = null;
      else {
        this.state.rateUnconfirmed = true;
        this.state.rateError = RATE_UNCONFIRMED_MESSAGE;
      }
      this.state.locationOpeningId = null;
      this.state.locationError = null;
      this.state.locationErrorId = null;
      this.emit();
      this.flushBulkReconcile();
      this.startTimer();
      await this.startProgressStream(epoch);
      return true;
    } finally {
      if (this.managedEpoch === epoch) this.managedEpoch = null;
    }
  }

  async refresh(): Promise<void> {
    if (this.disposed) return;
    if (this.refreshPromise !== null && this.refreshEpoch === this.epoch) {
      await this.refreshPromise;
      return;
    }
    await this.startRefresh();
  }

  private startRefresh(): Promise<RefreshResult> {
    if (this.disposed) return Promise.resolve({ ok: true, code: null });
    // Single-flight per connection epoch: every path (poll, control, add,
    // reconcile) joins the existing read instead of starting a duplicate.
    if (this.refreshPromise !== null && this.refreshEpoch === this.epoch) {
      return this.refreshPromise;
    }
    const epoch = this.epoch;
    const seq = (this.refreshSeq += 1);
    const task = this.executeRefresh(epoch, seq);
    this.refreshPromise = task;
    this.refreshEpoch = epoch;
    return task.then(
      (result) => {
        if (this.refreshPromise === task) {
          this.refreshPromise = null;
          this.refreshEpoch = null;
        }
        return result;
      },
      () => {
        if (this.refreshPromise === task) {
          this.refreshPromise = null;
          this.refreshEpoch = null;
        }
        return { ok: false, code: "state" };
      },
    );
  }

  private async executeRefresh(epoch: number, seq: number): Promise<RefreshResult> {
    if (this.disposed) return { ok: true, code: null };
    this.state.refreshing = true;
    this.state.error = null;
    this.emit();
    let raw: unknown;
    try {
      raw = await this.invoke("list_downloads_snapshot");
    } catch (err) {
      if (epoch !== this.epoch || this.disposed) return { ok: true, code: null };
      if (seq !== this.refreshSeq) return { ok: true, code: null };
      this.state.error = mapToSafeMessage(err, REFRESH_FALLBACK) ?? REFRESH_FALLBACK;
      this.state.refreshing = false;
      this.emit();
      return { ok: false, code: extractCodeText(err) };
    }
    if (epoch !== this.epoch || this.disposed) return { ok: true, code: null };
    if (seq !== this.refreshSeq) return { ok: true, code: null };
    let items: QueueItem[];
    let cursor = 0;
    try {
      const snapshot = sanitizeSnapshotPayload(raw);
      if (snapshot === null) throw "invalidResponse";
      items = sanitizeList(snapshot.items);
      cursor = snapshot.streamSeq;
    } catch (err) {
      if (epoch !== this.epoch || this.disposed) return { ok: true, code: null };
      if (seq !== this.refreshSeq) return { ok: true, code: null };
      this.state.error = mapToSafeMessage(err, REFRESH_FALLBACK) ?? REFRESH_FALLBACK;
      this.state.refreshing = false;
      this.emit();
      return { ok: false, code: extractCodeText(err) };
    }
    if (epoch !== this.epoch || this.disposed) return { ok: true, code: null };
    if (seq !== this.refreshSeq) return { ok: true, code: null };
    this.state.phase = "connected";
    // Capture the current suspicion set before this read replaces it, so a
    // fresh follow-up read that is still ambiguous does not trigger an
    // unbounded chain of reads. Only a newly suspected id starts one.
    const previouslyPending = new Set(this.pendingSnapshotConfirmation);
    const previousItems = this.state.items;
    const merged = mergeSnapshotWithProgress(
      previousItems,
      items,
      cursor,
      this.progressSeqById,
    );
    this.state.items = merged.items;
    const snapshotById = new Map(merged.items.map((item) => [item.id, item]));
    for (const id of merged.rolledBack) {
      const item = snapshotById.get(id);
      if (item !== undefined && item.status === "active") {
        this.progressQuarantined.set(id, item.addedAt);
      } else {
        this.progressQuarantined.delete(id);
      }
    }
    // Quarantine lasts for the reset generation only. A new `addedAt`, an
    // absent row, or an actual terminal status (`completed`/`failed`) is
    // provably a different state, so live events may be trusted again for a
    // replacement generation. Paused/pausing (and other non-terminal states)
    // are not terminal: the same id+addedAt stays quarantined across
    // pause/resume, otherwise a delayed old-generation sample would inflate
    // the reset after resume.
    for (const [id, addedAt] of this.progressQuarantined) {
      const item = snapshotById.get(id);
      if (
        item === undefined ||
        item.addedAt !== addedAt ||
        item.status === "completed" ||
        item.status === "failed"
      ) {
        this.progressQuarantined.delete(id);
      }
    }
    const nextPending = new Set<string>();
    for (const id of merged.pendingConfirmation) {
      const item = snapshotById.get(id);
      if (item !== undefined && item.status === "active") nextPending.add(id);
    }
    this.pendingSnapshotConfirmation = nextPending;
    const newlyPending = [...nextPending].some((id) => !previouslyPending.has(id));
    this.snapshotCursor = Math.max(this.snapshotCursor, cursor);
    for (const [id, appliedSeq] of this.progressSeqById) {
      if (appliedSeq <= this.snapshotCursor) this.progressSeqById.delete(id);
    }
    this.state.error = null;
    this.state.refreshing = false;
    this.emit();
    // A completion observed in this authoritative read is the only point where
    // the hash worker is nudged. The candidate is recorded even when the hash
    // row is not visible yet (an in-flight read or a later pending row), so the
    // deferred trigger survives until it is actually issued.
    this.noteHashCompletions(previousItems, merged.items);
    const triggered = await this.maybeTriggerHashWorker(epoch);
    if (this.disposed || epoch !== this.epoch) return { ok: true, code: null };
    if (!triggered) this.scheduleHashAutoRefresh(epoch);
    if (newlyPending) {
      // Confirm the suspected rollback with a fresh authoritative read that
      // starts after this list completes; the reconcile drain is single-flight
      // and bounded-backoff, and a read that catches up clears the suspicion.
      this.requestReconcile();
    }
    return { ok: true, code: null };
  }

  async control(action: "pause" | "resume" | "removeRecord", id: string): Promise<void> {
    if (this.disposed) return;
    if (this.state.phase !== "connected") return;
    if (this.controlEpoch === this.epoch) return;
    if (this.bulkEpoch === this.epoch) return;
    this.controlEpoch = this.epoch;
    const epoch = this.epoch;
    this.state.controllingId = id;
    this.emit();
    try {
      await this.invoke("control_download", { action, id });
      if (epoch !== this.epoch || this.disposed) return;
      const prior = this.refreshPromise !== null && this.refreshEpoch === epoch ? this.refreshPromise : null;
      if (prior) {
        await prior.then(() => {}, () => {});
        if (epoch !== this.epoch || this.disposed) return;
      }
      if (epoch !== this.epoch || this.disposed) return;
      await this.startRefresh();
    } catch (err) {
      if (epoch !== this.epoch || this.disposed) return;
      this.state.error = mapToSafeMessage(err, REFRESH_FALLBACK) ?? REFRESH_FALLBACK;
      const owned = this.refreshPromise !== null && this.refreshEpoch === epoch;
      if (!owned) {
        this.state.refreshing = false;
      }
      this.emit();
    } finally {
      const ownsControl = this.controlEpoch === epoch;
      if (ownsControl) this.controlEpoch = null;
      if (ownsControl && epoch === this.epoch && !this.disposed) {
        if (this.state.controllingId === id) {
          this.state.controllingId = null;
          this.emit();
        }
      }
    }
  }

  /**
   * Apply Pause all or Resume all to the daemon's whole queue.
   *
   * A single bulk request is sent; results and errors are fenced by connection
   * epoch and operation identity, so a disconnected/reconnected outcome is
   * never shown as the new connection's summary. Whatever the outcome, the
   * queue is reconciled with a fresh daemon read that starts after the action
   * (never an in-flight pre-action snapshot). The visible busy state is held
   * until that reconciliation settles. Nothing is retried automatically.
   * If the connection changes before the daemon answers, the outcome becomes a
   * persistent, previous-connection-scoped uncertainty notice instead of being
   * attributed to the replacement connection.
   * Shares the single-control exclusion: one Pause/Resume/Remove or bulk op can
   * be in flight per connection epoch.
   */
  async bulkControl(action: BulkAction): Promise<boolean> {
    if (this.disposed) return false;
    if (!isBulkAction(action)) return false;
    if (this.state.phase !== "connected") {
      this.state.bulkSummary = failedBulkSummary(action, BULK_NOT_CONNECTED_MESSAGE);
      this.bulkSummaryEpoch = this.epoch;
      this.emit();
      return false;
    }
    const epoch = this.epoch;
    if (this.controlEpoch === epoch || this.bulkEpoch === epoch) return false;
    // Only a request that actually settled may become a persistent notice. A
    // busy/rejected call must not downgrade the in-flight operation's confirmed
    // outcome to "uncertain".
    this.noteBulkUncertainty();
    this.bulkEpoch = epoch;
    const opId = ++this.bulkSeq;
    const owns = (): boolean =>
      !this.disposed &&
      this.epoch === epoch &&
      this.bulkEpoch === epoch &&
      this.bulkSeq === opId;
    this.bulkAwaitingReply = true;
    this.state.bulkAction = action;
    this.state.bulkSummary = null;
    this.state.error = null;
    this.emit();

    let summary: BulkSummary = unconfirmedBulkSummary(action, BULK_UNCONFIRMED_MESSAGE);
    try {
      let raw: unknown;
      try {
        raw = await this.invoke("bulk_control", { action });
      } catch (err) {
        const classification = classifyBulkError(err);
        summary =
          classification === "notconnected"
            ? failedBulkSummary(action, BULK_NOT_CONNECTED_MESSAGE)
            : classification === "failed"
              ? failedBulkSummary(action, BULK_FALLBACK)
              : unconfirmedBulkSummary(action, BULK_UNCONFIRMED_MESSAGE);
      }
      // The daemon has answered (or the transport failed): from here the
      // outcome is known even while the reconciliation read is still running.
      // Only this operation may clear the flag; a late reply from a superseded
      // operation must not clear a newer operation's in-flight marker.
      if (this.bulkSeq === opId) this.bulkAwaitingReply = false;
      if (raw !== undefined) {
        try {
          summary = sanitizeBulkOutcome(raw, action);
        } catch {
          summary = unconfirmedBulkSummary(action, BULK_UNCONFIRMED_MESSAGE);
        }
      }
      if (owns()) {
        this.state.bulkSummary = summary;
        this.bulkSummaryEpoch = epoch;
        this.state.error = null;
        this.emit();
      }
      // Keep `bulkAction` set through reconciliation so the visible busy state
      // matches the controller's actual exclusion. Even a fenced or
      // unconfirmed action may have applied; reconcile with a fresh read on
      // whichever connection is current, but never attribute it to a new one.
      await this.reconcileAfterBulk();
      return owns() && summary.status === "done";
    } finally {
      if (
        this.bulkEpoch === epoch &&
        this.epoch === epoch &&
        this.bulkSeq === opId
      ) {
        this.bulkEpoch = null;
        this.bulkAwaitingReply = false;
        if (this.state.bulkAction === action) {
          this.state.bulkAction = null;
          this.emit();
        }
      }
    }
  }

  /**
   * Start a reconciliation read after a possibly applied action. If a read was
   * already in flight when the action settled, it is a pre-action snapshot for
   * this purpose: wait for it to settle, then start a new read.
   */
  private async reconcileAfterBulk(): Promise<void> {
    if (this.disposed) return;
    if (this.state.phase === "connecting") {
      // The in-flight connect owns the next read; ask for one more after it.
      this.bulkReconcilePending = true;
      return;
    }
    if (this.state.phase !== "connected") return;
    const epoch = this.epoch;
    const prior =
      this.refreshPromise !== null && this.refreshEpoch === epoch ? this.refreshPromise : null;
    if (prior) {
      await prior.then(
        () => {},
        () => {},
      );
    }
    if (this.disposed || epoch !== this.epoch || this.state.phase !== "connected") return;
    await this.startRefresh();
  }

  private async addAttempt(
    url: string,
    reportError: boolean,
    onAck?: (outcome: AddOutcome, hashWarning?: string) => void,
    hashEnabled?: boolean,
  ): Promise<AddAttempt> {
    if (this.disposed) return { kind: "stopped", code: "disposed" };
    if (this.state.phase !== "connected") return { kind: "stopped", code: "notconnected" };
    const epoch = this.epoch;
    if (this.addEpoch === epoch) return { kind: "busy" };
    this.addEpoch = epoch;
    this.state.adding = true;
    this.state.addOutcome = null;
    if (reportError) this.state.error = null;
    this.emit();
    try {
      let raw: unknown;
      try {
        // Per-batch hash choice is snapshotted once at submission. When the
        // caller passes an explicit boolean, use the options IPC so the
        // durable intent is bound to the confirmed queued id. Legacy callers
        // without a snapshot keep the single-add route unchanged.
        if (typeof hashEnabled === "boolean") {
          raw = await this.invoke("add_download_with_options", { url, hash: hashEnabled });
        } else {
          raw = await this.invoke("add_download", { url });
        }
      } catch (err) {
        if (this.disposed) return { kind: "stopped", code: "disposed" };
        if (epoch !== this.epoch || this.addEpoch !== epoch) {
          return { kind: "ambiguous", code: "superseded" };
        }
        const classification = classifyAddError(err);
        if (reportError) {
          this.state.addOutcome = null;
          this.state.error =
            classification === "ambiguous"
              ? ADD_AMBIGUOUS_MESSAGE
              : mapToSafeMessage(err, ADD_FALLBACK) ?? ADD_FALLBACK;
        }
        return classification === "stopped"
          ? { kind: "stopped", code: extractCodeText(err) }
          : classification === "failed"
            ? { kind: "failed", code: extractCodeText(err) }
            : { kind: "ambiguous", code: extractCodeText(err) };
      }
      if (this.disposed) return { kind: "stopped", code: "disposed" };
      // If the connection changed before the daemon answered, the request may
      // never have reached it; that stays ambiguous. Once a known answer is in
      // hand it is the acknowledgement boundary and cannot be downgraded.
      if (epoch !== this.epoch || this.addEpoch !== epoch) {
        return { kind: "ambiguous", code: "superseded" };
      }
      // New IPC returns { status, id?, hashRecorded? }; legacy returns a bare
      // status string. Only the status is used for batch outcomes — ids never
      // enter display state, and pending/duplicate carry no download id by
      // contract. The hash flag only decides whether the accepted queued
      // download needs a bounded warning; it never enters a message as data.
      let outcome = "";
      let hashRecorded: unknown;
      if (typeof raw === "string") {
        outcome = raw;
      } else if (raw !== null && typeof raw === "object") {
        const receipt = raw as Record<string, unknown>;
        if (typeof receipt.status === "string") outcome = receipt.status;
        hashRecorded = receipt.hashRecorded;
      }
      if (!KNOWN_ADD_OUTCOMES.has(outcome)) {
        if (reportError) {
          this.state.addOutcome = null;
          this.state.error = ADD_FALLBACK;
        }
        return { kind: "ambiguous", code: "invalidresponse" };
      }
      const accepted = outcome as AddOutcome;
      // A queued add that explicitly requested hashing but did not persist the
      // intent leaves the accepted download without automatic hashing. Record
      // one bounded, redacted warning; the accepted download is never retried.
      const hashUnsaved =
        hashEnabled === true && accepted === "queued" && hashRecorded !== true;
      if (hashUnsaved) this.recordHashIntentWarning();
      onAck?.(accepted, hashUnsaved ? HASH_INTENT_UNSAVED_MESSAGE : undefined);
      this.state.addOutcome = accepted;
      this.state.error = null;
      if (accepted !== "queued") return { kind: "ok", outcome: accepted };

      // Row adoption is best-effort. The queued acknowledgement above is
      // already accepted; a reconnect during this refresh must not downgrade
      // it to ambiguous.
      const prior = this.refreshPromise !== null && this.refreshEpoch === epoch ? this.refreshPromise : null;
      if (prior) {
        await prior.then(
          () => {},
          () => {},
        );
      }
      if (!this.disposed && epoch === this.epoch && this.addEpoch === epoch) {
        await this.startRefresh();
      }
      return { kind: "ok", outcome: accepted };
    } finally {
      if (this.addEpoch === epoch && epoch === this.epoch && !this.disposed) {
        this.addEpoch = null;
        if (this.state.adding) {
          this.state.adding = false;
          this.emit();
        }
      }
    }
  }

  /** Preserve the existing single-add contract for callers that only need a boolean. */
  async add(url: string): Promise<boolean> {
    const result = await this.addAttempt(url, true);
    return result.kind === "ok";
  }

  /**
   * Submit one URL at a time through the same single-add path. Invalid links
   * are never sent. Any disconnect, definitive connection failure, or
   * unconfirmed request stops the batch; remaining links stay unattempted and
   * are returned for the input box. No link is ever retried automatically.
   * When `hashEnabled` is an explicit boolean it is snapshotted once for the
   * whole batch and sent via `add_download_with_options`; legacy callers
   * without a snapshot keep the single-add route unchanged.
   */
  async addBatch(input: string, hashEnabled?: boolean): Promise<BatchResult> {
    const plan = planUrlLines(input);
    if (plan.length === 0) return { results: [], recoverable: [] };
    if (this.disposed) {
      return {
        results: plan.map((entry) => this.batchResult(entry, "unattempted")),
        recoverable: plan.map((entry) => entry.line),
      };
    }
    const epoch = this.epoch;
    if (this.batchEpoch === epoch) return { results: [], recoverable: [] };
    this.batchEpoch = epoch;
    const identity = ++this.batchSeq;
    this.batchIdentity = identity;

    // Both the connection epoch and this batch's identity must still match
    // before any asynchronous result may touch the shared batch state. A stale
    // batch that settles after a reconnect must never repaint the new batch.
    const ownsBatch = (): boolean =>
      !this.disposed &&
      this.epoch === epoch &&
      this.batchEpoch === epoch &&
      this.batchIdentity === identity;

    const results: BatchLinkResult[] = [];
    const context: ActiveBatchContext = {
      epoch,
      identity,
      plan,
      results,
      nextIndex: 0,
      inFlight: false,
      recoveryRecorded: false,
      sealed: false,
    };
    this.activeBatch = context;
    let stopped = false;
    let stopReason: string | null = null;
    let settledOwned = false;
    const push = (result: BatchLinkResult): void => {
      results.push(result);
      if (!ownsBatch()) return;
      if (this.state.batch) {
        this.state.batch = {
          ...this.state.batch,
          completed: results.length,
          results: results.map((entry) => ({ ...entry })),
        };
        this.emit();
      }
    };
    const stop = (reason: string): void => {
      stopped = true;
      stopReason = reason;
    };
    const markRemainingUnattempted = (from: number): void => {
      for (let index = from; index < plan.length; index++) {
        push(this.batchResult(plan[index], "unattempted", undefined, "Batch stopped before this link was sent."));
      }
    };

    if (ownsBatch()) {
      this.state.batch = {
        running: true,
        total: plan.length,
        completed: 0,
        results: [],
        stopped: false,
        stopReason: null,
      };
      this.emit();
    }

    const seenUrls = new Set<string>();
    try {
      for (let index = 0; index < plan.length; index++) {
        context.nextIndex = index;
        const entry = plan[index];
        if (entry.kind === "invalid") {
          push(this.batchResult(entry, "invalid", entry.reason));
          continue;
        }
        if (entry.kind === "over-limit") {
          if (!stopped) stop("Batch limit reached. Extra links were not attempted.");
          push(
            this.batchResult(
              entry,
              "unattempted",
              undefined,
              "Batch limit reached; this link was not attempted.",
            ),
          );
          continue;
        }
        if (!ownsBatch() || this.state.phase !== "connected") {
          stop("Connection ended. Remaining links were not attempted.");
          push(this.batchResult(entry, "unattempted"));
          markRemainingUnattempted(index + 1);
          break;
        }
        // Deduplicate only byte-exact literals within this batch. The daemon
        // must not receive a second approval request for a repeated line, and
        // URL normalization is deliberately not used as an identity.
        if (seenUrls.has(entry.url)) {
          push(this.batchResult(entry, "duplicateInBatch"));
          continue;
        }
        seenUrls.add(entry.url);

        context.inFlight = true;
        let acknowledged = false;
        let attempt: AddAttempt;
        try {
          attempt = await this.addAttempt(
            entry.url,
            false,
            (outcome, hashWarning) => {
              // Commit the definitive acknowledgement before the adoption
              // refresh. If reconnect happens while that refresh is pending, the
              // link is already recorded as accepted, so recovery cannot mark it
              // uncertain or offer it for resend.
              context.inFlight = false;
              context.nextIndex = index + 1;
              acknowledged = true;
              push(this.batchResult(entry, outcome, undefined, hashWarning));
            },
            hashEnabled,
          );
        } finally {
          context.inFlight = false;
        }
        if (acknowledged) {
          // Already recorded at the acknowledgement boundary above.
        } else if (attempt.kind === "ok") {
          push(this.batchResult(entry, attempt.outcome));
        } else if (attempt.kind === "busy") {
          stop("Another add is already in progress. Remaining links were not attempted.");
          push(this.batchResult(entry, "unattempted"));
          markRemainingUnattempted(index + 1);
          break;
        } else if (attempt.kind === "stopped") {
          stop("Connection ended. Remaining links were not attempted.");
          push(this.batchResult(entry, "unattempted"));
          markRemainingUnattempted(index + 1);
          break;
        } else if (attempt.kind === "failed") {
          push(this.batchResult(entry, "failed"));
          stop("Batch stopped after a definitive failure. Remaining links were not attempted.");
          markRemainingUnattempted(index + 1);
          break;
        } else {
          push(this.batchResult(entry, "ambiguous"));
          stop("Batch stopped on an unconfirmed request. Remaining links were not attempted.");
          markRemainingUnattempted(index + 1);
          break;
        }

        if (!ownsBatch() || this.state.phase !== "connected") {
          stop("Connection ended. Remaining links were not attempted.");
          markRemainingUnattempted(index + 1);
          break;
        }
      }
    } finally {
      context.sealed = true;
      if (this.activeBatch === context) this.activeBatch = null;
      // Capture ownership before clearing it: a normal completion owns the
      // batch even though batchEpoch/batchIdentity are reset below.
      settledOwned = ownsBatch();
      if (this.batchEpoch === epoch && this.batchIdentity === identity) {
        this.batchEpoch = null;
        this.batchIdentity = null;
      }
      if (settledOwned && this.state.batch) {
        this.state.batch = {
          ...this.state.batch,
          running: false,
          completed: results.length,
          results: results.map((entry) => ({ ...entry })),
          stopped,
          stopReason,
        };
        this.emit();
      }
    }

    const recoverable = results
      .filter(
        (result) =>
          result.status === "invalid" ||
          result.status === "failed" ||
          result.status === "ambiguous" ||
          result.status === "unattempted",
      )
      .map((result) => plan[result.index]?.line ?? result.line);

    // Only a batch that lost ownership before settling is a detached recovery.
    // A normal completion (including one stopped by invalid or failed lines)
    // still owns its result and must never be reported as interrupted. The
    // visible batch state is never touched here, so a newer batch keeps
    // ownership of the summary.
    if (!this.disposed && !settledOwned && !context.recoveryRecorded && recoverable.length > 0) {
      const uncertain: string[] = [];
      const unsent: string[] = [];
      const rejected: string[] = [];
      for (const result of results) {
        const line = plan[result.index]?.line ?? result.line;
        if (result.status === "ambiguous") uncertain.push(line);
        else if (result.status === "unattempted") unsent.push(line);
        else if (result.status === "invalid" || result.status === "failed") rejected.push(line);
      }
      this.recordBatchRecovery(results, uncertain, unsent, rejected);
      context.recoveryRecorded = true;
    }

    return { results, recoverable };
  }

  private batchResult(
    entry: { index: number; line: string },
    status: BatchLinkStatus,
    reason?: UrlLineError,
    message?: string,
  ): BatchLinkResult {
    return {
      index: entry.index,
      line: redactUrlForDisplay(entry.line),
      status,
      message: message ?? batchStatusMessage(status, reason),
      ...(reason ? { reason } : {}),
    };
  }

  /**
   * One-shot gate for the UI auto-load: true once per connection epoch when
   * connected + managed + unpoisoned. Manual Retry calls loadRate() directly
   * and is never gated here, so a failed auto-load never loops.
   * Reconnect (epoch bump) allows exactly one new auto attempt.
   */
  tryClaimRateAutoLoad(): boolean {
    if (this.disposed) return false;
    if (this.state.phase !== "connected") return false;
    if (!this.state.managed) return false;
    if (this.ratePoisoned || this.state.rateUnconfirmed) return false;
    if (this.rateAutoLoadEpoch === this.epoch) return false;
    this.rateAutoLoadEpoch = this.epoch;
    return true;
  }

  /**
   * Load the current global download limit without disturbing local edits.
   * Only the confirmed value is stored; the input draft lives in the UI so
   * polls never overwrite what the user is typing. Stale responses after
   * disconnect/reconnect are discarded via the epoch fence.
   */
  async loadRate(): Promise<void> {
    if (this.disposed) return;
    if (this.state.phase !== "connected") return;
    if (this.rateLoadPromise !== null && this.rateLoadEpoch === this.epoch) {
      return this.rateLoadPromise;
    }
    const epoch = this.epoch;
    const seq = (this.rateSeq += 1);
    const task = this.executeLoadRate(epoch, seq);
    this.rateLoadPromise = task;
    this.rateLoadEpoch = epoch;
    return task.then(
      () => {
        if (this.rateLoadPromise === task) {
          this.rateLoadPromise = null;
          this.rateLoadEpoch = null;
        }
      },
      () => {
        if (this.rateLoadPromise === task) {
          this.rateLoadPromise = null;
          this.rateLoadEpoch = null;
        }
      },
    );
  }

  private async executeLoadRate(epoch: number, seq: number): Promise<void> {
    if (this.disposed) return;
    if (this.ratePoisoned) return;
    this.state.rateLoading = true;
    if (!this.state.rateUnconfirmed) this.state.rateError = null;
    this.emit();
    let raw: unknown;
    try {
      raw = await this.invoke("get_global_rate_limit");
    } catch (err) {
      if (epoch !== this.epoch || this.disposed) return;
      if (seq !== this.rateSeq) return;
      if (isSuperseded(err)) return;
      if (isRateUnconfirmed(err)) {
        this.ratePoisoned = true;
        this.state.rateUnconfirmed = true;
        this.state.rateError = RATE_UNCONFIRMED_MESSAGE;
        this.state.rateLoading = false;
        this.state.rateSaving = false;
        this.emit();
        return;
      }
      this.state.rateError = mapRateMessage(err, RATE_FALLBACK);
      this.state.rateLoading = false;
      this.emit();
      return;
    }
    if (epoch !== this.epoch || this.disposed) return;
    if (seq !== this.rateSeq) return;
    let value: number;
    try {
      value = sanitizeRateValue(raw);
    } catch (err) {
      if (epoch !== this.epoch || this.disposed) return;
      if (seq !== this.rateSeq) return;
      if (isSuperseded(err)) return;
      this.state.rateError = mapRateMessage(err, RATE_FALLBACK);
      this.state.rateLoading = false;
      this.emit();
      return;
    }
    if (epoch !== this.epoch || this.disposed) return;
    if (seq !== this.rateSeq) return;
    this.state.rateLimitBps = value;
    if (!this.state.rateUnconfirmed) this.state.rateError = null;
    this.state.rateLoading = false;
    this.emit();
  }

  /**
   * Apply a managed global download limit in integer bytes/s.
   * 0 means unlimited. Duplicate pending writes in the same epoch issue
   * exactly one IPC and report false. RateUnconfirmed poisons the writer
   * until the app restarts: later writes return false without sending.
   */
  async setRateLimit(rate: number): Promise<boolean> {
    if (this.disposed) return false;
    if (this.state.phase !== "connected") return false;
    if (this.ratePoisoned || this.state.rateUnconfirmed) {
      this.state.rateError = RATE_UNCONFIRMED_MESSAGE;
      this.emit();
      return false;
    }
    if (!Number.isInteger(rate) || rate < 0 || rate > JS_MAX_SAFE) {
      this.state.rateError = RATE_SAVE_FALLBACK;
      this.emit();
      return false;
    }
    const epoch = this.epoch;
    if (this.rateWriteEpoch === epoch) return false;
    this.rateWriteEpoch = epoch;
    this.state.rateSaving = true;
    this.state.rateError = null;
    this.emit();
    try {
      let confirmed: unknown;
      try {
        confirmed = await this.invoke("set_global_rate_limit", { rate });
      } catch (err) {
        if (isRateUnconfirmed(err)) {
          // Sticky poison survives epoch/reconnect: mark even when stale,
          // without overwriting the new connection's other state.
          this.ratePoisoned = true;
          if (!this.disposed) {
            this.state.rateUnconfirmed = true;
            this.state.rateError = RATE_UNCONFIRMED_MESSAGE;
            if (this.rateWriteEpoch === epoch && epoch === this.epoch) {
              this.state.rateSaving = false;
            }
            this.emit();
          }
          return false;
        }
        if (epoch !== this.epoch || this.disposed) return false;
        if (this.rateWriteEpoch !== epoch) return false;
        if (isSuperseded(err)) return false;
        this.state.rateError = mapRateMessage(err, RATE_SAVE_FALLBACK);
        this.state.rateSaving = false;
        this.emit();
        return false;
      }
      if (isRateUnconfirmed(confirmed)) {
        this.ratePoisoned = true;
        if (!this.disposed) {
          this.state.rateUnconfirmed = true;
          this.state.rateError = RATE_UNCONFIRMED_MESSAGE;
          if (this.rateWriteEpoch === epoch && epoch === this.epoch) {
            this.state.rateSaving = false;
          }
          this.emit();
        }
        return false;
      }
      if (epoch !== this.epoch || this.disposed) return false;
      if (this.rateWriteEpoch !== epoch) return false;
      let value: number;
      try {
        value = sanitizeRateValue(confirmed);
      } catch (err) {
        if (isRateUnconfirmed(err)) {
          this.ratePoisoned = true;
          if (!this.disposed) {
            this.state.rateUnconfirmed = true;
            this.state.rateError = RATE_UNCONFIRMED_MESSAGE;
            if (this.rateWriteEpoch === epoch && epoch === this.epoch) {
              this.state.rateSaving = false;
            }
            this.emit();
          }
          return false;
        }
        if (epoch !== this.epoch || this.disposed) return false;
        if (this.rateWriteEpoch !== epoch) return false;
        this.state.rateError = mapRateMessage(err, RATE_SAVE_FALLBACK);
        this.state.rateSaving = false;
        this.emit();
        return false;
      }
      if (epoch !== this.epoch || this.disposed) return false;
      if (this.rateWriteEpoch !== epoch) return false;
      if (value !== rate) {
        // Backend readback disagrees: treat as unconfirmed rather than
        // claiming a change that cannot be verified.
        this.ratePoisoned = true;
        this.state.rateUnconfirmed = true;
        this.state.rateError = RATE_UNCONFIRMED_MESSAGE;
        this.state.rateSaving = false;
        this.emit();
        return false;
      }
      this.state.rateLimitBps = value;
      this.state.rateError = null;
      this.state.rateSaving = false;
      this.emit();
      return true;
    } finally {
      if (this.rateWriteEpoch === epoch && epoch === this.epoch && !this.disposed) {
        this.rateWriteEpoch = null;
        if (this.state.rateSaving) {
          this.state.rateSaving = false;
          this.emit();
        }
      }
    }
  }

  /**
   * One-shot gate for the concurrency-setting auto-load. Reconnect (epoch
   * bump) allows exactly one new attempt; a failed read never loops.
   */
  tryClaimMaxConcurrentAutoLoad(): boolean {
    if (this.disposed) return false;
    if (this.state.phase !== "connected") return false;
    if (this.maxConcurrentPoisoned || this.state.maxConcurrentUnconfirmed) return false;
    if (this.maxConcurrentAutoLoadEpoch === this.epoch) return false;
    this.maxConcurrentAutoLoadEpoch = this.epoch;
    return true;
  }

  /**
   * Load the daemon's saved `network.max_concurrent_downloads` value. This is
   * the persisted settings value, not a claim about the running daemon's
   * active concurrency; a restart is required for a saved change to apply.
   */
  async loadMaxConcurrent(): Promise<void> {
    if (this.disposed) return;
    if (this.state.phase !== "connected") return;
    if (this.maxConcurrentLoadPromise !== null && this.maxConcurrentLoadEpoch === this.epoch) {
      return this.maxConcurrentLoadPromise;
    }
    const epoch = this.epoch;
    const seq = (this.maxConcurrentSeq += 1);
    const task = this.executeLoadMaxConcurrent(epoch, seq);
    this.maxConcurrentLoadPromise = task;
    this.maxConcurrentLoadEpoch = epoch;
    return task.then(
      () => {
        if (this.maxConcurrentLoadPromise === task) {
          this.maxConcurrentLoadPromise = null;
          this.maxConcurrentLoadEpoch = null;
        }
      },
      () => {
        if (this.maxConcurrentLoadPromise === task) {
          this.maxConcurrentLoadPromise = null;
          this.maxConcurrentLoadEpoch = null;
        }
      },
    );
  }

  private async executeLoadMaxConcurrent(epoch: number, seq: number): Promise<void> {
    if (this.disposed) return;
    if (this.maxConcurrentPoisoned) return;
    this.state.maxConcurrentLoading = true;
    if (!this.state.maxConcurrentUnconfirmed) this.state.maxConcurrentError = null;
    this.emit();
    let raw: unknown;
    try {
      raw = await this.invoke("get_max_concurrent_downloads");
    } catch (err) {
      if (epoch !== this.epoch || this.disposed) return;
      if (seq !== this.maxConcurrentSeq) return;
      if (isSuperseded(err)) return;
      this.state.maxConcurrentError = mapMaxConcurrentMessage(err, MAX_CONCURRENT_FALLBACK);
      this.state.maxConcurrentLoading = false;
      this.emit();
      return;
    }
    if (epoch !== this.epoch || this.disposed) return;
    if (seq !== this.maxConcurrentSeq) return;
    if (!isValidMaxConcurrentValue(raw)) {
      this.state.maxConcurrentError = "Daemon returned an unexpected concurrency value.";
      this.state.maxConcurrentLoading = false;
      this.emit();
      return;
    }
    this.state.maxConcurrent = raw;
    if (!this.state.maxConcurrentUnconfirmed) this.state.maxConcurrentError = null;
    this.state.maxConcurrentLoading = false;
    this.emit();
  }

  /**
   * Save `network.max_concurrent_downloads` on the owned bundled daemon.
   * The daemon is never restarted by the app; a confirmed save only marks
   * that a restart is required and never implies a live effect. `before` is
   * the value the UI loaded so the daemon can reject a stale edit instead of
   * overwriting a concurrent change. An unconfirmed outcome disables further
   * settings writes until the app restarts; nothing is retried silently.
   */
  async setMaxConcurrent(before: number, value: number): Promise<boolean> {
    if (this.disposed) return false;
    if (this.state.phase !== "connected") return false;
    if (!this.state.managed) {
      this.state.maxConcurrentError = "Max simultaneous downloads needs the bundled daemon connection.";
      this.emit();
      return false;
    }
    if (this.maxConcurrentPoisoned || this.state.maxConcurrentUnconfirmed) {
      this.state.maxConcurrentError = MAX_CONCURRENT_UNCONFIRMED_MESSAGE;
      this.emit();
      return false;
    }
    if (!isValidMaxConcurrentValue(before) || !isValidMaxConcurrentValue(value)) {
      this.state.maxConcurrentError = "Choose a whole number from 1 to 10.";
      this.emit();
      return false;
    }
    if (before === value) {
      this.state.maxConcurrentError = null;
      this.emit();
      return true;
    }
    const epoch = this.epoch;
    if (this.maxConcurrentWriteEpoch === epoch) return false;
    this.maxConcurrentWriteEpoch = epoch;
    this.state.maxConcurrentSaving = true;
    this.state.maxConcurrentError = null;
    this.emit();
    try {
      let confirmed: unknown;
      try {
        confirmed = await this.invoke("set_max_concurrent_downloads", { before, value });
      } catch (err) {
        if (isSettingsUnconfirmed(err)) {
          this.maxConcurrentPoisoned = true;
          if (!this.disposed) {
            this.state.maxConcurrentUnconfirmed = true;
            this.state.maxConcurrentError = MAX_CONCURRENT_UNCONFIRMED_MESSAGE;
            if (this.maxConcurrentWriteEpoch === epoch && epoch === this.epoch) {
              this.state.maxConcurrentSaving = false;
            }
            this.emit();
          }
          return false;
        }
        if (epoch !== this.epoch || this.disposed) return false;
        if (this.maxConcurrentWriteEpoch !== epoch) return false;
        if (isSuperseded(err)) return false;
        this.state.maxConcurrentError = mapMaxConcurrentMessage(err, MAX_CONCURRENT_SAVE_FALLBACK);
        this.state.maxConcurrentSaving = false;
        this.emit();
        return false;
      }
      if (isSettingsUnconfirmed(confirmed)) {
        this.maxConcurrentPoisoned = true;
        if (!this.disposed) {
          this.state.maxConcurrentUnconfirmed = true;
          this.state.maxConcurrentError = MAX_CONCURRENT_UNCONFIRMED_MESSAGE;
          if (this.maxConcurrentWriteEpoch === epoch && epoch === this.epoch) {
            this.state.maxConcurrentSaving = false;
          }
          this.emit();
        }
        return false;
      }
      if (epoch !== this.epoch || this.disposed) return false;
      if (this.maxConcurrentWriteEpoch !== epoch) return false;
      if (!isValidMaxConcurrentValue(confirmed) || confirmed !== value) {
        this.maxConcurrentPoisoned = true;
        this.state.maxConcurrentUnconfirmed = true;
        this.state.maxConcurrentError = MAX_CONCURRENT_UNCONFIRMED_MESSAGE;
        this.state.maxConcurrentSaving = false;
        this.emit();
        return false;
      }
      this.state.maxConcurrent = value;
      this.state.maxConcurrentRestartPending = true;
      this.state.maxConcurrentError = null;
      this.state.maxConcurrentSaving = false;
      this.emit();
      return true;
    } finally {
      if (this.maxConcurrentWriteEpoch === epoch && epoch === this.epoch && !this.disposed) {
        this.maxConcurrentWriteEpoch = null;
        if (this.state.maxConcurrentSaving) {
          this.state.maxConcurrentSaving = false;
          this.emit();
        }
      }
    }
  }

  /**
   * Open the containing folder of a completed download on the owned managed
   * daemon. Sends only the server-owned id; no path crosses IPC. Managed
   * only: manual/offline callers get a sanitized message without any IPC.
   */
  async openLocation(id: string): Promise<boolean> {
    if (this.disposed) return false;
    if (this.state.phase !== "connected") {
      this.state.locationError = "Not connected to a daemon.";
      this.state.locationErrorId = null;
      this.emit();
      return false;
    }
    if (!this.state.managed) {
      this.state.locationError = "Open location needs the bundled daemon connection.";
      this.state.locationErrorId = null;
      this.emit();
      return false;
    }
    if (!id || id.length > 4096) {
      this.state.locationError = "This download cannot be opened.";
      this.state.locationErrorId = id || null;
      this.emit();
      return false;
    }
    const epoch = this.epoch;
    if (this.locationEpoch === epoch) return false;
    this.locationEpoch = epoch;
    const requestSeq = (this.locationSeq += 1);
    this.state.locationOpeningId = id;
    this.state.locationError = null;
    this.state.locationErrorId = null;
    this.emit();
    try {
      await this.invoke("open_download_location", { id });
      if (epoch !== this.epoch || this.disposed) return false;
      if (this.locationEpoch !== epoch) return false;
      if (requestSeq !== this.locationSeq) return false;
      this.state.locationOpeningId = null;
      this.state.locationError = null;
      this.state.locationErrorId = null;
      this.emit();
      return true;
    } catch (err) {
      if (epoch !== this.epoch || this.disposed) return false;
      if (this.locationEpoch !== epoch) return false;
      if (requestSeq !== this.locationSeq) return false;
      if (isSuperseded(err)) return false;
      this.state.locationOpeningId = null;
      this.state.locationError = mapLocationMessage(err);
      this.state.locationErrorId = id;
      this.emit();
      return false;
    } finally {
      if (
        this.locationEpoch === epoch &&
        epoch === this.epoch &&
        !this.disposed &&
        requestSeq === this.locationSeq
      ) {
        this.locationEpoch = null;
        if (this.state.locationOpeningId === id) {
          this.state.locationOpeningId = null;
          this.emit();
        }
      }
    }
  }

  clearLocationError(): void {
    if (this.disposed) return;
    if (this.state.locationOpeningId !== null) return;
    if (this.state.locationError === null) return;
    this.state.locationError = null;
    this.state.locationErrorId = null;
    this.emit();
  }

  /** Origin id for the current location error, if any. Test seam for selection scoping. */
  locationErrorOrigin(): string | null {
    return this.state.locationErrorId ?? null;
  }

  async disconnect(): Promise<void> {
    this.epoch += 1;
    this.captureAbandonedBatch();
    this.clearStreamState();
    this.resetBulkState();
    this.stopTimer();
    this.addEpoch = null;
    this.managedEpoch = null;
    this.managedStoreEpoch = null;
    this.rateWriteEpoch = null;
    this.rateLoadPromise = null;
    this.rateLoadEpoch = null;
    this.maxConcurrentWriteEpoch = null;
    this.maxConcurrentLoadPromise = null;
    this.maxConcurrentLoadEpoch = null;
    this.batchEpoch = null;
    this.batchIdentity = null;
    this.state.batch = null;
    this.locationEpoch = null;
    this.state.phase = "offline";
    this.state.items = [];
    this.state.error = null;
    this.state.refreshing = false;
    this.state.controllingId = null;
    this.state.adding = false;
    this.state.addOutcome = null;
    this.state.batch = null;
    this.resetMaxConcurrentState();
    this.resetHashState();
    this.state.managed = false;
    this.state.rateLimitBps = null;
    this.state.rateLoading = false;
    this.state.rateSaving = false;
    if (!this.ratePoisoned) this.state.rateError = null;
    else {
      this.state.rateUnconfirmed = true;
      this.state.rateError = RATE_UNCONFIRMED_MESSAGE;
    }
    this.state.locationOpeningId = null;
    this.state.locationError = null;
    this.state.locationErrorId = null;
    if (!this.disposed) this.emit();
    try {
      await this.invoke("disconnect_surge");
    } catch {
      // Stay offline; disconnect never surfaces transport detail.
    }
  }

  dispose(): void {
    if (this.disposed) return;
    this.epoch += 1;
    this.disposed = true;
    this.hashDefaultStore.listeners.delete(this.hashDefaultStoreListener);
    this.clearStreamState();
    for (const unlisten of this.unlisteners.splice(0)) {
      try {
        unlisten();
      } catch {
        // Unlisten is best-effort and never surfaces transport detail.
      }
    }
    this.activeBatch = null;
    this.batchRecoveries = [];
    this.resetBulkState();
    this.stopTimer();
    this.state.controllingId = null;
    this.state.adding = false;
    this.state.addOutcome = null;
    this.addEpoch = null;
    this.managedEpoch = null;
    this.managedStoreEpoch = null;
    this.rateWriteEpoch = null;
    this.rateLoadPromise = null;
    this.rateLoadEpoch = null;
    this.maxConcurrentWriteEpoch = null;
    this.maxConcurrentLoadPromise = null;
    this.maxConcurrentLoadEpoch = null;
    this.batchEpoch = null;
    this.batchIdentity = null;
    this.state.batch = null;
    this.resetMaxConcurrentState();
    this.resetHashState();
    this.locationEpoch = null;
    this.state.managed = false;
    this.state.rateLoading = false;
    this.state.rateSaving = false;
    this.state.locationOpeningId = null;
    this.state.locationError = null;
    this.state.locationErrorId = null;
    try {
      const pending = this.invoke("disconnect_surge");
      if (pending && typeof (pending as Promise<unknown>).catch === "function") {
        (pending as Promise<unknown>).catch(() => {});
      }
    } catch {
      // Best-effort: clear Rust-held token, never surface transport detail.
    }
  }
}
