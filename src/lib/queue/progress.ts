/**
 * Truthful progress-merge helpers for the Surge `/events` SSE stream.
 *
 * The backend emits validated projections only; these helpers keep the
 * renderer honest: progress applies solely to existing active rows with a
 * per-row receipt sequence newer than the snapshot cursor, byte counters stay
 * monotonic for a download generation, and omitted daemon fields arrive as
 * explicit zeroes. Resets/restarts arrive through authoritative snapshots.
 */
import type { QueueItem } from "./types.ts";

const JS_MAX_SAFE = 9_007_199_254_740_991;
const MAX_PROGRESS_UPDATES = 8192;

export interface ProgressUpdate {
  id: string;
  /**
   * Receipt sequence assigned by the backend before coalescing. Snapshots use
   * the same clock, so per-update ordering survives a batched flush.
   */
  seq: number;
  bytesDone: number;
  bytesTotal: number;
  speedBps: number;
}

export interface SanitizedProgressBatch {
  subscription: number;
  epoch: number;
  seq: number;
  updates: ProgressUpdate[];
}

export interface SanitizedLifecycleNotice {
  subscription: number;
  epoch: number;
  seq: number;
  resync: boolean;
}

export interface SanitizedStreamState {
  subscription: number;
  epoch: number;
  seq: number;
  state: "ready" | "unavailable";
}

export interface SanitizedSnapshot {
  epoch: number | null;
  streamSeq: number;
  items: unknown[];
}

function safeCount(value: unknown): number {
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.min(JS_MAX_SAFE, Math.floor(n));
}

function safeId(value: unknown): string | null {
  if (typeof value !== "string" || value.length === 0) return null;
  if (value.length > 4096 || /[\u0000-\u001f\u007f]/.test(value)) return null;
  return value;
}

function sanitizeUpdate(raw: unknown, fallbackSeq: number): ProgressUpdate | null {
  if (raw === null || typeof raw !== "object") return null;
  const value = raw as Record<string, unknown>;
  const id = safeId(value.id);
  if (id === null) return null;
  const bytesTotal = safeCount(value.bytesTotal);
  let bytesDone = safeCount(value.bytesDone);
  if (bytesTotal > 0 && bytesDone > bytesTotal) bytesDone = bytesTotal;
  const seq = safeCount(value.seq);
  return {
    id,
    seq: seq > 0 ? seq : fallbackSeq,
    bytesDone,
    bytesTotal,
    speedBps: Math.min(JS_MAX_SAFE, Math.max(0, Number(value.speedBps) || 0)),
  };
}

/** Validate one backend progress batch; malformed payloads are dropped whole. */
export function sanitizeProgressBatch(raw: unknown): SanitizedProgressBatch | null {
  if (raw === null || typeof raw !== "object") return null;
  const value = raw as Record<string, unknown>;
  const subscription = safeCount(value.subscription);
  const epoch = safeCount(value.epoch);
  const seq = safeCount(value.seq);
  if (subscription === 0 || seq === 0 || !Array.isArray(value.updates)) return null;
  if (value.updates.length > MAX_PROGRESS_UPDATES) return null;
  const updates: ProgressUpdate[] = [];
  for (const item of value.updates) {
    const update = sanitizeUpdate(item, seq);
    if (update !== null) updates.push(update);
  }
  if (updates.length === 0) return null;
  return { subscription, epoch, seq, updates };
}

export function sanitizeLifecycleNotice(raw: unknown): SanitizedLifecycleNotice | null {
  if (raw === null || typeof raw !== "object") return null;
  const value = raw as Record<string, unknown>;
  const subscription = safeCount(value.subscription);
  const epoch = safeCount(value.epoch);
  const seq = safeCount(value.seq);
  if (subscription === 0 || seq === 0) return null;
  return { subscription, epoch, seq, resync: value.resync === true };
}

export function sanitizeStreamState(raw: unknown): SanitizedStreamState | null {
  if (raw === null || typeof raw !== "object") return null;
  const value = raw as Record<string, unknown>;
  const subscription = safeCount(value.subscription);
  const epoch = safeCount(value.epoch);
  const seq = safeCount(value.seq);
  const state = value.state;
  if (subscription === 0 || (state !== "ready" && state !== "unavailable")) return null;
  return { subscription, epoch, seq, state };
}

/** Accept the authoritative snapshot envelope, or a legacy bare array. */
export function sanitizeSnapshotPayload(raw: unknown): SanitizedSnapshot | null {
  if (Array.isArray(raw)) {
    return { epoch: null, streamSeq: 0, items: raw };
  }
  if (raw === null || typeof raw !== "object") return null;
  const value = raw as Record<string, unknown>;
  if (!Array.isArray(value.items)) return null;
  return {
    epoch: value.epoch === undefined ? null : safeCount(value.epoch),
    streamSeq: safeCount(value.streamSeq),
    items: value.items,
  };
}

/**
 * Apply progress only to rows that already exist and are active. Non-active and
 * unknown rows are never created or mutated from a progress frame. A progress
 * frame may advance a byte counter but never roll it back for the same
 * generation; a legitimate restart/reset arrives through an authoritative
 * snapshot/lifecycle reconciliation instead.
 */
export function applyProgressUpdates(items: QueueItem[], updates: ProgressUpdate[]): QueueItem[] {
  if (updates.length === 0) return items;
  const byId = new Map<string, ProgressUpdate>();
  for (const update of updates) byId.set(update.id, update);
  let changed = false;
  const next = items.map((item) => {
    const update = byId.get(item.id);
    if (update === undefined || item.status !== "active") return item;
    const bytesTotal = update.bytesTotal;
    let bytesDone = update.bytesDone;
    if (bytesTotal > 0 && bytesDone > bytesTotal) bytesDone = bytesTotal;
    if (bytesDone < item.bytesDone) return item;
    if (
      item.bytesDone === bytesDone &&
      item.bytesTotal === bytesTotal &&
      item.speedBps === update.speedBps
    ) {
      return item;
    }
    changed = true;
    return {
      ...item,
      bytesDone,
      bytesTotal,
      speedBps: update.speedBps,
    };
  });
  return changed ? next : items;
}

export interface SnapshotMergeResult {
  items: QueueItem[];
  /**
   * Ids that a later fresh read confirmed as a real daemon reset for the same
   * download generation. Live progress for these rows can no longer be trusted
   * (a delayed sample from the previous generation can still arrive with a
   * newer receipt sequence), so the caller should quarantine them until the
   * terminal state or a new `addedAt`.
   */
  rolledBack: string[];
  /**
   * Ids whose snapshot reported fewer bytes than the displayed live value while
   * a newer progress sample exists after the snapshot request started. This is
   * the ordinary in-flight case (the `/list` response is older than the stream),
   * so it is not yet safe to regress or quarantine. The live value is kept and
   * the caller must confirm the rollback with a fresh authoritative read.
   */
  pendingConfirmation: string[];
}

/**
 * Merge an authoritative snapshot with already-applied newer progress.
 *
 * The snapshot cursor is captured before the `/list` request, so a progress
 * sample received at or before the cursor is reflected by the authoritative
 * snapshot and the snapshot wins (including a legitimate restart/reset).
 * Progress received after the request started may be newer than the snapshot;
 * for the same download generation it is merged byte-monotonically so a stale
 * sample can never roll back newer bytes, while a terminal/non-active snapshot
 * row always wins outright.
 *
 * A snapshot that reports *fewer* bytes than the displayed value for the same
 * id/addedAt is ambiguous. Without a daemon generation identifier the receipt
 * sequence cannot distinguish a delayed old-generation sample from real new
 * progress. It is only a safe reset when no progress newer than the snapshot
 * request exists for the row (the snapshot cursor has caught up): then the
 * snapshot wins and the id is reported in `rolledBack`. When a newer progress
 * receipt exists (`seq > cursor`), the ordinary in-flight snapshot is likely
 * older than the stream: the live value is kept and the id is reported in
 * `pendingConfirmation` so the caller can run a fresh follow-up read. A later
 * read that still reports the lower value after covering the receipt confirms
 * the reset; a read that catches up clears the suspicion.
 */
export function mergeSnapshotWithProgress(
  existing: QueueItem[],
  snapshot: QueueItem[],
  cursor: number,
  progressSeqById: ReadonlyMap<string, number>,
): SnapshotMergeResult {
  const previous = new Map(existing.map((item) => [item.id, item]));
  const rolledBack: string[] = [];
  const pendingConfirmation: string[] = [];
  const items = snapshot.map((item) => {
    const prior = previous.get(item.id);
    if (prior === undefined) return item;
    if (prior.addedAt !== item.addedAt) return item;
    if (item.status !== "active" || prior.status !== "active") return item;
    const seq = progressSeqById.get(item.id) ?? -1;
    if (item.bytesDone < prior.bytesDone) {
      // A newer in-flight sample can explain the lower snapshot: keep the live
      // value and ask for a fresh read before rewinding or quarantining.
      if (seq > cursor) {
        pendingConfirmation.push(item.id);
        return {
          ...item,
          bytesDone: prior.bytesDone,
          bytesTotal: prior.bytesTotal,
          speedBps: prior.speedBps,
        };
      }
      // No newer progress explains the regression, so the snapshot owns it.
      rolledBack.push(item.id);
      return item;
    }
    if (seq <= cursor) return item;
    if (prior.bytesDone < item.bytesDone) return item;
    return {
      ...item,
      bytesDone: prior.bytesDone,
      bytesTotal: prior.bytesTotal,
      speedBps: prior.speedBps,
    };
  });
  return { items, rolledBack, pendingConfirmation };
}

/** Merge helper for callers that do not need the rolled-back id list. */
export function mergeSnapshotItems(
  existing: QueueItem[],
  snapshot: QueueItem[],
  cursor: number,
  progressSeqById: ReadonlyMap<string, number>,
): QueueItem[] {
  return mergeSnapshotWithProgress(existing, snapshot, cursor, progressSeqById).items;
}
