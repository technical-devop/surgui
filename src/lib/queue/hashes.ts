import type { QueueItem } from "./types.ts";

export type DownloadHashState =
  | "pending"
  | "complete"
  | "failed"
  | "unavailable"
  | "stale"
  | "disabled";

export interface DownloadHash {
  id: string;
  state: DownloadHashState;
  md5?: string;
  sha256?: string;
  bytes?: number;
  resultVersion?: number;
}

const KNOWN_STATES: ReadonlySet<string> = new Set([
  "pending",
  "complete",
  "failed",
  "unavailable",
  "stale",
  "disabled",
]);

const MAX_ID_BYTES = 4 * 1024;

function utf8Length(text: string): number {
  try {
    return new TextEncoder().encode(text).length;
  } catch {
    return text.length;
  }
}

function isValidId(id: unknown): id is string {
  if (typeof id !== "string") return false;
  if (id.length === 0 || utf8Length(id) > MAX_ID_BYTES) return false;
  // Control characters never cross IPC as identifiers.
  for (let i = 0; i < id.length; i++) {
    const code = id.charCodeAt(i);
    if (code <= 0x1f || code === 0x7f) return false;
  }
  return true;
}

function isLowerHex(value: unknown, length: number): value is string {
  if (typeof value !== "string" || value.length !== length) return false;
  for (let i = 0; i < value.length; i++) {
    const c = value.charCodeAt(i);
    const digit = c >= 48 && c <= 57;
    const lower = c >= 97 && c <= 102;
    if (!digit && !lower) return false;
  }
  return true;
}

function toSafeCount(value: unknown): number | undefined {
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n) || n < 0) return undefined;
  if (!Number.isInteger(n)) return undefined;
  if (n > Number.MAX_SAFE_INTEGER) return undefined;
  return n;
}

/**
 * Sanitize backend `DownloadHashDto` rows at the IPC boundary.
 * Only daemon ids, lowercase hex digests and byte counts survive;
 * paths, URLs, tokens and hook errors never cross IPC by contract.
 * Invalid rows are dropped, never guessed.
 */
export function sanitizeDownloadHashes(raw: unknown): DownloadHash[] {
  if (!Array.isArray(raw)) return [];
  const out: DownloadHash[] = [];
  for (const entry of raw) {
    if (entry === null || typeof entry !== "object") continue;
    const r = entry as Record<string, unknown>;
    if (!isValidId(r.id)) continue;
    if (typeof r.state !== "string" || !KNOWN_STATES.has(r.state)) continue;
    const state = r.state as DownloadHashState;
    if (state === "complete") {
      if (!isLowerHex(r.md5, 32)) continue;
      if (!isLowerHex(r.sha256, 64)) continue;
      const row: DownloadHash = { id: r.id as string, state, md5: r.md5 as string, sha256: r.sha256 as string };
      const bytes = r.bytes === undefined || r.bytes === null ? undefined : toSafeCount(r.bytes);
      if (r.bytes !== undefined && r.bytes !== null && bytes === undefined) continue;
      if (bytes !== undefined) row.bytes = bytes;
      const version = r.resultVersion ?? r.result_version;
      if (version !== undefined && version !== null) {
        const v = toSafeCount(version);
        if (v !== undefined) row.resultVersion = v;
      }
      out.push(row);
    } else {
      // Non-complete rows carry no digests by contract.
      out.push({ id: r.id as string, state });
    }
  }
  return out;
}

export function findDownloadHash(hashes: DownloadHash[], id: string): DownloadHash | null {
  for (const row of hashes) {
    if (row.id === id) return row;
  }
  return null;
}

/**
 * User-facing state label. Content-identification wording only;
 * never implies hashing equals security verification, trust or source safety.
 */
export function hashStateLabel(state: DownloadHashState): string {
  switch (state) {
    case "pending":
      return "Queued for hashing";
    case "complete":
      return "Hashes complete";
    case "failed":
      return "Hashing failed";
    case "unavailable":
      return "Hash unavailable";
    case "stale":
      return "Stale — reconnect to refresh";
    case "disabled":
      return "Hashing disabled";
  }
}

/**
 * Detail-pane status line for one row, combining queue completion with hash state.
 * Pending on a completed download reads as calculating; pending otherwise
 * reads as queued. Failure, unavailability and staleness stay explicit.
 */
export function hashDisplayForRow(
  hash: DownloadHash | null,
  queueStatus: QueueItem["status"] | null,
  managed: boolean,
): string | null {
  if (!managed) return null;
  if (hash === null) return null;
  if (hash.state === "complete") return hashStateLabel("complete");
  if (hash.state === "pending") {
    return queueStatus === "completed" ? "Calculating hashes…" : "Queued for hashing…";
  }
  return hashStateLabel(hash.state);
}

/** Exact digest for clipboard copy; empty unless the row is complete. */
export function hashCopyValue(hash: DownloadHash | null, kind: "md5" | "sha256"): string {
  if (hash === null || hash.state !== "complete") return "";
  const value = kind === "md5" ? hash.md5 : hash.sha256;
  return typeof value === "string" ? value : "";
}

export function isHashComplete(hash: DownloadHash | null): hash is DownloadHash & { md5: string; sha256: string } {
  return (
    hash !== null &&
    hash.state === "complete" &&
    typeof hash.md5 === "string" &&
    typeof hash.sha256 === "string"
  );
}
