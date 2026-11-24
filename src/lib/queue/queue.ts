import type { DownloadStatus, QueueItem, StatusFilter } from "./types";

export function groupByStatus(items: QueueItem[]): Record<DownloadStatus, QueueItem[]> {
  const groups: Record<DownloadStatus, QueueItem[]> = {
    active: [],
    queued: [],
    pausing: [],
    paused: [],
    failed: [],
    completed: [],
    unknown: [],
  };
  for (const item of items) {
    const bucket = (item.status in groups ? item.status : "unknown") as DownloadStatus;
    groups[bucket].push(item);
  }
  return groups;
}

export function filterQueue(items: QueueItem[], query: string, status: StatusFilter): QueueItem[] {
  const q = query.trim().toLowerCase();
  return items.filter((item) => {
    if (status !== "all" && item.status !== status) return false;
    if (q.length > 0 && !item.name.toLowerCase().includes(q)) return false;
    return true;
  });
}

export function progressRatio(item: QueueItem): number {
  if (item.bytesTotal <= 0) return 0;
  const r = item.bytesDone / item.bytesTotal;
  if (Number.isNaN(r)) return 0;
  return Math.min(1, Math.max(0, r));
}

function formatBytes(n: number): string {
  if (n < 1000) return `${n} B`;
  if (n < 1_000_000) return `${trim(n / 1000)} KB`;
  if (n < 1_000_000_000) return `${trim(n / 1_000_000)} MB`;
  return `${trim(n / 1_000_000_000)} GB`;
}

function trim(n: number): string {
  const r = Math.round(n * 10) / 10;
  return Number.isInteger(r) ? `${r}.0` : `${r}`;
}

export function formatSpeed(bps: number): string {
  if (!Number.isFinite(bps) || bps <= 0) return "—";
  return `${formatBytes(Math.round(bps))}/s`;
}

export function formatEta(item: QueueItem): string {
  if (item.status === "completed" || (item.bytesTotal > 0 && item.bytesDone >= item.bytesTotal)) {
    return "Done";
  }
  if (item.status === "failed") return "Failed";
  if (item.status === "paused") return "Paused";
  if (item.status === "pausing") return "Pausing…";
  if (item.status === "queued") return "Waiting";
  if (item.status === "unknown") return "—";
  if (!Number.isFinite(item.speedBps) || item.speedBps <= 0) return "—";
  const remaining = Math.max(0, item.bytesTotal - item.bytesDone);
  const secs = Math.ceil(remaining / item.speedBps);
  if (secs < 60) return `${secs} s left`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins} min left`;
  const hours = Math.floor(mins / 60);
  return `${hours} h left`;
}

export function formatSizePair(item: QueueItem): string {
  return `${formatBytes(item.bytesDone)} of ${formatBytes(item.bytesTotal)}`;
}

/* Global download limit helpers. Decimal MB/s with 1 MB = 1,000,000 bytes.
 * Parsing uses integer/string arithmetic only (no float) for exact bytes.
 */

/** Largest integer that survives the JSON/JS boundary; mirrors backend MAX_RATE_LIMIT_BPS. */
export const MAX_RATE_LIMIT_BPS = 9_007_199_254_740_991;

const RATE_MB_DIVISOR = 1_000_000;

export type RateParseError =
  | "empty"
  | "invalid"
  | "negative"
  | "too-precise"
  | "below-minimum"
  | "overflow";

export type RateParseResult = { ok: true; bytes: number } | { ok: false; reason: RateParseError };

/**
 * Parse decimal MB/s text to exact integer bytes/s.
 * - "0" (and "0.0", "0.000000") means unlimited.
 * - Max six fractional digits, JS safe-integer range, no floats.
 */
export function parseRateMbToBps(input: string): RateParseResult {
  const text = input.trim();
  if (text.length === 0) return { ok: false, reason: "empty" };
  if (text.startsWith("-") || text.startsWith("+")) {
    if (text.startsWith("-")) return { ok: false, reason: "negative" };
    return { ok: false, reason: "invalid" };
  }
  if (!/^\d+(\.\d+)?$/.test(text)) return { ok: false, reason: "invalid" };
  const dot = text.indexOf(".");
  const intPart = dot === -1 ? text : text.slice(0, dot);
  const fracPart = dot === -1 ? "" : text.slice(dot + 1);
  if (fracPart.length > 6) return { ok: false, reason: "too-precise" };
  const intNorm = intPart.replace(/^0+(?=\d)/, "");
  // Exact bytes via BigInt: bytes = int * 1e6 + frac padded to 6 digits.
  const padded = (fracPart + "000000").slice(0, 6);
  let bytesBig: bigint;
  try {
    const intBig = intNorm === "" ? 0n : BigInt(intNorm);
    const fracBig = padded === "" ? 0n : BigInt(padded);
    bytesBig = intBig * 1000000n + fracBig;
  } catch {
    return { ok: false, reason: "invalid" };
  }
  if (bytesBig > BigInt(MAX_RATE_LIMIT_BPS)) return { ok: false, reason: "overflow" };
  if (bytesBig === 0n) {
    // Exact zero (e.g. "0", "0.0") is unlimited; anything else that truncates
    // to zero cannot happen with ≤6 digits, but guard the boundary anyway.
    const isAllZero = /^0+$/.test(intNorm === "" ? "0" : intNorm) && /^0*$/.test(fracPart);
    if (!isAllZero) return { ok: false, reason: "below-minimum" };
    return { ok: true, bytes: 0 };
  }
  if (bytesBig < 1n) return { ok: false, reason: "below-minimum" };
  return { ok: true, bytes: Number(bytesBig) };
}

/** Format integer bytes/s as minimal decimal MB/s; "0" means unlimited. Round-trips via parseRateMbToBps. */
export function formatRateBpsToMb(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0";
  const clamped = Math.min(MAX_RATE_LIMIT_BPS, Math.floor(bytes));
  const whole = Math.floor(clamped / RATE_MB_DIVISOR);
  const rest = clamped % RATE_MB_DIVISOR;
  if (rest === 0) return `${whole}`;
  const frac = `${rest}`.padStart(6, "0").replace(/0+$/, "");
  return `${whole}.${frac}`;
}

/** User-facing message for a local rate-input validation failure. No raw input echoed. */
export function rateInputErrorMessage(reason: RateParseError): string {
  switch (reason) {
    case "empty":
      return "Enter a limit in MB/s, or 0 for unlimited.";
    case "negative":
      return "Limit cannot be negative. Enter 0 for unlimited or a positive MB/s value.";
    case "too-precise":
      return "Use at most six digits after the decimal point.";
    case "below-minimum":
      return "Positive limits must be at least 1 byte/s. Enter 0 for unlimited.";
    case "overflow":
      return "Limit is too large for this app to send safely.";
    case "invalid":
    default:
      return "Enter a number in MB/s, for example 2.5. Use 0 for unlimited.";
  }
}

/* Multi-URL batch helpers.
 *
 * Submission is bounded and sequential in the controller; these helpers only
 * parse, validate, and label. Validation mirrors the pinned Surge v0.12.4
 * `validate_download_url`: http(s), a host, no embedded credentials, no
 * whitespace/control characters, and an 8 KiB cap. Invalid links are never
 * sent, so they are reported locally as "invalid" rather than as a failure.
 */

/** Upper bound for one pasted batch; extra lines stay recoverable and unsent. */
export const MAX_BATCH_LINES = 100;

/** Same 8 KiB cap the Rust client enforces before any network request. */
export const MAX_DOWNLOAD_URL_BYTES = 8 * 1024;

export const MAX_CONCURRENT_DOWNLOADS_MIN = 1;
export const MAX_CONCURRENT_DOWNLOADS_MAX = 10;
export const MAX_CONCURRENT_DOWNLOADS_DEFAULT = 3;

export type UrlLineError =
  | "empty"
  | "too-long"
  | "whitespace"
  | "scheme"
  | "host"
  | "credentials"
  | "malformed";

export type UrlLineValidation = { ok: true; url: string } | { ok: false; reason: UrlLineError };

function utf8ByteLength(text: string): number {
  return new TextEncoder().encode(text).length;
}

export function validateDownloadUrl(raw: string): UrlLineValidation {
  const url = raw.trim();
  if (url.length === 0) return { ok: false, reason: "empty" };
  if (utf8ByteLength(url) > MAX_DOWNLOAD_URL_BYTES) return { ok: false, reason: "too-long" };
  if (/[\s\u0000-\u001f\u007f]/.test(url)) return { ok: false, reason: "whitespace" };
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return { ok: false, reason: "malformed" };
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    return { ok: false, reason: "scheme" };
  }
  if (parsed.hostname.length === 0) return { ok: false, reason: "host" };
  if (parsed.username.length > 0 || parsed.password.length > 0) {
    return { ok: false, reason: "credentials" };
  }
  return { ok: true, url };
}

export type BatchPlanEntry =
  | { index: number; line: string; kind: "url"; url: string }
  | { index: number; line: string; kind: "invalid"; reason: UrlLineError }
  | { index: number; line: string; kind: "over-limit" };

/** One line per URL; blank lines are ignored and order is preserved. */
export function planUrlLines(input: string, limit = MAX_BATCH_LINES): BatchPlanEntry[] {
  const lines = input
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  return lines.map((line, index) => {
    if (index >= limit) return { index, line, kind: "over-limit" };
    const validation = validateDownloadUrl(line);
    return validation.ok
      ? { index, line, kind: "url", url: validation.url }
      : { index, line, kind: "invalid", reason: validation.reason };
  });
}

export type BatchLinkStatus =
  | "queued"
  | "pendingApproval"
  | "duplicate"
  | "duplicateInBatch"
  | "invalid"
  | "failed"
  | "ambiguous"
  | "unattempted";

export const BATCH_STATUS_LABELS: Record<BatchLinkStatus, string> = {
  queued: "Accepted",
  pendingApproval: "Pending approval",
  duplicate: "Duplicate",
  duplicateInBatch: "Repeated in batch",
  invalid: "Invalid",
  failed: "Failed",
  ambiguous: "Unconfirmed",
  unattempted: "Not attempted",
};

export function urlLineErrorMessage(reason: UrlLineError): string {
  switch (reason) {
    case "too-long":
      return "URL is longer than the 8 KB limit.";
    case "whitespace":
      return "URL contains whitespace or control characters.";
    case "scheme":
      return "Use an http:// or https:// URL.";
    case "host":
      return "URL has no host.";
    case "credentials":
      return "Remove embedded username or password from the URL.";
    case "empty":
      return "Line is empty.";
    case "malformed":
    default:
      return "Not a parseable URL.";
  }
}

export function batchStatusMessage(status: BatchLinkStatus, reason?: UrlLineError): string {
  switch (status) {
    case "queued":
      return "Daemon queued it. The row may appear after refresh.";
    case "pendingApproval":
      return "Not queued yet. Approve it in the daemon TUI.";
    case "duplicate":
      return "Daemon already tracks this URL; not queued.";
    case "duplicateInBatch":
      return "This exact URL already appeared earlier in the batch; it was not sent again.";
    case "invalid":
      return reason ? urlLineErrorMessage(reason) : "Not a valid download URL.";
    case "failed":
      return "Daemon did not accept this URL. Not retried automatically.";
    case "ambiguous":
      return "Request may have reached the daemon. Not retried automatically.";
    case "unattempted":
    default:
      return "Batch stopped before this link was sent.";
  }
}

/** Generic label used whenever a URL cannot be safely shown (non-http or malformed). */
export const INVALID_URL_DISPLAY = "Invalid URL";

/**
 * Safe per-link label for the results list: strip embedded credentials, the
 * query string and the fragment so signed secrets pasted into a URL are not
 * echoed by the UI. The full original line is retained only in the editable
 * input when it is recoverable. Non-http(s) schemes (including opaque URLs such
 * as `mailto:alice:SECRET@example.com`) and unparseable input fail closed to a
 * generic label that contains no part of the original input.
 */
export function redactUrlForDisplay(raw: string): string {
  const line = raw.trim();
  let parsed: URL;
  try {
    parsed = new URL(line);
  } catch {
    return INVALID_URL_DISPLAY;
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    return INVALID_URL_DISPLAY;
  }
  parsed.username = "";
  parsed.password = "";
  parsed.search = parsed.search ? "?redacted" : "";
  parsed.hash = "";
  return parsed.toString();
}

/**
 * Merge recoverable batch lines into the editable input without discarding a
 * draft the user started while the request was in flight. Existing draft text
 * stays first; exact duplicate lines already present are not appended again.
 */
export function mergeRecoverableLines(draft: string, recoverable: string[]): string {
  const base = draft.trim();
  const seen = new Set(
    base
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line.length > 0),
  );
  const extras: string[] = [];
  for (const line of recoverable) {
    const trimmed = line.trim();
    if (trimmed.length === 0 || seen.has(trimmed)) continue;
    seen.add(trimmed);
    extras.push(line);
  }
  if (extras.length === 0) return base;
  return base.length > 0 ? `${base}\n${extras.join("\n")}` : extras.join("\n");
}

/**
 * Move only the not-yet-delivered recovery lines into the editable draft.
 * Existing draft text stays first and the pending list is cleared, so a later
 * connect/disconnect never resurrects a link the user removed or resubmitted.
 */
export function consumeRecoveryDraft(
  draft: string,
  pending: string[],
): { draft: string; pending: string[] } {
  if (pending.length === 0) return { draft, pending: [] };
  return { draft: mergeRecoverableLines(draft, pending), pending: [] };
}

export interface BatchDraftResolution {
  draft: string;
  submitted: string;
  hasResults: boolean;
  recoverable: string[];
  operationId: number;
  currentOperationId: number;
  epoch: number;
  currentEpoch: number;
  sameController: boolean;
}

/**
 * Decide the textarea value after a batch settles. Returns null when the
 * result belongs to a superseded controller, operation or connection epoch,
 * so callers leave the current draft completely untouched.
 */
export function resolveBatchDraft(resolution: BatchDraftResolution): string | null {
  if (!resolution.sameController) return null;
  if (resolution.operationId !== resolution.currentOperationId) return null;
  if (resolution.epoch !== resolution.currentEpoch) return null;
  if (!resolution.hasResults) {
    return mergeRecoverableLines(resolution.draft, [resolution.submitted]);
  }
  if (resolution.recoverable.length === 0) return resolution.draft;
  return mergeRecoverableLines(resolution.draft, resolution.recoverable);
}

/** True only for a whole number inside the daemon's 1–10 contract. */
export function isValidMaxConcurrentValue(value: unknown): value is number {
  return (
    typeof value === "number" &&
    Number.isInteger(value) &&
    value >= MAX_CONCURRENT_DOWNLOADS_MIN &&
    value <= MAX_CONCURRENT_DOWNLOADS_MAX
  );
}
