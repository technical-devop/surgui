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
