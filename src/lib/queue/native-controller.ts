import type { DownloadStatus, QueueItem } from "./types.ts";

export type NativePhase = "offline" | "connecting" | "connected" | "error";

export type AddOutcome = "queued" | "pendingApproval" | "duplicate";

export interface NativeState {
  phase: NativePhase;
  items: QueueItem[];
  error: string | null;
  refreshing: boolean;
  controllingId?: string | null;
  adding?: boolean;
  addOutcome?: AddOutcome | null;
  /** True only when the active connection is the owned bundled (managed) daemon. */
  managed?: boolean;
  /** Confirmed global limit in bytes/s; 0 = unlimited; null = unknown/not loaded. */
  rateLimitBps?: number | null;
  rateLoading?: boolean;
  rateSaving?: boolean;
  rateError?: string | null;
  /** True after RateUnconfirmed: writer poisoned until the app restarts. */
  rateUnconfirmed?: boolean;
  locationOpeningId?: string | null;
  locationError?: string | null;
  locationErrorId?: string | null;
}

export type InvokeFn = (command: string, args?: Record<string, unknown>) => Promise<unknown>;
export type OnChangeFn = (snapshot: NativeState) => void;

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

function sanitizeItem(raw: unknown): QueueItem {
  const r = (raw ?? {}) as Record<string, unknown>;
  const statusRaw = typeof r.status === "string" ? r.status : "unknown";
  const status: DownloadStatus = KNOWN_STATUSES.has(statusRaw) ? (statusRaw as DownloadStatus) : "unknown";
  const bytesTotal = toSafeNumber(r.bytesTotal);
  let bytesDone = toSafeNumber(r.bytesDone);
  if (bytesTotal > 0 && bytesDone > bytesTotal) bytesDone = bytesTotal;
  return {
    id: String(r.id ?? ""),
    name: String(r.name ?? "download"),
    status,
    bytesDone,
    bytesTotal,
    speedBps: toSafeSpeed(r.speedBps),
    addedAt: toSafeNumber(r.addedAt),
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
const MANAGED_FALLBACK = "Could not start the bundled daemon. Try again.";
const RATE_FALLBACK = "Could not load the download speed limit. Try again.";
const RATE_SAVE_FALLBACK = "Could not save the download speed limit. Try again.";
const LOCATION_FALLBACK = "Download location could not be opened.";
const RATE_UNCONFIRMED_MESSAGE =
  "Speed limit state could not be confirmed. Speed control is disabled until the app restarts to avoid unsafe writes.";

function isRateUnconfirmed(err: unknown): boolean {
  return extractCodeText(err).toLowerCase().includes("rateunconfirmed");
}

function isSuperseded(err: unknown): boolean {
  return extractCodeText(err).toLowerCase().includes("superseded");
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
    managed: false,
    rateLimitBps: null,
    rateLoading: false,
    rateSaving: false,
    rateError: null,
    rateUnconfirmed: false,
    locationOpeningId: null,
    locationError: null,
    locationErrorId: null,
  };

  private epoch = 0;
  private timer: ReturnType<typeof setInterval> | null = null;
  private disposed = false;
  private controlEpoch: number | null = null;
  private addEpoch: number | null = null;
  private managedEpoch: number | null = null;
  private refreshPromise: Promise<void> | null = null;
  private refreshEpoch: number | null = null;
  private refreshSeq = 0;
  private rateSeq = 0;
  private rateWriteEpoch: number | null = null;
  private ratePoisoned = false;
  private rateAutoLoadEpoch: number | null = null;
  private rateLoadPromise: Promise<void> | null = null;
  private rateLoadEpoch: number | null = null;
  private locationEpoch: number | null = null;
  private locationSeq = 0;
  private invoke: InvokeFn;
  private onChange: OnChangeFn;
  private pollMs: number;

  constructor(invoke: InvokeFn, onChange: OnChangeFn, pollMs = 5000) {
    this.invoke = invoke;
    this.onChange = onChange;
    this.pollMs = pollMs;
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
      managed: this.state.managed ?? false,
      rateLimitBps: this.state.rateLimitBps ?? null,
      rateLoading: this.state.rateLoading ?? false,
      rateSaving: this.state.rateSaving ?? false,
      rateError: this.state.rateError ?? null,
      rateUnconfirmed: this.state.rateUnconfirmed ?? false,
      locationOpeningId: this.state.locationOpeningId ?? null,
      locationError: this.state.locationError ?? null,
      locationErrorId: this.state.locationErrorId ?? null,
    });
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

  async connect(endpoint: string, token: string): Promise<void> {
    if (this.disposed) return;
    this.epoch += 1;
    const epoch = this.epoch;
    this.stopTimer();
    this.addEpoch = null;
    this.managedEpoch = null;
    this.rateWriteEpoch = null;
    this.rateLoadPromise = null;
    this.rateLoadEpoch = null;
    this.locationEpoch = null;
    this.state.phase = "connecting";
    this.state.items = [];
    this.state.refreshing = true;
    this.state.error = null;
    this.state.controllingId = null;
    this.state.adding = false;
    this.state.addOutcome = null;
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
    this.startTimer();
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
    const epoch = this.epoch;
    this.managedEpoch = epoch;
    this.stopTimer();
    this.addEpoch = null;
    this.rateWriteEpoch = null;
    this.rateLoadPromise = null;
    this.rateLoadEpoch = null;
    this.locationEpoch = null;
    this.state.phase = "connecting";
    this.state.items = [];
    this.state.refreshing = true;
    this.state.error = null;
    this.state.controllingId = null;
    this.state.adding = false;
    this.state.addOutcome = null;
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
      this.state.managed = true;
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
      this.startTimer();
      return true;
    } finally {
      if (this.managedEpoch === epoch) this.managedEpoch = null;
    }
  }

  async refresh(): Promise<void> {
    if (this.disposed) return;
    if (this.refreshPromise !== null && this.refreshEpoch === this.epoch) {
      return this.refreshPromise;
    }
    return this.startRefresh();
  }

  private startRefresh(): Promise<void> {
    if (this.disposed) return Promise.resolve();
    const epoch = this.epoch;
    const seq = (this.refreshSeq += 1);
    const task = this.executeRefresh(epoch, seq);
    this.refreshPromise = task;
    this.refreshEpoch = epoch;
    return task.then(
      () => {
        if (this.refreshPromise === task) {
          this.refreshPromise = null;
          this.refreshEpoch = null;
        }
      },
      () => {
        if (this.refreshPromise === task) {
          this.refreshPromise = null;
          this.refreshEpoch = null;
        }
      },
    );
  }

  private async executeRefresh(epoch: number, seq: number): Promise<void> {
    if (this.disposed) return;
    this.state.refreshing = true;
    this.state.error = null;
    this.emit();
    let raw: unknown;
    try {
      raw = await this.invoke("list_downloads");
    } catch (err) {
      if (epoch !== this.epoch || this.disposed) return;
      if (seq !== this.refreshSeq) return;
      this.state.error = mapToSafeMessage(err, REFRESH_FALLBACK) ?? REFRESH_FALLBACK;
      this.state.refreshing = false;
      this.emit();
      return;
    }
    if (epoch !== this.epoch || this.disposed) return;
    if (seq !== this.refreshSeq) return;
    let items: QueueItem[];
    try {
      items = sanitizeList(raw);
    } catch (err) {
      if (epoch !== this.epoch || this.disposed) return;
      if (seq !== this.refreshSeq) return;
      this.state.error = mapToSafeMessage(err, REFRESH_FALLBACK) ?? REFRESH_FALLBACK;
      this.state.refreshing = false;
      this.emit();
      return;
    }
    if (epoch !== this.epoch || this.disposed) return;
    if (seq !== this.refreshSeq) return;
    this.state.phase = "connected";
    this.state.items = items;
    this.state.error = null;
    this.state.refreshing = false;
    this.emit();
  }

  async control(action: "pause" | "resume" | "removeRecord", id: string): Promise<void> {
    if (this.disposed) return;
    if (this.state.phase !== "connected") return;
    if (this.controlEpoch === this.epoch) return;
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

  async add(url: string): Promise<boolean> {
    if (this.disposed) return false;
    if (this.state.phase !== "connected") return false;
    const epoch = this.epoch;
    if (this.addEpoch === epoch) return false;
    this.addEpoch = epoch;
    this.state.adding = true;
    this.state.addOutcome = null;
    this.state.error = null;
    this.emit();
    try {
      let raw: unknown;
      try {
        raw = await this.invoke("add_download", { url });
      } catch (err) {
        if (epoch !== this.epoch || this.disposed) return false;
        if (this.addEpoch !== epoch) return false;
        this.state.addOutcome = null;
        this.state.error = mapToSafeMessage(err, ADD_FALLBACK) ?? ADD_FALLBACK;
        return false;
      }
      if (epoch !== this.epoch || this.disposed) return false;
      if (this.addEpoch !== epoch) return false;
      const outcome = typeof raw === "string" ? raw : "";
      if (!KNOWN_ADD_OUTCOMES.has(outcome)) {
        this.state.addOutcome = null;
        this.state.error = ADD_FALLBACK;
        return false;
      }
      if (outcome === "pendingApproval" || outcome === "duplicate") {
        this.state.addOutcome = outcome as AddOutcome;
        this.state.error = null;
        return true;
      }
      const prior = this.refreshPromise !== null && this.refreshEpoch === epoch ? this.refreshPromise : null;
      if (prior) {
        await prior.then(
          () => {},
          () => {},
        );
        if (epoch !== this.epoch || this.disposed) return false;
        if (this.addEpoch !== epoch) return false;
      }
      if (epoch !== this.epoch || this.disposed) return false;
      if (this.addEpoch !== epoch) return false;
      await this.startRefresh();
      if (epoch !== this.epoch || this.disposed) return false;
      if (this.addEpoch !== epoch) return false;
      this.state.addOutcome = "queued";
      return true;
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
    this.stopTimer();
    this.addEpoch = null;
    this.managedEpoch = null;
    this.rateWriteEpoch = null;
    this.rateLoadPromise = null;
    this.rateLoadEpoch = null;
    this.locationEpoch = null;
    this.state.phase = "offline";
    this.state.items = [];
    this.state.error = null;
    this.state.refreshing = false;
    this.state.controllingId = null;
    this.state.adding = false;
    this.state.addOutcome = null;
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
    this.stopTimer();
    this.state.controllingId = null;
    this.state.adding = false;
    this.state.addOutcome = null;
    this.addEpoch = null;
    this.managedEpoch = null;
    this.rateWriteEpoch = null;
    this.rateLoadPromise = null;
    this.rateLoadEpoch = null;
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
