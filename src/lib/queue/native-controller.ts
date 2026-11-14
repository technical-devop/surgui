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
    this.state.phase = "connecting";
    this.state.items = [];
    this.state.refreshing = true;
    this.state.error = null;
    this.state.controllingId = null;
    this.state.adding = false;
    this.state.addOutcome = null;
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
    this.state.phase = "connecting";
    this.state.items = [];
    this.state.refreshing = true;
    this.state.error = null;
    this.state.controllingId = null;
    this.state.adding = false;
    this.state.addOutcome = null;
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
        this.state.error = mapToSafeMessage(err, MANAGED_FALLBACK) ?? MANAGED_FALLBACK;
        this.state.refreshing = false;
        this.state.controllingId = null;
        this.state.adding = false;
        this.state.addOutcome = null;
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
        this.state.error = mapToSafeMessage(err, MANAGED_FALLBACK) ?? MANAGED_FALLBACK;
        this.state.refreshing = false;
        this.state.controllingId = null;
        this.state.adding = false;
        this.state.addOutcome = null;
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

  async disconnect(): Promise<void> {
    this.epoch += 1;
    this.stopTimer();
    this.addEpoch = null;
    this.managedEpoch = null;
    this.state.phase = "offline";
    this.state.items = [];
    this.state.error = null;
    this.state.refreshing = false;
    this.state.controllingId = null;
    this.state.adding = false;
    this.state.addOutcome = null;
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
