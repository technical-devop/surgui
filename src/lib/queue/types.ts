export type DownloadStatus = "active" | "queued" | "pausing" | "paused" | "failed" | "completed" | "unknown";

export type QueuePhase = "queued" | "retrying" | "downloading" | "verifying" | "finalizing" | "unknown";

export interface QueueItem {
  id: string;
  name: string;
  status: DownloadStatus;
  bytesDone: number;
  bytesTotal: number;
  /** bytes per second; 0 when stalled or not started */
  speedBps: number;
  addedAt: number;
  error?: string;
  /** Sanitized daemon phase; absent when an older daemon omits it. */
  phase?: QueuePhase;
  /** A post-download hook is running; never treat the row as Done while true. */
  processing?: boolean;
  /** A post-download hook has finished (successfully or not). */
  processed?: boolean;
  /** Only the failed bit is exposed; hook text/commands/secrets never cross IPC. */
  hookFailed?: boolean;
}

export type StatusFilter = "all" | DownloadStatus;

export const STATUS_ORDER: DownloadStatus[] = ["active", "queued", "pausing", "paused", "failed", "completed", "unknown"];

/** Bulk queue actions; each maps to one allowlisted daemon route. */
export type BulkAction = "pauseAll" | "resumeAll";

export type BulkStatus = "done" | "failed" | "unconfirmed";

/** One sanitized failed item from a bulk response; safe to display. */
export interface BulkItemFailure {
  id: string;
  error?: string;
}

/**
 * Display-safe result of the last bulk action. `accepted`/`failed` are null
 * only when the daemon outcome could not be confirmed; the queue is then
 * reconciled with a fresh daemon read rather than guessed.
 */
export interface BulkSummary {
  action: BulkAction;
  status: BulkStatus;
  accepted: number | null;
  failed: number | null;
  total: number | null;
  failures: BulkItemFailure[];
  message: string;
}

/**
 * Persistent warning for a bulk request whose outcome was never confirmed. It
 * survives reconnect, is dismissed individually, and never claims the current
 * connection is unaffected (it may be the same daemon); nothing is retried.
 */
export interface BulkUncertainty {
  /** Locally generated monotonic id; never a daemon/server identifier. */
  id: number;
  action: BulkAction;
  message: string;
}
