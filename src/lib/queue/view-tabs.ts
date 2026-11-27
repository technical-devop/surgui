/**
 * View-tab + hash-control helpers for the Surgui cleanup.
 * Pure logic only (no Svelte, no DOM) so `npm test` (node --test)
 * can cover tab navigation, toggle enablement, and saving/failure
 * states without mounting components.
 */

export type AppTab = "downloads" | "settings";

export const APP_TABS: readonly AppTab[] = ["downloads", "settings"] as const;

export const DEFAULT_APP_TAB: AppTab = "downloads";

export function isAppTab(value: unknown): value is AppTab {
  return value === "downloads" || value === "settings";
}

/** Resolve any stored/requested tab to a safe tab. Unknown → downloads. */
export function resolveAppTab(value: unknown): AppTab {
  return isAppTab(value) ? value : DEFAULT_APP_TAB;
}

/** Keyboard navigation for the tablist: ArrowLeft/Right, Home/End. */
export function nextAppTab(
  current: AppTab,
  key: "ArrowLeft" | "ArrowRight" | "Home" | "End",
): AppTab {
  const order: AppTab[] = ["downloads", "settings"];
  const index = order.indexOf(current);
  if (key === "Home") return "downloads";
  if (key === "End") return "settings";
  if (key === "ArrowRight") return order[(index + 1) % order.length] as AppTab;
  return order[(index - 1 + order.length) % order.length] as AppTab;
}

export interface HashControlsInput {
  isManaged: boolean;
  isConnected: boolean;
  addBusy: boolean;
  hashForBatch: boolean;
  hashDefaultEnabled: boolean | null;
  hashDefaultLoading: boolean;
  hashDefaultSaving: boolean;
  hashDefaultError: string | null;
}

export interface HashControlsState {
  /** Per-batch toggle disabled (managed + connected + idle required). */
  perBatchDisabled: boolean;
  /** Persist-as-default control disabled. */
  persistDisabled: boolean;
  /** Show the managed-only disabled reason. */
  showManagedOnlyNote: boolean;
  /** Show the "Default for future batches: on/off" status line. */
  showDefaultStatus: boolean;
  defaultStatusLabel: string | null;
  /** Persist control needs user attention (dirty vs default). */
  persistDirty: boolean;
}

/**
 * Enablement matrix for the compact hash controls.
 * - Per-batch toggle works only on the managed connected daemon while idle.
 * - Persist control saves the current per-batch choice as the app default;
 *   it is disabled while loading/saving, when offline/unmanaged, or while
 *   a batch runs. Dirty = choice differs from the persisted default.
 */
export function getHashControlsState(input: HashControlsInput): HashControlsState {
  const { isManaged, isConnected, addBusy } = input;
  const online = isManaged && isConnected;
  const perBatchDisabled = !online || addBusy;
  const persistDisabled =
    !online || addBusy || input.hashDefaultLoading || input.hashDefaultSaving;
  const showManagedOnlyNote = !isManaged;
  const showDefaultStatus =
    isManaged && input.hashDefaultEnabled !== null && input.hashDefaultEnabled !== undefined;
  const defaultStatusLabel =
    input.hashDefaultEnabled === null || input.hashDefaultEnabled === undefined
      ? null
      : `Default for future batches: ${input.hashDefaultEnabled ? "on" : "off"}.`;
  const persistDirty =
    input.hashDefaultEnabled !== null &&
    input.hashDefaultEnabled !== undefined &&
    input.hashForBatch !== input.hashDefaultEnabled;
  return {
    perBatchDisabled,
    persistDisabled,
    showManagedOnlyNote,
    showDefaultStatus,
    defaultStatusLabel,
    persistDirty,
  };
}

export interface DraftAfterDisconnect {
  draft: string;
  dirty: boolean;
  touched: boolean;
}

/**
 * Settings drafts survive a reconnect only when the user has unsaved edits.
 * A dirty draft is never sent automatically; it stays visible until the user
 * explicitly saves it on the next connection. A clean draft is cleared so an
 * old daemon's value can never masquerade as the new daemon's setting.
 */
export function draftAfterConnectionLoss(
  draft: string,
  dirty: boolean,
  touched: boolean,
): DraftAfterDisconnect {
  if (dirty) return { draft, dirty: true, touched };
  return { draft: "", dirty: false, touched: false };
}

export interface SettingsSaveState {
  canSave: boolean;
  disabled: boolean;
  showError: boolean;
}

/** Shared saving/failure matrix for Settings controls (rate + max-concurrent). */
export function getSettingsSaveState(options: {
  isManaged: boolean;
  isConnected: boolean;
  loading: boolean;
  saving: boolean;
  unconfirmed: boolean;
  dirty: boolean;
  valid: boolean;
}): SettingsSaveState {
  const { isManaged, isConnected, loading, saving, unconfirmed, dirty, valid } = options;
  const canSave =
    isManaged && isConnected && !loading && !saving && !unconfirmed && dirty && valid;
  const disabled = !isManaged || !isConnected || loading || saving || unconfirmed;
  const showError = !loading;
  return { canSave, disabled, showError };
}
