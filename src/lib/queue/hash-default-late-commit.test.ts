// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Deterministic fake-IPC regression for a timed-out automatic-hash default save
// whose old side effect commits only after a matching read and an attempted
// opposite write. A matching read is not a completion fence, so the controller
// must stay unknown, reject future writes before IPC, and keep that uncertainty
// across reconnect and manual connections.
import { beforeEach, describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  NativeQueueController,
  resetSharedHashDefaultStoreForTests,
} from "./native-controller.ts";

beforeEach(resetSharedHashDefaultStoreForTests);

const HASH_DEFAULT_UNCONFIRMED =
  "The saved hashing default could not be confirmed after a save. Changing it is disabled until the app restarts to avoid unsafe writes; a per-batch choice is still available.";
const HASH_DEFAULT_UNCONFIRMED_MANUAL =
  "The saved hashing default could not be confirmed after a save. Changing it is disabled until the app restarts to avoid unsafe writes; per-batch hashing is available only on the bundled daemon connection.";

function createLateCommitBridge(initialDefault = true) {
  let serverDefault = initialDefault;
  let reads = 0;
  let writes = 0;
  let lateCommit = null;
  const calls = [];

  const invoke = async (command, args) => {
    calls.push({ command, args });
    if (command === "start_managed_surge") return [];
    if (command === "connect_surge") return [];
    if (command === "disconnect_surge") return { ok: true };
    if (command === "list_downloads") return [];
    if (command === "get_download_hashes") return [];
    if (command === "refresh_download_hashes") return [];
    if (command === "get_automatic_hash_settings") {
      reads += 1;
      return { defaultEnabled: serverDefault };
    }
    if (command === "set_automatic_hash_default") {
      writes += 1;
      const target = args.enabled;
      if (writes === 1) {
        // The timeout crosses IPC independently of the old side effect. The
        // daemon has not committed yet; commitLate() models the delayed write.
        lateCommit = () => {
          serverDefault = target;
          lateCommit = null;
        };
        throw "timeout";
      }
      // This path must never run after the poison; if it does, the test's
      // writes counter records the duplicate IPC.
      serverDefault = target;
      return { defaultEnabled: serverDefault };
    }
    throw new Error(`unexpected command: ${command}`);
  };

  return {
    invoke,
    calls,
    get reads() {
      return reads;
    },
    get writes() {
      return writes;
    },
    get serverDefault() {
      return serverDefault;
    },
    commitLate() {
      assert.ok(lateCommit, "expected a deferred old side effect");
      lateCommit();
    },
    setDefault(value) {
      serverDefault = value;
    },
  };
}

describe("timed-out automatic-hash default save with a late commit", () => {
  it("keeps the UI unknown and rejects opposite writes before IPC across reconnect/manual", async () => {
    const bridge = createLateCommitBridge(true);
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, true);

      // Save the same value that is already stored. The reply times out, but a
      // matching read below cannot prove this write will not commit later.
      const saved = await c.setHashDefault(true);
      assert.equal(saved, false);
      assert.equal(c.state.hashDefaultEnabled, null, "the UI is unknown, not the old value");
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(c.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);
      assert.doesNotMatch(c.state.hashDefaultError, /retry|try again/i);
      assert.equal(bridge.writes, 1, "the timed-out mutation is not retried");

      // The matching read is deliberately performed before the old side effect
      // and before the opposite write attempt.
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, null, "the matching read is not proof");
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(c.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);

      // The opposite write must be rejected before IPC, even though its target
      // differs from the ambiguous write.
      assert.equal(await c.setHashDefault(false), false);
      assert.equal(bridge.writes, 1, "the opposite write never crossed IPC");

      // Only now does the old timed-out save commit its original target.
      bridge.commitLate();
      assert.equal(bridge.serverDefault, true);
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, null, "the delayed commit stays unknown");
      assert.equal(c.state.hashDefaultUnconfirmed, true);

      // Uncertainty is app-wide and survives a managed reconnect and reload.
      await c.disconnect();
      await c.startManaged();
      await c.loadHashDefault();
      assert.equal(c.state.phase, "connected");
      assert.equal(c.state.hashDefaultEnabled, null);
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(c.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);
      assert.equal(await c.setHashDefault(false), false);
      assert.equal(bridge.writes, 1, "managed reconnect does not allow a stale write");

      // It also survives switching to a manual connection, still rejecting
      // writes before IPC there.
      await c.disconnect();
      await c.connect("http://127.0.0.1:9", "token");
      await c.loadHashDefault();
      assert.equal(c.state.managed, false);
      assert.equal(c.state.hashDefaultEnabled, null);
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(c.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED_MANUAL);
      assert.doesNotMatch(c.state.hashDefaultError, /per-batch choice is still available/i);
      assert.doesNotMatch(c.state.hashDefaultError, /retry|try again/i);
      assert.equal(await c.setHashDefault(true), false);
      assert.equal(bridge.writes, 1, "manual mode also rejects before IPC");
    } finally {
      c.dispose();
    }
  });

  it("keeps uncertainty after a matching read and a managed reconnect", async () => {
    const bridge = createLateCommitBridge(true);
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashDefault();

      const saved = await c.setHashDefault(false);
      assert.equal(saved, false);
      assert.equal(c.state.hashDefaultUnconfirmed, true);

      // The first attempt is still the only mutation. Neither a matching read
      // nor a reconnect may let a success-shaped state repaint the default.
      bridge.setDefault(true);
      await c.loadHashDefault();
      await c.disconnect();
      await c.startManaged();
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, null);
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(bridge.writes, 1, "the blocked retry never crossed IPC");
    } finally {
      c.dispose();
    }
  });
});
