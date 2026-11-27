// @ts-nocheck — exercised at runtime by node --test (type stripping).
// The persisted hash default belongs to the backend, so two controllers in the
// same module must share its in-flight lock and ambiguity poison. Fake IPC only.
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

function tick() {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

function createBridge(initialDefault = true) {
  let serverDefault = initialDefault;
  let reads = 0;
  let writes = 0;
  let deferredWrites = 0;
  const pendingWrites = [];

  const invoke = async (command, args) => {
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
      if (deferredWrites > 0) {
        deferredWrites -= 1;
        return new Promise((resolve, reject) =>
          pendingWrites.push({ enabled: args.enabled, resolve, reject }),
        );
      }
      serverDefault = args.enabled;
      return { defaultEnabled: serverDefault };
    }
    throw new Error(`unexpected command: ${command}`);
  };

  return {
    invoke,
    get reads() {
      return reads;
    },
    get writes() {
      return writes;
    },
    get pending() {
      return pendingWrites.length;
    },
    get serverDefault() {
      return serverDefault;
    },
    deferNextWrite() {
      deferredWrites += 1;
    },
    settleNextWrite({ commit = false, error = "timeout" } = {}) {
      const pending = pendingWrites.shift();
      assert.ok(pending, "expected a deferred default write");
      if (commit) serverDefault = pending.enabled;
      if (error !== null) pending.reject(error);
      else pending.resolve({ defaultEnabled: serverDefault });
    },
    setDefault(value) {
      serverDefault = value;
    },
  };
}

describe("shared automatic-hash default coordinator", () => {
  it("blocks a second controller while one write is pending and shares poison after its late timeout", async () => {
    const bridge = createBridge(true);
    const a = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    const b = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await a.startManaged();
      await a.loadHashDefault();
      await b.startManaged();
      await b.loadHashDefault();
      assert.equal(a.state.hashDefaultEnabled, true);
      assert.equal(b.state.hashDefaultEnabled, true);

      bridge.deferNextWrite();
      const timedOutWrite = a.setHashDefault(false);
      await tick();
      assert.equal(bridge.pending, 1);
      assert.equal(bridge.writes, 1);

      assert.equal(
        await b.setHashDefault(true),
        false,
        "the second controller is blocked while the first write is pending",
      );
      assert.equal(bridge.writes, 1, "the overlapping opposite write never crossed IPC");

      bridge.settleNextWrite({ commit: false, error: "timeout" });
      assert.equal(await timedOutWrite, false);
      assert.equal(a.state.hashDefaultUnconfirmed, true);
      assert.equal(a.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);

      await b.loadHashDefault();
      assert.equal(b.state.hashDefaultEnabled, null, "the second controller sees shared uncertainty");
      assert.equal(b.state.hashDefaultUnconfirmed, true);
      assert.equal(b.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);

      assert.equal(
        await b.setHashDefault(true),
        false,
        "the opposite write is still rejected before IPC after the timeout",
      );
      assert.equal(bridge.writes, 1, "the opposite write is never sent");

      bridge.setDefault(false);
      await b.loadHashDefault();
      assert.equal(b.state.hashDefaultEnabled, null, "a later late commit is still not proof");
      assert.equal(b.state.hashDefaultUnconfirmed, true);
      assert.equal(await a.setHashDefault(true), false);
      assert.equal(bridge.writes, 1, "both controllers keep the poison");
    } finally {
      a.dispose();
      b.dispose();
    }
  });

  it("reports mode-specific recovery copy on managed and manual connections", async () => {
    const bridge = createBridge(true);
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashDefault();

      bridge.deferNextWrite();
      const timedOutWrite = c.setHashDefault(false);
      await tick();
      bridge.settleNextWrite({ commit: false, error: "timeout" });
      assert.equal(await timedOutWrite, false);

      assert.equal(c.state.managed, true);
      assert.equal(c.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);
      assert.match(c.state.hashDefaultError, /per-batch choice is still available/i);
      assert.doesNotMatch(c.state.hashDefaultError, /retry|try again/i);

      await c.disconnect();
      await c.connect("http://127.0.0.1:9", "token");
      await c.loadHashDefault();

      assert.equal(c.state.managed, false);
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(c.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED_MANUAL);
      assert.doesNotMatch(
        c.state.hashDefaultError,
        /per-batch choice is still available/i,
        "manual mode must not claim an enabled per-batch choice",
      );
      assert.match(c.state.hashDefaultError, /bundled daemon connection/i);
      assert.doesNotMatch(c.state.hashDefaultError, /retry|try again/i);
    } finally {
      c.dispose();
    }
  });

  it("poisons the shared store when a disposed controller's save times out, without emitting", async () => {
    const bridge = createBridge(true);
    let emitsA = 0;
    const a = new NativeQueueController(
      bridge.invoke,
      () => {
        emitsA += 1;
      },
      60_000,
    );
    const b = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await a.startManaged();
      await a.loadHashDefault();
      await b.startManaged();
      await b.loadHashDefault();
      assert.equal(a.state.hashDefaultEnabled, true);
      assert.equal(b.state.hashDefaultEnabled, true);

      bridge.deferNextWrite();
      const timedOutWrite = a.setHashDefault(false);
      await tick();
      assert.equal(b.state.hashDefaultSaving, true, "B sees A's unresolved shared save");
      assert.equal(await b.setHashDefault(true), false);
      assert.equal(bridge.writes, 1, "B's opposite write is blocked while A is pending");

      a.dispose();
      const emitsAfterDispose = emitsA;

      // The timed-out reply arrives after disposal, and the old side effect
      // commits later. The shared store must still be poisoned.
      bridge.settleNextWrite({ commit: false, error: "timeout" });
      assert.equal(await timedOutWrite, false);
      await tick();
      assert.equal(emitsA, emitsAfterDispose, "a disposed controller never emits");
      assert.equal(b.state.hashDefaultSaving, false);
      assert.equal(b.state.hashDefaultEnabled, null);
      assert.equal(b.state.hashDefaultUnconfirmed, true);
      assert.equal(b.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);

      bridge.setDefault(false);
      assert.equal(await b.setHashDefault(true), false);
      assert.equal(bridge.writes, 1, "the opposite write never crosses IPC after the late commit");
      await b.loadHashDefault();
      assert.equal(b.state.hashDefaultEnabled, null);
      assert.equal(b.state.hashDefaultUnconfirmed, true);
      assert.doesNotMatch(b.state.hashDefaultError, /timeout|raw/i);
    } finally {
      a.dispose();
      b.dispose();
    }
  });

  it("invalidates the other controller's baseline on a definite success", async () => {
    const bridge = createBridge(false);
    const a = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    const b = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await a.startManaged();
      await a.loadHashDefault();
      await b.startManaged();
      await b.loadHashDefault();
      assert.equal(b.state.hashDefaultEnabled, false);

      bridge.deferNextWrite();
      const save = a.setHashDefault(true);
      await tick();
      assert.equal(b.state.hashDefaultSaving, true);
      assert.equal(b.state.hashDefaultEnabled, false, "the old value stays visible while pending");
      assert.equal(await b.setHashDefault(false), false);
      assert.equal(bridge.writes, 1, "B is blocked before IPC while A is pending");

      bridge.settleNextWrite({ commit: true, error: null });
      assert.equal(await save, true);
      await tick();

      assert.equal(a.state.hashDefaultEnabled, true);
      assert.equal(a.state.hashDefaultUnconfirmed, false);
      assert.equal(b.state.hashDefaultSaving, false);
      assert.equal(b.state.hashDefaultEnabled, null, "B must re-read after A's definite success");
      assert.equal(b.state.hashDefaultUnconfirmed, false);
      assert.equal(b.state.hashDefaultError, null);

      await b.loadHashDefault();
      assert.equal(b.state.hashDefaultEnabled, true, "B recovers the confirmed value");
      assert.equal(b.state.hashDefaultError, null);
      assert.equal(bridge.writes, 1, "the success was not retried");
    } finally {
      a.dispose();
      b.dispose();
    }
  });

  it("propagates poison and manual-mode copy across controllers without leaking raw detail", async () => {
    const bridge = createBridge(true);
    const a = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    const b = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await a.startManaged();
      await a.loadHashDefault();
      await b.connect("http://127.0.0.1:9", "token");
      await b.loadHashDefault();
      assert.equal(b.state.managed, false);
      assert.equal(b.state.hashDefaultEnabled, true);

      bridge.deferNextWrite();
      const save = a.setHashDefault(false);
      await tick();
      assert.equal(b.state.hashDefaultSaving, true);

      bridge.settleNextWrite({
        commit: false,
        error: { code: "HashUnconfirmed", detail: "RAW-SECRET" },
      });
      assert.equal(await save, false);
      await tick();

      assert.equal(b.state.hashDefaultSaving, false);
      assert.equal(b.state.hashDefaultEnabled, null);
      assert.equal(b.state.hashDefaultUnconfirmed, true);
      assert.equal(b.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED_MANUAL);
      assert.doesNotMatch(b.state.hashDefaultError, /HashUnconfirmed|RAW-SECRET|retry|try again/i);
      assert.equal(await b.setHashDefault(true), false);
      assert.equal(bridge.writes, 1, "a manual controller is blocked before IPC too");
    } finally {
      a.dispose();
      b.dispose();
    }
  });
});
