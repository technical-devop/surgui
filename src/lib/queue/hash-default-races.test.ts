// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Deterministic fake-IPC regressions for automatic-hash default read/write
// ordering across epochs. No daemon, paths, URLs or secrets are used.
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

function createBridge(options = {}) {
  let serverDefault = options.initialDefault ?? false;
  let reads = 0;
  let writes = 0;
  let deferredGets = 0;
  let deferredSets = 0;
  const pendingGets = [];
  const pendingSets = [];
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
      // Capture the value at invocation time: a delayed read must be able to
      // return the old snapshot even after a concurrent write commits.
      const value = serverDefault;
      if (deferredGets > 0) {
        deferredGets -= 1;
        return new Promise((resolve) => pendingGets.push({ value, resolve }));
      }
      return { defaultEnabled: value };
    }
    if (command === "set_automatic_hash_default") {
      writes += 1;
      if (deferredSets > 0) {
        deferredSets -= 1;
        return new Promise((resolve, reject) =>
          pendingSets.push({ enabled: args.enabled, resolve, reject }),
        );
      }
      serverDefault = args.enabled;
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
    get pendingGets() {
      return pendingGets.length;
    },
    get pendingSets() {
      return pendingSets.length;
    },
    get serverDefault() {
      return serverDefault;
    },
    setDefault(value) {
      serverDefault = value;
    },
    deferNextGet() {
      deferredGets += 1;
    },
    releaseNextGet() {
      const pending = pendingGets.shift();
      assert.ok(pending, "expected a deferred default read");
      pending.resolve({ defaultEnabled: pending.value });
    },
    deferNextSet() {
      deferredSets += 1;
    },
    settleNextSet({ commit = true, error = null, response } = {}, index = 0) {
      const [pending] = pendingSets.splice(index, 1);
      assert.ok(pending, "expected a deferred default write");
      if (commit) serverDefault = pending.enabled;
      if (error !== null) pending.reject(error);
      else pending.resolve({ defaultEnabled: response ?? pending.enabled });
    },
  };
}

function makeController(bridge) {
  return new NativeQueueController(bridge.invoke, () => {}, 60_000);
}

describe("automatic-hash default generation and mutation barrier", () => {
  it("repro 1: a stale read begun before a committed-but-failed save cannot restore the old value", async () => {
    const bridge = createBridge({ initialDefault: false });
    const c = makeController(bridge);
    try {
      await c.startManaged();
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, false);

      // A read snapshots the stale false value, then a save commits true but
      // loses its reply before the read resolves.
      bridge.deferNextGet();
      const staleRead = c.loadHashDefault();
      assert.equal(bridge.pendingGets, 1);

      bridge.deferNextSet();
      const save = c.setHashDefault(true);
      assert.equal(bridge.pendingSets, 1);
      bridge.settleNextSet({ commit: true, error: "network" });
      assert.equal(await save, false);
      assert.equal(c.state.hashDefaultEnabled, null, "the save uncertainty clears the baseline");
      assert.equal(c.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);

      bridge.releaseNextGet();
      await staleRead;
      await tick();

      assert.equal(
        c.state.hashDefaultEnabled,
        null,
        "the stale read must not resurrect the pre-save value",
      );
      assert.equal(
        c.state.hashDefaultError,
        HASH_DEFAULT_UNCONFIRMED,
        "the stale read must not clear the save uncertainty",
      );
      assert.equal(bridge.reads, 2, "one baseline read plus the deliberately stale read");
      assert.equal(bridge.writes, 1, "the mutation is never retried");
      assert.equal(bridge.serverDefault, true, "the fake daemon did commit the save");
    } finally {
      c.dispose();
    }
  });

  it("repro 2: a stale failed save invalidates the new epoch baseline on the shared managed store", async () => {
    const bridge = createBridge({ initialDefault: false });
    const c = makeController(bridge);
    try {
      await c.startManaged();
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, false);

      bridge.deferNextSet();
      const oldSave = c.setHashDefault(true);

      await c.disconnect();
      await c.startManaged();
      await c.loadHashDefault();
      assert.equal(
        c.state.hashDefaultEnabled,
        null,
        "the outstanding old mutation prevents an unproven new-epoch read from applying",
      );

      bridge.settleNextSet({ commit: true, error: "network" });
      assert.equal(await oldSave, false);
      await tick();

      assert.equal(bridge.serverDefault, true, "the old save committed on the shared store");
      assert.equal(
        c.state.hashDefaultEnabled,
        null,
        "the stale failure invalidates the replacement baseline",
      );
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(c.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);
      assert.equal(bridge.writes, 1, "the stale mutation is never retried");
      assert.equal(await c.setHashDefault(false), false);
      assert.equal(bridge.writes, 1, "the poison blocks later writes before IPC");
    } finally {
      c.dispose();
    }
  });

  it("invalidates the new epoch baseline when a stale save acknowledgement succeeds", async () => {
    const bridge = createBridge({ initialDefault: false });
    const c = makeController(bridge);
    try {
      await c.startManaged();
      await c.loadHashDefault();

      bridge.deferNextSet();
      const oldSave = c.setHashDefault(true);

      await c.disconnect();
      await c.startManaged();
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, null);

      bridge.settleNextSet({ commit: true });
      assert.equal(await oldSave, false, "a replaced epoch never reports the old save as current");
      await tick();

      assert.equal(c.state.hashDefaultEnabled, null);
      assert.notEqual(c.state.hashDefaultError, null);

      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, true, "an explicit current read recovers");
      assert.equal(c.state.hashDefaultError, null);
      assert.equal(bridge.writes, 1, "the mutation is never retried");
    } finally {
      c.dispose();
    }
  });

  it("fences a new-epoch read that began before the old save settled", async () => {
    const bridge = createBridge({ initialDefault: false });
    const c = makeController(bridge);
    try {
      await c.startManaged();
      await c.loadHashDefault();

      bridge.deferNextSet();
      const oldSave = c.setHashDefault(true);

      await c.disconnect();
      await c.startManaged();
      bridge.deferNextGet();
      const newRead = c.loadHashDefault();
      assert.equal(bridge.pendingGets, 1);

      bridge.settleNextSet({ commit: true, error: "network" });
      assert.equal(await oldSave, false);
      await tick();
      assert.equal(c.state.hashDefaultEnabled, null);

      bridge.releaseNextGet();
      await newRead;
      await tick();

      assert.equal(
        c.state.hashDefaultEnabled,
        null,
        "a read begun before the old write settled cannot repaint false",
      );
      assert.equal(bridge.writes, 1);
    } finally {
      c.dispose();
    }
  });

  it("never treats a matching read as proof after a timeout and blocks the opposite write", async () => {
    const bridge = createBridge({ initialDefault: false });
    const c = makeController(bridge);
    try {
      await c.startManaged();
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, false);

      bridge.deferNextSet();
      const save = c.setHashDefault(true);
      bridge.settleNextSet({ commit: true, error: "timeout" });
      assert.equal(await save, false);
      assert.equal(c.state.hashDefaultEnabled, null);
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(c.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);

      // A read matching the attempted target is not a completion fence: the
      // timed-out write may still commit later, so the uncertainty remains.
      bridge.setDefault(true);
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, null);
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(c.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);
      assert.equal(bridge.writes, 1, "the mutation is never retried");

      // An opposite write is rejected before IPC while the default is unknown.
      assert.equal(await c.setHashDefault(false), false);
      assert.equal(bridge.writes, 1, "no mutation crossed IPC after poison");
    } finally {
      c.dispose();
    }
  });

  it("keeps a HashUnconfirmed save poisoned even when a read returns the target", async () => {
    const bridge = createBridge({ initialDefault: true });
    const c = makeController(bridge);
    try {
      await c.startManaged();
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, true);

      bridge.deferNextSet();
      const save = c.setHashDefault(false);
      bridge.settleNextSet({ commit: false, error: { code: "HashUnconfirmed", detail: "RAW" } });
      assert.equal(await save, false);
      assert.equal(c.state.hashDefaultEnabled, null);
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(c.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);

      // Same baseline value as before the write: not proof.
      bridge.setDefault(true);
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, null);
      assert.equal(c.state.hashDefaultUnconfirmed, true);

      // The target value is also not proof; the write may still commit later.
      bridge.setDefault(false);
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, null);
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(bridge.writes, 1);

      assert.equal(await c.setHashDefault(true), false);
      assert.equal(bridge.writes, 1, "writes stay blocked after poison");
    } finally {
      c.dispose();
    }
  });

  it("treats a superseded save as ambiguous instead of silently dismissing it", async () => {
    const bridge = createBridge({ initialDefault: true });
    const c = makeController(bridge);
    try {
      await c.startManaged();
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, true);

      bridge.deferNextSet();
      const save = c.setHashDefault(false);
      bridge.settleNextSet({ commit: false, error: "superseded" });
      assert.equal(await save, false);
      assert.equal(c.state.hashDefaultEnabled, null);
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(c.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);

      assert.equal(await c.setHashDefault(true), false);
      assert.equal(bridge.writes, 1, "a superseded save still blocks future writes");
    } finally {
      c.dispose();
    }
  });

  it("allows exactly one mutation in flight globally and rejects the second before IPC", async () => {
    const bridge = createBridge({ initialDefault: false });
    const c = makeController(bridge);
    try {
      await c.startManaged();
      await c.loadHashDefault();

      bridge.deferNextSet();
      const first = c.setHashDefault(true);
      assert.equal(bridge.pendingSets, 1);
      assert.equal(bridge.writes, 1);

      // The second write is simultaneous but must not cross IPC because the
      // app-wide default already has one mutation in flight.
      const second = await c.setHashDefault(false);
      assert.equal(second, false);
      assert.equal(bridge.writes, 1, "the rejected write issues no IPC");
      assert.equal(bridge.pendingSets, 1, "no duplicate mutation was queued");

      bridge.settleNextSet({ commit: true });
      assert.equal(await first, true);
      await tick();
      assert.equal(c.state.hashDefaultEnabled, true);
      assert.equal(bridge.writes, 1, "still exactly one mutation");
    } finally {
      c.dispose();
    }
  });

  it("rejects a new-epoch write while an old-epoch mutation is still pending", async () => {
    const bridge = createBridge({ initialDefault: false });
    const c = makeController(bridge);
    try {
      await c.startManaged();
      await c.loadHashDefault();

      bridge.deferNextSet();
      const oldSave = c.setHashDefault(true);
      assert.equal(bridge.pendingSets, 1);

      await c.disconnect();
      await c.startManaged();
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, null);

      assert.equal(await c.setHashDefault(false), false);
      assert.equal(bridge.writes, 1, "the new epoch write is rejected before IPC");
      assert.equal(bridge.pendingSets, 1);

      bridge.settleNextSet({ commit: true });
      assert.equal(await oldSave, false, "the old epoch never reports as current");
      await tick();
      assert.equal(c.state.hashDefaultEnabled, null);
      assert.equal(c.state.hashDefaultUnconfirmed, false, "a success is not ambiguity");

      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, true, "a definite completion allows a fresh read");
      assert.equal(c.state.hashDefaultError, null);
      assert.equal(bridge.writes, 1);
    } finally {
      c.dispose();
    }
  });

  it("keeps an ambiguous older write visible after a later opposite attempt is rejected", async () => {
    const bridge = createBridge({ initialDefault: false });
    const c = makeController(bridge);
    try {
      await c.startManaged();
      await c.loadHashDefault();

      bridge.deferNextSet();
      const older = c.setHashDefault(true);
      bridge.settleNextSet({ commit: true, error: "network" });
      assert.equal(await older, false);
      await tick();
      assert.equal(c.state.hashDefaultEnabled, null);
      assert.equal(c.state.hashDefaultUnconfirmed, true);

      // The newer opposite write cannot be issued after the poison, so it
      // cannot overwrite the uncertainty with a false success.
      assert.equal(await c.setHashDefault(false), false);
      assert.equal(bridge.writes, 1, "no mutation retry or duplicate");

      // The old write committed true. Even a matching read cannot prove that
      // no further delayed commit remains.
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, null);
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(bridge.writes, 1);
    } finally {
      c.dispose();
    }
  });

  it("deduplicates concurrent default reads without losing the mutation barrier", async () => {
    const bridge = createBridge({ initialDefault: false });
    const c = makeController(bridge);
    try {
      await c.startManaged();
      bridge.deferNextGet();
      const first = c.loadHashDefault();
      const second = c.loadHashDefault();
      assert.equal(bridge.reads, 1, "concurrent reads share one request");
      assert.equal(bridge.pendingGets, 1);

      bridge.releaseNextGet();
      await Promise.all([first, second]);
      assert.equal(c.state.hashDefaultEnabled, false);
      assert.equal(bridge.reads, 1);
    } finally {
      c.dispose();
    }
  });

  it("poisons a manual connection baseline too when a stale managed save errors ambiguously", async () => {
    const bridge = createBridge({ initialDefault: true });
    const c = makeController(bridge);
    try {
      await c.startManaged();
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, true);

      bridge.deferNextSet();
      const managedSave = c.setHashDefault(false);

      await c.disconnect();
      await c.connect("http://127.0.0.1:9", "token");
      await c.loadHashDefault();
      assert.equal(c.state.managed, false);
      // The old managed write is still pending; a new-epoch read cannot apply.
      assert.equal(c.state.hashDefaultEnabled, null);

      bridge.settleNextSet({ commit: false, error: "network" });
      assert.equal(await managedSave, false);
      await tick();

      assert.equal(
        c.state.hashDefaultEnabled,
        null,
        "an ambiguous write on any connection poisons every mode",
      );
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(c.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED_MANUAL);
      assert.equal(bridge.writes, 1);

      assert.equal(await c.setHashDefault(true), false);
      assert.equal(bridge.writes, 1, "manual writes are blocked before IPC too");
    } finally {
      c.dispose();
    }
  });
});
