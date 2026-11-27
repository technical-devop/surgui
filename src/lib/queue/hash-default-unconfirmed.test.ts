// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Regression tests for an ambiguous automatic-hash default save: the daemon
// may have committed the new value even though the reply failed. The prior
// value must stop being treated as confirmed until a fresh current-epoch read.
import { beforeEach, describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  NativeQueueController,
  resetSharedHashDefaultStoreForTests,
} from "./native-controller.ts";

beforeEach(resetSharedHashDefaultStoreForTests);

const HASH_DEFAULT_UNCONFIRMED =
  "The saved hashing default could not be confirmed after a save. Changing it is disabled until the app restarts to avoid unsafe writes; a per-batch choice is still available.";

function makeInvoke(handler) {
  const calls = [];
  const invoke = async (command, args) => {
    calls.push({ command, args });
    if (command === "list_downloads_snapshot") {
      calls.push({ command: "list_downloads", args });
      const items = await handler("list_downloads", args);
      return { epoch: 1, streamSeq: 0, items };
    }
    return handler(command, args);
  };
  return { invoke, calls };
}

function createBridge(options = {}) {
  let serverDefault = options.initialDefault ?? false;
  let reads = 0;
  let writes = 0;
  let nextReadError = null;
  let nextSave = null;

  const { invoke, calls } = makeInvoke((command, args) => {
    if (command === "start_managed_surge") return [];
    if (command === "disconnect_surge") return { ok: true };
    if (command === "list_downloads") return [];
    if (command === "get_download_hashes") return [];
    if (command === "refresh_download_hashes") return [];
    if (command === "get_automatic_hash_settings") {
      reads += 1;
      if (nextReadError !== null) {
        const error = nextReadError;
        nextReadError = null;
        throw error;
      }
      return { defaultEnabled: serverDefault };
    }
    if (command === "set_automatic_hash_default") {
      writes += 1;
      const save = nextSave;
      nextSave = null;
      if (save !== null) {
        if (save.apply) serverDefault = save.value;
        throw save.error;
      }
      serverDefault = args.enabled;
      return { defaultEnabled: serverDefault };
    }
    throw new Error(`unexpected command: ${command}`);
  });

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
    failNextRead(error = "network") {
      nextReadError = error;
    },
    failNextSave(error, value, apply) {
      nextSave = { error, value, apply };
    },
  };
}

describe("ambiguous automatic-hash default save", () => {
  it("invalidates the prior default when the server applied the save but the reply failed", async () => {
    const bridge = createBridge({ initialDefault: false });
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, false);
      assert.equal(bridge.reads, 1);

      bridge.failNextSave("network", true, true);
      const saved = await c.setHashDefault(true);

      assert.equal(saved, false, "an ambiguous save never reports success");
      assert.equal(bridge.writes, 1, "the mutation is never retried");
      assert.equal(bridge.serverDefault, true, "the fake daemon did commit the change");
      assert.equal(
        c.state.hashDefaultEnabled,
        null,
        "the prior default is unconfirmed, not reused",
      );
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(c.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);
      assert.equal(c.state.hashDefaultSaving, false);
      assert.equal(bridge.reads, 1, "an ambiguous save does not automatically reload");
    } finally {
      c.dispose();
    }
  });

  it("stays unconfirmed when the reload fails and never recovers from a matching read", async () => {
    const bridge = createBridge({ initialDefault: false });
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, false);

      bridge.failNextSave("network", true, true);
      assert.equal(await c.setHashDefault(true), false);
      assert.equal(c.state.hashDefaultEnabled, null);

      bridge.failNextRead("network");
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, null, "a failed reload never guesses the old value");
      assert.equal(c.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);
      assert.equal(bridge.writes, 1, "the failed reload is not a mutation retry");

      // The fake daemon committed true, but a matching read is not a completion
      // fence: the timed-out write may still commit later. Keep it unknown.
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, null);
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(c.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);
      assert.equal(bridge.reads, 3, "only explicit reloads performed reads");
      assert.equal(bridge.writes, 1, "the mutation was never retried");
      assert.equal(await c.setHashDefault(false), false);
      assert.equal(bridge.writes, 1, "future default writes stay blocked");
    } finally {
      c.dispose();
    }
  });

  it("invalidates the prior default when an applied save returns a malformed reply", async () => {
    const bridge = createBridge({ initialDefault: false });
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, false);

      bridge.failNextSave({ code: "invalidResponse", detail: "RAW-LEAK" }, true, true);
      const saved = await c.setHashDefault(true);

      assert.equal(saved, false);
      assert.equal(bridge.writes, 1, "the mutation is never retried");
      assert.equal(bridge.serverDefault, true, "the fake daemon committed before the bad reply");
      assert.equal(c.state.hashDefaultEnabled, null);
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(c.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);
      assert.equal(c.state.hashDefaultSaving, false);
      assert.doesNotMatch(c.state.hashDefaultError, /RAW-LEAK|invalidResponse/);
    } finally {
      c.dispose();
    }
  });

  it("keeps a confirmed default for a request rejected before any IPC", async () => {
    const bridge = createBridge({ initialDefault: true });
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultEnabled, true);

      const saved = await c.setHashDefault("yes");

      assert.equal(saved, false);
      assert.equal(bridge.writes, 0, "an invalid request is never sent");
      assert.equal(
        c.state.hashDefaultEnabled,
        true,
        "a definitively rejected request leaves the proven confirmed value intact",
      );
    } finally {
      c.dispose();
    }
  });
});
