// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Deterministic regression tests for the HashUnconfirmed backend error.
// Fake IPC only: no daemon, no real paths/URLs/secrets.
import { beforeEach, describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  NativeQueueController,
  resetSharedHashDefaultStoreForTests,
} from "./native-controller.ts";

beforeEach(resetSharedHashDefaultStoreForTests);

const MD5 = "900150983cd24fb0d6963f7d28e17f72";
const SHA256 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

/** The single user-facing notice for a quarantined (unconfirmed) hash state. */
const HASH_UNCONFIRMED = "Hash state could not be confirmed. Existing downloads are unaffected.";
const HASH_DEFAULT_UNCONFIRMED =
  "The saved hashing default could not be confirmed after a save. Changing it is disabled until the app restarts to avoid unsafe writes; a per-batch choice is still available.";

/** A hostile backend detail that must never cross into controller state. */
const RAW_LEAK = "RAW-HASH-UNCONFIRMED-LEAK C:\\state\\hash.marker";

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

function queueRow(overrides = {}) {
  return {
    id: "dl-1",
    name: "bundle.bin",
    status: "active",
    bytesDone: 10,
    bytesTotal: 100,
    speedBps: 1,
    addedAt: 1,
    ...overrides,
  };
}

function complete(id = "dl-1") {
  return { id, state: "complete", md5: MD5, sha256: SHA256, bytes: 3, resultVersion: 1 };
}

function pending(id = "dl-1") {
  return { id, state: "pending" };
}

function ids(state) {
  return (state.hashes ?? []).map((row) => row.id).sort();
}

function rowsJson(state) {
  return JSON.stringify(state.hashes ?? []);
}

function delayed(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate, message, timeout = 1500) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await delayed(2);
  }
  assert.fail(message);
}

function count(calls, command) {
  return calls.filter((call) => call.command === command).length;
}

/** No advice to delete a marker file, and no blind-retry instructions. */
function assertNoAdvice(message) {
  assert.doesNotMatch(message, /delete|erase|remove|removeFile|marker|\.marker/i);
  assert.doesNotMatch(message, /retry|try again|force|overwrite/i);
}

/** No backend payload text, raw code or JSON ever reaches user-visible state. */
function assertNoPayloadLeak(message) {
  assert.doesNotMatch(message, /RAW-HASH-UNCONFIRMED-LEAK|hash\.marker|C:\\/);
  assert.doesNotMatch(message, /hashunconfirmed|unconfirmed/i);
  assert.doesNotMatch(message, /\{|"|}/);
}

/**
 * Handler with one-shot read/write failure injection. Reads return the given
 * rows unless a failure was armed for the next read.
 */
function createBridge(options = {}) {
  const { initialRows = [complete()], queue = [queueRow()] } = options;
  let nextReadError = null;
  let nextWriteError = null;
  let nextDefaultError = null;
  let nextDefaultApplies = false;
  let defaultEnabled = true;
  let reads = 0;
  let writes = 0;
  let defaultReads = 0;

  const { invoke, calls } = makeInvoke((command, args) => {
    if (command === "start_managed_surge") return queue;
    if (command === "list_downloads") return queue;
    if (command === "get_automatic_hash_settings") {
      defaultReads += 1;
      if (nextDefaultError !== null) {
        const error = nextDefaultError;
        nextDefaultError = null;
        nextDefaultApplies = false;
        throw error;
      }
      return { defaultEnabled };
    }
    if (command === "set_automatic_hash_default") {
      if (nextDefaultError !== null) {
        const error = nextDefaultError;
        if (nextDefaultApplies) defaultEnabled = args.enabled;
        nextDefaultError = null;
        nextDefaultApplies = false;
        throw error;
      }
      defaultEnabled = args.enabled;
      return { defaultEnabled };
    }
    if (command === "refresh_download_hashes" || command === "get_download_hashes") {
      reads += 1;
      if (nextReadError !== null) {
        const error = nextReadError;
        nextReadError = null;
        throw error;
      }
      return initialRows;
    }
    if (command === "set_download_hash_intents") {
      writes += 1;
      if (nextWriteError !== null) {
        const error = nextWriteError;
        nextWriteError = null;
        throw error;
      }
      return initialRows;
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
    get defaultReads() {
      return defaultReads;
    },
    failNextRead(error) {
      nextReadError = error;
    },
    failNextWrite(error) {
      nextWriteError = error;
    },
    failNextDefault(error, apply = false) {
      nextDefaultError = error;
      nextDefaultApplies = apply;
    },
  };
}

function makeController(bridge, options) {
  return new NativeQueueController(bridge.invoke, () => {}, 60_000, undefined, options);
}

describe("HashUnconfirmed hash-state mapping", () => {
  it("maps a HashUnconfirmed get read to the sanitized notice and keeps the confirmed rows", async () => {
    const bridge = createBridge();
    const c = makeController(bridge);
    try {
      await c.startManaged();
      await c.loadHashes();
      assert.deepEqual(ids(c.state), ["dl-1"]);
      const confirmed = rowsJson(c.state);

      bridge.failNextRead({ code: "HashUnconfirmed", detail: RAW_LEAK });
      await c.loadHashes();

      assert.equal(c.state.hashError, HASH_UNCONFIRMED);
      assert.equal(
        rowsJson(c.state),
        confirmed,
        "a quarantined read must preserve the last confirmed rows instead of showing an empty successful list",
      );
      assert.equal(c.state.hashesLoading, false);
      assertNoAdvice(c.state.hashError);
      assertNoPayloadLeak(c.state.hashError);
    } finally {
      c.dispose();
    }
  });

  it("maps a plain-string HashUnconfirmed with payload text to the same constant notice", async () => {
    const bridge = createBridge();
    const c = makeController(bridge);
    try {
      await c.startManaged();
      await c.loadHashes();
      const confirmed = rowsJson(c.state);

      bridge.failNextRead(`HashUnconfirmed: ${RAW_LEAK}`);
      await c.loadHashes();

      assert.equal(c.state.hashError, HASH_UNCONFIRMED);
      assert.equal(rowsJson(c.state), confirmed);
      assertNoAdvice(c.state.hashError);
      assertNoPayloadLeak(c.state.hashError);
    } finally {
      c.dispose();
    }
  });

  it("reports the notice on the first failed read instead of an empty successful list", async () => {
    const bridge = createBridge({ initialRows: [pending()] });
    const c = makeController(bridge);
    try {
      await c.startManaged();
      bridge.failNextRead({ code: "HashUnconfirmed" });
      await c.loadHashes();

      assert.equal(c.state.hashError, HASH_UNCONFIRMED, "the notice must be visible, not silent");
      assert.equal(c.state.hashesLoading, false);
      assert.deepEqual(ids(c.state), [], "no rows were ever confirmed");
      assertNoAdvice(c.state.hashError);
      assertNoPayloadLeak(c.state.hashError);
    } finally {
      c.dispose();
    }
  });

  it("maps a HashUnconfirmed refresh read to the same notice and keeps the rows", async () => {
    const bridge = createBridge();
    const c = makeController(bridge);
    try {
      await c.startManaged();
      await c.loadHashes();
      const confirmed = rowsJson(c.state);

      bridge.failNextRead({ code: "HashUnconfirmed", detail: RAW_LEAK });
      await c.refreshHashes();

      assert.equal(c.state.hashError, HASH_UNCONFIRMED);
      assert.equal(rowsJson(c.state), confirmed, "the refresh failure must not clear row state");
      assert.equal(c.state.hashesLoading, false);
      assertNoAdvice(c.state.hashError);
      assertNoPayloadLeak(c.state.hashError);
    } finally {
      c.dispose();
    }
  });

  it("keeps the current generic network, timeout and protocol semantics for ordinary failures", async () => {
    const bridge = createBridge();
    const c = makeController(bridge);
    try {
      await c.startManaged();
      await c.loadHashes();

      bridge.failNextRead("network");
      await c.loadHashes();
      assert.equal(c.state.hashError, "Cannot reach the daemon for hash state.");

      bridge.failNextRead("timeout");
      await c.loadHashes();
      assert.equal(c.state.hashError, "Hash state timed out. Try again.");

      bridge.failNextRead({ code: "invalidResponse", detail: RAW_LEAK });
      await c.loadHashes();
      assert.equal(c.state.hashError, "Daemon returned an unexpected hash response.");

      bridge.failNextRead("notconnected");
      await c.loadHashes();
      assert.equal(c.state.hashError, "Not connected to a daemon.");

      assert.deepEqual(
        ids(c.state),
        ["dl-1"],
        "every transport failure preserves the confirmed rows",
      );
    } finally {
      c.dispose();
    }
  });

  it("treats a HashUnconfirmed intent write as unconfirmed without retrying it", async () => {
    const bridge = createBridge();
    const c = makeController(bridge);
    try {
      await c.startManaged();
      await c.loadHashes();
      const confirmed = rowsJson(c.state);
      const readsBefore = bridge.reads;

      bridge.failNextWrite({ code: "HashUnconfirmed", detail: RAW_LEAK });
      const applied = await c.setHashIntents([{ id: "dl-1", enabled: true }]);

      assert.equal(applied, false, "an unconfirmed write never reports success");
      assert.equal(bridge.writes, 1, "the mutation is never retried");
      assert.equal(bridge.reads, readsBefore + 1, "exactly one reconciliation read follows");
      assert.equal(c.state.hashError, HASH_UNCONFIRMED);
      assert.equal(rowsJson(c.state), confirmed, "the write outcome must not empty the rows");
      assertNoAdvice(c.state.hashError);
      assertNoPayloadLeak(c.state.hashError);
    } finally {
      c.dispose();
    }
  });

  it("keeps the committed rows when the reconciliation read after a failed write is HashUnconfirmed", async () => {
    const bridge = createBridge();
    const c = makeController(bridge);
    try {
      await c.startManaged();
      await c.loadHashes();

      bridge.failNextWrite("network");
      bridge.failNextRead({ code: "HashUnconfirmed", detail: RAW_LEAK });
      const applied = await c.setHashIntents([{ id: "dl-1", enabled: true }]);

      assert.equal(applied, false);
      assert.equal(bridge.writes, 1, "the mutation is never retried");
      assert.deepEqual(ids(c.state), ["dl-1"], "the confirmed rows survive the quarantined read");
      assert.ok(
        (c.state.hashError ?? "").includes("Cannot reach the daemon for hash state."),
        "the write failure stays visible",
      );
      assert.ok(
        (c.state.hashError ?? "").includes(HASH_UNCONFIRMED),
        "the quarantined reconciliation read is also visible",
      );
      assert.equal(
        (c.state.hashError ?? "").split(HASH_UNCONFIRMED).length,
        2,
        "the same notice is never duplicated",
      );
      assertNoAdvice(c.state.hashError);
      assertNoPayloadLeak(c.state.hashError);
    } finally {
      c.dispose();
    }
  });

  it("maps HashUnconfirmed on the automatic-hash default load and save paths", async () => {
    const bridge = createBridge();
    const c = makeController(bridge);
    try {
      await c.startManaged();

      bridge.failNextDefault({ code: "HashUnconfirmed", detail: RAW_LEAK });
      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultError, HASH_UNCONFIRMED);
      assert.equal(c.state.hashDefaultEnabled, null, "an unconfirmed default is never guessed");
      assert.equal(c.state.hashDefaultLoading, false);
      assertNoAdvice(c.state.hashDefaultError);
      assertNoPayloadLeak(c.state.hashDefaultError);

      await c.loadHashDefault();
      assert.equal(c.state.hashDefaultError, null, "a later confirmed read recovers");
      assert.equal(c.state.hashDefaultEnabled, true);

      bridge.failNextDefault(`HashUnconfirmed: ${RAW_LEAK}`, true);
      const saved = await c.setHashDefault(false);
      assert.equal(saved, false, "an unconfirmed save never reports success");
      assert.equal(c.state.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);
      assert.equal(
        c.state.hashDefaultEnabled,
        null,
        "an unconfirmed save invalidates the prior default instead of guessing",
      );
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(c.state.hashDefaultSaving, false);
      assertNoAdvice(c.state.hashDefaultError);
      assertNoPayloadLeak(c.state.hashDefaultError);

      await c.loadHashDefault();
      assert.equal(
        c.state.hashDefaultError,
        HASH_DEFAULT_UNCONFIRMED,
        "a matching read cannot clear a possibly late commit",
      );
      assert.equal(c.state.hashDefaultEnabled, null);
      assert.equal(c.state.hashDefaultUnconfirmed, true);
      assert.equal(await c.setHashDefault(true), false, "future writes stay blocked");
    } finally {
      c.dispose();
    }
  });

  it("stops automatic polling after a quarantined read without busy retrying", async () => {
    const bridge = createBridge({ initialRows: [pending()] });
    const c = makeController(bridge, { hashRefreshMs: 5 });
    try {
      await c.startManaged();
      await c.loadHashes();
      assert.deepEqual(ids(c.state), ["dl-1"]);
      assert.equal(bridge.reads, 1);

      bridge.failNextRead({ code: "HashUnconfirmed", detail: RAW_LEAK });
      await waitFor(
        () => c.state.hashError !== null,
        "expected the quarantined hash state to surface",
      );
      assert.equal(c.state.hashError, HASH_UNCONFIRMED);

      const settledReads = bridge.reads;
      await delayed(60);
      assert.equal(bridge.reads, settledReads, "the notice must not trigger a retry loop");
      assert.deepEqual(ids(c.state), ["dl-1"], "the pending row is preserved while quarantined");
      assert.equal(c.state.hashesLoading, false);
      assertNoAdvice(c.state.hashError);
      assertNoPayloadLeak(c.state.hashError);
    } finally {
      c.dispose();
    }
  });

  it("lets an automatic hash read recover from the quarantined state on an explicit refresh", async () => {
    const bridge = createBridge({ initialRows: [pending()] });
    const c = makeController(bridge, { hashRefreshMs: 5 });
    try {
      await c.startManaged();
      await c.loadHashes();

      bridge.failNextRead({ code: "HashUnconfirmed", detail: RAW_LEAK });
      await c.loadHashes();
      assert.equal(c.state.hashError, HASH_UNCONFIRMED);

      await c.refreshHashes();
      assert.equal(c.state.hashError, null, "a confirmed read clears the notice");
      assert.deepEqual(ids(c.state), ["dl-1"]);
      assert.equal(count(bridge.calls, "refresh_download_hashes"), 1);
      assert.equal(count(bridge.calls, "add_download"), 0, "hash reads never touch downloads");
    } finally {
      c.dispose();
    }
  });
});
