// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Deterministic regression tests for concurrent hash-intent writes.
// Fake IPC bridge only: no daemon, no paths, no URLs, no secrets.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { NativeQueueController } from "./native-controller.ts";

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

function waitFor(predicate, message, timeout = 1500) {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeout;
    const tick = () => {
      if (predicate()) return resolve();
      if (Date.now() > deadline) return reject(new Error(message));
      setTimeout(tick, 2);
    };
    tick();
  });
}

function tick() {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

function createBridge() {
  const backend = new Map([
    ["a", false],
    ["b", false],
  ]);
  const releases = new Map();
  let intentCalls = 0;
  let reads = 0;
  let deferredReadRelease = null;
  let nextReadError = null;
  let nextReadResponse;

  const rows = () =>
    [...backend.entries()]
      .filter(([, enabled]) => enabled)
      .map(([id]) => ({ id, state: "pending" }));

  const { invoke, calls } = makeInvoke((command, args) => {
    if (command === "start_managed_surge") return [];
    if (command === "disconnect_surge") return { ok: true };
    if (command === "list_downloads") return [];
    if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
    if (command === "refresh_download_hashes") return rows();
    if (command === "get_download_hashes") {
      reads += 1;
      if (nextReadError !== null) {
        const error = nextReadError;
        nextReadError = null;
        throw error;
      }
      if (nextReadResponse !== undefined) {
        const response = nextReadResponse;
        nextReadResponse = undefined;
        return typeof response === "function" ? response() : response;
      }
      if (deferredReadRelease !== null && reads === 2) {
        return new Promise((resolve) => {
          deferredReadRelease = () => resolve(rows());
        });
      }
      return rows();
    }
    if (command === "set_download_hash_intents") {
      intentCalls += 1;
      const id = args.intents[0].id;
      return new Promise((resolve, reject) => {
        releases.set(id, (fail = false, commitOnFailure = false, response) => {
          if (fail) {
            if (commitOnFailure) backend.set(id, true);
            reject("network");
            return;
          }
          backend.set(id, true);
          resolve(response === undefined ? rows() : response);
        });
      });
    }
    throw new Error(`unexpected command: ${command}`);
  });

  return {
    invoke,
    calls,
    backend,
    releases,
    rows,
    get intentCalls() {
      return intentCalls;
    },
    get reads() {
      return reads;
    },
    deferNextRead() {
      deferredReadRelease = "pending";
    },
    releaseDeferredRead() {
      const release = deferredReadRelease;
      deferredReadRelease = null;
      release();
    },
    failNextRead(error = "invalidResponse") {
      nextReadError = error;
    },
    setNextReadResponse(response) {
      nextReadResponse = response;
    },
  };
}

function ids(state) {
  return (state.hashes ?? []).map((row) => row.id).sort();
}

describe("concurrent setHashIntents settlement", () => {
  it("keeps both committed rows when the newer write settles first", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();
      assert.deepEqual(ids(c.state), []);

      const writeA = c.setHashIntents([{ id: "a", enabled: true }]);
      const writeB = c.setHashIntents([{ id: "b", enabled: true }]);
      await waitFor(
        () => bridge.releases.has("a") && bridge.releases.has("b"),
        "expected both intent writes in flight",
      );

      bridge.releases.get("b")();
      await tick();
      bridge.releases.get("a")();
      assert.deepEqual(await Promise.all([writeA, writeB]), [true, true]);

      assert.deepEqual(
        ids(c.state),
        ["a", "b"],
        "the post-final-write read must recover the row committed by the earlier write",
      );
      assert.equal(bridge.intentCalls, 2, "intent writes are never retried");
      assert.equal(c.state.hashError, null);
    } finally {
      c.dispose();
    }
  });

  it("keeps both committed rows when the older write settles first", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();

      const writeA = c.setHashIntents([{ id: "a", enabled: true }]);
      const writeB = c.setHashIntents([{ id: "b", enabled: true }]);
      await waitFor(
        () => bridge.releases.has("a") && bridge.releases.has("b"),
        "expected both intent writes in flight",
      );

      bridge.releases.get("a")();
      await tick();
      bridge.releases.get("b")();
      assert.deepEqual(await Promise.all([writeA, writeB]), [true, true]);

      assert.deepEqual(ids(c.state), ["a", "b"]);
      assert.equal(bridge.intentCalls, 2, "intent writes are never retried");
      assert.equal(c.state.hashError, null);
    } finally {
      c.dispose();
    }
  });

  it("reads once after the last write when an overlapping read outlives the wave", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();
      bridge.deferNextRead();

      const writeA = c.setHashIntents([{ id: "a", enabled: true }]);
      const writeB = c.setHashIntents([{ id: "b", enabled: true }]);
      await waitFor(
        () => bridge.releases.has("a") && bridge.releases.has("b"),
        "expected both intent writes in flight",
      );

      const oldRead = c.loadHashes();
      await tick();

      bridge.releases.get("b")();
      await tick();
      bridge.releases.get("a")();
      assert.deepEqual(await Promise.all([writeA, writeB]), [true, true]);

      assert.deepEqual(
        ids(c.state),
        ["a", "b"],
        "the final write must issue its own authoritative read",
      );
      const readsAtSettlement = bridge.reads;

      bridge.releaseDeferredRead();
      await oldRead;
      await tick();

      assert.deepEqual(ids(c.state), ["a", "b"], "the stale overlapping read is fenced");
      assert.equal(bridge.reads, readsAtSettlement, "the stale read is not reissued");
      assert.equal(bridge.intentCalls, 2, "intent writes are never retried");
      assert.equal(c.state.hashError, null);
    } finally {
      c.dispose();
    }
  });

  it("reads once after the wave when the last write fails", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();

      const writeA = c.setHashIntents([{ id: "a", enabled: true }]);
      const writeB = c.setHashIntents([{ id: "b", enabled: true }]);
      await waitFor(
        () => bridge.releases.has("a") && bridge.releases.has("b"),
        "expected both intent writes in flight",
      );

      bridge.releases.get("a")();
      await tick();
      bridge.releases.get("b")(true);
      assert.deepEqual(await Promise.all([writeA, writeB]), [true, false]);

      assert.deepEqual(ids(c.state), ["a"], "the follow-up read paints the committed first write");
      assert.equal(bridge.reads, 2, "the final failed write issues exactly one follow-up read");
      assert.equal(bridge.intentCalls, 2, "intent writes are never retried");
      assert.equal(c.state.hashError, "Cannot reach the daemon for hash state.");
    } finally {
      c.dispose();
    }
  });

  it("reads once after the wave when the first write fails and the last succeeds", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();

      const writeA = c.setHashIntents([{ id: "a", enabled: true }]);
      const writeB = c.setHashIntents([{ id: "b", enabled: true }]);
      await waitFor(
        () => bridge.releases.has("a") && bridge.releases.has("b"),
        "expected both intent writes in flight",
      );

      bridge.releases.get("a")(true);
      await tick();
      bridge.releases.get("b")();
      assert.deepEqual(await Promise.all([writeA, writeB]), [false, true]);

      assert.deepEqual(ids(c.state), ["b"], "the follow-up read paints the committed last write");
      assert.equal(bridge.reads, 2, "the wave still issues exactly one follow-up read");
      assert.equal(bridge.intentCalls, 2, "intent writes are never retried");
      assert.equal(
        c.state.hashError,
        "Cannot reach the daemon for hash state.",
        "a later success must not hide the earlier write failure",
      );
    } finally {
      c.dispose();
    }
  });

  it("reads once after the wave when both writes fail", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();

      const writeA = c.setHashIntents([{ id: "a", enabled: true }]);
      const writeB = c.setHashIntents([{ id: "b", enabled: true }]);
      await waitFor(
        () => bridge.releases.has("a") && bridge.releases.has("b"),
        "expected both intent writes in flight",
      );

      bridge.releases.get("a")(true);
      await tick();
      bridge.releases.get("b")(true);
      assert.deepEqual(await Promise.all([writeA, writeB]), [false, false]);

      assert.deepEqual(ids(c.state), []);
      assert.equal(bridge.reads, 2, "the final failure issues exactly one follow-up read");
      assert.equal(bridge.intentCalls, 2, "intent writes are never retried");
      assert.equal(c.state.hashError, "Cannot reach the daemon for hash state.");
    } finally {
      c.dispose();
    }
  });

  it("reconciles a single uncertain failure without retrying the mutation", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();

      const write = c.setHashIntents([{ id: "a", enabled: true }]);
      await waitFor(() => bridge.releases.has("a"), "expected the intent write in flight");
      bridge.releases.get("a")(true);
      assert.equal(await write, false);

      assert.deepEqual(ids(c.state), []);
      assert.equal(bridge.reads, 2, "the uncertain final failure gets one authoritative read");
      assert.equal(bridge.intentCalls, 1, "a failed intent write is never retried");
      assert.equal(c.state.hashError, "Cannot reach the daemon for hash state.");
    } finally {
      c.dispose();
    }
  });

  it("reconciles when the older write fails after a newer success committed", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();

      const writeA = c.setHashIntents([{ id: "a", enabled: true }]);
      const writeB = c.setHashIntents([{ id: "b", enabled: true }]);
      await waitFor(
        () => bridge.releases.has("a") && bridge.releases.has("b"),
        "expected both intent writes in flight",
      );

      bridge.releases.get("b")();
      await tick();
      // The older request commits on the daemon, then its transport fails. Its
      // request order must not skip the final wave reconciliation.
      bridge.releases.get("a")(true, true);
      assert.deepEqual(await Promise.all([writeA, writeB]), [false, true]);

      assert.deepEqual(
        ids(c.state),
        ["a", "b"],
        "the final authoritative read recovers the later-committed older write",
      );
      assert.equal(bridge.reads, 2, "exactly one bounded follow-up read is issued");
      assert.equal(bridge.intentCalls, 2, "intent writes are never retried");
      assert.equal(c.state.hashError, "Cannot reach the daemon for hash state.");
    } finally {
      c.dispose();
    }
  });

  it("still reconciles when the older write fails first and commits server-side", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();

      const writeA = c.setHashIntents([{ id: "a", enabled: true }]);
      const writeB = c.setHashIntents([{ id: "b", enabled: true }]);
      await waitFor(
        () => bridge.releases.has("a") && bridge.releases.has("b"),
        "expected both intent writes in flight",
      );

      bridge.releases.get("a")(true, true);
      await tick();
      bridge.releases.get("b")();
      assert.deepEqual(await Promise.all([writeA, writeB]), [false, true]);

      assert.deepEqual(ids(c.state), ["a", "b"]);
      assert.equal(bridge.reads, 2, "the wave still issues exactly one follow-up read");
      assert.equal(bridge.intentCalls, 2, "intent writes are never retried");
      assert.equal(c.state.hashError, "Cannot reach the daemon for hash state.");
    } finally {
      c.dispose();
    }
  });

  it("preserves mutation uncertainty when the final follow-up read fails", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();

      const writeA = c.setHashIntents([{ id: "a", enabled: true }]);
      const writeB = c.setHashIntents([{ id: "b", enabled: true }]);
      await waitFor(
        () => bridge.releases.has("a") && bridge.releases.has("b"),
        "expected both intent writes in flight",
      );

      bridge.releases.get("b")();
      await tick();
      bridge.failNextRead("invalidResponse");
      bridge.releases.get("a")(true, true);
      assert.deepEqual(await Promise.all([writeA, writeB]), [false, true]);

      assert.equal(bridge.reads, 2, "the failed follow-up read is still exactly one read");
      assert.equal(bridge.intentCalls, 2, "intent writes are never retried");
      assert.match(
        c.state.hashError,
        /Cannot reach the daemon for hash state/,
        "the original mutation uncertainty stays visible",
      );
      assert.match(
        c.state.hashError,
        /unexpected hash response/,
        "the follow-up read failure is also visible",
      );
      assert.doesNotMatch(c.state.hashError, /invalidResponse|{"|"id"/);
    } finally {
      c.dispose();
    }
  });

  it("combines mutation and read failures when the older write fails first", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();

      const writeA = c.setHashIntents([{ id: "a", enabled: true }]);
      const writeB = c.setHashIntents([{ id: "b", enabled: true }]);
      await waitFor(
        () => bridge.releases.has("a") && bridge.releases.has("b"),
        "expected both intent writes in flight",
      );

      bridge.releases.get("a")(true, true);
      await tick();
      bridge.failNextRead("invalidResponse");
      bridge.releases.get("b")();
      assert.deepEqual(await Promise.all([writeA, writeB]), [false, true]);

      assert.deepEqual(ids(c.state), ["a", "b"]);
      assert.equal(bridge.reads, 2);
      assert.equal(bridge.intentCalls, 2, "intent writes are never retried");
      assert.match(c.state.hashError, /Cannot reach the daemon for hash state/);
      assert.match(c.state.hashError, /unexpected hash response/);
    } finally {
      c.dispose();
    }
  });

  it("does not advance authoritative state on a malformed read during a write", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();

      const write = c.setHashIntents([{ id: "a", enabled: true }]);
      await waitFor(() => bridge.releases.has("a"), "expected the intent write in flight");

      bridge.setNextReadResponse({ items: [] });
      await c.loadHashes();
      assert.deepEqual(ids(c.state), [], "the malformed read preserves the validated rows");
      assert.equal(c.state.hashError, "Daemon returned an unexpected hash response.");

      // If the malformed read had advanced the authoritative sequence, the
      // older write response below would be fenced and never paint its rows.
      bridge.failNextRead("invalidResponse");
      bridge.releases.get("a")();
      assert.equal(await write, true);

      assert.deepEqual(
        ids(c.state),
        ["a"],
        "the committed write response still applies after a malformed older read",
      );
      assert.equal(bridge.reads, 3);
      assert.equal(bridge.intentCalls, 1, "the write is not retried");
    } finally {
      c.dispose();
    }
  });

  it("counts an earlier malformed write as a wave failure before a later success", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();

      const writeA = c.setHashIntents([{ id: "a", enabled: true }]);
      const writeB = c.setHashIntents([{ id: "b", enabled: true }]);
      await waitFor(
        () => bridge.releases.has("a") && bridge.releases.has("b"),
        "expected both intent writes in flight",
      );

      bridge.releases.get("a")(false, false, {
        marker: "RAW-MALFORMED-MARKER",
        items: [],
      });
      await tick();
      assert.match(c.state.hashError, /unexpected hash response/);

      bridge.failNextRead("network");
      bridge.releases.get("b")();
      assert.deepEqual(
        await Promise.all([writeA, writeB]),
        [false, true],
        "the malformed write never reports success, even though the later write does",
      );

      assert.deepEqual(ids(c.state), ["a", "b"]);
      assert.equal(bridge.reads, 2, "the final wave still issues exactly one follow-up read");
      assert.equal(bridge.intentCalls, 2, "intent writes are never retried");
      assert.match(
        c.state.hashError,
        /unexpected hash response/,
        "the later success must not clear the earlier protocol error",
      );
      assert.match(
        c.state.hashError,
        /Cannot reach the daemon for hash state/,
        "the failed reconciliation is also visible",
      );
      assert.doesNotMatch(c.state.hashError, /RAW-MALFORMED-MARKER|invalidResponse/);
    } finally {
      c.dispose();
    }
  });

  it("preserves an earlier malformed write when it settles after a later success", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();

      const writeA = c.setHashIntents([{ id: "a", enabled: true }]);
      const writeB = c.setHashIntents([{ id: "b", enabled: true }]);
      await waitFor(
        () => bridge.releases.has("a") && bridge.releases.has("b"),
        "expected both intent writes in flight",
      );

      bridge.releases.get("b")();
      await tick();
      assert.equal(c.state.hashError, null);

      bridge.failNextRead("network");
      bridge.releases.get("a")(false, false, {
        marker: "RAW-MALFORMED-MARKER",
        items: [],
      });
      assert.deepEqual(
        await Promise.all([writeA, writeB]),
        [false, true],
        "the malformed write stays unconfirmed even when it settles after a success",
      );

      assert.deepEqual(ids(c.state), ["b"], "the later success remains until reconciliation");
      assert.equal(bridge.reads, 2, "the final wave still issues exactly one follow-up read");
      assert.equal(bridge.intentCalls, 2, "intent writes are never retried");
      assert.match(
        c.state.hashError,
        /unexpected hash response/,
        "the earlier malformed response must still surface",
      );
      assert.match(
        c.state.hashError,
        /Cannot reach the daemon for hash state/,
        "the failed reconciliation is also visible",
      );
      assert.doesNotMatch(c.state.hashError, /RAW-MALFORMED-MARKER|invalidResponse/);
    } finally {
      c.dispose();
    }
  });

  it("keeps a malformed write unconfirmed when it settles last and reconciliation succeeds", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();

      const writeA = c.setHashIntents([{ id: "a", enabled: true }]);
      const writeB = c.setHashIntents([{ id: "b", enabled: true }]);
      await waitFor(
        () => bridge.releases.has("a") && bridge.releases.has("b"),
        "expected both intent writes in flight",
      );

      bridge.releases.get("b")();
      await tick();
      bridge.releases.get("a")(false, false, {
        marker: "RAW-MALFORMED-MARKER",
        items: [],
      });
      assert.deepEqual(
        await Promise.all([writeA, writeB]),
        [false, true],
        "a malformed response stays false even after a successful reconciliation",
      );

      assert.deepEqual(ids(c.state), ["a", "b"], "the reconciliation still paints the committed rows");
      assert.equal(bridge.reads, 2, "the final malformed write still reconciles exactly once");
      assert.equal(bridge.intentCalls, 2, "intent writes are never retried");
      assert.equal(c.state.hashError, "Daemon returned an unexpected hash response.");
      assert.doesNotMatch(c.state.hashError, /RAW-MALFORMED-MARKER|invalidResponse/);
    } finally {
      c.dispose();
    }
  });

  it("keeps a malformed write unconfirmed when it settles first and a later write reconciles", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();

      const writeA = c.setHashIntents([{ id: "a", enabled: true }]);
      const writeB = c.setHashIntents([{ id: "b", enabled: true }]);
      await waitFor(
        () => bridge.releases.has("a") && bridge.releases.has("b"),
        "expected both intent writes in flight",
      );

      bridge.releases.get("a")(false, false, {
        marker: "RAW-MALFORMED-MARKER",
        items: [],
      });
      await tick();
      bridge.releases.get("b")();
      assert.deepEqual(
        await Promise.all([writeA, writeB]),
        [false, true],
        "the earlier malformed write never reports success",
      );

      assert.deepEqual(ids(c.state), ["a", "b"], "the later write's reconciliation paints both rows");
      assert.equal(bridge.reads, 2, "the wave still issues exactly one follow-up read");
      assert.equal(bridge.intentCalls, 2, "intent writes are never retried");
      assert.equal(c.state.hashError, "Daemon returned an unexpected hash response.");
      assert.doesNotMatch(c.state.hashError, /RAW-MALFORMED-MARKER|invalidResponse/);
    } finally {
      c.dispose();
    }
  });

  it("returns false for a lone malformed intent response even when the follow-up read succeeds", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();

      const write = c.setHashIntents([{ id: "a", enabled: true }]);
      await waitFor(() => bridge.releases.has("a"), "expected the intent write in flight");
      bridge.releases.get("a")(false, false, {
        marker: "RAW-MALFORMED-MARKER",
        items: [],
      });

      assert.equal(await write, false, "a malformed mutation response is never success");
      assert.deepEqual(ids(c.state), ["a"], "the successful reconciliation still paints committed rows");
      assert.equal(bridge.reads, 2, "exactly one reconciliation read follows");
      assert.equal(bridge.intentCalls, 1, "the mutation is never retried");
      assert.equal(c.state.hashError, "Daemon returned an unexpected hash response.");
    } finally {
      c.dispose();
    }
  });

  it("fences a malformed write from a replaced epoch without retrying it", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();

      const write = c.setHashIntents([{ id: "a", enabled: true }]);
      await waitFor(() => bridge.releases.has("a"), "expected the intent write in flight");
      await c.disconnect();
      bridge.releases.get("a")(false, false, {
        marker: "RAW-MALFORMED-MARKER",
        items: [],
      });

      assert.equal(await write, false);
      assert.equal(bridge.intentCalls, 1, "the stale write is not retried");
      assert.equal(bridge.reads, 1, "no follow-up read leaks into the new epoch");
      assert.equal(c.state.hashError, null, "the stale malformed response must not surface");
      assert.deepEqual(ids(c.state), []);

      await c.startManaged();
      await c.loadHashes();
      assert.deepEqual(ids(c.state), ["a"], "the new epoch reads committed rows fresh");
      assert.equal(bridge.reads, 2);
      assert.equal(bridge.intentCalls, 1, "reconnect does not duplicate the mutation");
    } finally {
      c.dispose();
    }
  });

  it("does not advance authoritative sequence on a non-overlap malformed read", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();
      const authoritative = c.hashAuthoritativeSeq;
      const applied = c.hashAppliedSeq;
      assert.equal(c.state.hashError, null);

      bridge.setNextReadResponse({ marker: "RAW-MALFORMED-MARKER", items: [] });
      await c.loadHashes();

      assert.equal(
        c.hashAuthoritativeSeq,
        authoritative,
        "a malformed non-overlap read must not advance the authoritative sequence",
      );
      assert.equal(
        c.hashAppliedSeq,
        applied,
        "a malformed non-overlap read must not advance the applied sequence",
      );
      assert.deepEqual(ids(c.state), [], "the malformed read preserves the validated rows");
      assert.equal(c.state.hashError, "Daemon returned an unexpected hash response.");
      assert.doesNotMatch(c.state.hashError, /RAW-MALFORMED-MARKER/);
      assert.equal(bridge.intentCalls, 0);
    } finally {
      c.dispose();
    }
  });

  it("does not reconcile or mutate into a replaced connection epoch", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();

      const writeA = c.setHashIntents([{ id: "a", enabled: true }]);
      const writeB = c.setHashIntents([{ id: "b", enabled: true }]);
      await waitFor(
        () => bridge.releases.has("a") && bridge.releases.has("b"),
        "expected both intent writes in flight",
      );

      await c.disconnect();
      bridge.releases.get("b")();
      bridge.releases.get("a")(true, true);
      assert.deepEqual(await Promise.all([writeA, writeB]), [false, false]);
      assert.deepEqual(ids(c.state), [], "the stale epoch stays empty");
      assert.equal(bridge.reads, 1, "no follow-up read leaks into the new epoch");
      assert.equal(bridge.intentCalls, 2, "the stale writes are not retried");

      await c.startManaged();
      await c.loadHashes();
      assert.deepEqual(ids(c.state), ["a", "b"], "the new epoch reads committed rows fresh");
      assert.equal(bridge.reads, 2);
      assert.equal(bridge.intentCalls, 2, "reconnect does not duplicate mutations");
    } finally {
      c.dispose();
    }
  });

  it("does not reconcile or emit after dispose", async () => {
    const bridge = createBridge();
    const c = new NativeQueueController(bridge.invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashes();

      const write = c.setHashIntents([{ id: "a", enabled: true }]);
      await waitFor(() => bridge.releases.has("a"), "expected the intent write in flight");
      c.dispose();
      bridge.releases.get("a")(true, true);
      assert.equal(await write, false);
      assert.equal(bridge.intentCalls, 1, "the disposed write is not retried");
      assert.equal(bridge.reads, 1, "dispose prevents the follow-up read");
    } finally {
      c.dispose();
    }
  });
});
