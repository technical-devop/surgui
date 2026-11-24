// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Event-stream controller behaviour: epoch fencing, authoritative snapshot
// merge, coalesced reconciliation, bounded retry, fallback and unlisten.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { NativeQueueController } from "./native-controller.ts";

const ENDPOINT = "http://127.0.0.1:9";
const TOKEN = "test-only-token-progress";

function row(overrides = {}) {
  return {
    id: "dl-1",
    name: "alpha.bin",
    status: "active",
    bytesDone: 10,
    bytesTotal: 100,
    speedBps: 5,
    addedAt: 1,
    ...overrides,
  };
}

function makeInvoke(handler) {
  const calls = [];
  const invoke = async (command, args) => {
    calls.push({ command, args });
    return handler(command, args);
  };
  return { invoke, calls };
}

function makeListen() {
  const handlers = new Map();
  const unlistened = [];
  const listen = async (event, handler) => {
    handlers.set(event, handler);
    return () => {
      unlistened.push(event);
      handlers.delete(event);
    };
  };
  const emit = (event, payload) => {
    const handler = handlers.get(event);
    if (handler === undefined) throw new Error(`no listener for ${event}`);
    handler({ payload });
  };
  return { listen, emit, handlers, unlistened };
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function connected(items, extra = {}) {
  const listen = makeListen();
  let snapshotCalls = 0;
  const { invoke, calls } = makeInvoke((command) => {
    if (command === "connect_surge") return items;
    if (command === "start_progress_stream") return undefined;
    if (command === "disconnect_surge") return undefined;
    if (command === "list_downloads_snapshot") {
      snapshotCalls += 1;
      if (extra.snapshot) return extra.snapshot(snapshotCalls);
      return { epoch: 1, streamSeq: 0, items: extra.snapshotItems ?? items };
    }
    return undefined;
  });
  const seen = [];
  const controller = new NativeQueueController(
    invoke,
    (snapshot) => seen.push(snapshot),
    60_000,
    listen.listen,
    { reconcileRetryMs: extra.retryMs ?? [1, 1, 1] },
  );
  await controller.connect(ENDPOINT, TOKEN);
  const latest = () => seen[seen.length - 1];
  return { controller, listen, calls, seen, latest, snapshotCalls: () => snapshotCalls };
}

describe("progress stream controller", () => {
  it("applies progress only to existing active rows and fences stale epochs/sequences", async () => {
    const paused = row({ id: "dl-2", status: "paused", speedBps: 0, bytesDone: 50 });
    const c = await connected([row(), paused]);
    const subscription = c.controller.connectionEpoch();

    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 1,
      updates: [
        { id: "dl-1", bytesDone: 40, bytesTotal: 100, speedBps: 8 },
        { id: "dl-2", bytesDone: 90, bytesTotal: 100, speedBps: 8 },
        { id: "ghost", bytesDone: 99, bytesTotal: 100, speedBps: 9 },
      ],
    });
    let state = c.latest();
    assert.deepEqual(state.items.map((item) => item.id), ["dl-1", "dl-2"]);
    assert.equal(state.items[0].bytesDone, 40);
    assert.equal(state.items[0].speedBps, 8);
    assert.equal(state.items[1].bytesDone, 50, "paused row must not move");
    assert.equal(state.items[1].speedBps, 0);

    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 1,
      updates: [{ id: "dl-1", bytesDone: 99, bytesTotal: 100, speedBps: 99 }],
    });
    assert.equal(c.latest().items[0].bytesDone, 40, "duplicate seq is stale");

    c.listen.emit("surge://progress", {
      subscription: subscription - 1,
      epoch: 1,
      seq: 3,
      updates: [{ id: "dl-1", bytesDone: 99, bytesTotal: 100, speedBps: 99 }],
    });
    assert.equal(c.latest().items[0].bytesDone, 40, "old epoch is fenced");

    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 4,
      updates: [{ id: "dl-1", bytesDone: 0, bytesTotal: 0, speedBps: 0 }],
    });
    state = c.latest();
    assert.equal(state.items[0].bytesDone, 40, "progress cannot roll bytes back; reconcile may");
    c.controller.dispose();
  });

  it("uses ready/lifecycle notices to refresh the authoritative list", async () => {
    const c = await connected([row({ bytesDone: 10 })], {
      snapshotItems: [row({ bytesDone: 20 })],
    });
    const subscription = c.controller.connectionEpoch();

    c.listen.emit("surge://stream", {
      subscription,
      epoch: 1,
      seq: 1,
      state: "ready",
    });
    await tick();
    assert.equal(c.snapshotCalls(), 1);
    assert.equal(c.latest().items[0].bytesDone, 20);

    c.listen.emit("surge://lifecycle", {
      subscription,
      epoch: 1,
      seq: 2,
      resync: false,
    });
    await tick();
    assert.equal(c.snapshotCalls(), 2);
    c.controller.dispose();
  });

  it("keeps a newer in-flight sample when a slow stale snapshot is followed by a catching-up read", async () => {
    const slow = deferred();
    let calls = 0;
    const c = await connected([row({ bytesDone: 10 })], {
      snapshot: () => {
        calls += 1;
        if (calls === 1) return slow.promise;
        return { epoch: 1, streamSeq: 3, items: [row({ bytesDone: 80, speedBps: 8 })] };
      },
    });
    const subscription = c.controller.connectionEpoch();

    c.listen.emit("surge://stream", {
      subscription,
      epoch: 1,
      seq: 1,
      state: "ready",
    });
    await tick();
    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 3,
      updates: [{ id: "dl-1", seq: 3, bytesDone: 80, bytesTotal: 100, speedBps: 9 }],
    });
    assert.equal(c.latest().items[0].bytesDone, 80);

    // The list response is older than the stream: 50 captured at cursor 1.
    slow.resolve({ epoch: 1, streamSeq: 1, items: [row({ bytesDone: 50, speedBps: 1 })] });
    await tick();
    await tick();
    assert.equal(c.latest().items[0].bytesDone, 80, "a stale snapshot must not rewind");
    assert.equal(calls, 2, "a fresh authoritative read confirmed the live value");

    // Streaming continues; the row was never quarantined.
    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 4,
      updates: [{ id: "dl-1", seq: 4, bytesDone: 90, bytesTotal: 100, speedBps: 9 }],
    });
    assert.equal(c.latest().items[0].bytesDone, 90, "continuous progress keeps applying");
    c.controller.dispose();
  });

  it("confirms a real reset against an in-flight pre-reset event and quarantines delayed samples", async () => {
    const slow = deferred();
    let calls = 0;
    const c = await connected([row({ bytesDone: 10 })], {
      snapshot: () => {
        calls += 1;
        if (calls === 1) return slow.promise;
        if (calls === 2) {
          return { epoch: 1, streamSeq: 12, items: [row({ bytesDone: 2, speedBps: 1 })] };
        }
        if (calls === 3) {
          return { epoch: 1, streamSeq: 20, items: [row({ bytesDone: 30, speedBps: 3 })] };
        }
        return {
          epoch: 1,
          streamSeq: 30,
          items: [row({ status: "completed", bytesDone: 100, speedBps: 0 })],
        };
      },
    });
    const subscription = c.controller.connectionEpoch();

    // Pre-reset live value 90 at receipt seq 11.
    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 11,
      updates: [{ id: "dl-1", seq: 11, bytesDone: 90, bytesTotal: 100, speedBps: 9 }],
    });
    assert.equal(c.latest().items[0].bytesDone, 90);

    c.listen.emit("surge://stream", { subscription, epoch: 1, seq: 1, state: "ready" });
    await tick();
    assert.equal(calls, 1);

    // The daemon really reset to 2, but a delayed old-generation sample
    // arrives while the list is in flight and carries a newer receipt.
    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 12,
      updates: [{ id: "dl-1", seq: 12, bytesDone: 95, bytesTotal: 100, speedBps: 9 }],
    });
    assert.equal(c.latest().items[0].bytesDone, 95, "the late sample is kept temporarily");

    slow.resolve({ epoch: 1, streamSeq: 5, items: [row({ bytesDone: 2, speedBps: 1 })] });
    await tick();
    await tick();
    assert.equal(calls, 2, "a fresh read confirmed the reset");
    assert.equal(c.latest().items[0].bytesDone, 2, "the confirmed reset lowers the row");

    // Delayed old-generation samples with newer receipt sequences are ignored.
    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 13,
      updates: [{ id: "dl-1", seq: 13, bytesDone: 95, bytesTotal: 100, speedBps: 9 }],
    });
    assert.equal(c.latest().items[0].bytesDone, 2, "quarantined live event is ignored");

    // Authoritative snapshots keep advancing while live events stay quarantined.
    c.listen.emit("surge://lifecycle", { subscription, epoch: 1, seq: 14, resync: false });
    await tick();
    assert.equal(calls, 3);
    assert.equal(c.latest().items[0].bytesDone, 30, "later snapshot progress is shown");
    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 15,
      updates: [{ id: "dl-1", seq: 15, bytesDone: 80, bytesTotal: 100, speedBps: 9 }],
    });
    assert.equal(c.latest().items[0].bytesDone, 30, "live events stay quarantined");

    // Terminal authoritative state wins and cannot be revived by progress.
    c.listen.emit("surge://lifecycle", { subscription, epoch: 1, seq: 16, resync: false });
    await tick();
    assert.equal(calls, 4);
    assert.equal(c.latest().items[0].status, "completed");
    assert.equal(c.latest().items[0].bytesDone, 100);
    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 17,
      updates: [{ id: "dl-1", seq: 17, bytesDone: 100, bytesTotal: 100, speedBps: 0 }],
    });
    assert.equal(c.latest().items[0].status, "completed");
    c.controller.dispose();
  });

  it("retries a failed confirmation read with bounded backoff", async () => {
    const slow = deferred();
    const failed = deferred();
    let calls = 0;
    const c = await connected([row({ bytesDone: 10 })], {
      snapshot: () => {
        calls += 1;
        if (calls === 1) return slow.promise;
        if (calls === 2) return failed.promise;
        return { epoch: 1, streamSeq: 3, items: [row({ bytesDone: 80, speedBps: 8 })] };
      },
      retryMs: [1, 1, 1],
    });
    const subscription = c.controller.connectionEpoch();

    c.listen.emit("surge://stream", { subscription, epoch: 1, seq: 1, state: "ready" });
    await tick();
    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 3,
      updates: [{ id: "dl-1", seq: 3, bytesDone: 80, bytesTotal: 100, speedBps: 9 }],
    });
    slow.resolve({ epoch: 1, streamSeq: 1, items: [row({ bytesDone: 50, speedBps: 1 })] });
    await tick();
    await tick();
    assert.equal(c.latest().items[0].bytesDone, 80, "the live value is kept while unconfirmed");
    assert.equal(calls, 2, "the failed confirmation read was attempted");

    failed.reject(new Error("network"));
    await wait(25);
    assert.equal(calls, 3, "the confirmation read retried after backoff");
    assert.equal(c.latest().items[0].bytesDone, 80, "the catching-up read clears the suspicion");

    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 4,
      updates: [{ id: "dl-1", seq: 4, bytesDone: 90, bytesTotal: 100, speedBps: 9 }],
    });
    assert.equal(c.latest().items[0].bytesDone, 90, "streaming continues after confirmation");
    c.controller.dispose();
  });

  it("recognizes a direct same-id reset when no newer in-flight progress exists", async () => {
    let calls = 0;
    const c = await connected([row({ bytesDone: 10 })], {
      snapshot: () => {
        calls += 1;
        if (calls === 1) return { epoch: 1, streamSeq: 5, items: [row({ bytesDone: 80 })] };
        return { epoch: 1, streamSeq: 10, items: [row({ bytesDone: 2, speedBps: 1 })] };
      },
    });
    const subscription = c.controller.connectionEpoch();

    c.listen.emit("surge://stream", { subscription, epoch: 1, seq: 1, state: "ready" });
    await tick();
    assert.equal(c.latest().items[0].bytesDone, 80);

    c.listen.emit("surge://lifecycle", { subscription, epoch: 1, seq: 2, resync: true });
    await tick();
    await tick();
    assert.equal(calls, 2);
    assert.equal(c.latest().items[0].bytesDone, 2, "the direct reset is accepted");

    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 11,
      updates: [{ id: "dl-1", seq: 11, bytesDone: 70, bytesTotal: 100, speedBps: 9 }],
    });
    assert.equal(c.latest().items[0].bytesDone, 2, "quarantined live event is ignored");
    c.controller.dispose();
  });

  it("releases a quarantined generation when the row identity changes", async () => {
    let calls = 0;
    const c = await connected([row({ bytesDone: 10, addedAt: 1 })], {
      snapshot: () => {
        calls += 1;
        if (calls === 1) return { epoch: 1, streamSeq: 5, items: [row({ bytesDone: 80 })] };
        if (calls === 2) return { epoch: 1, streamSeq: 10, items: [row({ bytesDone: 2 })] };
        return { epoch: 1, streamSeq: 20, items: [row({ bytesDone: 0, addedAt: 2 })] };
      },
    });
    const subscription = c.controller.connectionEpoch();

    c.listen.emit("surge://stream", { subscription, epoch: 1, seq: 1, state: "ready" });
    await tick();
    c.listen.emit("surge://lifecycle", { subscription, epoch: 1, seq: 2, resync: true });
    await tick();
    assert.equal(c.latest().items[0].bytesDone, 2);

    c.listen.emit("surge://lifecycle", { subscription, epoch: 1, seq: 3, resync: false });
    await tick();
    assert.equal(calls, 3);
    assert.equal(c.latest().items[0].addedAt, 2);
    assert.equal(c.latest().items[0].bytesDone, 0);

    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 21,
      updates: [{ id: "dl-1", seq: 21, bytesDone: 40, bytesTotal: 100, speedBps: 4 }],
    });
    assert.equal(
      c.latest().items[0].bytesDone,
      40,
      "a replacement generation is not quarantined",
    );
    c.controller.dispose();
  });

  it("preserves reset quarantine across pause/resume for the same id+addedAt", async () => {
    let calls = 0;
    const c = await connected([row({ bytesDone: 10, addedAt: 1 })], {
      snapshot: () => {
        calls += 1;
        if (calls === 1) return { epoch: 1, streamSeq: 5, items: [row({ bytesDone: 80, addedAt: 1 })] };
        if (calls === 2) return { epoch: 1, streamSeq: 10, items: [row({ bytesDone: 2, addedAt: 1 })] };
        if (calls === 3)
          return {
            epoch: 1,
            streamSeq: 15,
            items: [row({ status: "paused", bytesDone: 2, speedBps: 0, addedAt: 1 })],
          };
        if (calls === 4) return { epoch: 1, streamSeq: 20, items: [row({ bytesDone: 2, addedAt: 1 })] };
        return { epoch: 1, streamSeq: 25, items: [row({ bytesDone: 30, addedAt: 1 })] };
      },
    });
    const subscription = c.controller.connectionEpoch();

    c.listen.emit("surge://stream", { subscription, epoch: 1, seq: 1, state: "ready" });
    await tick();
    assert.equal(c.latest().items[0].bytesDone, 80);

    c.listen.emit("surge://lifecycle", { subscription, epoch: 1, seq: 2, resync: true });
    await tick();
    assert.equal(calls, 2);
    assert.equal(c.latest().items[0].bytesDone, 2, "authoritative reset lowers the row");

    // Pause keeps the same id+addedAt: quarantine must survive.
    c.listen.emit("surge://lifecycle", { subscription, epoch: 1, seq: 3, resync: false });
    await tick();
    assert.equal(c.latest().items[0].status, "paused");
    assert.equal(c.latest().items[0].bytesDone, 2);

    // Resume the same generation: quarantine must still hold.
    c.listen.emit("surge://lifecycle", { subscription, epoch: 1, seq: 4, resync: false });
    await tick();
    assert.equal(c.latest().items[0].status, "active");
    assert.equal(c.latest().items[0].bytesDone, 2);

    // Delayed old-generation sample with a newer receipt sequence is ignored.
    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 21,
      updates: [{ id: "dl-1", seq: 21, bytesDone: 80, bytesTotal: 100, speedBps: 9 }],
    });
    assert.equal(c.latest().items[0].bytesDone, 2, "old progress must not inflate the reset");

    // Legitimate authoritative snapshots still advance the quarantined row.
    c.listen.emit("surge://lifecycle", { subscription, epoch: 1, seq: 22, resync: false });
    await tick();
    assert.equal(c.latest().items[0].bytesDone, 30, "later snapshot progress is shown");
    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 26,
      updates: [{ id: "dl-1", seq: 26, bytesDone: 80, bytesTotal: 100, speedBps: 9 }],
    });
    assert.equal(c.latest().items[0].bytesDone, 30, "live events stay quarantined");
    c.controller.dispose();
  });

  it("coalesces lifecycle transitions during a slow snapshot into exactly one follow-up read", async () => {
    const first = deferred();
    let calls = 0;
    const c = await connected([row()], {
      snapshot: () => {
        calls += 1;
        if (calls === 1) return first.promise;
        return { epoch: 1, streamSeq: 2, items: [row({ bytesDone: 30 })] };
      },
    });
    const subscription = c.controller.connectionEpoch();

    c.listen.emit("surge://lifecycle", { subscription, epoch: 1, seq: 1, resync: true });
    await tick();
    c.listen.emit("surge://lifecycle", { subscription, epoch: 1, seq: 2, resync: false });
    await tick();
    assert.equal(calls, 1, "one snapshot in flight, second transition queued");
    first.resolve({ epoch: 1, streamSeq: 2, items: [row({ bytesDone: 20 })] });
    await tick();
    await tick();
    assert.equal(calls, 2, "queued transition ran exactly one follow-up read");
    c.controller.dispose();
  });

  it("retries transient snapshot failures with bounded backoff", async () => {
    let calls = 0;
    const c = await connected([row()], {
      snapshot: () => {
        calls += 1;
        if (calls === 1) throw new Error("network");
        return { epoch: 1, streamSeq: 2, items: [row({ bytesDone: 35 })] };
      },
      retryMs: [1],
    });
    const subscription = c.controller.connectionEpoch();

    c.listen.emit("surge://stream", {
      subscription,
      epoch: 1,
      seq: 1,
      state: "ready",
    });
    await tick();
    assert.equal(calls, 1);
    await wait(25);
    assert.equal(calls, 2, "transient failure retried once after backoff");
    assert.equal(c.latest().items[0].bytesDone, 35);
    c.controller.dispose();
  });

  it("falls back without error and keeps polling when the stream cannot start", async () => {
    const listen = makeListen();
    let snapshotCalls = 0;
    const { invoke } = makeInvoke((command) => {
      if (command === "connect_surge") return [row()];
      if (command === "start_progress_stream") throw new Error("network");
      if (command === "disconnect_surge") return undefined;
      if (command === "list_downloads_snapshot") {
        snapshotCalls += 1;
        return { epoch: 1, streamSeq: 0, items: [row({ bytesDone: 40 })] };
      }
      return undefined;
    });
    const seen = [];
    const controller = new NativeQueueController(
      invoke,
      (snapshot) => seen.push(snapshot),
      10,
      listen.listen,
    );
    await controller.connect(ENDPOINT, TOKEN);
    assert.equal(seen[seen.length - 1].streamState, "fallback");
    await wait(35);
    assert.ok(snapshotCalls >= 1, "5s-style polling fallback remains active");
    assert.equal(seen[seen.length - 1].error, null);
    controller.dispose();
  });

  it("orders list-start, in-read and buffered-before-read progress by receipt sequence", async () => {
    const slow = deferred();
    let calls = 0;
    const c = await connected([row({ bytesDone: 10 })], {
      snapshot: () => {
        calls += 1;
        if (calls === 1) return slow.promise;
        return {
          epoch: 1,
          streamSeq: 20,
          items: [row({ status: "completed", bytesDone: 80, bytesTotal: 100, speedBps: 0 })],
        };
      },
    });
    const subscription = c.controller.connectionEpoch();

    c.listen.emit("surge://stream", {
      subscription,
      epoch: 1,
      seq: 1,
      state: "ready",
    });
    await tick();

    // Received at the snapshot cursor: the authoritative list owns it.
    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 5,
      updates: [{ id: "dl-1", seq: 5, bytesDone: 60, bytesTotal: 100, speedBps: 6 }],
    });
    // Received after the list request started: newer than the snapshot.
    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 6,
      updates: [{ id: "dl-1", seq: 6, bytesDone: 70, bytesTotal: 100, speedBps: 7 }],
    });
    await tick();
    assert.equal(c.latest().items[0].bytesDone, 70);

    slow.resolve({ epoch: 1, streamSeq: 5, items: [row({ bytesDone: 70 })] });
    await tick();
    assert.equal(c.latest().items[0].bytesDone, 70, "in-read progress matches the list");

    // Buffered before the read but delivered afterwards: its receipt seq is
    // below the cursor even though the batch was flushed later.
    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 8,
      updates: [{ id: "dl-1", seq: 4, bytesDone: 99, bytesTotal: 100, speedBps: 9 }],
    });
    await tick();
    assert.equal(c.latest().items[0].bytesDone, 70, "pre-read sample must not override the list");

    // A newer sample that reports fewer bytes cannot roll the row back.
    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 9,
      updates: [{ id: "dl-1", seq: 9, bytesDone: 65, bytesTotal: 100, speedBps: 4 }],
    });
    await tick();
    assert.equal(c.latest().items[0].bytesDone, 70, "bytes stay monotonic for the generation");

    // Terminal authoritative state wins and progress cannot revive it.
    c.listen.emit("surge://lifecycle", {
      subscription,
      epoch: 1,
      seq: 10,
      resync: false,
    });
    await tick();
    assert.equal(c.latest().items[0].status, "completed");
    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 11,
      updates: [{ id: "dl-1", seq: 11, bytesDone: 90, bytesTotal: 100, speedBps: 9 }],
    });
    await tick();
    assert.equal(c.latest().items[0].bytesDone, 80, "terminal row is never mutated by progress");
    c.controller.dispose();
  });

  it("accepts a legitimate reset only through authoritative reconciliation", async () => {
    let calls = 0;
    const c = await connected([row({ bytesDone: 10 })], {
      snapshot: () => {
        calls += 1;
        if (calls === 1) {
          return { epoch: 1, streamSeq: 5, items: [row({ bytesDone: 80 })] };
        }
        if (calls === 2) {
          return { epoch: 1, streamSeq: 10, items: [row({ bytesDone: 0 })] };
        }
        return { epoch: 1, streamSeq: 20, items: [row({ bytesDone: 20 })] };
      },
    });
    const subscription = c.controller.connectionEpoch();

    c.listen.emit("surge://stream", { subscription, epoch: 1, seq: 1, state: "ready" });
    await tick();
    assert.equal(c.latest().items[0].bytesDone, 80);

    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 6,
      updates: [{ id: "dl-1", seq: 6, bytesDone: 90, bytesTotal: 100, speedBps: 9 }],
    });
    assert.equal(c.latest().items[0].bytesDone, 90);

    c.listen.emit("surge://lifecycle", { subscription, epoch: 1, seq: 7, resync: true });
    await tick();
    assert.equal(c.latest().items[0].bytesDone, 0, "authoritative snapshot may reset the row");

    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 11,
      updates: [{ id: "dl-1", seq: 11, bytesDone: 20, bytesTotal: 100, speedBps: 2 }],
    });
    assert.equal(
      c.latest().items[0].bytesDone,
      0,
      "live progress stays quarantined after the rollback",
    );

    c.listen.emit("surge://lifecycle", { subscription, epoch: 1, seq: 12, resync: false });
    await tick();
    assert.equal(
      c.latest().items[0].bytesDone,
      20,
      "authoritative progress resumes after the reset",
    );
    c.controller.dispose();
  });

  it("does not let a delayed unavailable notice override a newer ready notice", async () => {
    const c = await connected([row()]);
    const subscription = c.controller.connectionEpoch();

    c.listen.emit("surge://stream", { subscription, epoch: 1, seq: 5, state: "ready" });
    c.listen.emit("surge://stream", { subscription, epoch: 1, seq: 3, state: "unavailable" });
    assert.equal(c.latest().streamState, "live", "older unavailable must be fenced out");

    c.listen.emit("surge://stream", { subscription, epoch: 1, seq: 6, state: "unavailable" });
    assert.equal(c.latest().streamState, "fallback");
    c.controller.dispose();
  });

  it("retries listener registration after a first failure and never blocks connect", async () => {
    let attempts = 0;
    const handlers = new Map();
    const unlistened = [];
    const listen = async (event, handler) => {
      attempts += 1;
      if (attempts === 1) throw new Error("registration failed");
      handlers.set(event, handler);
      return () => {
        unlistened.push(event);
        handlers.delete(event);
      };
    };
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [row()];
      if (command === "start_progress_stream") return undefined;
      if (command === "list_downloads_snapshot") return { epoch: 1, streamSeq: 0, items: [row()] };
      return undefined;
    });
    const controller = new NativeQueueController(invoke, () => {}, 60_000, listen, {
      listenerTimeoutMs: 50,
    });
    await controller.connect(ENDPOINT, TOKEN);
    assert.equal(controller.state.phase, "connected");
    assert.equal(handlers.size, 0, "failed registration is not cached as success");
    assert.equal(
      calls.filter((call) => call.command === "start_progress_stream").length,
      0,
      "the native stream must not start without registered listeners",
    );
    assert.equal(controller.state.streamState, "fallback");

    await controller.connect(ENDPOINT, TOKEN);
    assert.equal(handlers.size, 3, "next reconnect retries the full registration");
    assert.equal(
      calls.filter((call) => call.command === "start_progress_stream").length,
      1,
      "the stream starts once listeners are ready",
    );
    controller.dispose();
    assert.deepEqual(
      unlistened.sort(),
      ["surge://lifecycle", "surge://progress", "surge://stream"],
    );
  });

  it("times out a hung listener, cleans partial registration and keeps connect unblocked", async () => {
    let attempts = 0;
    const unlistened = [];
    const hung = deferred();
    const listen = async (event) => {
      attempts += 1;
      if (attempts === 1) {
        return () => {
          unlistened.push(event);
        };
      }
      if (attempts === 2) {
        return hung.promise;
      }
      return () => {
        unlistened.push(event);
      };
    };
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [row()];
      if (command === "start_progress_stream") return undefined;
      if (command === "list_downloads_snapshot") return { epoch: 1, streamSeq: 0, items: [row()] };
      return undefined;
    });
    const controller = new NativeQueueController(invoke, () => {}, 60_000, listen, {
      listenerTimeoutMs: 20,
    });
    const started = Date.now();
    await controller.connect(ENDPOINT, TOKEN);
    assert.ok(Date.now() - started < 1000, "a hung listener must not block the connection");
    assert.equal(controller.state.phase, "connected");
    assert.deepEqual(unlistened, ["surge://progress"], "partial registration is cleaned up");
    assert.equal(
      calls.filter((call) => call.command === "start_progress_stream").length,
      0,
      "a timed-out registration must not start the native stream",
    );
    assert.equal(controller.state.streamState, "fallback");

    // A late resolution from the timed-out attempt is unlistened, not leaked.
    hung.resolve(() => {
      unlistened.push("late");
    });
    await tick();
    assert.deepEqual(unlistened, ["surge://progress", "late"]);
    controller.dispose();
  });

  it("drains queued work on the new epoch after a blocked stale refresh releases", async () => {
    let listCalls = 0;
    const first = deferred();
    const second = deferred();
    const { invoke } = makeInvoke((command, args) => {
      if (command === "connect_surge") return [row()];
      if (command === "start_progress_stream") return undefined;
      if (command === "disconnect_surge") return undefined;
      if (command === "list_downloads_snapshot") {
        listCalls += 1;
        if (listCalls === 1) return first.promise;
        if (listCalls === 2) return second.promise;
        return { epoch: 2, streamSeq: 0, items: [row({ id: "dl-B", name: "bravo.bin" })] };
      }
      throw new Error(`unexpected command: ${command}`);
    });
    const listen = makeListen();
    const controller = new NativeQueueController(
      invoke,
      () => {},
      60_000,
      listen.listen,
    );
    await controller.connect(ENDPOINT, TOKEN);
    const firstSubscription = controller.connectionEpoch();
    listen.emit("surge://lifecycle", {
      subscription: firstSubscription,
      epoch: 1,
      seq: 1,
      resync: false,
    });
    await tick();
    assert.equal(listCalls, 1, "old epoch read is in flight");

    // Replace the connection while the old reconciliation is blocked.
    await controller.connect(ENDPOINT, TOKEN);
    const secondSubscription = controller.connectionEpoch();
    // A concurrent poll for the new epoch joins behind the single-flight read.
    const poll = controller.refresh();
    await tick();
    assert.equal(listCalls, 2, "new epoch poll is in flight");

    // New-epoch lifecycle queued while the old drain still owns the flag.
    listen.emit("surge://lifecycle", {
      subscription: secondSubscription,
      epoch: 2,
      seq: 1,
      resync: false,
    });
    await tick();

    first.resolve({ epoch: 1, streamSeq: 0, items: [row()] });
    await tick();
    second.resolve({ epoch: 2, streamSeq: 0, items: [row({ id: "dl-B" })] });
    await poll;
    await tick();
    await tick();
    assert.ok(listCalls >= 3, "new epoch queued work drains after the old read finishes");
    assert.equal(controller.state.items[0].id, "dl-B");
    controller.dispose();
  });

  it("ignores late events after disconnect and unlistens on dispose", async () => {
    const c = await connected([row()]);
    const subscription = c.controller.connectionEpoch();
    await c.controller.disconnect();
    assert.equal(c.latest().phase, "offline");
    assert.deepEqual(c.latest().items, []);
    c.listen.emit("surge://progress", {
      subscription,
      epoch: 1,
      seq: 5,
      updates: [{ id: "dl-1", bytesDone: 99, bytesTotal: 100, speedBps: 9 }],
    });
    assert.equal(c.latest().phase, "offline");
    assert.deepEqual(c.latest().items, []);

    c.controller.dispose();
    assert.deepEqual(
      [...c.listen.unlistened].sort(),
      ["surge://lifecycle", "surge://progress", "surge://stream"],
    );
  });
});
