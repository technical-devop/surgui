// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Pure merge/validation tests for truthful progress events and snapshots.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  applyProgressUpdates,
  mergeSnapshotItems,
  mergeSnapshotWithProgress,
  sanitizeLifecycleNotice,
  sanitizeProgressBatch,
  sanitizeSnapshotPayload,
  sanitizeStreamState,
} from "./progress.ts";

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

describe("progress validation", () => {
  it("sanitizes a valid batch and keeps explicit zeroes", () => {
    const batch = sanitizeProgressBatch({
      subscription: 3,
      epoch: 2,
      seq: 7,
      updates: [
        { id: "dl-1", bytesDone: 0, bytesTotal: 0, speedBps: 0 },
        { id: "dl-2", bytesDone: 5, bytesTotal: 10, speedBps: 2 },
      ],
    });
    assert.deepEqual(batch, {
      subscription: 3,
      epoch: 2,
      seq: 7,
      updates: [
        { id: "dl-1", seq: 7, bytesDone: 0, bytesTotal: 0, speedBps: 0 },
        { id: "dl-2", seq: 7, bytesDone: 5, bytesTotal: 10, speedBps: 2 },
      ],
    });
  });

  it("drops malformed, empty, oversized and control-bearing payloads", () => {
    assert.equal(sanitizeProgressBatch(null), null);
    assert.equal(sanitizeProgressBatch({ subscription: 0, seq: 1, updates: [] }), null);
    assert.equal(sanitizeProgressBatch({ subscription: 1, seq: 0, updates: [] }), null);
    assert.equal(
      sanitizeProgressBatch({ subscription: 1, seq: 1, updates: [{ id: "bad\u0000id" }] }),
      null,
    );
    assert.equal(
      sanitizeProgressBatch({
        subscription: 1,
        seq: 1,
        updates: Array.from({ length: 9000 }, (_, i) => ({ id: `id-${i}` })),
      }),
      null,
    );
  });

  it("validates lifecycle and stream-state notices", () => {
    assert.deepEqual(sanitizeLifecycleNotice({ subscription: 1, epoch: 2, seq: 3, resync: true }), {
      subscription: 1,
      epoch: 2,
      seq: 3,
      resync: true,
    });
    assert.equal(sanitizeLifecycleNotice({ subscription: 1, seq: 0 }), null);
    assert.deepEqual(sanitizeStreamState({ subscription: 1, epoch: 2, seq: 4, state: "ready" }), {
      subscription: 1,
      epoch: 2,
      seq: 4,
      state: "ready",
    });
    assert.equal(sanitizeStreamState({ subscription: 1, state: "weird" }), null);
  });
});

describe("progress application", () => {
  it("only updates existing active rows and preserves identity", () => {
    const active = row();
    const paused = row({ id: "dl-2", status: "paused", bytesDone: 50, speedBps: 0 });
    const next = applyProgressUpdates([active, paused], [
      { id: "dl-1", bytesDone: 40, bytesTotal: 100, speedBps: 8 },
      { id: "dl-2", bytesDone: 90, bytesTotal: 100, speedBps: 8 },
      { id: "ghost", bytesDone: 90, bytesTotal: 100, speedBps: 8 },
    ]);
    assert.equal(next.length, 2);
    assert.deepEqual(next[0], {
      ...active,
      bytesDone: 40,
      bytesTotal: 100,
      speedBps: 8,
    });
    assert.equal(next[1], paused, "paused row must not be mutated by progress");
  });

  it("keeps bytes monotonic for a generation instead of rolling back", () => {
    const items = [row({ bytesDone: 90, speedBps: 12 })];
    assert.equal(
      applyProgressUpdates(items, [
        { id: "dl-1", bytesDone: 0, bytesTotal: 0, speedBps: 0 },
      ]),
      items,
      "a lower progress sample must not reset the row",
    );
    const advanced = applyProgressUpdates(items, [
      { id: "dl-1", bytesDone: 95, bytesTotal: 100, speedBps: 0 },
    ]);
    assert.equal(advanced[0].bytesDone, 95);
    assert.equal(advanced[0].speedBps, 0, "a stall may update speed without new bytes");
  });

  it("returns the same array when nothing applied", () => {
    const items = [row()];
    assert.equal(
      applyProgressUpdates(items, [{ id: "ghost", bytesDone: 1, bytesTotal: 2, speedBps: 3 }]),
      items,
    );
  });
});

describe("snapshot merge ordering", () => {
  it("defers a lower snapshot when newer in-flight progress can explain it", () => {
    const current = [row({ bytesDone: 80, bytesTotal: 100, speedBps: 9 })];
    const snapshot = [row({ bytesDone: 50, bytesTotal: 100, speedBps: 1 })];
    // The list cursor is 1 but a real event with receipt 3 arrived during it.
    const seq = new Map([["dl-1", 3]]);
    const merged = mergeSnapshotWithProgress(current, snapshot, 1, seq);
    assert.equal(merged.items[0].bytesDone, 80, "the newer live value is kept");
    assert.deepEqual(merged.rolledBack, [], "an in-flight snapshot is not a reset");
    assert.deepEqual(
      merged.pendingConfirmation,
      ["dl-1"],
      "the row must be confirmed by a fresh read",
    );
  });

  it("confirms a reset once the read cursor covers the newer progress", () => {
    const current = [row({ bytesDone: 90, bytesTotal: 100, speedBps: 9 })];
    const snapshot = [row({ bytesDone: 2, bytesTotal: 100, speedBps: 1 })];
    // The fresh read started after receipt 11, so the lower value is real.
    const seq = new Map([["dl-1", 11]]);
    const merged = mergeSnapshotWithProgress(current, snapshot, 12, seq);
    assert.equal(merged.items[0].bytesDone, 2, "the confirmed snapshot owns the reset");
    assert.deepEqual(merged.rolledBack, ["dl-1"], "the reset row must be quarantined");
    assert.deepEqual(merged.pendingConfirmation, []);
  });

  it("treats a lower snapshot with no newer progress as a direct reset", () => {
    const current = [row({ bytesDone: 90, bytesTotal: 100, speedBps: 9 })];
    const snapshot = [row({ bytesDone: 2, bytesTotal: 100, speedBps: 1 })];
    const merged = mergeSnapshotWithProgress(current, snapshot, 10, new Map());
    assert.equal(merged.items[0].bytesDone, 2);
    assert.deepEqual(merged.rolledBack, ["dl-1"]);
    assert.deepEqual(merged.pendingConfirmation, []);
  });

  it("keeps an advancing snapshot and does not mark it rolled back", () => {
    const current = [row({ bytesDone: 20, bytesTotal: 100, speedBps: 2 })];
    const snapshot = [row({ bytesDone: 40, bytesTotal: 100, speedBps: 1 })];
    const seq = new Map([["dl-1", 11]]);
    const merged = mergeSnapshotWithProgress(current, snapshot, 10, seq);
    assert.equal(merged.items[0].bytesDone, 40);
    assert.deepEqual(merged.rolledBack, []);
  });

  it("lets the authoritative snapshot reset bytes when progress is not newer", () => {
    const current = [row({ bytesDone: 90, speedBps: 12 })];
    const reset = [row({ bytesDone: 0, bytesTotal: 100, speedBps: 0 })];
    const seq = new Map([["dl-1", 6]]);
    const merged = mergeSnapshotItems(current, reset, 10, seq);
    assert.equal(merged[0].bytesDone, 0, "cursor-fenced snapshot owns the reset");
  });

  it("does not carry progress across a different download generation", () => {
    const current = [row({ bytesDone: 90, addedAt: 1 })];
    const replacement = [row({ bytesDone: 0, addedAt: 2 })];
    const seq = new Map([["dl-1", 11]]);
    const merged = mergeSnapshotItems(current, replacement, 10, seq);
    assert.equal(merged[0].bytesDone, 0);
    assert.equal(merged[0].addedAt, 2);
  });

  it("lets a terminal snapshot win over older progress and preserves unknown size", () => {
    const current = [row({ bytesDone: 80 })];
    const completed = [row({ status: "completed", bytesDone: 100, speedBps: 0 })];
    const seq = new Map([["dl-1", 10]]);
    assert.equal(mergeSnapshotItems(current, completed, 10, seq)[0].status, "completed");
    assert.equal(mergeSnapshotItems(current, completed, 10, seq)[0].bytesDone, 100);

    const unknown = [row({ bytesDone: 40, bytesTotal: 0, speedBps: 2 })];
    const mergedUnknown = mergeSnapshotItems([row()], unknown, 0, new Map());
    assert.equal(mergedUnknown[0].bytesTotal, 0);
    assert.equal(mergedUnknown[0].bytesDone, 40);
  });

  it("applies snapshot rows newer than recorded progress", () => {
    const current = [row({ bytesDone: 80 })];
    const snapshot = [row({ bytesDone: 20 })];
    const seq = new Map([["dl-1", 3]]);
    const merged = mergeSnapshotItems(current, snapshot, 8, seq);
    assert.equal(merged[0].bytesDone, 20);
  });
});

describe("snapshot payload", () => {
  it("accepts the envelope and legacy bare arrays", () => {
    assert.deepEqual(sanitizeSnapshotPayload({ epoch: 2, streamSeq: 9, items: [1] }), {
      epoch: 2,
      streamSeq: 9,
      items: [1],
    });
    assert.deepEqual(sanitizeSnapshotPayload([1, 2]), {
      epoch: null,
      streamSeq: 0,
      items: [1, 2],
    });
    assert.equal(sanitizeSnapshotPayload({ epoch: 2 }), null);
  });
});
