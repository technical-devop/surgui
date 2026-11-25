// @ts-nocheck — runtime assertions via node --test (type stripping).
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { formatEta, hasHookFailed, isProcessing, lifecycleLabel, lifecycleSuffix } from "./queue.ts";
import { NativeQueueController } from "./native-controller.ts";

const base = { id: "dl-1", name: "archive.tar.gz", bytesDone: 1000, bytesTotal: 1000, speedBps: 0, addedAt: 1 };

describe("lifecycle ETA/copy truthfulness", () => {
  it("never says Done while processing even when bytes are complete", () => {
    const eta = formatEta({ ...base, status: "completed", processing: true, hookFailed: false });
    assert.equal(eta, "Processing…");
  });

  it("reports hook failure distinctly without pretending transfer failed", () => {
    const item = { ...base, status: "completed", processing: false, hookFailed: true };
    assert.equal(formatEta(item), "Hook failed");
    assert.notEqual(formatEta(item), "Done");
    assert.notEqual(formatEta(item), "Failed");
    assert.equal(hasHookFailed(item), true);
    assert.equal(isProcessing(item), false);
    assert.equal(lifecycleLabel(item), "Hook failed");
    assert.match(lifecycleSuffix(item), /post-download step failed/);
  });

  it("keeps real transfer failure copy unchanged", () => {
    assert.equal(formatEta({ ...base, status: "failed", bytesDone: 100, processing: false, hookFailed: false }), "Failed");
  });

  it("treats absent lifecycle fields (old daemon) as idle with silent copy", () => {
    const old = { ...base, status: "active", bytesDone: 100, speedBps: 10 };
    assert.equal(isProcessing(old), false);
    assert.equal(hasHookFailed(old), false);
    assert.equal(lifecycleLabel(old), null);
    assert.equal(lifecycleSuffix(old), "");
    assert.equal(formatEta(old), "1 min left");
  });

  it("failed status with all bytes has precedence over Done", () => {
    const item = { ...base, status: "failed", bytesDone: 1000, bytesTotal: 1000, speedBps: 0, processing: false, hookFailed: false, phase: undefined };
    assert.equal(formatEta(item), "Failed");
    assert.notEqual(formatEta(item), "Done");
    // Badge stays on the authoritative failed status; no lifecycle override.
    assert.equal(lifecycleLabel(item), null);
    assert.equal(lifecycleSuffix(item), "");
  });

  it("verifying phase with all bytes shows Verifying… rather than Done", () => {
    const item = { ...base, status: "active", bytesDone: 1000, bytesTotal: 1000, speedBps: 0, processing: false, hookFailed: false, phase: "verifying" };
    assert.equal(formatEta(item), "Verifying…");
    assert.notEqual(formatEta(item), "Done");
    assert.equal(lifecycleLabel(item), "Verifying…");
    assert.match(lifecycleSuffix(item), /verifying/);
    assert.ok(!lifecycleSuffix(item).toLowerCase().includes("complete"), "suffix must not claim completed");
    assert.ok(!String(lifecycleLabel(item)).toLowerCase().includes("done"), "badge must not claim Done");
  });

  it("finalizing phase with all bytes shows Finalizing… rather than Done", () => {
    const item = { ...base, status: "active", bytesDone: 1000, bytesTotal: 1000, speedBps: 0, processing: false, hookFailed: false, phase: "finalizing" };
    assert.equal(formatEta(item), "Finalizing…");
    assert.notEqual(formatEta(item), "Done");
    assert.equal(lifecycleLabel(item), "Finalizing…");
    assert.match(lifecycleSuffix(item), /finalizing/);
    assert.ok(!lifecycleSuffix(item).toLowerCase().includes("complete"), "suffix must not claim completed");
  });

  it("completed + finalizing is real (not stale) and shows Finalizing…", () => {
    // attachPostDownloadState sets phase finalizing whenever the scheduler
    // IsFinalizing(id), independent of status — so completed + finalizing
    // means finalization is still running, not Done.
    const item = { ...base, status: "completed", bytesDone: 1000, bytesTotal: 1000, speedBps: 0, processing: false, hookFailed: false, phase: "finalizing" };
    assert.equal(formatEta(item), "Finalizing…");
    assert.notEqual(formatEta(item), "Done");
    assert.equal(lifecycleLabel(item), "Finalizing…");
    assert.match(lifecycleSuffix(item), /finalizing/);
    assert.match(lifecycleSuffix(item), /not done yet/);
    assert.ok(!lifecycleSuffix(item).toLowerCase().includes("complete") || lifecycleSuffix(item).toLowerCase().includes("not done"), "suffix must not unqualifiedly assert completion");
    assert.ok(!String(lifecycleLabel(item)).toLowerCase().includes("done"), "badge must not claim Done");
  });

  it("completed + finalizing transitions to Done once the phase clears", () => {
    const finalizing = { ...base, status: "completed", bytesDone: 1000, bytesTotal: 1000, speedBps: 0, processing: false, hookFailed: false, phase: "finalizing" };
    assert.equal(formatEta(finalizing), "Finalizing…");
    const complete = { ...finalizing, phase: undefined };
    assert.equal(formatEta(complete), "Done");
    assert.equal(lifecycleLabel(complete), null);
    assert.equal(lifecycleSuffix(complete), "");
  });

  it("ignores stale/contradictory phases on failed/paused rows", () => {
    const failedStale = { ...base, status: "failed", bytesDone: 1000, bytesTotal: 1000, speedBps: 0, processing: false, hookFailed: false, phase: "finalizing" };
    assert.equal(formatEta(failedStale), "Failed");
    assert.equal(lifecycleLabel(failedStale), null);
    assert.equal(lifecycleSuffix(failedStale), "");

    const failedVerifying = { ...base, status: "failed", bytesDone: 1000, bytesTotal: 1000, speedBps: 0, processing: false, hookFailed: false, phase: "verifying" };
    assert.equal(formatEta(failedVerifying), "Failed");
    assert.equal(lifecycleLabel(failedVerifying), null);

    const pausedStale = { ...base, status: "paused", bytesDone: 1000, bytesTotal: 1000, speedBps: 0, processing: false, hookFailed: false, phase: "finalizing" };
    // Legacy precedence: bytes-complete Done beats paused; the stale phase
    // must still not surface as Finalizing…/Verifying….
    assert.equal(formatEta(pausedStale), "Done");
    assert.equal(lifecycleLabel(pausedStale), null);
    assert.equal(lifecycleSuffix(pausedStale), "");
  });

  it("old daemon with all bytes and absent phase keeps legacy Done copy", () => {
    const oldComplete = { ...base, status: "active", bytesDone: 1000, bytesTotal: 1000, speedBps: 0, processing: false, hookFailed: false, phase: undefined };
    assert.equal(formatEta(oldComplete), "Done");
    assert.equal(lifecycleLabel(oldComplete), null);
    assert.equal(lifecycleSuffix(oldComplete), "");
  });

  it("lifecycle copy never leaks hook text or secrets", () => {
    const secret = "Bearer-super-secret-hook-cmd-9f3a";
    const item = {
      ...base,
      status: "active",
      bytesDone: 1000,
      bytesTotal: 1000,
      speedBps: 0,
      processing: false,
      hookFailed: false,
      phase: "finalizing",
      hook_error: `post-download failed: ${secret}`,
      hookError: secret,
      error: secret,
    };
    for (const text of [formatEta(item), lifecycleLabel(item) ?? "", lifecycleSuffix(item)]) {
      assert.ok(!text.includes(secret), "lifecycle copy must not leak hook text");
      assert.ok(!text.includes("Bearer"), "lifecycle copy must not leak secrets");
    }
  });
});

describe("controller lifecycle propagation + privacy", () => {
  function makeInvoke(handler) {
    const invoke = async (command, args) => handler(command, args);
    return invoke;
  }

  it("propagates v0.13 bits and never stores hook text/commands/secrets", async () => {
    const secret = "hook-cmd-sh-super-secret-9f3a-Bearer-token";
    const row = {
      id: "dl-9",
      name: "bundle.tar.gz",
      status: "completed",
      bytesDone: 1000,
      bytesTotal: 1000,
      speedBps: 0,
      addedAt: 7,
      phase: "finalizing",
      processing: true,
      processed: false,
      hookFailed: false,
      hook_error: `post-download failed: ${secret} --exec rm -rf /`,
      hookError: secret,
      error: secret,
    };
    const invoke = makeInvoke((command) => {
      if (command === "connect_surge") return [row];
      throw new Error(`unexpected ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect("http://127.0.0.1:9", "test-token");
      const item = c.state.items[0];
      assert.equal(item.processing, true);
      assert.equal(item.processed, false);
      assert.equal(item.hookFailed, false);
      assert.equal(item.phase, "finalizing");
      const dumped = JSON.stringify(c.state);
      assert.ok(!dumped.includes(secret), "state must not leak hook text/secret");
      assert.ok(!dumped.includes("hook_error"), "raw hook key must not survive");
      assert.ok(!dumped.includes("hookError"), "raw hook key must not survive");
      assert.equal(formatEta(item), "Processing…");
    } finally {
      c.dispose();
    }
  });

  it("maps hook_failed snake_case and supports absent fields for older servers", async () => {
    const invoke = makeInvoke((command) => {
      if (command === "connect_surge") {
        return [
          { id: "old-1", name: "old.bin", status: "active", bytesDone: 10, bytesTotal: 100, speedBps: 5, addedAt: 1 },
          { id: "new-1", name: "new.bin", status: "active", bytesDone: 100, bytesTotal: 100, speedBps: 0, addedAt: 2, phase: "  ", processing: false, processed: true, hook_failed: true },
        ];
      }
      throw new Error(`unexpected ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect("http://127.0.0.1:9", "t");
      const old = c.state.items.find((i) => i.id === "old-1");
      const hooked = c.state.items.find((i) => i.id === "new-1");
      assert.equal(old.processing, false);
      assert.equal(old.hookFailed, false);
      assert.equal(old.phase, undefined);
      assert.equal(hooked.hookFailed, true);
      assert.equal(formatEta(hooked), "Hook failed");
    } finally {
      c.dispose();
    }
  });
});
