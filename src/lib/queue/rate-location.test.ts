// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Focused coverage for global rate control + open-location.
// Mocked IPC only; no daemon, no paths, no secrets.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { NativeQueueController } from "./native-controller.ts";
import {
  MAX_RATE_LIMIT_BPS,
  formatRateBpsToMb,
  parseRateMbToBps,
  rateInputErrorMessage,
} from "./queue.ts";

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

describe("rate parsing boundaries", () => {
  it("zero means unlimited and round-trips", () => {
    assert.deepEqual(parseRateMbToBps("0"), { ok: true, bytes: 0 });
    assert.deepEqual(parseRateMbToBps("0.0"), { ok: true, bytes: 0 });
    assert.deepEqual(parseRateMbToBps("0.000000"), { ok: true, bytes: 0 });
    assert.equal(formatRateBpsToMb(0), "0");
  });

  it("converts decimal MB/s with exact integer arithmetic", () => {
    assert.deepEqual(parseRateMbToBps("1"), { ok: true, bytes: 1_000_000 });
    assert.deepEqual(parseRateMbToBps("1.5"), { ok: true, bytes: 1_500_000 });
    assert.deepEqual(parseRateMbToBps("0.000001"), { ok: true, bytes: 1 });
    assert.deepEqual(parseRateMbToBps("2.500000"), { ok: true, bytes: 2_500_000 });
    assert.equal(formatRateBpsToMb(1_500_000), "1.5");
    assert.equal(formatRateBpsToMb(1), "0.000001");
    assert.equal(formatRateBpsToMb(1_000_000), "1");
  });

  it("round-trips exact values", () => {
    for (const bytes of [0, 1, 1_000_000, 1_500_000, 2_500_000, 1_234_567, MAX_RATE_LIMIT_BPS]) {
      const text = formatRateBpsToMb(bytes);
      const parsed = parseRateMbToBps(text);
      assert.equal(parsed.ok, true, `expected ok for ${bytes} -> ${text}`);
      assert.equal(parsed.bytes, bytes, `round trip failed for ${bytes}`);
    }
  });

  it("rejects invalid, negative, too-precise and overflow", () => {
    assert.equal(parseRateMbToBps("").reason, "empty");
    assert.equal(parseRateMbToBps("abc").reason, "invalid");
    assert.equal(parseRateMbToBps("1.2.3").reason, "invalid");
    assert.equal(parseRateMbToBps("-1").reason, "negative");
    assert.equal(parseRateMbToBps("-0.5").reason, "negative");
    assert.equal(parseRateMbToBps("1.0000000").reason, "too-precise");
    // MAX exactly allowed.
    assert.deepEqual(parseRateMbToBps("9007199254.740991"), { ok: true, bytes: MAX_RATE_LIMIT_BPS });
    // One byte over MAX rejected.
    assert.equal(parseRateMbToBps("9007199254.740992").reason, "overflow");
    assert.equal(parseRateMbToBps("99999999999").reason, "overflow");
  });
});

describe("rate IPC", () => {
  it("loadRate calls get_global_rate_limit with no args and stores exact integer", async () => {
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "get_global_rate_limit") return 2_500_000;
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      assert.equal(await c.startManaged(), true);
      assert.equal(c.state.managed, true);
      await c.loadRate();
      const call = calls.find((x) => x.command === "get_global_rate_limit");
      assert.ok(call, "expected get_global_rate_limit");
      assert.ok(call.args === undefined, "get takes no args");
      assert.equal(c.state.rateLimitBps, 2_500_000);
      assert.equal(c.state.rateError, null);
    } finally {
      c.dispose();
    }
  });

  it("setRateLimit calls set_global_rate_limit with {rate} and confirms", async () => {
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "set_global_rate_limit") return 1_500_000;
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      const ok = await c.setRateLimit(1_500_000);
      assert.equal(ok, true);
      const call = calls.find((x) => x.command === "set_global_rate_limit");
      assert.ok(call, "expected set_global_rate_limit");
      assert.deepEqual(call.args, { rate: 1_500_000 });
      assert.equal(c.state.rateLimitBps, 1_500_000);
    } finally {
      c.dispose();
    }
  });

  it("duplicate pending set issues exactly one IPC and second reports false", async () => {
    let release;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "set_global_rate_limit") {
        return new Promise((resolve) => {
          release = () => resolve(1_000_000);
        });
      }
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let first = null;
    try {
      await c.startManaged();
      first = c.setRateLimit(1_000_000);
      for (let i = 0; i < 5; i++) await Promise.resolve();
      const second = await c.setRateLimit(2_000_000);
      assert.equal(second, false, "duplicate pending write must report false");
      assert.equal(calls.filter((x) => x.command === "set_global_rate_limit").length, 1);
      release();
      assert.equal(await first, true);
      first = null;
      assert.equal(c.state.rateLimitBps, 1_000_000);
    } finally {
      try {
        if (first && release) release();
      } catch {}
      try {
        if (first) await first;
      } catch {}
      c.dispose();
    }
  });

  it("stale load after disconnect/reconnect is discarded", async () => {
    let releaseLoad;
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "connect_surge") return [];
      if (command === "disconnect_surge") return { ok: true };
      if (command === "get_global_rate_limit") {
        return new Promise((resolve) => {
          releaseLoad = () => resolve(9_000_000);
        });
      }
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pending = null;
    try {
      await c.startManaged();
      pending = c.loadRate();
      for (let i = 0; i < 3; i++) await Promise.resolve();
      await c.disconnect();
      assert.equal(c.state.phase, "offline");
      releaseLoad();
      await pending;
      pending = null;
      assert.equal(c.state.phase, "offline");
      assert.equal(c.state.rateLimitBps, null, "stale load must not populate new state");
    } finally {
      try {
        if (pending && releaseLoad) releaseLoad();
      } catch {}
      try {
        if (pending) await pending;
      } catch {}
      c.dispose();
    }
  });

  it("RateUnconfirmed poisons writer until restart and never implies failure", async () => {
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "set_global_rate_limit") throw "RateUnconfirmed";
      if (command === "disconnect_surge") return { ok: true };
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      const ok = await c.setRateLimit(1_000_000);
      assert.equal(ok, false);
      assert.equal(c.state.rateUnconfirmed, true);
      assert.ok(
        (c.state.rateError ?? "").toLowerCase().includes("could not be confirmed"),
        "must show explicit unconfirmed status",
      );
      const before = calls.filter((x) => x.command === "set_global_rate_limit").length;
      const again = await c.setRateLimit(2_000_000);
      assert.equal(again, false);
      assert.equal(
        calls.filter((x) => x.command === "set_global_rate_limit").length,
        before,
        "poisoned writer must not send IPC",
      );
      // Reconnect does not clear the poison.
      await c.disconnect();
      await c.startManaged();
      assert.equal(c.state.rateUnconfirmed, true);
      const afterReconnect = await c.setRateLimit(3_000_000);
      assert.equal(afterReconnect, false);
      assert.equal(
        calls.filter((x) => x.command === "set_global_rate_limit").length,
        before,
        "poison survives reconnect",
      );
    } finally {
      c.dispose();
    }
  });

  it("unknown readback mismatch is treated as unconfirmed, not success", async () => {
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "set_global_rate_limit") return 999;
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      const ok = await c.setRateLimit(1_000_000);
      assert.equal(ok, false);
      assert.equal(c.state.rateUnconfirmed, true);
    } finally {
      c.dispose();
    }
  });
});

describe("open location IPC", () => {
  it("managed completed id calls open_download_location with {id} only", async () => {
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "start_managed_surge") {
        return [{ id: "dl-c1", name: "done.iso", status: "completed", bytesDone: 10, bytesTotal: 10, speedBps: 0, addedAt: 1 }];
      }
      if (command === "open_download_location") return undefined;
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      const ok = await c.openLocation("dl-c1");
      assert.equal(ok, true);
      const call = calls.find((x) => x.command === "open_download_location");
      assert.ok(call, "expected open_download_location");
      assert.deepEqual(call.args, { id: "dl-c1" });
      assert.equal(Object.keys(call.args).length, 1, "only {id} crosses IPC");
      assert.equal(c.state.locationError, null);
    } finally {
      c.dispose();
    }
  });

  it("manual connection refuses without IPC and explains", async () => {
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [];
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect("http://127.0.0.1:9", "test-only-token");
      assert.equal(c.state.managed, false);
      const ok = await c.openLocation("dl-c1");
      assert.equal(ok, false);
      assert.ok(!calls.some((x) => x.command === "open_download_location"), "manual must not send IPC");
      assert.ok(
        (c.state.locationError ?? "").toLowerCase().includes("bundled"),
        "must explain managed-only",
      );
    } finally {
      c.dispose();
    }
  });

  it("sanitizes location errors without raw detail", async () => {
    const raw = "raw-secret-path-/private-xyz-9f3a";
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "open_download_location") throw new Error(`DownloadNotCompleted ${raw}`);
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      const ok = await c.openLocation("dl-q1");
      assert.equal(ok, false);
      assert.ok(typeof c.state.locationError === "string" && c.state.locationError.length > 0);
      assert.ok(!c.state.locationError.includes(raw), "must not echo raw detail");
      assert.ok(!JSON.stringify(c.state).includes(raw), "state must not leak raw detail");
    } finally {
      c.dispose();
    }
  });

  it("stale open failure after reconnect never clears newer same-id open", async () => {
    let rejectOld = null;
    let resolveNew = null;
    let openCount = 0;
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "disconnect_surge") return { ok: true };
      if (command === "open_download_location") {
        openCount += 1;
        if (openCount === 1) {
          return new Promise((_, reject) => {
            rejectOld = () => reject("Superseded");
          });
        }
        return new Promise((resolve) => {
          resolveNew = () => resolve(undefined);
        });
      }
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let first = null;
    let second = null;
    try {
      await c.startManaged();
      first = c.openLocation("id1");
      for (let i = 0; i < 3; i++) await Promise.resolve();
      assert.equal(c.state.locationOpeningId, "id1");
      await c.disconnect();
      await c.startManaged();
      second = c.openLocation("id1");
      for (let i = 0; i < 3; i++) await Promise.resolve();
      assert.equal(c.state.locationOpeningId, "id1");
      rejectOld();
      assert.equal(await first, false);
      first = null;
      assert.equal(c.state.locationOpeningId, "id1", "new open must stay busy after stale Superseded");
      assert.equal(c.state.locationError, null, "stale Superseded must not set old error");
      assert.equal(c.state.locationErrorId, null);
      resolveNew();
      assert.equal(await second, true);
      second = null;
      assert.equal(c.state.locationOpeningId, null);
      assert.equal(c.state.locationError, null);
    } finally {
      try {
        if (first && rejectOld) rejectOld();
      } catch {}
      try {
        if (second && resolveNew) resolveNew();
      } catch {}
      try {
        if (first) await first;
      } catch {}
      try {
        if (second) await second;
      } catch {}
      c.dispose();
    }
  });
});

describe("rate auto-load gate (one per epoch, explicit retry)", () => {
  it("claims once per epoch, reconnect allows one more, no auto loop", async () => {
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "disconnect_surge") return { ok: true };
      if (command === "get_global_rate_limit") return 1_000_000;
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      // Offline: no claim.
      assert.equal(c.tryClaimRateAutoLoad(), false);
      await c.startManaged();
      assert.equal(c.tryClaimRateAutoLoad(), true, "first auto attempt allowed");
      assert.equal(c.tryClaimRateAutoLoad(), false, "second auto attempt blocked (no loop)");
      assert.equal(c.tryClaimRateAutoLoad(), false, "third still blocked");
      // Manual loadRate still works after auto claim (explicit retry path).
      await c.loadRate();
      assert.equal(c.state.rateLimitBps, 1_000_000);
      // Failed auto-load must not re-claim in same epoch.
      assert.equal(c.tryClaimRateAutoLoad(), false);
      // Reconnect allows exactly one new attempt.
      await c.disconnect();
      await c.startManaged();
      assert.equal(c.tryClaimRateAutoLoad(), true, "reconnect allows one attempt");
      assert.equal(c.tryClaimRateAutoLoad(), false, "no loop after reconnect");
    } finally {
      c.dispose();
    }
  });

  it("poison blocks auto claim and manual retry never sends IPC", async () => {
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "disconnect_surge") return { ok: true };
      if (command === "set_global_rate_limit") throw "RateUnconfirmed";
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      assert.equal(c.tryClaimRateAutoLoad(), true);
      const ok = await c.setRateLimit(1_000_000);
      assert.equal(ok, false);
      assert.equal(c.state.rateUnconfirmed, true);
      assert.equal(c.tryClaimRateAutoLoad(), false, "poison blocks auto");
      const before = calls.filter((x) => x.command === "set_global_rate_limit").length;
      await c.loadRate();
      assert.equal(calls.filter((x) => x.command === "set_global_rate_limit").length, before);
    } finally {
      c.dispose();
    }
  });
});

describe("stale write RateUnconfirmed poisons across reconnect", () => {
  it("delayed uncertain response after reconnect still poisons and blocks writes", async () => {
    let releaseWrite;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "connect_surge") return [];
      if (command === "disconnect_surge") return { ok: true };
      if (command === "set_global_rate_limit") {
        return new Promise((_, reject) => {
          releaseWrite = () => reject("RateUnconfirmed");
        });
      }
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pending = null;
    try {
      await c.startManaged();
      pending = c.setRateLimit(1_000_000);
      for (let i = 0; i < 3; i++) await Promise.resolve();
      // Reconnect while the write is still in flight (epoch bump).
      await c.disconnect();
      await c.startManaged();
      assert.equal(c.state.rateUnconfirmed !== true, true, "no poison yet before stale settles");
      releaseWrite();
      assert.equal(await pending, false);
      pending = null;
      assert.equal(c.state.rateUnconfirmed, true, "stale uncertain outcome must poison");
      assert.ok(
        (c.state.rateError ?? "").toLowerCase().includes("could not be confirmed"),
        "must show unconfirmed status",
      );
      const before = calls.filter((x) => x.command === "set_global_rate_limit").length;
      const again = await c.setRateLimit(2_000_000);
      assert.equal(again, false);
      assert.equal(
        calls.filter((x) => x.command === "set_global_rate_limit").length,
        before,
        "poisoned writer must not send further IPC",
      );
      // Stale success must not overwrite new state: rateLimit stays null after poison.
      assert.equal(c.state.rateLimitBps, null);
    } finally {
      try {
        if (pending && releaseWrite) releaseWrite();
      } catch {}
      try {
        if (pending) await pending;
      } catch {}
      c.dispose();
    }
  });
});

describe("location error origin scoping", () => {
  it("pending A, A fails -> origin A, no error on B, no raw details", async () => {
    const raw = "secret-path-/private-xyz-9f3a";
    let releaseOpen;
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") {
        return [
          { id: "dl-a", name: "a.iso", status: "completed", bytesDone: 10, bytesTotal: 10, speedBps: 0, addedAt: 1 },
          { id: "dl-b", name: "b.iso", status: "completed", bytesDone: 10, bytesTotal: 10, speedBps: 0, addedAt: 2 },
        ];
      }
      if (command === "open_download_location") {
        return new Promise((_, reject) => {
          releaseOpen = () => reject(new Error(`DownloadNotCompleted ${raw}`));
        });
      }
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pending = null;
    try {
      await c.startManaged();
      pending = c.openLocation("dl-a");
      for (let i = 0; i < 3; i++) await Promise.resolve();
      // UI selects B mid-flight: controller has no selection, but origin must stay A.
      releaseOpen();
      assert.equal(await pending, false);
      pending = null;
      assert.ok(typeof c.state.locationError === "string" && c.state.locationError.length > 0);
      assert.equal(c.state.locationErrorId, "dl-a", "error must carry origin id");
      assert.ok(!c.state.locationError.includes(raw), "must not echo raw detail");
      assert.ok(!JSON.stringify(c.state).includes(raw), "state must not leak raw detail");
      // UI render rule: only when current selection equals origin.
      const visibleOnB = !!c.state.locationError && (c.state.locationErrorId == null || c.state.locationErrorId === "dl-b");
      const visibleOnA = !!c.state.locationError && (c.state.locationErrorId == null || c.state.locationErrorId === "dl-a");
      assert.equal(visibleOnB, false, "no error on B");
      assert.equal(visibleOnA, true, "error visible on origin A");
      assert.equal(c.locationErrorOrigin(), "dl-a");
    } finally {
      try {
        if (pending && releaseOpen) releaseOpen();
      } catch {}
      try {
        if (pending) await pending;
      } catch {}
      c.dispose();
    }
  });
});

describe("rate validation messages (component-state behavior)", () => {
  it("invalid drafts map to stable messages without echoing raw input", () => {
    const cases = [
      ["", "empty"],
      ["abc", "invalid"],
      ["-1", "negative"],
      ["1.0000000", "too-precise"],
      ["99999999999", "overflow"],
    ];
    for (const [draft, reason] of cases) {
      const parsed = parseRateMbToBps(draft);
      assert.equal(parsed.ok, false, `expected invalid for ${JSON.stringify(draft)}`);
      assert.equal(parsed.reason, reason);
      const msg = rateInputErrorMessage(parsed.reason);
      assert.ok(typeof msg === "string" && msg.length > 0);
      assert.ok(!msg.includes(draft) || draft === "", "message must not echo raw input");
    }
    // Valid drafts produce no message (input valid -> aria-invalid false).
    for (const draft of ["0", "2.5", "1.5"]) {
      const parsed = parseRateMbToBps(draft);
      assert.equal(parsed.ok, true, `expected ok for ${draft}`);
    }
    // Each reason has a distinct stable message (help/error ID contract).
    const messages = new Set(["empty", "invalid", "negative", "too-precise", "below-minimum", "overflow"].map((r) => rateInputErrorMessage(r)));
    assert.ok(messages.size >= 5, "messages must be stable and distinct per reason");
  });
});
