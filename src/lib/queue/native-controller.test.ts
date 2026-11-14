// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Tests for the native Surge queue controller. Uses a fake invoke bridge;
// no daemon, no real token.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { NativeQueueController } from "./native-controller.ts";

const TEST_ENDPOINT = "http://127.0.0.1:9";
const TEST_TOKEN = "test-only-token-red-001";

function makeInvoke(handler) {
  const calls = [];
  const invoke = async (command, args) => {
    calls.push({ command, args });
    return handler(command, args);
  };
  return { invoke, calls };
}

describe("NativeQueueController", () => {
  it("connect sanitizes camelCase row, calls connect_surge, never leaks token into state", async () => {
    const row = {
      id: "dl-1",
      name: "surge-macos-aarch64.tar.gz",
      status: "active",
      bytesDone: 500,
      bytesTotal: 1000,
      speedBps: 100,
      addedAt: 1,
    };
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [row];
      if (command === "list_downloads") throw new Error("list_downloads should not be required during connect");
      throw new Error(`unexpected command: ${command}`);
    });
    const seen = [];
    const c = new NativeQueueController(invoke, (s) => seen.push(s), 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.phase, "connected");
      assert.equal(c.state.items.length, 1);
      assert.equal(c.state.items[0].id, "dl-1");
      assert.equal(c.state.items[0].bytesDone, 500);
      const connectCall = calls.find((x) => x.command === "connect_surge");
      assert.ok(connectCall, "expected connect_surge call");
      assert.equal(connectCall.args.endpoint, TEST_ENDPOINT);
      assert.equal(connectCall.args.token, TEST_TOKEN);
      assert.ok(!JSON.stringify(c.state).includes(TEST_TOKEN), "state must not leak token");
    } finally {
      c.dispose();
    }
  });

  it("stale list_downloads after disconnect stays offline with empty rows", async () => {
    let releaseList;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [];
      if (command === "disconnect_surge") return { ok: true };
      if (command === "list_downloads") {
        return new Promise((resolve) => {
          releaseList = () => resolve([{ id: "stale-1", name: "stale", status: "queued", bytesDone: 0, bytesTotal: 10, speedBps: 0, addedAt: 2 }]);
        });
      }
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.phase, "connected");
      const pending = c.refresh();
      await c.disconnect();
      assert.equal(c.state.phase, "offline");
      releaseList();
      await pending;
      assert.equal(c.state.phase, "offline");
      assert.deepEqual(c.state.items, []);
      assert.ok(calls.some((x) => x.command === "disconnect_surge"), "expected disconnect_surge call");
    } finally {
      c.dispose();
    }
  });

  it("unauthorized BridgeError maps to safe error without token or raw response", async () => {
    const { invoke } = makeInvoke(() => {
      // Rust BridgeError::Unauthorized serializes camelCase as exact string `unauthorized`.
      throw "unauthorized";
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.phase, "error");
      assert.ok(typeof c.state.error === "string" && c.state.error.length > 0, "expected nonempty safe error");
      assert.ok(!c.state.error.includes(TEST_TOKEN), "error must not echo token");
      assert.ok(!JSON.stringify(c.state).includes(TEST_TOKEN), "state must not leak token");
    } finally {
      c.dispose();
    }
  });

  it("unexpected raw rejection never echoes raw detail", async () => {
    const rawDetail = "raw-detail-7f3a-server-says-bad-signature";
    const { invoke } = makeInvoke(() => {
      throw new Error(`transport boom ${rawDetail}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.phase, "error");
      assert.ok(typeof c.state.error === "string" && c.state.error.length > 0, "expected nonempty safe error");
      assert.ok(!c.state.error.includes(TEST_TOKEN), "error must not echo token");
      assert.ok(!c.state.error.includes(rawDetail), "error must not echo raw server response");
      assert.ok(!JSON.stringify(c.state).includes(rawDetail), "state must not echo raw detail");
      assert.ok(!JSON.stringify(c.state).includes(TEST_TOKEN), "state must not leak token");
    } finally {
      c.dispose();
    }
  });

  it("second connect clears stale rows while connecting to a new server", async () => {
    const row = {
      id: "dl-1",
      name: "surge-macos-aarch64.tar.gz",
      status: "active",
      bytesDone: 500,
      bytesTotal: 1000,
      speedBps: 100,
      addedAt: 1,
    };
    let releaseSecond;
    let connectCount = 0;
    const { invoke } = makeInvoke((command) => {
      if (command === "connect_surge") {
        connectCount += 1;
        if (connectCount === 1) return [row];
        return new Promise((resolve) => {
          releaseSecond = () => resolve([]);
        });
      }
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let second = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.items.length, 1);
      second = c.connect(TEST_ENDPOINT, TEST_TOKEN);
      try {
        assert.equal(c.state.phase, "connecting");
        assert.deepEqual(c.state.items, [], "old daemon rows must not appear while connecting to a new server");
      } finally {
        releaseSecond();
        await second;
        second = null;
      }
    } finally {
      try {
        if (second) {
          releaseSecond();
          await second;
        }
      } catch {}
      c.dispose();
    }
  });

  it("dispose issues best-effort disconnect_surge and suppresses late connect emit", async () => {
    let releaseConnect;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") {
        return new Promise((resolve) => {
          releaseConnect = () => resolve([]);
        });
      }
      if (command === "disconnect_surge") return { ok: true };
      throw new Error(`unexpected command: ${command}`);
    });
    const seen = [];
    const c = new NativeQueueController(invoke, (s) => seen.push(s), 60_000);
    let pending = null;
    try {
      pending = c.connect(TEST_ENDPOINT, TEST_TOKEN);
      c.dispose();
      assert.ok(calls.some((x) => x.command === "disconnect_surge"), "expected best-effort disconnect_surge on dispose");
      const emittedAfterDispose = seen.length;
      releaseConnect();
      await pending;
      pending = null;
      assert.equal(seen.length, emittedAfterDispose, "no onChange after late connect resolves post-dispose");
    } finally {
      try {
        if (pending && releaseConnect) releaseConnect();
      } catch {}
      try {
        if (pending) await pending;
      } catch {}
      try {
        c.dispose();
      } catch {}
    }
  });

  it("control pause defers ack, keeps active row, then adopts paused row via list_downloads", async () => {
    const activeRow = {
      id: "dl-1",
      name: "surge-macos-aarch64.tar.gz",
      status: "active",
      bytesDone: 500,
      bytesTotal: 1000,
      speedBps: 100,
      addedAt: 1,
    };
    const pausedRow = {
      id: "dl-1",
      name: "surge-macos-aarch64.tar.gz",
      status: "paused",
      bytesDone: 500,
      bytesTotal: 1000,
      speedBps: 0,
      addedAt: 1,
    };
    let releaseControl;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow];
      if (command === "control_download") {
        return new Promise((resolve) => {
          releaseControl = () => resolve({ ok: true });
        });
      }
      if (command === "list_downloads") return [pausedRow];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pendingControl = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.items[0].status, "active");
      pendingControl = c.control("pause", "dl-1");
      await new Promise((r) => setTimeout(r, 0));
      const controlCall = calls.find((x) => x.command === "control_download");
      assert.ok(controlCall, "expected control_download call");
      assert.deepEqual(controlCall.args, { action: "pause", id: "dl-1" });
      assert.equal(c.state.items[0].status, "active", "no optimistic fake status while ack deferred");
      assert.equal(c.state.controllingId, "dl-1", "busy state must expose in-flight control id while ack deferred");
      releaseControl();
      await pendingControl;
      pendingControl = null;
      assert.ok(calls.some((x) => x.command === "list_downloads"), "expected list_downloads re-query after ack");
      assert.equal(c.state.items.length, 1);
      assert.equal(c.state.items[0].id, "dl-1");
      assert.equal(c.state.items[0].status, "paused");
      assert.equal(c.state.controllingId, null, "busy state must clear after ack + re-query");
    } finally {
      try {
        if (pendingControl && releaseControl) releaseControl();
      } catch {}
      try {
        if (pendingControl) await pendingControl;
      } catch {}
      c.dispose();
    }
  });

  it("stale A control ack must not block B control after reconnect", async () => {
    const rowA = {
      id: "dl-A",
      name: "a.bin",
      status: "active",
      bytesDone: 10,
      bytesTotal: 100,
      speedBps: 5,
      addedAt: 1,
    };
    const rowB = {
      id: "dl-B",
      name: "b.bin",
      status: "active",
      bytesDone: 20,
      bytesTotal: 200,
      speedBps: 7,
      addedAt: 2,
    };
    let connectCount = 0;
    const controlReleases = [];
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") {
        connectCount += 1;
        return connectCount === 1 ? [rowA] : [rowB];
      }
      if (command === "disconnect_surge") return { ok: true };
      if (command === "control_download") {
        return new Promise((resolve) => {
          controlReleases.push(() => resolve({ ok: true }));
        });
      }
      if (command === "list_downloads") return [rowB];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pendingA = null;
    let pendingB = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.items[0].id, "dl-A");
      pendingA = c.control("pause", "dl-A");
      assert.equal(calls.filter((x) => x.command === "control_download").length, 1, "expected first A control_download");
      await c.disconnect();
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.phase, "connected");
      assert.equal(c.state.items.length, 1);
      assert.equal(c.state.items[0].id, "dl-B");
      pendingB = c.control("pause", "dl-B");
      assert.equal(calls.filter((x) => x.command === "control_download").length, 2, "old A ack must not block B control");
      assert.deepEqual(calls.filter((x) => x.command === "control_download")[1].args, { action: "pause", id: "dl-B" });
      while (controlReleases.length) controlReleases.shift()();
      await pendingA;
      pendingA = null;
      await pendingB;
      pendingB = null;
      assert.equal(c.state.phase, "connected");
      assert.equal(c.state.items.length, 1);
      assert.equal(c.state.items[0].id, "dl-B", "stale A result must not override B");
    } finally {
      try {
        while (controlReleases.length) controlReleases.shift()();
      } catch {}
      try {
        if (pendingA) await pendingA;
      } catch {}
      try {
        if (pendingB) await pendingB;
      } catch {}
      c.dispose();
    }
  });

  it("control ack during in-flight refresh re-queries after stale list resolves", async () => {
    const activeRow = {
      id: "dl-1",
      name: "surge-macos-aarch64.tar.gz",
      status: "active",
      bytesDone: 500,
      bytesTotal: 1000,
      speedBps: 100,
      addedAt: 1,
    };
    const pausedRow = {
      id: "dl-1",
      name: "surge-macos-aarch64.tar.gz",
      status: "paused",
      bytesDone: 500,
      bytesTotal: 1000,
      speedBps: 0,
      addedAt: 1,
    };
    let releaseFirst = null;
    let listCount = 0;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow];
      if (command === "control_download") return { ok: true };
      if (command === "list_downloads") {
        listCount += 1;
        if (listCount === 1) {
          return new Promise((resolve) => {
            releaseFirst = () => resolve([activeRow]);
          });
        }
        return [pausedRow];
      }
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let firstRefresh = null;
    let pendingControl = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.items[0].status, "active");
      firstRefresh = c.refresh();
      await Promise.resolve();
      await Promise.resolve();
      assert.equal(calls.filter((x) => x.command === "list_downloads").length, 1);
      pendingControl = c.control("pause", "dl-1");
      for (let i = 0; i < 5; i++) await Promise.resolve();
      assert.ok(calls.some((x) => x.command === "control_download"), "expected control_download");
      assert.equal(calls.filter((x) => x.command === "list_downloads").length, 1, "control must overlap first refresh");
      releaseFirst();
      await firstRefresh;
      firstRefresh = null;
      assert.equal(c.state.items[0].status, "active", "first stale list keeps active row");
      await pendingControl;
      pendingControl = null;
      assert.equal(calls.filter((x) => x.command === "list_downloads").length, 2, "expected second list_downloads after ack despite in-flight refresh");
      assert.equal(c.state.items[0].status, "paused");
    } finally {
      try {
        if (releaseFirst && firstRefresh) releaseFirst();
      } catch {}
      try {
        if (firstRefresh) await firstRefresh;
      } catch {}
      try {
        if (pendingControl) await pendingControl;
      } catch {}
      c.dispose();
    }
  });

  it("add defers ack, hides URL, then adopts queued row via list_downloads", async () => {
    const fixtureUrl = "https://example.com/benign-download.bin";
    const existingRow = {
      id: "dl-1",
      name: "surge-macos-aarch64.tar.gz",
      status: "active",
      bytesDone: 500,
      bytesTotal: 1000,
      speedBps: 100,
      addedAt: 1,
    };
    const newRow = {
      id: "dl-2",
      name: "benign-download.bin",
      status: "queued",
      bytesDone: 0,
      bytesTotal: 1000,
      speedBps: 0,
      addedAt: 2,
    };
    let releaseAdd;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [existingRow];
      if (command === "add_download") {
        return new Promise((resolve) => {
          releaseAdd = () => resolve("queued");
        });
      }
      if (command === "list_downloads") return [existingRow, newRow];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pendingAdd = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.items.length, 1);
      pendingAdd = c.add(fixtureUrl);
      for (let i = 0; i < 5; i++) await Promise.resolve();
      const addCall = calls.find((x) => x.command === "add_download");
      assert.ok(addCall, "expected add_download call");
      assert.deepEqual(addCall.args, { url: fixtureUrl });
      assert.ok(!JSON.stringify(addCall.args).includes(TEST_TOKEN), "add must not carry bearer");
      assert.ok(!JSON.stringify(addCall.args).includes(TEST_ENDPOINT), "add must not carry endpoint");
      assert.equal(c.state.adding, true);
      assert.equal(c.state.items.length, 1);
      assert.equal(c.state.items[0].id, "dl-1");
      assert.ok(!JSON.stringify(c.state).includes(fixtureUrl), "state must not leak fixture URL while ack deferred");
      assert.ok(!JSON.stringify(c.state).includes("queued"), "state must not leak signature while ack deferred");
      releaseAdd();
      const ok = await pendingAdd;
      pendingAdd = null;
      assert.equal(ok, true);
      assert.ok(calls.some((x) => x.command === "list_downloads"), "expected list_downloads re-query after ack");
      assert.equal(c.state.items.length, 2);
      assert.equal(c.state.items[1].id, "dl-2");
      assert.equal(c.state.adding, false);
      assert.equal(c.state.addOutcome, "queued");
    } finally {
      try {
        if (pendingAdd && releaseAdd) releaseAdd();
      } catch {}
      try {
        if (pendingAdd) await pendingAdd;
      } catch {}
      c.dispose();
    }
  });

  it("add pendingApproval reports pending outcome without faking a queue row", async () => {
    const fixtureUrl = "https://example.com/benign-pending.bin";
    const existingRow = {
      id: "dl-1",
      name: "surge-macos-aarch64.tar.gz",
      status: "active",
      bytesDone: 500,
      bytesTotal: 1000,
      speedBps: 100,
      addedAt: 1,
    };
    let releaseAdd;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [existingRow];
      if (command === "add_download") {
        return new Promise((resolve) => {
          releaseAdd = () => resolve("pendingApproval");
        });
      }
      if (command === "list_downloads") return [existingRow];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pendingAdd = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.items.length, 1);
      pendingAdd = c.add(fixtureUrl);
      for (let i = 0; i < 5; i++) await Promise.resolve();
      const addCall = calls.find((x) => x.command === "add_download");
      assert.ok(addCall, "expected add_download call");
      assert.deepEqual(addCall.args, { url: fixtureUrl });
      assert.ok(!JSON.stringify(addCall.args).includes(TEST_TOKEN), "add must not carry bearer");
      assert.ok(!JSON.stringify(addCall.args).includes(TEST_ENDPOINT), "add must not carry endpoint");
      assert.equal(c.state.adding, true);
      assert.equal(c.state.items.length, 1);
      assert.equal(c.state.items[0].id, "dl-1");
      assert.ok(!JSON.stringify(c.state).includes(fixtureUrl), "state must not leak fixture URL while ack deferred");
      assert.ok(!JSON.stringify(c.state).includes("pendingApproval"), "state must not leak signature while ack deferred");
      assert.ok(!JSON.stringify(c.state).includes(TEST_TOKEN), "state must not leak token");
      releaseAdd();
      const ok = await pendingAdd;
      pendingAdd = null;
      assert.equal(ok, true);
      assert.equal(c.state.adding, false);
      assert.equal(c.state.addOutcome, "pendingApproval");
      assert.equal(c.state.items.length, 1, "pending approval must not fake a new queue row");
      assert.equal(c.state.items[0].id, "dl-1");
      assert.ok(!JSON.stringify(c.state).includes(TEST_TOKEN), "state must not leak token");
      assert.ok(!JSON.stringify(c.state).includes(TEST_ENDPOINT), "state must not leak endpoint");
    } finally {
      try {
        if (pendingAdd && releaseAdd) releaseAdd();
      } catch {}
      try {
        if (pendingAdd) await pendingAdd;
      } catch {}
      c.dispose();
    }
  });

  it("add duplicate reports duplicate outcome without faking a queued row", async () => {
    const fixtureUrl = "https://example.com/benign-duplicate.bin";
    const existingRow = {
      id: "dl-1",
      name: "surge-macos-aarch64.tar.gz",
      status: "active",
      bytesDone: 500,
      bytesTotal: 1000,
      speedBps: 100,
      addedAt: 1,
    };
    let releaseAdd;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [existingRow];
      if (command === "add_download") {
        return new Promise((resolve) => {
          releaseAdd = () => resolve("duplicate");
        });
      }
      if (command === "list_downloads") return [existingRow];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pendingAdd = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.items.length, 1);
      pendingAdd = c.add(fixtureUrl);
      for (let i = 0; i < 5; i++) await Promise.resolve();
      const addCall = calls.find((x) => x.command === "add_download");
      assert.ok(addCall, "expected add_download call");
      assert.deepEqual(addCall.args, { url: fixtureUrl });
      assert.ok(!JSON.stringify(addCall.args).includes(TEST_TOKEN), "add must not carry bearer");
      assert.ok(!JSON.stringify(addCall.args).includes(TEST_ENDPOINT), "add must not carry endpoint");
      assert.equal(c.state.adding, true);
      assert.equal(c.state.items.length, 1);
      assert.equal(c.state.items[0].id, "dl-1");
      assert.ok(!JSON.stringify(c.state).includes(fixtureUrl), "state must not leak fixture URL while ack deferred");
      assert.ok(!JSON.stringify(c.state).includes("duplicate"), "state must not leak signature while ack deferred");
      assert.ok(!JSON.stringify(c.state).includes(TEST_TOKEN), "state must not leak token");
      releaseAdd();
      const ok = await pendingAdd;
      pendingAdd = null;
      assert.equal(ok, true);
      assert.equal(c.state.adding, false);
      assert.equal(c.state.addOutcome, "duplicate");
      assert.equal(c.state.items.length, 1, "duplicate must not fake a new queued row");
      assert.equal(c.state.items[0].id, "dl-1");
      assert.ok(!JSON.stringify(c.state).includes(TEST_TOKEN), "state must not leak token");
      assert.ok(!JSON.stringify(c.state).includes(TEST_ENDPOINT), "state must not leak endpoint");
    } finally {
      try {
        if (pendingAdd && releaseAdd) releaseAdd();
      } catch {}
      try {
        if (pendingAdd) await pendingAdd;
      } catch {}
      c.dispose();
    }
  });

  it("stale A add ack after reconnect does not mutate B or block a new B add", async () => {
    const urlA = "https://example.com/benign-a.bin";
    const urlB = "https://example.com/benign-b.bin";
    const rowA = {
      id: "dl-A",
      name: "a.bin",
      status: "active",
      bytesDone: 10,
      bytesTotal: 100,
      speedBps: 5,
      addedAt: 1,
    };
    const rowB = {
      id: "dl-B",
      name: "b.bin",
      status: "active",
      bytesDone: 20,
      bytesTotal: 200,
      speedBps: 7,
      addedAt: 2,
    };
    const newRowB = {
      id: "dl-B2",
      name: "benign-b.bin",
      status: "queued",
      bytesDone: 0,
      bytesTotal: 200,
      speedBps: 0,
      addedAt: 3,
    };
    let connectCount = 0;
    const addReleases = [];
    let listCalls = 0;
    let bAcked = false;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") {
        connectCount += 1;
        return connectCount === 1 ? [rowA] : [rowB];
      }
      if (command === "disconnect_surge") return { ok: true };
      if (command === "add_download") {
        return new Promise((resolve) => {
          addReleases.push(() => resolve("queued"));
        });
      }
      if (command === "list_downloads") {
        listCalls += 1;
        return bAcked ? [rowB, newRowB] : [rowB];
      }
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pendingA = null;
    let pendingB = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.items[0].id, "dl-A");
      pendingA = c.add(urlA);
      for (let i = 0; i < 5; i++) await Promise.resolve();
      assert.equal(calls.filter((x) => x.command === "add_download").length, 1, "expected first A add_download");
      assert.deepEqual(calls.filter((x) => x.command === "add_download")[0].args, { url: urlA });
      await c.disconnect();
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.phase, "connected");
      assert.equal(c.state.items.length, 1);
      assert.equal(c.state.items[0].id, "dl-B");
      const listBeforeStale = listCalls;
      const itemsBeforeStale = JSON.stringify(c.state.items);
      addReleases.shift()();
      await pendingA;
      pendingA = null;
      assert.equal(c.state.phase, "connected");
      assert.equal(c.state.items.length, 1);
      assert.equal(c.state.items[0].id, "dl-B", "stale A result must not override B");
      assert.equal(JSON.stringify(c.state.items), itemsBeforeStale, "stale A must not mutate B rows");
      assert.equal(listCalls, listBeforeStale, "stale A ack must not launch B queue read");
      assert.equal(c.state.adding, false, "stale A must not leave B adding");
      pendingB = c.add(urlB);
      for (let i = 0; i < 5; i++) await Promise.resolve();
      assert.equal(calls.filter((x) => x.command === "add_download").length, 2, "old A ack must not block B add");
      assert.deepEqual(calls.filter((x) => x.command === "add_download")[1].args, { url: urlB });
      bAcked = true;
      addReleases.shift()();
      const okB = await pendingB;
      pendingB = null;
      assert.equal(okB, true);
      assert.equal(c.state.adding, false);
      assert.equal(c.state.addOutcome, "queued");
      assert.equal(c.state.items.length, 2);
      assert.equal(c.state.items[1].id, "dl-B2");
      assert.ok(!JSON.stringify(c.state).includes(TEST_TOKEN), "state must not leak token");
    } finally {
      try {
        while (addReleases.length) addReleases.shift()();
      } catch {}
      try {
        if (pendingA) await pendingA;
      } catch {}
      try {
        if (pendingB) await pendingB;
      } catch {}
      c.dispose();
    }
  });

  it("same-epoch duplicate add() issues exactly one add_download and second reports false", async () => {
    const urlA = "https://example.com/benign-first.bin";
    const urlB = "https://example.com/benign-second.bin";
    const existingRow = {
      id: "dl-1",
      name: "surge-macos-aarch64.tar.gz",
      status: "active",
      bytesDone: 500,
      bytesTotal: 1000,
      speedBps: 100,
      addedAt: 1,
    };
    const newRow = {
      id: "dl-2",
      name: "benign-first.bin",
      status: "queued",
      bytesDone: 0,
      bytesTotal: 1000,
      speedBps: 0,
      addedAt: 2,
    };
    let releaseAdd = null;
    let addCount = 0;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [existingRow];
      if (command === "add_download") {
        addCount += 1;
        if (addCount === 1) {
          return new Promise((resolve) => {
            releaseAdd = () => resolve("queued");
          });
        }
        return "queued";
      }
      if (command === "list_downloads") return [existingRow, newRow];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pendingFirst = null;
    let pendingSecond = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.phase, "connected");
      assert.equal(c.state.items.length, 1);
      pendingFirst = c.add(urlA);
      for (let i = 0; i < 5; i++) await Promise.resolve();
      const addCalls = calls.filter((x) => x.command === "add_download");
      assert.equal(addCalls.length, 1, "expected first add_download call");
      assert.deepEqual(addCalls[0].args, { url: urlA });
      pendingSecond = c.add(urlB);
      for (let i = 0; i < 5; i++) await Promise.resolve();
      assert.equal(calls.filter((x) => x.command === "add_download").length, 1, "same-epoch duplicate add() must not issue a second add_download IPC");
      const secondResult = await pendingSecond;
      pendingSecond = null;
      assert.equal(secondResult, false, "second same-epoch add() must report false");
      assert.equal(c.state.adding, true, "first same-epoch add() must stay in flight");
      assert.ok(!JSON.stringify(c.state).includes(urlA), "state must not leak first URL");
      assert.ok(!JSON.stringify(c.state).includes(urlB), "state must not leak second URL");
      assert.ok(!JSON.stringify(c.state).includes(TEST_TOKEN), "state must not leak token");
      releaseAdd();
      const firstResult = await pendingFirst;
      pendingFirst = null;
      assert.equal(firstResult, true, "first same-epoch add() must report true");
      assert.ok(calls.some((x) => x.command === "list_downloads"), "expected list_downloads re-query after ack");
      assert.equal(c.state.adding, false, "adding must clear once the first add settles");
      assert.equal(c.state.items.length, 2);
      assert.equal(c.state.items[0].id, "dl-1");
      assert.equal(c.state.items[1].id, "dl-2");
      assert.ok(!JSON.stringify(c.state).includes(urlA), "state must not leak first URL");
      assert.ok(!JSON.stringify(c.state).includes(urlB), "state must not leak second URL");
      assert.ok(!JSON.stringify(c.state).includes(TEST_TOKEN), "state must not leak token");
    } finally {
      try {
        if (pendingFirst && releaseAdd) releaseAdd();
      } catch {}
      try {
        if (pendingSecond && releaseAdd) releaseAdd();
      } catch {}
      try {
        if (pendingFirst) await pendingFirst;
      } catch {}
      try {
        if (pendingSecond) await pendingSecond;
      } catch {}
      c.dispose();
    }
  });
});

describe("NativeQueueController managed start", () => {
  it("startManaged success calls with no args and sanitizes rows", async () => {
    const rawRow = {
      id: "dl-m1",
      name: "managed.iso",
      status: "active",
      bytesDone: 200,
      bytesTotal: 1000,
      speedBps: 50,
      addedAt: 7,
      url: "https://example.com/secret-managed.iso",
      dest_path: "/secret/path",
      error: "raw-server-text",
    };
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [rawRow];
      throw new Error(`unexpected command: ${command}`);
    });
    const seen = [];
    const c = new NativeQueueController(invoke, (s) => seen.push(s), 60_000);
    try {
      const ok = await c.startManaged();
      assert.equal(ok, true);
      assert.equal(c.state.phase, "connected");
      assert.equal(c.state.items.length, 1);
      assert.equal(c.state.items[0].id, "dl-m1");
      assert.equal(c.state.items[0].bytesDone, 200);
      // Sanitized DTO drops server-only fields.
      assert.ok(!("url" in c.state.items[0]), "managed row must not carry url");
      const startCall = calls.find((x) => x.command === "start_managed_surge");
      assert.ok(startCall, "expected start_managed_surge call");
      assert.ok(startCall.args === undefined, "managed start takes no args");
      assert.ok(!JSON.stringify(c.state).includes("secret-managed"), "state must not leak URL");
      assert.ok(!JSON.stringify(c.state).includes("/secret/path"), "state must not leak path");
      assert.ok(!JSON.stringify(c.state).includes("raw-server-text"), "state must not leak raw text");
    } finally {
      c.dispose();
    }
  });

  it("startManaged failure maps to sanitized fallback and retry succeeds", async () => {
    const rawDetail = "raw-managed-boom-/secret/xyz-9f3a";
    let attempts = 0;
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") {
        attempts += 1;
        if (attempts === 1) throw new Error(`startup failed ${rawDetail}`);
        return [];
      }
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      const first = await c.startManaged();
      assert.equal(first, false);
      assert.equal(c.state.phase, "error");
      assert.ok(typeof c.state.error === "string" && c.state.error.length > 0);
      assert.equal(c.state.error, "Could not start the bundled daemon. Try again.");
      assert.ok(!c.state.error.includes(rawDetail), "error must not echo raw detail");
      assert.ok(!JSON.stringify(c.state).includes(rawDetail), "state must not echo raw detail");
      const second = await c.startManaged();
      assert.equal(second, true);
      assert.equal(c.state.phase, "connected");
      assert.equal(c.state.error, null);
      assert.deepEqual(c.state.items, []);
    } finally {
      c.dispose();
    }
  });

  it("startManaged maps fixed camelCase rejection codes to exact Code suffix and sanitizes unknown secret", async () => {
    const fixedCodes = ["resource", "paths", "identity", "process", "readiness", "invalidInput", "unsupportedPlatform"];
    for (const code of fixedCodes) {
      const { invoke } = makeInvoke((command) => {
        if (command === "start_managed_surge") throw code;
        throw new Error(`unexpected command: ${command}`);
      });
      const c = new NativeQueueController(invoke, () => {}, 60_000);
      try {
        const ok = await c.startManaged();
        assert.equal(ok, false);
        assert.equal(c.state.phase, "error");
        assert.equal(c.state.error, `Could not start the bundled daemon. Code: ${code}.`);
      } finally {
        c.dispose();
      }
    }
    const rawSecret = "raw-managed-secret-unknown-7f3a-/secret/xyz-9f3a";
    const { invoke: unknownInvoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") throw rawSecret;
      throw new Error(`unexpected command: ${command}`);
    });
    const cu = new NativeQueueController(unknownInvoke, () => {}, 60_000);
    try {
      const ok = await cu.startManaged();
      assert.equal(ok, false);
      assert.equal(cu.state.phase, "error");
      assert.equal(cu.state.error, "Could not start the bundled daemon. Try again.");
      assert.ok(!cu.state.error.includes(rawSecret), "error must not echo raw secret");
      assert.ok(!JSON.stringify(cu.state).includes(rawSecret), "state must not echo raw secret");
    } finally {
      cu.dispose();
    }
  });

  it("same-epoch duplicate startManaged issues exactly one IPC and second reports false", async () => {
    let releaseStart = null;
    let startCount = 0;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "start_managed_surge") {
        startCount += 1;
        return new Promise((resolve) => {
          releaseStart = () => resolve([]);
        });
      }
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let first = null;
    let secondResult = null;
    try {
      first = c.startManaged();
      for (let i = 0; i < 5; i++) await Promise.resolve();
      assert.equal(c.state.phase, "connecting");
      assert.equal(calls.filter((x) => x.command === "start_managed_surge").length, 1);
      secondResult = await c.startManaged();
      assert.equal(secondResult, false, "duplicate start must report false");
      for (let i = 0; i < 5; i++) await Promise.resolve();
      assert.equal(calls.filter((x) => x.command === "start_managed_surge").length, 1, "duplicate must not issue second IPC");
      assert.equal(c.state.phase, "connecting", "duplicate must not disturb in-flight start");
      releaseStart();
      const firstResult = await first;
      first = null;
      assert.equal(firstResult, true);
      assert.equal(c.state.phase, "connected");
      assert.equal(startCount, 1);
    } finally {
      try {
        if (first && releaseStart) releaseStart();
      } catch {}
      try {
        if (first) await first;
      } catch {}
      c.dispose();
    }
  });

  it("stale startManaged after disconnect stays offline with empty rows", async () => {
    let releaseStart = null;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "start_managed_surge") {
        return new Promise((resolve) => {
          releaseStart = () => resolve([{ id: "stale-m", name: "stale", status: "queued", bytesDone: 0, bytesTotal: 10, speedBps: 0, addedAt: 3 }]);
        });
      }
      if (command === "disconnect_surge") return { ok: true };
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pending = null;
    try {
      pending = c.startManaged();
      for (let i = 0; i < 5; i++) await Promise.resolve();
      assert.equal(c.state.phase, "connecting");
      await c.disconnect();
      assert.equal(c.state.phase, "offline");
      releaseStart();
      const ok = await pending;
      pending = null;
      assert.equal(ok, false);
      assert.equal(c.state.phase, "offline");
      assert.deepEqual(c.state.items, []);
      assert.ok(calls.some((x) => x.command === "disconnect_surge"), "expected disconnect_surge call");
    } finally {
      try {
        if (pending && releaseStart) releaseStart();
      } catch {}
      try {
        if (pending) await pending;
      } catch {}
      c.dispose();
    }
  });

  it("dispose suppresses late startManaged emit", async () => {
    let releaseStart = null;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "start_managed_surge") {
        return new Promise((resolve) => {
          releaseStart = () => resolve([]);
        });
      }
      if (command === "disconnect_surge") return { ok: true };
      throw new Error(`unexpected command: ${command}`);
    });
    const seen = [];
    const c = new NativeQueueController(invoke, (s) => seen.push(s), 60_000);
    let pending = null;
    try {
      pending = c.startManaged();
      for (let i = 0; i < 3; i++) await Promise.resolve();
      c.dispose();
      assert.ok(calls.some((x) => x.command === "disconnect_surge"), "expected best-effort disconnect_surge on dispose");
      const emittedAfterDispose = seen.length;
      releaseStart();
      const ok = await pending;
      pending = null;
      assert.equal(ok, false);
      assert.equal(seen.length, emittedAfterDispose, "no onChange after late managed resolve post-dispose");
    } finally {
      try {
        if (pending && releaseStart) releaseStart();
      } catch {}
      try {
        if (pending) await pending;
      } catch {}
      try {
        c.dispose();
      } catch {}
    }
  });

  it("stale managed start discarded when manual connect supersedes it", async () => {
    const managedRow = { id: "dl-managed", name: "m.bin", status: "active", bytesDone: 1, bytesTotal: 10, speedBps: 1, addedAt: 1 };
    const manualRow = { id: "dl-manual", name: "manual.bin", status: "active", bytesDone: 2, bytesTotal: 20, speedBps: 2, addedAt: 2 };
    let releaseManaged = null;
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") {
        return new Promise((resolve) => {
          releaseManaged = () => resolve([managedRow]);
        });
      }
      if (command === "connect_surge") return [manualRow];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pendingManaged = null;
    try {
      pendingManaged = c.startManaged();
      for (let i = 0; i < 5; i++) await Promise.resolve();
      assert.equal(c.state.phase, "connecting");
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.phase, "connected");
      assert.equal(c.state.items[0].id, "dl-manual");
      releaseManaged();
      const ok = await pendingManaged;
      pendingManaged = null;
      assert.equal(ok, false);
      assert.equal(c.state.phase, "connected");
      assert.equal(c.state.items[0].id, "dl-manual", "stale managed result must not override manual");
    } finally {
      try {
        if (pendingManaged && releaseManaged) releaseManaged();
      } catch {}
      try {
        if (pendingManaged) await pendingManaged;
      } catch {}
      c.dispose();
    }
  });

  it("manual connect path unchanged after managed failure", async () => {
    const manualRow = { id: "dl-1", name: "surge-macos-aarch64.tar.gz", status: "active", bytesDone: 500, bytesTotal: 1000, speedBps: 100, addedAt: 1 };
    let managedAttempts = 0;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "start_managed_surge") {
        managedAttempts += 1;
        throw "Startup";
      }
      if (command === "connect_surge") return [manualRow];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      const managedOk = await c.startManaged();
      assert.equal(managedOk, false);
      assert.equal(c.state.phase, "error");
      assert.equal(managedAttempts, 1);
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.phase, "connected");
      assert.equal(c.state.items[0].id, "dl-1");
      const connectCall = calls.find((x) => x.command === "connect_surge");
      assert.ok(connectCall, "expected connect_surge call");
      assert.deepEqual(connectCall.args, { endpoint: TEST_ENDPOINT, token: TEST_TOKEN });
      assert.ok(!JSON.stringify(c.state).includes(TEST_TOKEN), "state must not leak token");
    } finally {
      c.dispose();
    }
  });
});
