// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Bulk Pause all / Resume all controller tests. Fake invoke bridge; no daemon.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { NativeQueueController } from "./native-controller.ts";

const TEST_ENDPOINT = "http://127.0.0.1:9";
const TEST_TOKEN = "test-only-token-red-002";

const activeRow = {
  id: "dl-1",
  name: "alpha.bin",
  status: "active",
  bytesDone: 500,
  bytesTotal: 1000,
  speedBps: 100,
  addedAt: 1,
};

const queuedRow = {
  id: "dl-2",
  name: "beta.bin",
  status: "queued",
  bytesDone: 0,
  bytesTotal: 2000,
  speedBps: 0,
  addedAt: 2,
};

function pausedRows() {
  return [
    { ...activeRow, status: "paused", speedBps: 0 },
    { ...queuedRow, status: "queued" },
  ];
}

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

function flush() {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

describe("NativeQueueController bulk controls", () => {
  it("pause all sends one IPC, shows accepted/failed counts, then reconciles fresh", async () => {
    const bulkBody = {
      status: "done",
      count: 1,
      results: [
        { id: "dl-1", ok: true },
        { id: "dl-2", ok: false, error: "daemon said no" },
      ],
    };
    let listCalls = 0;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow, queuedRow];
      if (command === "bulk_control") return bulkBody;
      if (command === "list_downloads") {
        listCalls += 1;
        return pausedRows();
      }
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      const ok = await c.bulkControl("pauseAll");
      assert.equal(ok, true);
      const bulkCalls = calls.filter((x) => x.command === "bulk_control");
      assert.equal(bulkCalls.length, 1, "bulk must be exactly one IPC");
      assert.deepEqual(bulkCalls[0].args, { action: "pauseAll" });
      assert.equal(listCalls, 1, "a fresh list_downloads must follow the action");
      assert.equal(c.state.bulkAction, null);
      assert.equal(c.state.controllingId, null);
      assert.equal(c.state.bulkSummary.status, "done");
      assert.equal(c.state.bulkSummary.accepted, 1);
      assert.equal(c.state.bulkSummary.failed, 1);
      assert.equal(c.state.bulkSummary.total, 2);
      assert.equal(c.state.bulkSummary.failures.length, 1);
      assert.equal(c.state.bulkSummary.failures[0].id, "dl-2");
      assert.match(c.state.bulkSummary.message, /1 accepted, 1 failed/);
      assert.match(c.state.bulkSummary.message, /asynchronous/i);
      assert.match(c.state.bulkSummary.message, /keeps refreshing periodically/i);
      assert.doesNotMatch(c.state.bulkSummary.message, /after the daemon settles/i);
      assert.equal(c.state.items[0].status, "paused");
    } finally {
      c.dispose();
    }
  });

  it("resume all uses the resume action and reports zero eligible rows", async () => {
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow];
      if (command === "bulk_control") return { status: "done", count: 0, results: [] };
      if (command === "list_downloads") return [activeRow];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      const ok = await c.bulkControl("resumeAll");
      assert.equal(ok, true);
      assert.deepEqual(calls.find((x) => x.command === "bulk_control").args, {
        action: "resumeAll",
      });
      assert.equal(c.state.bulkSummary.status, "done");
      assert.equal(c.state.bulkSummary.accepted, 0);
      assert.equal(c.state.bulkSummary.failed, 0);
      assert.equal(c.state.bulkSummary.total, 0);
      assert.match(c.state.bulkSummary.message, /no downloads were eligible/i);
    } finally {
      c.dispose();
    }
  });

  it("malformed 200 (count inconsistency) is unconfirmed, not rendered as a result", async () => {
    let listCalls = 0;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow];
      if (command === "bulk_control") {
        return { status: "done", count: 2, results: [{ id: "dl-1", ok: true }] };
      }
      if (command === "list_downloads") {
        listCalls += 1;
        return [activeRow];
      }
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      const ok = await c.bulkControl("pauseAll");
      assert.equal(ok, false);
      assert.equal(calls.filter((x) => x.command === "bulk_control").length, 1);
      assert.equal(listCalls, 1, "malformed outcome still reconciles");
      assert.equal(c.state.bulkSummary.status, "unconfirmed");
      assert.equal(c.state.bulkSummary.accepted, null);
      assert.equal(c.state.bulkSummary.failed, null);
      assert.match(c.state.bulkSummary.message, /Not retried automatically/i);
    } finally {
      c.dispose();
    }
  });

  it("duplicate ids and wrong types are unconfirmed", async () => {
    const cases = [
      { status: "done", count: 1, results: [{ id: "a", ok: true }, { id: "a", ok: false, error: "x" }] },
      { status: "done", count: 1, results: [{ id: "a", ok: "yes" }] },
      { status: "done", count: 1, results: [{ id: "a", ok: false }] },
      { status: "done", count: "1", results: [] },
      { status: "error", count: 0, results: [] },
    ];
    for (const body of cases) {
      const { invoke } = makeInvoke((command) => {
        if (command === "connect_surge") return [activeRow];
        if (command === "bulk_control") return body;
        if (command === "list_downloads") return [activeRow];
        throw new Error(`unexpected command: ${command}`);
      });
      const c = new NativeQueueController(invoke, () => {}, 60_000);
      try {
        await c.connect(TEST_ENDPOINT, TEST_TOKEN);
        const ok = await c.bulkControl("pauseAll");
        assert.equal(ok, false, `payload must not be accepted: ${JSON.stringify(body)}`);
        assert.equal(c.state.bulkSummary.status, "unconfirmed");
      } finally {
        c.dispose();
      }
    }
  });

  it("transport rejection is unconfirmed, still reconciles, and is never retried", async () => {
    let listCalls = 0;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow];
      if (command === "bulk_control") throw "network";
      if (command === "list_downloads") {
        listCalls += 1;
        return [activeRow];
      }
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      const ok = await c.bulkControl("pauseAll");
      assert.equal(ok, false);
      assert.equal(calls.filter((x) => x.command === "bulk_control").length, 1, "no retry");
      assert.equal(listCalls, 1, "uncertain outcome still reconciles");
      assert.equal(c.state.bulkSummary.status, "unconfirmed");
      assert.match(c.state.bulkSummary.message, /Not retried automatically/i);
    } finally {
      c.dispose();
    }
  });

  it("stale bulk result after reconnect is fenced out and reconciled on the new connection", async () => {
    let connectCount = 0;
    let releaseBulk;
    let listCalls = 0;
    const rowB = { ...activeRow, id: "dl-B", name: "bravo.bin" };
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") {
        connectCount += 1;
        return connectCount === 1 ? [activeRow] : [rowB];
      }
      if (command === "disconnect_surge") return { ok: true };
      if (command === "bulk_control") {
        return new Promise((resolve) => {
          releaseBulk = () =>
            resolve({
              status: "done",
              count: 1,
              results: [{ id: "dl-1", ok: true }],
            });
        });
      }
      if (command === "list_downloads") {
        listCalls += 1;
        return [rowB];
      }
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pending = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      pending = c.bulkControl("pauseAll");
      await flush();
      assert.equal(c.state.bulkAction, "pauseAll");
      await c.disconnect();
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.items[0].id, "dl-B");
      releaseBulk();
      const ok = await pending;
      pending = null;
      assert.equal(ok, false, "stale outcome must not report success on the new connection");
      assert.equal(c.state.bulkSummary, null, "prior daemon summary must not be attributed");
      assert.equal(c.state.bulkAction, null);
      assert.equal(c.state.bulkUncertainties.length, 1);
      assert.equal(c.state.bulkUncertainties[0].action, "pauseAll");
      assert.match(c.state.bulkUncertainties[0].message, /previous connection/);
      assert.equal(c.state.items[0].id, "dl-B");
      assert.ok(listCalls >= 1, "new connection must still reconcile");
      assert.equal(calls.filter((x) => x.command === "bulk_control").length, 1);
    } finally {
      try {
        if (releaseBulk) releaseBulk();
      } catch {}
      try {
        if (pending) await pending;
      } catch {}
      c.dispose();
    }
  });

  it("single control in flight rejects a bulk request without a second IPC", async () => {
    let releaseControl;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow];
      if (command === "control_download") {
        return new Promise((resolve) => {
          releaseControl = () => resolve({ ok: true });
        });
      }
      if (command === "bulk_control") return { status: "done", count: 0, results: [] };
      if (command === "list_downloads") return [activeRow];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pendingControl = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      pendingControl = c.control("pause", "dl-1");
      await flush();
      const ok = await c.bulkControl("pauseAll");
      assert.equal(ok, false);
      assert.equal(calls.filter((x) => x.command === "bulk_control").length, 0);
      assert.equal(c.state.bulkSummary, null);
      releaseControl();
      await pendingControl;
      pendingControl = null;
    } finally {
      try {
        if (releaseControl) releaseControl();
      } catch {}
      try {
        if (pendingControl) await pendingControl;
      } catch {}
      c.dispose();
    }
  });

  it("bulk in flight rejects single control without a control IPC", async () => {
    let releaseBulk;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow];
      if (command === "bulk_control") {
        return new Promise((resolve) => {
          releaseBulk = () => resolve({ status: "done", count: 1, results: [{ id: "dl-1", ok: true }] });
        });
      }
      if (command === "control_download") return { ok: true };
      if (command === "list_downloads") return [activeRow];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pendingBulk = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      pendingBulk = c.bulkControl("resumeAll");
      await flush();
      assert.equal(c.state.bulkAction, "resumeAll");
      await c.control("pause", "dl-1");
      assert.equal(calls.filter((x) => x.command === "control_download").length, 0);
      releaseBulk();
      await pendingBulk;
      pendingBulk = null;
    } finally {
      try {
        if (releaseBulk) releaseBulk();
      } catch {}
      try {
        if (pendingBulk) await pendingBulk;
      } catch {}
      c.dispose();
    }
  });

  it("duplicate bulk in the same epoch issues exactly one IPC", async () => {
    let releaseBulk;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow];
      if (command === "bulk_control") {
        return new Promise((resolve) => {
          releaseBulk = () => resolve({ status: "done", count: 0, results: [] });
        });
      }
      if (command === "list_downloads") return [activeRow];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let first = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      first = c.bulkControl("pauseAll");
      await flush();
      const second = await c.bulkControl("resumeAll");
      assert.equal(second, false);
      assert.equal(calls.filter((x) => x.command === "bulk_control").length, 1);
      releaseBulk();
      await first;
      first = null;
    } finally {
      try {
        if (releaseBulk) releaseBulk();
      } catch {}
      try {
        if (first) await first;
      } catch {}
      c.dispose();
    }
  });

  it("an in-flight pre-action refresh is not reused as the post-action snapshot", async () => {
    let releaseFirst;
    let listCount = 0;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow];
      if (command === "bulk_control") {
        return { status: "done", count: 1, results: [{ id: "dl-1", ok: true }] };
      }
      if (command === "list_downloads") {
        listCount += 1;
        if (listCount === 1) {
          return new Promise((resolve) => {
            releaseFirst = () => resolve([activeRow]);
          });
        }
        return [{ ...activeRow, status: "paused", speedBps: 0 }];
      }
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let firstRefresh = null;
    let pendingBulk = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      firstRefresh = c.refresh();
      await flush();
      assert.equal(listCount, 1);
      pendingBulk = c.bulkControl("pauseAll");
      await flush();
      assert.equal(listCount, 1, "bulk reconciliation must wait behind the in-flight read");
      releaseFirst();
      await firstRefresh;
      firstRefresh = null;
      assert.equal(c.state.items[0].status, "active", "pre-action snapshot must not be final");
      await pendingBulk;
      pendingBulk = null;
      assert.equal(listCount, 2, "a new post-action list must be issued");
      assert.equal(c.state.items[0].status, "paused");
      assert.equal(c.state.bulkSummary.status, "done");
    } finally {
      try {
        if (releaseFirst && firstRefresh) releaseFirst();
      } catch {}
      try {
        if (firstRefresh) await firstRefresh;
      } catch {}
      try {
        if (pendingBulk) await pendingBulk;
      } catch {}
      c.dispose();
    }
  });

  it("refresh failure preserves the bulk warning and surfaces the refresh error", async () => {
    const { invoke } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow];
      if (command === "bulk_control") {
        return { status: "done", count: 1, results: [{ id: "dl-1", ok: true }] };
      }
      if (command === "list_downloads") throw "network";
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      const ok = await c.bulkControl("pauseAll");
      assert.equal(ok, true);
      assert.equal(c.state.bulkSummary.status, "done", "bulk warning must survive refresh failure");
      assert.equal(c.state.bulkSummary.accepted, 1);
      assert.ok(c.state.error, "refresh failure must surface a safe error");
      assert.match(c.state.error, /Cannot reach the daemon|Refresh failed/i);
    } finally {
      c.dispose();
    }
  });

  it("bulk settling during a reconnect waits for the new connection then reconciles", async () => {
    let connectCount = 0;
    let releaseBulk;
    let releaseConnect;
    let listCalls = 0;
    const rowB = { ...activeRow, id: "dl-B", name: "bravo.bin" };
    const { invoke } = makeInvoke((command) => {
      if (command === "connect_surge") {
        connectCount += 1;
        if (connectCount === 1) return [activeRow];
        return new Promise((resolve) => {
          releaseConnect = () => resolve([rowB]);
        });
      }
      if (command === "disconnect_surge") return { ok: true };
      if (command === "bulk_control") {
        return new Promise((resolve) => {
          releaseBulk = () =>
            resolve({ status: "done", count: 1, results: [{ id: "dl-1", ok: true }] });
        });
      }
      if (command === "list_downloads") {
        listCalls += 1;
        return [rowB];
      }
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pendingBulk = null;
    let pendingConnect = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      pendingBulk = c.bulkControl("pauseAll");
      await flush();
      pendingConnect = c.connect(TEST_ENDPOINT, TEST_TOKEN);
      await flush();
      assert.equal(c.state.phase, "connecting");
      releaseBulk();
      await pendingBulk;
      pendingBulk = null;
      assert.equal(listCalls, 0, "no pre-connect reconciliation may run");
      assert.equal(c.state.bulkSummary, null);
      releaseConnect();
      await pendingConnect;
      pendingConnect = null;
      await flush();
      assert.ok(listCalls >= 1, "the new connection must get one post-action read");
      assert.equal(c.state.phase, "connected");
      assert.equal(c.state.items[0].id, "dl-B");
      assert.equal(c.state.bulkSummary, null, "prior daemon summary must not be attributed");
      assert.equal(c.state.bulkUncertainties[0]?.action, "pauseAll");
    } finally {
      try {
        if (releaseBulk) releaseBulk();
      } catch {}
      try {
        if (releaseConnect) releaseConnect();
      } catch {}
      try {
        if (pendingBulk) await pendingBulk;
      } catch {}
      try {
        if (pendingConnect) await pendingConnect;
      } catch {}
      c.dispose();
    }
  });


  it("keeps the visible busy state until reconciliation settles", async () => {
    let releaseList;
    let listCalls = 0;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow];
      if (command === "bulk_control") {
        return { status: "done", count: 1, results: [{ id: "dl-1", ok: true }] };
      }
      if (command === "list_downloads") {
        listCalls += 1;
        return new Promise((resolve) => {
          releaseList = () => resolve([{ ...activeRow, status: "paused", speedBps: 0 }]);
        });
      }
      if (command === "control_download") return { ok: true };
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pending = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      pending = c.bulkControl("pauseAll");
      await flush();
      assert.equal(listCalls, 1, "reconciliation read must be in flight");
      assert.equal(c.state.bulkAction, "pauseAll", "busy must stay visible during reconciliation");
      assert.equal(c.state.bulkSummary.status, "done");

      const second = await c.bulkControl("resumeAll");
      assert.equal(second, false, "controller must still exclude a second bulk action");
      assert.equal(calls.filter((x) => x.command === "bulk_control").length, 1);
      await c.control("pause", "dl-1");
      assert.equal(calls.filter((x) => x.command === "control_download").length, 0);
      assert.equal(c.state.controllingId, null);

      releaseList();
      await pending;
      pending = null;
      assert.equal(c.state.bulkAction, null, "busy must release with the same operation");
      assert.equal(listCalls, 1);
    } finally {
      try {
        if (releaseList) releaseList();
      } catch {}
      try {
        if (pending) await pending;
      } catch {}
      c.dispose();
    }
  });

  it("disconnect while a bulk request is in flight keeps a scoped notice across reconnect", async () => {
    let releaseBulk;
    let connectCount = 0;
    const rowB = { ...activeRow, id: "dl-B", name: "bravo.bin" };
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") {
        connectCount += 1;
        return connectCount === 1 ? [activeRow] : [rowB];
      }
      if (command === "disconnect_surge") return { ok: true };
      if (command === "bulk_control") {
        return new Promise((resolve) => {
          releaseBulk = () =>
            resolve({ status: "done", count: 1, results: [{ id: "dl-1", ok: true }] });
        });
      }
      if (command === "list_downloads") return [rowB];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pending = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      pending = c.bulkControl("pauseAll");
      await flush();
      assert.equal(c.state.bulkAction, "pauseAll");

      await c.disconnect();
      assert.equal(c.state.bulkAction, null);
      assert.equal(c.state.bulkSummary, null);
      assert.equal(c.state.bulkUncertainties.length, 1);
      assert.equal(c.state.bulkUncertainties[0].action, "pauseAll");
      assert.match(c.state.bulkUncertainties[0].message, /previous connection/i);
      assert.match(c.state.bulkUncertainties[0].message, /may be the same daemon/i);
      assert.match(c.state.bulkUncertainties[0].message, /may still apply/i);
      assert.match(c.state.bulkUncertainties[0].message, /not retried/i);
      assert.doesNotMatch(c.state.bulkUncertainties[0].message, /not the current one/i);

      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.phase, "connected");
      assert.equal(c.state.bulkUncertainties[0]?.action, "pauseAll", "notice must survive reconnect");

      releaseBulk();
      await pending;
      pending = null;
      assert.equal(c.state.bulkSummary, null, "late success must not be attributed to the new daemon");
      assert.equal(c.state.bulkUncertainties.length, 1);
      assert.equal(calls.filter((x) => x.command === "bulk_control").length, 1, "no replay");

      c.dismissBulkUncertainty(c.state.bulkUncertainties[0].id);
      assert.equal(c.state.bulkUncertainties.length, 0);
    } finally {
      try {
        if (releaseBulk) releaseBulk();
      } catch {}
      try {
        if (pending) await pending;
      } catch {}
      c.dispose();
    }
  });

  it("a late outcome cannot clear a newer operation's busy state or summary", async () => {
    let releaseFirst;
    let releaseSecond;
    let bulkCalls = 0;
    let connectCount = 0;
    const rowB = { ...activeRow, id: "dl-B", name: "bravo.bin" };
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") {
        connectCount += 1;
        return connectCount === 1 ? [activeRow] : [rowB];
      }
      if (command === "disconnect_surge") return { ok: true };
      if (command === "bulk_control") {
        bulkCalls += 1;
        if (bulkCalls === 1) {
          return new Promise((resolve) => {
            releaseFirst = () =>
              resolve({ status: "done", count: 1, results: [{ id: "dl-1", ok: true }] });
          });
        }
        return new Promise((resolve) => {
          releaseSecond = () =>
            resolve({ status: "done", count: 1, results: [{ id: "dl-B", ok: true }] });
        });
      }
      if (command === "list_downloads") return [rowB];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let first = null;
    let second = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      first = c.bulkControl("pauseAll");
      await flush();
      await c.disconnect();
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);

      second = c.bulkControl("resumeAll");
      await flush();
      assert.equal(c.state.bulkAction, "resumeAll");

      releaseFirst();
      await first;
      first = null;
      assert.equal(c.state.bulkAction, "resumeAll", "stale completion must not clear replacement busy");
      assert.equal(c.state.bulkSummary, null, "stale completion must not write a summary");

      releaseSecond();
      const ok = await second;
      second = null;
      assert.equal(ok, true);
      assert.equal(c.state.bulkAction, null);
      assert.equal(c.state.bulkSummary?.action, "resumeAll");
      assert.equal(calls.filter((x) => x.command === "bulk_control").length, 2);
    } finally {
      try {
        if (releaseFirst) releaseFirst();
      } catch {}
      try {
        if (releaseSecond) releaseSecond();
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

  it("never forwards a secret-bearing per-item error", async () => {
    const secret = "super-secret-token-xyz";
    const signedUrl = "https://dl.example.test/file.bin?sig=signed-secret-abc";
    const privatePath = "/home/alice/private/downloads/file.bin";
    const { invoke } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow, queuedRow];
      if (command === "bulk_control") {
        return {
          status: "done",
          count: 1,
          results: [
            { id: "dl-1", ok: true },
            { id: "dl-2", ok: false, error: `Bearer ${secret} ${signedUrl} ${privatePath}` },
          ],
        };
      }
      if (command === "list_downloads") return [activeRow, queuedRow];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      await c.bulkControl("pauseAll");
      assert.equal(c.state.bulkSummary.failures.length, 1);
      assert.equal(c.state.bulkSummary.failures[0].id, "dl-2");
      assert.equal(
        c.state.bulkSummary.failures[0].error,
        "Daemon reported an error for this item.",
      );
      const serialized = JSON.stringify(c.state);
      for (const leaked of [secret, signedUrl, privatePath, "Bearer"]) {
        assert.equal(serialized.includes(leaked), false, `state must not contain ${leaked}`);
      }
    } finally {
      c.dispose();
    }
  });

  it("an unconfirmed outcome survives the connection change as a notice, not as a new summary", async () => {
    let releaseList;
    const { invoke } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow];
      if (command === "bulk_control") throw "network";
      if (command === "disconnect_surge") return { ok: true };
      if (command === "list_downloads") {
        return new Promise((resolve) => {
          releaseList = () => resolve([activeRow]);
        });
      }
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pending = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      pending = c.bulkControl("pauseAll");
      await flush();
      assert.equal(c.state.bulkSummary.status, "unconfirmed");
      assert.equal(c.state.bulkAction, "pauseAll", "busy holds during reconciliation");

      await c.disconnect();
      assert.equal(c.state.bulkSummary, null);
      assert.equal(c.state.bulkAction, null);
      assert.equal(c.state.bulkUncertainties[0]?.action, "pauseAll");
      assert.match(c.state.bulkUncertainties[0].message, /not retried/i);

      releaseList();
      await pending;
      pending = null;
      assert.equal(c.state.bulkSummary, null);
      assert.equal(c.state.bulkUncertainties[0]?.action, "pauseAll");
    } finally {
      try {
        if (releaseList) releaseList();
      } catch {}
      try {
        if (pending) await pending;
      } catch {}
      c.dispose();
    }
  });

  it("keeps an unconfirmed outcome after a later bulk success until dismissed", async () => {
    let bulkCalls = 0;
    const { invoke } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow];
      if (command === "bulk_control") {
        bulkCalls += 1;
        if (bulkCalls === 1) throw "network";
        return { status: "done", count: 1, results: [{ id: "dl-1", ok: true }] };
      }
      if (command === "list_downloads") return [activeRow];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(await c.bulkControl("pauseAll"), false);
      assert.equal(c.state.bulkUncertainties.length, 0, "notice is created before the next action");

      assert.equal(await c.bulkControl("resumeAll"), true);
      assert.equal(c.state.bulkSummary?.status, "done");
      assert.equal(c.state.bulkUncertainties.length, 1);
      assert.equal(c.state.bulkUncertainties[0].action, "pauseAll");

      c.dismissBulkUncertainty(c.state.bulkUncertainties[0].id);
      assert.equal(c.state.bulkUncertainties.length, 0);
      assert.equal(c.state.bulkSummary?.status, "done", "dismissal does not touch the summary");
    } finally {
      c.dispose();
    }
  });

  it("retains independent notices across reconnect and shows them while offline", async () => {
    const secret = "server-id-or-secret-xyz";
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow];
      if (command === "disconnect_surge") return { ok: true };
      if (command === "bulk_control") throw `network ${secret}`;
      if (command === "list_downloads") return [activeRow];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      await c.bulkControl("pauseAll");
      await c.disconnect();
      assert.equal(c.state.phase, "offline");
      assert.equal(c.state.bulkUncertainties.length, 1);

      // Same daemon reconnects; a second, different bulk action is also
      // unconfirmed. Neither notice replaces or clears the other.
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      await c.bulkControl("resumeAll");
      await c.disconnect();
      assert.equal(c.state.phase, "offline");
      assert.equal(c.state.bulkUncertainties.length, 2);
      assert.deepEqual(
        c.state.bulkUncertainties.map((notice) => notice.action),
        ["pauseAll", "resumeAll"],
      );
      assert.notEqual(c.state.bulkUncertainties[0].id, c.state.bulkUncertainties[1].id);
      for (const notice of c.state.bulkUncertainties) {
        assert.match(notice.message, /may be the same daemon/i);
        assert.doesNotMatch(notice.message, /current daemon is safe/i);
        assert.doesNotMatch(notice.message, new RegExp(secret), "no server/error text leaks");
      }
      assert.equal(calls.filter((x) => x.command === "bulk_control").length, 2, "no replay");

      const firstId = c.state.bulkUncertainties[0].id;
      c.dismissBulkUncertainty(firstId);
      assert.equal(c.state.bulkUncertainties.length, 1);
      assert.equal(c.state.bulkUncertainties[0].action, "resumeAll");
    } finally {
      c.dispose();
    }
  });

  it("bounds retained notices and conservatively reports overflow", async () => {
    const { invoke } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow];
      if (command === "disconnect_surge") return { ok: true };
      if (command === "bulk_control") throw "network";
      if (command === "list_downloads") return [activeRow];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      for (let index = 0; index < 7; index++) {
        await c.bulkControl(index % 2 === 0 ? "pauseAll" : "resumeAll");
        await c.disconnect();
        await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      }
      assert.equal(c.state.bulkUncertainties.length, 5);
      assert.equal(c.state.bulkUncertaintyOverflow, 2);
      assert.equal(c.state.bulkUncertaintyOverflow > 0, true);

      const retained = c.state.bulkUncertainties.map((notice) => notice.id);
      c.dismissBulkUncertaintyOverflow();
      assert.equal(c.state.bulkUncertaintyOverflow, 0, "overflow acknowledgement is dismissible");
      assert.deepEqual(
        c.state.bulkUncertainties.map((notice) => notice.id),
        retained,
        "individual notices survive overflow acknowledgement",
      );
      c.dismissBulkUncertainty(retained[0]);
      assert.equal(c.state.bulkUncertainties.length, 4);
    } finally {
      c.dispose();
    }
  });

  it("a rejected bulk attempt does not mark an in-flight request uncertain", async () => {
    let releaseBulk;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow];
      if (command === "bulk_control") {
        return new Promise((resolve) => {
          releaseBulk = () => resolve({ status: "done", count: 1, results: [{ id: "dl-1", ok: true }] });
        });
      }
      if (command === "list_downloads") return [activeRow];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    let pending = null;
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      pending = c.bulkControl("pauseAll");
      await flush();
      assert.equal(c.state.bulkAction, "pauseAll");
      assert.equal(c.state.bulkUncertainties.length, 0);

      const rejected = await c.bulkControl("resumeAll");
      assert.equal(rejected, false);
      assert.equal(
        c.state.bulkUncertainties.length,
        0,
        "a busy rejection must not create an uncertainty notice",
      );
      assert.equal(calls.filter((x) => x.command === "bulk_control").length, 1);

      releaseBulk();
      assert.equal(await pending, true);
      pending = null;
      assert.equal(c.state.bulkSummary?.status, "done", "the confirmed outcome survives");
      assert.equal(c.state.bulkUncertainties.length, 0);
    } finally {
      try {
        if (releaseBulk) releaseBulk();
      } catch {}
      try {
        if (pending) await pending;
      } catch {}
      c.dispose();
    }
  });

  it("describes a same-connection timeout without blaming a previous connection", async () => {
    let bulkCalls = 0;
    const { invoke } = makeInvoke((command) => {
      if (command === "connect_surge") return [activeRow];
      if (command === "bulk_control") {
        bulkCalls += 1;
        if (bulkCalls === 1) throw "network";
        return { status: "done", count: 1, results: [{ id: "dl-1", ok: true }] };
      }
      if (command === "list_downloads") return [activeRow];
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(await c.bulkControl("pauseAll"), false);
      assert.equal(c.state.bulkUncertainties.length, 0);

      assert.equal(await c.bulkControl("resumeAll"), true);
      assert.equal(c.state.bulkUncertainties.length, 1);
      const message = c.state.bulkUncertainties[0].message;
      assert.match(message, /this connection/i);
      assert.match(message, /not retried/i);
      assert.doesNotMatch(message, /previous connection/i);
      assert.doesNotMatch(message, /may be the same daemon/i);
    } finally {
      c.dispose();
    }
  });

  it("bulk while offline reports that nothing was sent and issues no IPC", async () => {
    const { invoke, calls } = makeInvoke((command) => {
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      const ok = await c.bulkControl("pauseAll");
      assert.equal(ok, false);
      assert.equal(calls.length, 0);
      assert.equal(c.state.bulkSummary.status, "failed");
      assert.match(c.state.bulkSummary.message, /No bulk action was sent/i);
    } finally {
      c.dispose();
    }
  });
});
