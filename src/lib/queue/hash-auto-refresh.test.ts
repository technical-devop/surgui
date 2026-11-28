// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Deterministic regression tests for automatic hash-row propagation.
// Fake IPC only: no daemon, no real paths/URLs/secrets.
import { describe, it, mock } from "node:test";
import assert from "node:assert/strict";
import { NativeQueueController } from "./native-controller.ts";

const MD5 = "900150983cd24fb0d6963f7d28e17f72";
const SHA256 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

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

function pending(id = "dl-1") {
  return { id, state: "pending" };
}

function complete(id = "dl-1") {
  return { id, state: "complete", md5: MD5, sha256: SHA256, bytes: 3, resultVersion: 1 };
}

function delayed(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

describe("automatic hash row refresh", () => {
  it("renders completed validated hashes automatically after pending without a click", { timeout: 5000 }, async () => {
    let reads = 0;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [queueRow()];
      if (command === "get_download_hashes") {
        reads += 1;
        return reads === 1 ? [pending()] : [complete()];
      }
      if (command === "refresh_download_hashes") return [pending()];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      if (command === "list_downloads") return [queueRow()];
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
      hashRefreshMs: 5,
    });
    try {
      await c.startManaged();
      // The initial UI load surfaces the pending row; from there the worker
      // finishes without any frontend event or manual Refresh hashes click.
      await c.loadHashes();
      assert.equal(c.state.hashes.length, 1);
      assert.equal(c.state.hashes[0].state, "pending");

      await waitFor(
        () => c.state.hashes.length === 1 && c.state.hashes[0].state === "complete",
        "expected automatic pending -> complete propagation",
      );
      assert.equal(c.state.hashes[0].md5, MD5);
      assert.equal(c.state.hashes[0].sha256, SHA256);
      assert.equal(c.state.hashError, null);

      const settledReads = count(calls, "get_download_hashes");
      await delayed(50);
      assert.equal(
        count(calls, "get_download_hashes"),
        settledReads,
        "terminal complete rows must stop the automatic timer",
      );
      assert.equal(count(calls, "add_download"), 0, "hash refresh must never retry downloads");
      assert.equal(
        count(calls, "add_download_with_options"),
        0,
        "hash refresh must never retry downloads",
      );
    } finally {
      c.dispose();
    }
  });

  it("refreshes through the validated path on authoritative queue completion", { timeout: 5000 }, async () => {
    let completed = false;
    let refreshes = 0;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [queueRow()];
      if (command === "list_downloads") {
        return completed ? [queueRow({ status: "completed", bytesDone: 100 })] : [queueRow()];
      }
      if (command === "get_download_hashes") return [pending()];
      if (command === "refresh_download_hashes") {
        refreshes += 1;
        return completed ? [complete()] : [pending()];
      }
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
      // Long enough that only the authoritative completion may refresh here.
      hashRefreshMs: 1_000,
    });
    try {
      await c.startManaged();
      await c.loadHashes();
      assert.equal(c.state.hashes[0].state, "pending");

      completed = true;
      await c.refresh();
      assert.equal(refreshes, 1, "one completion must trigger exactly one validated hash refresh");
      assert.equal(c.state.hashes[0].state, "complete");
      assert.equal(c.state.hashes[0].sha256, SHA256);

      await c.refresh();
      await delayed(30);
      assert.equal(refreshes, 1, "the same completion generation must not refresh repeatedly");
      assert.equal(c.state.hashes[0].state, "complete");
    } finally {
      c.dispose();
    }
  });

  it(
    "propagates an authoritative completion promptly while a backed-off hash poll is armed",
    { timeout: 5000 },
    async () => {
      // Controlled clock only: no wall-clock sleeps. hashRefreshMs 5ms makes
      // the production 2s x 2^attempt backoff deterministic and fast while
      // preserving its exact timer/backoff behavior.
      mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
      const flush = async () => {
        await new Promise((resolve) => setImmediate(resolve));
        await new Promise((resolve) => setImmediate(resolve));
      };

      let completed = false;
      let completionWorkerSeen = false;
      let workerState = "pending";
      let hashReads = 0;
      let completionRefreshes = 0;
      const { invoke } = makeInvoke((command) => {
        if (command === "start_managed_surge") return [queueRow()];
        if (command === "list_downloads") {
          return completed
            ? [queueRow({ status: "completed", bytesDone: 100 })]
            : [queueRow()];
        }
        if (command === "get_download_hashes") {
          hashReads += 1;
          return [workerState === "complete" ? complete() : pending()];
        }
        if (command === "refresh_download_hashes") {
          if (completed && !completionWorkerSeen) {
            completionWorkerSeen = true;
            completionRefreshes += 1;
            // The worker reply observed at completion is still pending; it
            // becomes complete immediately afterwards, as on the slow host.
            workerState = "complete";
            return [pending()];
          }
          return [workerState === "complete" ? complete() : pending()];
        }
        if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
        throw new Error(`unexpected: ${command}`);
      });
      const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
        hashRefreshMs: 5,
        hashAutoMaxAttempts: 12,
      });
      try {
        await c.startManaged();
        await c.loadHashes();
        assert.equal(c.state.hashes[0]?.state, "pending");
        assert.equal(hashReads, 1);

        // Drive the accelerated pending polling through its natural backoff.
        // These delays (5, 10, 20, 40, 80) leave the next poll armed for 160ms.
        const backoffDelays = [5, 10, 20, 40, 80];
        for (let i = 0; i < backoffDelays.length; i++) {
          mock.timers.tick(backoffDelays[i]);
          await flush();
          assert.equal(hashReads, i + 2, `expected armed pending poll #${i + 2}`);
        }
        assert.equal(c.state.hashes[0]?.state, "pending");
        assert.equal(
          c.hashAutoTimer !== null,
          true,
          "expected a backed-off automatic poll to be armed",
        );

        // Authoritative completion lands while the 160ms poll is still armed.
        completed = true;
        await c.refresh();
        await flush();
        assert.equal(completionRefreshes, 1, "completion must trigger exactly one worker pass");
        assert.equal(
          c.state.hashes[0]?.state,
          "pending",
          "the worker reply at completion is still pending",
        );

        // 20ms is a short documented display bound (4x the accelerated base
        // retry, equivalent to a few seconds in production): far below the
        // remaining ~160ms stale timer. No Refresh hashes click is issued.
        mock.timers.tick(20);
        await flush();
        const withinBound = c.state.hashes[0]?.state;

        // Prove the setup is sound: after the stale 160ms timer finally fires,
        // the already-complete worker is consumed and the rows do finish. The
        // only defect is the stale display inside the short bound above.
        mock.timers.tick(141);
        await flush();
        const eventual = c.state.hashes[0]?.state;

        assert.equal(
          withinBound,
          "complete",
          `hash rows stayed ${withinBound} after completion while a stale backed-off timer was armed (they only became ${eventual} once the stale timer fired)`,
        );
        assert.equal(eventual, "complete");
        assert.equal(c.state.hashes[0]?.md5, MD5);
        assert.equal(c.state.hashes[0]?.sha256, SHA256);
        assert.equal(c.state.hashError, null);
      } finally {
        c.dispose();
        mock.timers.reset();
      }
    },
  );

  it(
    "starts the worker from the completion snapshot and rechecks promptly on the fresh timer",
    { timeout: 5000 },
    async () => {
      mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
      const flush = async () => {
        await new Promise((resolve) => setImmediate(resolve));
        await new Promise((resolve) => setImmediate(resolve));
      };

      let completed = false;
      let workerStarted = false;
      let workerState = "pending";
      let hashReads = 0;
      let workerRefreshes = 0;
      const { invoke, calls } = makeInvoke((command) => {
        if (command === "start_managed_surge") return [queueRow()];
        if (command === "list_downloads") {
          if (!completed) return [queueRow()];
          if (!workerStarted) {
            workerStarted = true;
            // Production: the authoritative snapshot itself nudges/coalesces
            // the backend worker. Readiness flips shortly afterwards, so the
            // immediate worker pass still observes pending.
            setTimeout(() => {
              workerState = "complete";
            }, 1);
          }
          return [queueRow({ status: "completed", bytesDone: 100 })];
        }
        if (command === "get_download_hashes") {
          hashReads += 1;
          return [workerState === "complete" ? complete() : pending()];
        }
        if (command === "refresh_download_hashes") {
          workerRefreshes += 1;
          return [workerState === "complete" ? complete() : pending()];
        }
        if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
        throw new Error(`unexpected: ${command}`);
      });
      const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
        hashRefreshMs: 5,
        hashAutoMaxAttempts: 12,
      });
      try {
        await c.startManaged();
        await c.loadHashes();
        assert.equal(c.state.hashes[0]?.state, "pending");
        assert.equal(hashReads, 1);

        const backoffDelays = [5, 10, 20, 40, 80];
        for (let i = 0; i < backoffDelays.length; i++) {
          mock.timers.tick(backoffDelays[i]);
          await flush();
          assert.equal(hashReads, i + 2, `expected armed pending poll #${i + 2}`);
        }
        assert.equal(c.hashAutoTimer !== null, true);
        const readsBeforeCompletion = hashReads;
        const callsBeforeCompletion = calls.length;

        completed = true;
        await c.refresh();
        await flush();
        assert.equal(workerStarted, true, "the completion snapshot must start the worker");
        assert.equal(workerRefreshes, 1, "the snapshot completion must issue one worker pass");
        assert.equal(
          c.state.hashes[0]?.state,
          "pending",
          "the immediate worker reply still predates readiness",
        );
        assert.deepEqual(
          calls.slice(callsBeforeCompletion).map((call) => call.command),
          ["list_downloads_snapshot", "list_downloads", "refresh_download_hashes"],
          "the snapshot must precede exactly one worker pass",
        );

        // 20ms is far below the remaining ~160ms stale poll but above the
        // fresh base cadence after the completion reset.
        mock.timers.tick(20);
        await flush();
        assert.equal(c.state.hashes[0]?.state, "complete");
        assert.equal(c.state.hashes[0]?.sha256, SHA256);
        assert.equal(hashReads, readsBeforeCompletion + 1, "one fresh recheck, no duplicate reads");
        assert.equal(workerRefreshes, 1, "completion must not retry the worker");
        assert.equal(
          calls[calls.length - 1].command,
          "get_download_hashes",
          "the fresh timer must perform the recheck",
        );

        const settledReads = hashReads;
        mock.timers.tick(500);
        await flush();
        assert.equal(hashReads, settledReads, "terminal rows stop the automatic timer");
        assert.equal(workerRefreshes, 1, "the worker pass is never retried");
        assert.equal(count(calls, "add_download"), 0, "hash refresh must never retry downloads");
        assert.equal(
          count(calls, "add_download_with_options"),
          0,
          "hash refresh must never retry downloads",
        );
        assert.equal(c.state.hashError, null);
      } finally {
        c.dispose();
        mock.timers.reset();
      }
    },
  );

  it(
    "defers a completion reset while a hash read is in flight and fires the replacement timer",
    { timeout: 5000 },
    async () => {
      mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
      const flush = async () => {
        await new Promise((resolve) => setImmediate(resolve));
        await new Promise((resolve) => setImmediate(resolve));
      };

      let completed = false;
      let workerState = "pending";
      let hashReads = 0;
      let workerRefreshes = 0;
      let releaseRead = null;
      const { invoke, calls } = makeInvoke((command) => {
        if (command === "start_managed_surge") return [queueRow()];
        if (command === "list_downloads") {
          if (!completed) return [queueRow()];
          if (workerState !== "complete") {
            setTimeout(() => {
              workerState = "complete";
            }, 1);
          }
          return [queueRow({ status: "completed", bytesDone: 100 })];
        }
        if (command === "get_download_hashes") {
          hashReads += 1;
          if (hashReads === 7) {
            return new Promise((resolve) => {
              releaseRead = () => resolve([pending()]);
            });
          }
          return [workerState === "complete" ? complete() : pending()];
        }
        if (command === "refresh_download_hashes") {
          workerRefreshes += 1;
          return [workerState === "complete" ? complete() : pending()];
        }
        if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
        throw new Error(`unexpected: ${command}`);
      });
      const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
        hashRefreshMs: 5,
        hashAutoMaxAttempts: 12,
      });
      try {
        await c.startManaged();
        await c.loadHashes();
        assert.equal(c.state.hashes[0]?.state, "pending");

        const backoffDelays = [5, 10, 20, 40, 80];
        for (let i = 0; i < backoffDelays.length; i++) {
          mock.timers.tick(backoffDelays[i]);
          await flush();
          assert.equal(hashReads, i + 2, `expected armed pending poll #${i + 2}`);
        }
        assert.equal(c.hashAutoTimer !== null, true);

        const inFlight = c.loadHashes();
        await flush();
        assert.equal(hashReads, 7, "the read is in flight");
        assert.equal(typeof releaseRead, "function");
        const callsBeforeCompletion = calls.length;

        completed = true;
        await c.refresh();
        await flush();
        assert.equal(workerRefreshes, 0, "a completion cannot reset through an in-flight read");
        assert.equal(hashReads, 7, "no duplicate read starts while one is in flight");
        assert.deepEqual(
          calls.slice(callsBeforeCompletion).map((call) => call.command),
          ["list_downloads_snapshot", "list_downloads"],
          "the completion is deferred without issuing the worker",
        );

        releaseRead();
        await inFlight;
        await flush();
        assert.equal(workerRefreshes, 1, "the deferred worker pass is issued once after the read");
        assert.equal(c.state.hashes[0]?.state, "pending");

        mock.timers.tick(20);
        await flush();
        assert.equal(c.state.hashes[0]?.state, "complete");
        assert.equal(c.state.hashes[0]?.sha256, SHA256);
        assert.equal(hashReads, 8, "the replacement timer performs exactly one recheck");
        assert.equal(workerRefreshes, 1, "the deferred worker is never retried");
        assert.deepEqual(
          calls.slice(callsBeforeCompletion).map((call) => call.command),
          [
            "list_downloads_snapshot",
            "list_downloads",
            "refresh_download_hashes",
            "get_download_hashes",
          ],
          "completion, deferred worker, then one replacement recheck",
        );

        const settledReads = hashReads;
        mock.timers.tick(500);
        await flush();
        assert.equal(hashReads, settledReads, "terminal rows stop the replacement timer");
        assert.equal(count(calls, "add_download"), 0);
        assert.equal(count(calls, "add_download_with_options"), 0);
        assert.equal(c.state.hashError, null);
      } finally {
        c.dispose();
        mock.timers.reset();
      }
    },
  );

  it(
    "fences a completion refresh still pending when the connection drops",
    { timeout: 5000 },
    async () => {
      mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
      const flush = async () => {
        await new Promise((resolve) => setImmediate(resolve));
        await new Promise((resolve) => setImmediate(resolve));
      };

      let completed = false;
      let workerRefreshes = 0;
      const workerReply = deferred();
      const { invoke, calls } = makeInvoke((command) => {
        if (command === "start_managed_surge") return [queueRow()];
        if (command === "list_downloads") {
          return completed
            ? [queueRow({ status: "completed", bytesDone: 100 })]
            : [queueRow()];
        }
        if (command === "get_download_hashes") return [pending()];
        if (command === "refresh_download_hashes") {
          workerRefreshes += 1;
          return workerReply.promise;
        }
        if (command === "disconnect_surge") return { ok: true };
        if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
        throw new Error(`unexpected: ${command}`);
      });
      const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
        hashRefreshMs: 5,
        hashAutoMaxAttempts: 12,
      });
      try {
        await c.startManaged();
        await c.loadHashes();
        assert.equal(c.state.hashes[0]?.state, "pending");

        completed = true;
        const completionRefresh = c.refresh();
        await flush();
        assert.equal(workerRefreshes, 1, "the completion refresh is pending");

        await c.disconnect();
        const callsAfterDisconnect = calls.length;
        assert.deepEqual(c.state.hashes, [], "disconnect clears the old epoch rows");
        assert.equal(c.hashAutoTimer, null, "disconnect cancels the automatic timer");

        workerReply.resolve([complete()]);
        await completionRefresh;
        await flush();
        assert.deepEqual(c.state.hashes, [], "the stale completion is fenced");
        assert.equal(c.state.hashError, null);
        assert.equal(c.hashAutoTimer, null, "no timer is re-armed on the old epoch");

        mock.timers.tick(500);
        await flush();
        assert.equal(calls.length, callsAfterDisconnect, "no stale commands run after disconnect");
        assert.equal(workerRefreshes, 1, "the pending worker is never retried");
        assert.equal(c.state.phase, "offline");
      } finally {
        c.dispose();
        mock.timers.reset();
      }
    },
  );

  it(
    "does not retry a failed completion worker and leaves recovery manual",
    { timeout: 5000 },
    async () => {
      mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
      const flush = async () => {
        await new Promise((resolve) => setImmediate(resolve));
        await new Promise((resolve) => setImmediate(resolve));
      };

      let completed = false;
      let workerRefreshes = 0;
      let hashReads = 0;
      const { invoke, calls } = makeInvoke((command) => {
        if (command === "start_managed_surge") return [queueRow()];
        if (command === "list_downloads") {
          return completed
            ? [queueRow({ status: "completed", bytesDone: 100 })]
            : [queueRow()];
        }
        if (command === "get_download_hashes") {
          hashReads += 1;
          return [pending()];
        }
        if (command === "refresh_download_hashes") {
          workerRefreshes += 1;
          throw "network";
        }
        if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
        throw new Error(`unexpected: ${command}`);
      });
      const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
        hashRefreshMs: 5,
        hashAutoMaxAttempts: 12,
      });
      try {
        await c.startManaged();
        await c.loadHashes();
        assert.equal(c.state.hashes[0]?.state, "pending");
        const readsBeforeCompletion = hashReads;

        completed = true;
        await c.refresh();
        await flush();
        assert.equal(workerRefreshes, 1, "the completion worker is issued once");
        assert.equal(c.state.hashes[0]?.state, "pending");
        assert.equal(c.state.hashError, "Cannot reach the daemon for hash state.");
        assert.equal(c.hashAutoTimer, null, "a failed worker stops the automatic timer");
        assert.equal(hashReads, readsBeforeCompletion, "the failed worker is not re-read");

        mock.timers.tick(500);
        await flush();
        assert.equal(workerRefreshes, 1, "the failed command is never retried");
        assert.equal(hashReads, readsBeforeCompletion, "no retry read is issued");
        assert.equal(count(calls, "add_download"), 0);
        assert.equal(count(calls, "add_download_with_options"), 0);
        assert.equal(c.state.hashError, "Cannot reach the daemon for hash state.");
      } finally {
        c.dispose();
        mock.timers.reset();
      }
    },
  );

  it("tracks two jobs independently and stops only when every row is terminal", { timeout: 5000 }, async () => {
    let reads = 0;
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [queueRow({ id: "dl-1" }), queueRow({ id: "dl-2" })];
      if (command === "get_download_hashes") {
        reads += 1;
        if (reads === 1) return [pending("dl-1"), pending("dl-2")];
        if (reads === 2) return [complete("dl-1"), pending("dl-2")];
        return [complete("dl-1"), complete("dl-2")];
      }
      if (command === "refresh_download_hashes") return [pending("dl-1"), pending("dl-2")];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      if (command === "list_downloads") return [queueRow({ id: "dl-1" }), queueRow({ id: "dl-2" })];
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
      hashRefreshMs: 5,
    });
    try {
      await c.startManaged();
      await c.loadHashes();
      await waitFor(
        () =>
          c.state.hashes.length === 2 &&
          c.state.hashes.every((row) => row.state === "complete"),
        "expected both independent jobs to finish automatically",
      );
      const settledReads = reads;
      await delayed(40);
      assert.equal(reads, settledReads, "no reads after the last terminal row");
    } finally {
      c.dispose();
    }
  });

  it("stops after a terminal failure and preserves the row status", { timeout: 5000 }, async () => {
    let reads = 0;
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [queueRow()];
      if (command === "get_download_hashes") {
        reads += 1;
        return reads === 1 ? [pending()] : [{ id: "dl-1", state: "failed" }];
      }
      if (command === "refresh_download_hashes") return [pending()];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      if (command === "list_downloads") return [queueRow()];
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
      hashRefreshMs: 5,
    });
    try {
      await c.startManaged();
      await c.loadHashes();
      await waitFor(() => c.state.hashes[0]?.state === "failed", "expected failed hash row");
      const settledReads = reads;
      await delayed(50);
      assert.equal(reads, settledReads, "failed rows are terminal and must not poll forever");
      assert.equal(c.state.hashError, null, "row failure is not a transport warning");
    } finally {
      c.dispose();
    }
  });

  it("preserves a hash transport error and does not busy-retry it", { timeout: 5000 }, async () => {
    let reads = 0;
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [queueRow()];
      if (command === "get_download_hashes") {
        reads += 1;
        if (reads === 1) return [pending()];
        throw "network";
      }
      if (command === "refresh_download_hashes") return [pending()];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      if (command === "list_downloads") return [queueRow()];
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
      hashRefreshMs: 5,
    });
    try {
      await c.startManaged();
      await c.loadHashes();
      await waitFor(() => c.state.hashError !== null, "expected a sanitized hash error");
      assert.equal(c.state.hashError, "Cannot reach the daemon for hash state.");
      const settledReads = reads;
      await delayed(60);
      assert.equal(reads, settledReads, "a failed automatic read must not form a retry loop");
      assert.equal(c.state.hashError, "Cannot reach the daemon for hash state.");
    } finally {
      c.dispose();
    }
  });

  it("does not issue duplicate concurrent hash reads while one is in flight", { timeout: 5000 }, async () => {
    let reads = 0;
    let release;
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [queueRow()];
      if (command === "set_download_hash_intents") return [pending()];
      if (command === "get_download_hashes") {
        reads += 1;
        if (reads === 1) {
          return new Promise((resolve) => {
            release = () => resolve([complete()]);
          });
        }
        return [complete()];
      }
      if (command === "refresh_download_hashes") return [pending()];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
      hashRefreshMs: 5,
    });
    try {
      await c.startManaged();
      await c.setHashIntents([{ id: "dl-1", enabled: true }]);
      assert.equal(c.state.hashes[0].state, "pending");

      const load = c.loadHashes();
      await waitFor(() => reads === 1, "expected one in-flight read");
      await delayed(40);
      assert.equal(reads, 1, "the timer must not start a second concurrent read");

      release();
      await load;
      await waitFor(() => c.state.hashes[0]?.state === "complete", "expected the in-flight read applied");
    } finally {
      c.dispose();
    }
  });

  it("fences stale automatic responses across reconnect and never paints cross-epoch rows", { timeout: 5000 }, async () => {
    let reads = 0;
    let releaseOld;
    let managedStarts = 0;
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") {
        managedStarts += 1;
        return [queueRow({ id: managedStarts === 1 ? "old" : "new" })];
      }
      if (command === "disconnect_surge") return { ok: true };
      if (command === "get_download_hashes") {
        reads += 1;
        if (reads === 1) return [{ id: "old", state: "pending" }];
        if (reads === 2) {
          return new Promise((resolve) => {
            releaseOld = () => resolve([complete("old")]);
          });
        }
        return [];
      }
      if (command === "refresh_download_hashes") return [];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      if (command === "list_downloads") return [queueRow({ id: managedStarts === 1 ? "old" : "new" })];
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
      hashRefreshMs: 5,
    });
    try {
      await c.startManaged();
      await c.loadHashes();
      await waitFor(() => typeof releaseOld === "function", "expected a deferred old-epoch read");

      await c.disconnect();
      await c.startManaged();
      assert.deepEqual(c.state.hashes, [], "new connection starts without old digests");
      releaseOld();
      await delayed(40);
      assert.deepEqual(c.state.hashes, [], "late old-epoch completion must never be displayed");
    } finally {
      c.dispose();
    }
  });

  it("cancels automatic timers on disconnect and dispose", { timeout: 5000 }, async () => {
    function harness() {
      let reads = 0;
      const made = makeInvoke((command) => {
        if (command === "start_managed_surge") return [queueRow()];
        if (command === "get_download_hashes") {
          reads += 1;
          return [pending()];
        }
        if (command === "refresh_download_hashes") return [pending()];
        if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
        if (command === "connect_surge") return [];
        if (command === "disconnect_surge") return { ok: true };
        if (command === "list_downloads") return [queueRow()];
        throw new Error(`unexpected: ${command}`);
      });
      return { ...made, reads: () => reads };
    }

    const disconnectHarness = harness();
    const disconnectController = new NativeQueueController(
      disconnectHarness.invoke,
      () => {},
      60_000,
      undefined,
      { hashRefreshMs: 40 },
    );
    try {
      await disconnectController.startManaged();
      await disconnectController.loadHashes();
      assert.equal(disconnectHarness.reads(), 1);
      await disconnectController.disconnect();
      await delayed(90);
      assert.equal(disconnectHarness.reads(), 1, "disconnect must cancel the pending hash timer");
    } finally {
      disconnectController.dispose();
    }

    const disposeHarness = harness();
    const disposeController = new NativeQueueController(
      disposeHarness.invoke,
      () => {},
      60_000,
      undefined,
      { hashRefreshMs: 40 },
    );
    try {
      await disposeController.startManaged();
      await disposeController.loadHashes();
      assert.equal(disposeHarness.reads(), 1);
      disposeController.dispose();
      await delayed(90);
      assert.equal(disposeHarness.reads(), 1, "dispose must cancel the pending hash timer");
    } finally {
      disposeController.dispose();
    }
  });

  it("never auto-loads hashes on a manual connection", { timeout: 5000 }, async () => {
    let reads = 0;
    const { invoke } = makeInvoke((command) => {
      if (command === "connect_surge") return [queueRow()];
      if (command === "get_download_hashes") {
        reads += 1;
        return [pending()];
      }
      if (command === "refresh_download_hashes") return [pending()];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: false };
      if (command === "list_downloads") return [queueRow()];
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
      hashRefreshMs: 5,
    });
    try {
      await c.connect("http://127.0.0.1:9", "test-only-token");
      assert.equal(c.state.managed, false);
      await c.loadHashes();
      await delayed(50);
      assert.equal(reads, 0, "manual connections must not poll automatic-hash rows");
      assert.deepEqual(c.state.hashes, []);
    } finally {
      c.dispose();
    }
  });

  it(
    "preserves a completion trigger while a hash read is in flight and issues it when pending arrives",
    { timeout: 5000 },
    async () => {
      let completed = false;
      let releaseFirst = null;
      let hashReads = 0;
      let refreshes = 0;
      const { invoke } = makeInvoke((command) => {
        if (command === "start_managed_surge") return [queueRow()];
        if (command === "list_downloads") {
          return completed ? [queueRow({ status: "completed", bytesDone: 100 })] : [queueRow()];
        }
        if (command === "get_download_hashes") {
          hashReads += 1;
          if (hashReads === 1) {
            return new Promise((resolve) => {
              releaseFirst = () => resolve([pending()]);
            });
          }
          return [complete()];
        }
        if (command === "refresh_download_hashes") {
          refreshes += 1;
          return [complete()];
        }
        if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
        throw new Error(`unexpected: ${command}`);
      });
      const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
        hashRefreshMs: 1_000,
      });
      try {
        await c.startManaged();
        const firstRead = c.loadHashes();
        await waitFor(() => typeof releaseFirst === "function", "expected a deferred hash read");

        completed = true;
        await c.refresh();
        assert.equal(refreshes, 0, "an in-flight read must defer the worker pass");
        assert.equal(c.state.hashes.length, 0);

        releaseFirst();
        await firstRead;
        assert.equal(refreshes, 1, "the deferred worker pass must be issued exactly once");
        assert.equal(c.state.hashes[0]?.state, "complete");

        await c.refresh();
        await delayed(30);
        assert.equal(refreshes, 1, "the same completion must not retrigger the worker");
      } finally {
        c.dispose();
      }
    },
  );

  it(
    "triggers the worker when a pending hash row arrives after an already completed queue row",
    { timeout: 5000 },
    async () => {
      let refreshes = 0;
      const { invoke } = makeInvoke((command) => {
        if (command === "start_managed_surge") return [queueRow({ status: "completed", bytesDone: 100 })];
        if (command === "list_downloads") return [queueRow({ status: "completed", bytesDone: 100 })];
        if (command === "get_download_hashes") return [];
        if (command === "set_download_hash_intents") return [pending()];
        if (command === "refresh_download_hashes") {
          refreshes += 1;
          return [complete()];
        }
        if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
        throw new Error(`unexpected: ${command}`);
      });
      const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
        hashRefreshMs: 1_000,
      });
      try {
        await c.startManaged();
        await c.loadHashes();
        assert.deepEqual(c.state.hashes, []);

        assert.equal(await c.setHashIntents([{ id: "dl-1", enabled: true }]), true);
        assert.equal(refreshes, 1, "a late pending row on a completed download must trigger a worker pass");
        assert.equal(c.state.hashes[0]?.state, "complete");
      } finally {
        c.dispose();
      }
    },
  );

  it("does not poll forever for a paused download with a pending hash row", { timeout: 5000 }, async () => {
    let reads = 0;
    let refreshes = 0;
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [queueRow({ status: "paused" })];
      if (command === "list_downloads") return [queueRow({ status: "paused" })];
      if (command === "get_download_hashes") {
        reads += 1;
        return [pending()];
      }
      if (command === "refresh_download_hashes") {
        refreshes += 1;
        return [pending()];
      }
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
      hashRefreshMs: 5,
    });
    try {
      await c.startManaged();
      await c.loadHashes();
      assert.equal(reads, 1);
      await delayed(80);
      assert.equal(reads, 1, "paused downloads must not keep the propagation timer alive");
      assert.equal(refreshes, 0);
      assert.equal(c.state.hashError, null);
    } finally {
      c.dispose();
    }
  });

  it("bounds active propagation with backoff and surfaces an explicit recovery state", { timeout: 5000 }, async () => {
    let reads = 0;
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [queueRow()];
      if (command === "list_downloads") return [queueRow()];
      if (command === "get_download_hashes") {
        reads += 1;
        return [pending()];
      }
      if (command === "refresh_download_hashes") return [pending()];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
      hashRefreshMs: 5,
      hashAutoMaxAttempts: 2,
    });
    try {
      await c.startManaged();
      await c.loadHashes();
      await waitFor(() => c.state.hashError !== null, "expected the bounded propagation to stop");
      assert.match(c.state.hashError, /Automatic hash refresh paused/);
      const settledReads = reads;
      assert.ok(settledReads >= 2 && settledReads <= 3, "only the bounded attempts may be issued");
      await delayed(80);
      assert.equal(reads, settledReads, "the timer must stay stopped until an explicit refresh");
    } finally {
      c.dispose();
    }
  });

  it("lets an authoritative completion restart a stopped propagation budget", { timeout: 5000 }, async () => {
    let completed = false;
    let refreshes = 0;
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [queueRow()];
      if (command === "list_downloads") {
        return completed ? [queueRow({ status: "completed", bytesDone: 100 })] : [queueRow()];
      }
      if (command === "get_download_hashes") return [pending()];
      if (command === "refresh_download_hashes") {
        refreshes += 1;
        return [complete()];
      }
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
      hashRefreshMs: 5,
      hashAutoMaxAttempts: 1,
    });
    try {
      await c.startManaged();
      await c.loadHashes();
      await waitFor(() => c.state.hashError !== null, "expected the bounded propagation to stop");

      completed = true;
      await c.refresh();
      assert.equal(refreshes, 1, "a real completion must still trigger one worker pass");
      assert.equal(c.state.hashes[0]?.state, "complete");
      assert.equal(c.state.hashError, null, "successful recovery must clear the stopped state");
    } finally {
      c.dispose();
    }
  });

  it("fences a late intent response behind a newer hash read", { timeout: 5000 }, async () => {
    let releaseIntent = null;
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [queueRow()];
      if (command === "list_downloads") return [queueRow()];
      if (command === "get_download_hashes") return [pending()];
      if (command === "refresh_download_hashes") return [complete()];
      if (command === "set_download_hash_intents") {
        return new Promise((resolve) => {
          releaseIntent = () => resolve([pending()]);
        });
      }
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
      hashRefreshMs: 1_000,
    });
    try {
      await c.startManaged();
      await c.loadHashes();
      assert.equal(c.state.hashes[0]?.state, "pending");

      const intent = c.setHashIntents([{ id: "dl-1", enabled: true }]);
      await waitFor(() => typeof releaseIntent === "function", "expected a deferred intent response");
      await c.refreshHashes();
      assert.equal(c.state.hashes[0]?.state, "complete");

      releaseIntent();
      await intent;
      assert.equal(c.state.hashes[0]?.state, "complete", "a late older response must not regress newer rows");
    } finally {
      c.dispose();
    }
  });

  it("fences a late hash read behind a newer intent response", { timeout: 5000 }, async () => {
    let releaseRead = null;
    let reads = 0;
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [queueRow()];
      if (command === "list_downloads") return [queueRow()];
      if (command === "get_download_hashes") {
        reads += 1;
        if (reads === 1) {
          return new Promise((resolve) => {
            releaseRead = () => resolve([complete()]);
          });
        }
        return [];
      }
      if (command === "set_download_hash_intents") return [pending()];
      if (command === "refresh_download_hashes") return [];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
      hashRefreshMs: 1_000,
    });
    try {
      await c.startManaged();
      const read = c.loadHashes();
      await waitFor(() => typeof releaseRead === "function", "expected a deferred hash read");

      assert.equal(await c.setHashIntents([{ id: "dl-1", enabled: true }]), true);
      assert.equal(c.state.hashes[0]?.state, "pending");

      releaseRead();
      await read;
      assert.equal(
        c.state.hashes[0]?.state,
        "pending",
        "a late older read must not overwrite a newer intent response",
      );
    } finally {
      c.dispose();
    }
  });
});

describe("hash intent write/read ordering", () => {
  it(
    "keeps a committed intent and triggers hashing when a stale overlapping read settles first",
    { timeout: 5000 },
    async () => {
      let intents = 0;
      let reads = 0;
      let refreshes = 0;
      const writeReply = deferred();
      const { invoke } = makeInvoke((command) => {
        if (command === "start_managed_surge")
          return [queueRow({ status: "completed", bytesDone: 100 })];
        if (command === "list_downloads")
          return [queueRow({ status: "completed", bytesDone: 100 })];
        if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
        if (command === "set_download_hash_intents") {
          intents += 1;
          return writeReply.promise;
        }
        if (command === "get_download_hashes") {
          reads += 1;
          if (reads === 1) return [];
          if (reads === 2) return [];
          return [pending()];
        }
        if (command === "refresh_download_hashes") {
          refreshes += 1;
          return [complete()];
        }
        throw new Error(`unexpected: ${command}`);
      });
      const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
        hashRefreshMs: 1_000,
      });
      try {
        await c.startManaged();
        await c.loadHashes();
        assert.deepEqual(c.state.hashes, []);

        const write = c.setHashIntents([{ id: "dl-1", enabled: true }]);
        const overlapping = c.loadHashes();
        await overlapping;
        assert.deepEqual(c.state.hashes, [], "the overlapping read snapshots the pre-write value");

        writeReply.resolve([pending()]);
        assert.equal(await write, true);
        assert.equal(intents, 1, "the intent write is never retried");
        assert.equal(reads, 3, "one follow-up read runs after the write settles");
        assert.equal(refreshes, 1, "the committed pending intent triggers one worker pass");
        assert.equal(c.state.hashes[0]?.state, "complete");
        assert.equal(c.state.hashes[0]?.sha256, SHA256);
      } finally {
        c.dispose();
      }
    },
  );

  it(
    "does not let an older overlapping read overwrite a write that settles first",
    { timeout: 5000 },
    async () => {
      let intents = 0;
      let reads = 0;
      let refreshes = 0;
      let releaseOldRead = null;
      const writeReply = deferred();
      const { invoke } = makeInvoke((command) => {
        if (command === "start_managed_surge") return [queueRow()];
        if (command === "list_downloads") return [queueRow()];
        if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
        if (command === "set_download_hash_intents") {
          intents += 1;
          return writeReply.promise;
        }
        if (command === "get_download_hashes") {
          reads += 1;
          if (reads === 1) return [];
          if (reads === 2) {
            return new Promise((resolve) => {
              releaseOldRead = () => resolve([]);
            });
          }
          return [pending()];
        }
        if (command === "refresh_download_hashes") {
          refreshes += 1;
          return [complete()];
        }
        throw new Error(`unexpected: ${command}`);
      });
      const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
        hashRefreshMs: 1_000,
      });
      try {
        await c.startManaged();
        await c.loadHashes();
        assert.deepEqual(c.state.hashes, []);

        const write = c.setHashIntents([{ id: "dl-1", enabled: true }]);
        const oldRead = c.loadHashes();
        await waitFor(() => typeof releaseOldRead === "function", "expected the overlapping read");

        writeReply.resolve([pending()]);
        assert.equal(await write, true);
        assert.equal(reads, 3, "the follow-up read runs before the old read is released");
        assert.equal(intents, 1, "the intent write is never retried");

        releaseOldRead();
        await oldRead;
        assert.equal(reads, 3, "a pre-write read is fenced, not reissued");
        assert.equal(refreshes, 0);
        assert.equal(c.state.hashes[0]?.state, "pending");
        assert.equal(c.state.hashError, null);
      } finally {
        c.dispose();
      }
    },
  );

  it(
    "surfaces a failed write even when an overlapping read already applied",
    { timeout: 5000 },
    async () => {
      let intents = 0;
      let reads = 0;
      let refreshes = 0;
      const writeReply = deferred();
      const { invoke } = makeInvoke((command) => {
        if (command === "start_managed_surge") return [queueRow()];
        if (command === "list_downloads") return [queueRow()];
        if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
        if (command === "set_download_hash_intents") {
          intents += 1;
          return writeReply.promise;
        }
        if (command === "get_download_hashes") {
          reads += 1;
          return [];
        }
        if (command === "refresh_download_hashes") {
          refreshes += 1;
          return [complete()];
        }
        throw new Error(`unexpected: ${command}`);
      });
      const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
        hashRefreshMs: 1_000,
      });
      try {
        await c.startManaged();
        await c.loadHashes();

        const write = c.setHashIntents([{ id: "dl-1", enabled: true }]);
        await c.loadHashes();
        writeReply.reject("network");
        assert.equal(await write, false);
        assert.equal(intents, 1, "a failed intent write is never retried");
        assert.equal(reads, 3, "a failed final write still gets one authoritative follow-up read");
        assert.equal(refreshes, 0);
        assert.equal(c.state.hashError, "Cannot reach the daemon for hash state.");
        assert.deepEqual(c.state.hashes, []);
      } finally {
        c.dispose();
      }
    },
  );

  it(
    "keeps a failed write visible when an older overlapping read settles last",
    { timeout: 5000 },
    async () => {
      let intents = 0;
      let reads = 0;
      let refreshes = 0;
      let releaseOldRead = null;
      const writeReply = deferred();
      const { invoke } = makeInvoke((command) => {
        if (command === "start_managed_surge") return [queueRow()];
        if (command === "list_downloads") return [queueRow()];
        if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
        if (command === "set_download_hash_intents") {
          intents += 1;
          return writeReply.promise;
        }
        if (command === "get_download_hashes") {
          reads += 1;
          if (reads === 1) return [];
          if (reads === 2) {
            return new Promise((resolve) => {
              releaseOldRead = () => resolve([]);
            });
          }
          return [];
        }
        if (command === "refresh_download_hashes") {
          refreshes += 1;
          return [complete()];
        }
        throw new Error(`unexpected: ${command}`);
      });
      const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
        hashRefreshMs: 1_000,
      });
      try {
        await c.startManaged();
        await c.loadHashes();

        const write = c.setHashIntents([{ id: "dl-1", enabled: true }]);
        const oldRead = c.loadHashes();
        await waitFor(() => typeof releaseOldRead === "function", "expected the overlapping read");

        writeReply.reject("network");
        assert.equal(await write, false);
        assert.equal(c.state.hashError, "Cannot reach the daemon for hash state.");
        assert.equal(reads, 3, "the failed final write issues one follow-up before the old read settles");

        releaseOldRead();
        await oldRead;
        assert.equal(intents, 1, "a failed intent write is never retried");
        assert.equal(reads, 3, "the old overlapping read is fenced, not reissued");
        assert.equal(refreshes, 0);
        assert.equal(
          c.state.hashError,
          "Cannot reach the daemon for hash state.",
          "a late pre-write read must not clear the write failure",
        );
        assert.deepEqual(c.state.hashes, []);
      } finally {
        c.dispose();
      }
    },
  );

  it(
    "fences a pending intent write and its overlapping read across reconnect",
    { timeout: 5000 },
    async () => {
      let managedStarts = 0;
      let intents = 0;
      let reads = 0;
      let refreshes = 0;
      let releaseOldRead = null;
      const writeReply = deferred();
      const { invoke } = makeInvoke((command) => {
        if (command === "start_managed_surge") {
          managedStarts += 1;
          return [
            managedStarts === 1
              ? queueRow({ id: "old", status: "completed", bytesDone: 100 })
              : queueRow({ id: "new" }),
          ];
        }
        if (command === "disconnect_surge") return { ok: true };
        if (command === "list_downloads") {
          return [
            managedStarts === 1
              ? queueRow({ id: "old", status: "completed", bytesDone: 100 })
              : queueRow({ id: "new" }),
          ];
        }
        if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
        if (command === "set_download_hash_intents") {
          intents += 1;
          return writeReply.promise;
        }
        if (command === "get_download_hashes") {
          reads += 1;
          if (reads === 1) return [];
          return new Promise((resolve) => {
            releaseOldRead = () => resolve([pending("old")]);
          });
        }
        if (command === "refresh_download_hashes") {
          refreshes += 1;
          return [complete("old")];
        }
        throw new Error(`unexpected: ${command}`);
      });
      const c = new NativeQueueController(invoke, () => {}, 60_000, undefined, {
        hashRefreshMs: 1_000,
      });
      try {
        await c.startManaged();
        await c.loadHashes();

        const write = c.setHashIntents([{ id: "old", enabled: true }]);
        const oldRead = c.loadHashes();
        await waitFor(() => typeof releaseOldRead === "function", "expected the overlapping read");

        await c.disconnect();
        await c.startManaged();
        assert.deepEqual(c.state.hashes, [], "the new epoch starts empty");

        writeReply.resolve([pending("old")]);
        releaseOldRead();
        assert.equal(await write, false, "a stale write must not report success on the new epoch");
        await oldRead;

        assert.equal(intents, 1, "the stale write is never retried");
        assert.equal(reads, 2, "no follow-up read leaks into the new epoch");
        assert.equal(refreshes, 0, "a stale intent must not trigger hashing on the new epoch");
        assert.deepEqual(c.state.hashes, [], "stale rows never cross the reconnect fence");
      } finally {
        c.dispose();
      }
    },
  );
});
