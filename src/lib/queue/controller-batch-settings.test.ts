// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Fake invoke bridge only: no daemon, no real token, no network.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { NativeQueueController, projectBatchRecovery } from "./native-controller.ts";
import { mergeRecoverableLines, redactUrlForDisplay } from "./queue.ts";

const TEST_ENDPOINT = "http://127.0.0.1:9";
const TEST_TOKEN = "test-only-token-batch-settings";

function makeInvoke(handler) {
  const calls = [];
  const invoke = async (command, args) => {
    calls.push({ command, args });
    return handler(command, args);
  };
  return { invoke, calls };
}

function urls(...names) {
  return names.map((name) => `https://example.com/${name}.bin`).join("\n");
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

describe("NativeQueueController batch add", () => {
  it("submits sequentially and reports accepted, pending, duplicate and ambiguous outcomes without retrying", async () => {
    let active = 0;
    let maxActive = 0;
    const seen = [];
    const { invoke, calls } = makeInvoke((command, args) => {
      if (command === "connect_surge") return [];
      if (command === "list_downloads") return [];
      if (command === "add_download") {
        const url = args.url;
        seen.push(url);
        active += 1;
        maxActive = Math.max(maxActive, active);
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            active -= 1;
            if (url.endsWith("/a.bin")) resolve("queued");
            else if (url.endsWith("/b.bin")) resolve("duplicate");
            else if (url.endsWith("/c.bin")) resolve("pendingApproval");
            else reject("invalidresponse");
          }, 5);
        });
      }
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.phase, "connected");
      const result = await c.addBatch(urls("a", "b", "c", "d", "e"));
      assert.equal(maxActive, 1, "submissions must be sequential, never concurrent");
      assert.deepEqual(
        seen.map((url) => url.split("/").pop()),
        ["a.bin", "b.bin", "c.bin", "d.bin"],
        "the batch must stop on an unconfirmed request and never attempt the tail",
      );
      assert.deepEqual(
        result.results.map((entry) => entry.status),
        ["queued", "duplicate", "pendingApproval", "ambiguous", "unattempted"],
      );
      assert.equal(result.results[3].line, "https://example.com/d.bin");
      assert.equal(result.results[4].line, "https://example.com/e.bin");
      assert.deepEqual(result.recoverable, [
        "https://example.com/d.bin",
        "https://example.com/e.bin",
      ]);
      assert.equal(c.state.batch.running, false);
      assert.equal(c.state.batch.stopped, true);
      assert.match(c.state.batch.stopReason, /unconfirmed/i);
      assert.equal(
        calls.filter((call) => call.command === "add_download").length,
        4,
        "an ambiguous request must not be retried",
      );
      assert.ok(!JSON.stringify(c.state).includes(TEST_TOKEN), "state must not leak the token");
    } finally {
      c.dispose();
    }
  });

  it("classifies invalid lines locally, keeps them recoverable, and never sends them", async () => {
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [];
      if (command === "list_downloads") return [];
      if (command === "add_download") return "queued";
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      const result = await c.addBatch(
        "ftp://example.com/bad\nhttps://example.com/good\nhttps://user:pass@example.com/bad",
      );
      assert.deepEqual(
        result.results.map((entry) => entry.status),
        ["invalid", "queued", "invalid"],
      );
      assert.deepEqual(result.recoverable, [
        "ftp://example.com/bad",
        "https://user:pass@example.com/bad",
      ]);
      const addCalls = calls.filter((call) => call.command === "add_download");
      assert.equal(addCalls.length, 1, "only the valid URL may be sent");
      assert.equal(addCalls[0].args.url, "https://example.com/good");
    } finally {
      c.dispose();
    }
  });

  it("keeps malformed query secrets out of displayed outcomes but recoverable for editing", async () => {
    const secretUrl = "https://[bad?token=secret123";
    const { invoke } = makeInvoke((command) => {
      if (command === "connect_surge") return [];
      if (command === "disconnect_surge") return { ok: true };
      if (command === "list_downloads") return [];
      if (command === "add_download") return "queued";
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      const result = await c.addBatch(secretUrl);
      assert.equal(result.results[0].status, "invalid");
      assert.ok(
        !result.results[0].line.includes("secret123"),
        "the redacted display label must not echo the malformed query secret",
      );
      assert.ok(
        !JSON.stringify(c.state.batch?.results ?? []).includes("secret123"),
        "no emitted batch display state may echo the secret",
      );
      assert.deepEqual(
        result.recoverable,
        [secretUrl],
        "the full original line stays only in the editable recovery path",
      );
    } finally {
      c.dispose();
    }
  });

  it("does not record a detached recovery for a normally completed batch with invalid lines", async () => {
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [];
      if (command === "disconnect_surge") return { ok: true };
      if (command === "list_downloads") return [];
      if (command === "add_download") return "queued";
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      const result = await c.addBatch(
        "ftp://example.com/bad\nhttps://example.com/good.bin",
      );
      assert.deepEqual(
        result.results.map((entry) => entry.status),
        ["invalid", "queued"],
      );
      assert.deepEqual(result.recoverable, ["ftp://example.com/bad"]);
      assert.equal(
        calls.filter((call) => call.command === "add_download").length,
        1,
        "the invalid line must never be sent",
      );
      assert.equal(c.state.batch.running, false);
      assert.equal(c.state.batch.stopped, false, "invalid lines alone do not interrupt the batch");
      assert.equal(
        c.drainBatchRecoveries().length,
        0,
        "a normal completed batch must not be reported as an interrupted recovery",
      );
    } finally {
      c.dispose();
    }
  });

  it("stops on disconnect and leaves the current and remaining links unattempted", async () => {
    let controller = null;
    const { invoke, calls } = makeInvoke(async (command, args) => {
      if (command === "connect_surge") return [];
      if (command === "disconnect_surge") return { ok: true };
      if (command === "list_downloads") return [];
      if (command === "add_download") {
        if (args.url.endsWith("/one.bin")) {
          await controller.disconnect();
          return "queued";
        }
        return "queued";
      }
      throw new Error(`unexpected command: ${command}`);
    });
    controller = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await controller.connect(TEST_ENDPOINT, TEST_TOKEN);
      const result = await controller.addBatch(urls("one", "two", "three"));
      assert.deepEqual(
        result.results.map((entry) => entry.status),
        ["ambiguous", "unattempted", "unattempted"],
      );
      assert.deepEqual(result.recoverable, [
        "https://example.com/one.bin",
        "https://example.com/two.bin",
        "https://example.com/three.bin",
      ]);
      assert.equal(
        calls.filter((call) => call.command === "add_download").length,
        1,
        "no link after a disconnect may be attempted",
      );
      assert.equal(controller.state.phase, "offline");
    } finally {
      controller.dispose();
    }
  });

  it("bounds a batch to 100 submitted links and keeps the rest recoverable", async () => {
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [];
      if (command === "list_downloads") return [];
      if (command === "add_download") return "duplicate";
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      const lines = Array.from({ length: 102 }, (_, index) => `https://example.com/${index}.bin`);
      const result = await c.addBatch(lines.join("\n"));
      assert.equal(
        calls.filter((call) => call.command === "add_download").length,
        100,
        "only the first 100 lines may be submitted",
      );
      assert.equal(result.results[99].status, "duplicate");
      assert.equal(result.results[100].status, "unattempted");
      assert.equal(result.results[101].status, "unattempted");
      assert.deepEqual(result.recoverable, [lines[100], lines[101]]);
    } finally {
      c.dispose();
    }
  });

  it("does not let a stale batch A settlement overwrite active batch B after reconnect", async () => {
    const releaseA = deferred();
    const releaseB = deferred();
    const { invoke, calls } = makeInvoke((command, args) => {
      if (command === "connect_surge") return [];
      if (command === "disconnect_surge") return { ok: true };
      if (command === "list_downloads") return [];
      if (command === "add_download") {
        if (args.url.endsWith("/a.bin")) return releaseA.promise;
        if (args.url.endsWith("/b1.bin")) return releaseB.promise;
        if (args.url.endsWith("/b2.bin")) return "duplicate";
        throw new Error(`unexpected add url: ${args.url}`);
      }
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      const batchA = c.addBatch(urls("a", "a2", "a3"));
      assert.equal(c.state.batch.total, 3, "batch A owns the visible summary");
      assert.equal(c.state.batch.running, true);

      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.batch, null, "reconnect clears the old batch summary");
      const batchB = c.addBatch(urls("b1", "b2"));
      assert.equal(c.state.batch.total, 2, "batch B owns the visible summary");
      assert.equal(c.state.batch.results.length, 0);

      releaseA.resolve("queued");
      const resultA = await batchA;
      assert.deepEqual(
        resultA.results.map((entry) => entry.status),
        ["ambiguous", "unattempted", "unattempted"],
      );
      assert.deepEqual(resultA.recoverable, [
        "https://example.com/a.bin",
        "https://example.com/a2.bin",
        "https://example.com/a3.bin",
      ]);
      assert.equal(c.state.batch.total, 2, "stale A must not overwrite B total");
      assert.equal(c.state.batch.running, true, "stale A must not finish B");
      assert.equal(c.state.batch.results.length, 0, "stale A results must not enter B");
      assert.equal(c.state.batch.stopped, false);
      assert.equal(c.state.batch.stopReason, null);

      releaseB.resolve("pendingApproval");
      const resultB = await batchB;
      assert.deepEqual(
        resultB.results.map((entry) => entry.status),
        ["pendingApproval", "duplicate"],
      );
      assert.equal(c.state.batch.running, false);
      assert.equal(c.state.batch.total, 2);
      assert.deepEqual(
        c.state.batch.results.map((entry) => entry.line),
        ["https://example.com/b1.bin", "https://example.com/b2.bin"],
      );
      assert.equal(
        calls.filter((call) => call.command === "add_download").length,
        3,
        "stale A must not trigger extra sends after reconnect",
      );
    } finally {
      c.dispose();
    }
  });

  it("deduplicates byte-exact repeated links before sending and labels the repeat honestly", async () => {
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [];
      if (command === "list_downloads") return [];
      if (command === "add_download") return "pendingApproval";
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      const repeated = "https://example.com/Same.bin";
      const caseVariant = "https://example.com/same.bin";
      const result = await c.addBatch([repeated, repeated, caseVariant].join("\n"));
      const addCalls = calls.filter((call) => call.command === "add_download");
      assert.deepEqual(
        addCalls.map((call) => call.args.url),
        [repeated, caseVariant],
        "only the first exact literal and a genuinely different literal may be sent",
      );
      assert.equal(addCalls.length, 2, "a repeated line must not create a second pending approval");
      assert.deepEqual(
        result.results.map((entry) => entry.status),
        ["pendingApproval", "duplicateInBatch", "pendingApproval"],
      );
      assert.match(result.results[1].message, /earlier in the batch/i);
      assert.deepEqual(result.recoverable, [], "a repeated link is not recoverable for resend");
      assert.equal(c.state.batch.results[1].status, "duplicateInBatch");
    } finally {
      c.dispose();
    }
  });
});

describe("NativeQueueController detached batch recovery", () => {
  it("recovers the in-flight ambiguous link and unsent tail across reconnect without a newer draft or retry", async () => {
    const head = "https://example.com/head.bin?signature=head-secret";
    const inFlight = "https://example.com/a.bin?signature=in-flight-secret";
    const tailOne = "https://example.com/b.bin?signature=tail-secret";
    const tailTwo = "https://example.com/c.bin?signature=tail2-secret";
    const newUrl = "https://example.com/new.bin?signature=new-secret";
    const newerDraft = "https://draft.example/typed.bin?signature=draft-secret";

    const aStarted = deferred();
    const newStarted = deferred();
    const releaseA = deferred();
    const releaseNew = deferred();
    const seen = [];

    const { invoke, calls } = makeInvoke((command, args) => {
      if (command === "connect_surge") return [];
      if (command === "disconnect_surge") return { ok: true };
      if (command === "list_downloads") return [];
      if (command === "add_download") {
        seen.push(args.url);
        if (args.url === head) return "queued";
        if (args.url === inFlight) {
          aStarted.resolve();
          return releaseA.promise;
        }
        if (args.url === newUrl) {
          newStarted.resolve();
          return releaseNew.promise;
        }
        throw new Error(`unexpected add url: ${args.url}`);
      }
      throw new Error(`unexpected command: ${command}`);
    });

    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      const batchA = c.addBatch([head, inFlight, tailOne, tailTwo].join("\n"));
      await aStarted.promise;

      // Disconnect while A's request is still deferred. The in-flight and
      // unsent lines must be recoverable immediately, before A settles.
      await c.disconnect();
      const recoveries = c.drainBatchRecoveries();
      assert.equal(recoveries.length, 1, "the detached batch must be recoverable");
      const recovery = recoveries[0];
      assert.equal(recovery.acceptedCount, 1, "the definitively accepted head stays distinct");
      assert.deepEqual(recovery.uncertain, [inFlight], "full signed in-flight URL is preserved");
      assert.deepEqual(recovery.unsent, [tailOne, tailTwo], "full signed unsent tail is preserved");
      assert.deepEqual(
        recovery.results.map((entry) => entry.status),
        ["queued", "ambiguous", "unattempted", "unattempted"],
      );
      assert.equal(recovery.results[0].line, redactUrlForDisplay(head));
      assert.equal(recovery.results[1].line, redactUrlForDisplay(inFlight));
      assert.ok(
        !JSON.stringify(recovery.results).includes("signature="),
        "recovery display data must stay redacted",
      );

      // Reconnect, start a newer batch, and keep a newer draft before A settles.
      let newDraft = newerDraft;
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      const batchB = c.addBatch(newUrl);
      await newStarted.promise;

      releaseA.resolve("queued");
      const resultA = await batchA;
      assert.deepEqual(
        resultA.results.map((entry) => entry.status),
        ["queued", "ambiguous", "unattempted", "unattempted"],
      );
      assert.deepEqual(resultA.recoverable, [inFlight, tailOne, tailTwo]);
      assert.equal(
        c.drainBatchRecoveries().length,
        0,
        "settling the stale batch must not duplicate the recovery",
      );

      // App-level merge: recovered lines append after the newer draft.
      newDraft = mergeRecoverableLines(newDraft, [
        ...recovery.uncertain,
        ...recovery.unsent,
        ...recovery.rejected,
      ]);
      assert.ok(newDraft.startsWith(newerDraft), "the newer draft must remain intact first");
      assert.ok(newDraft.includes(inFlight), "the uncertain signed URL is restorable");
      assert.ok(newDraft.includes(tailOne), "the first unsent signed URL is restorable");
      assert.ok(newDraft.includes(tailTwo), "the second unsent signed URL is restorable");

      const attempted = calls.filter((call) => call.command === "add_download");
      assert.equal(
        attempted.filter((call) => call.args.url === inFlight).length,
        1,
        "the in-flight link must never be retried automatically",
      );
      assert.equal(
        attempted.filter((call) => call.args.url === tailOne).length,
        0,
        "the unsent tail must not be attempted after recovery",
      );
      assert.equal(
        attempted.filter((call) => call.args.url === tailTwo).length,
        0,
        "the unsent tail must not be attempted after recovery",
      );

      assert.equal(c.state.batch.total, 1, "the newer batch still owns the summary");
      assert.equal(c.state.batch.running, true);
      releaseNew.resolve("pendingApproval");
      const resultB = await batchB;
      assert.deepEqual(
        resultB.results.map((entry) => entry.status),
        ["pendingApproval"],
      );
      assert.equal(c.state.batch.running, false);
      assert.equal(c.state.batch.results[0].line, redactUrlForDisplay(newUrl));
      assert.ok(
        !JSON.stringify(c.state).includes("signature="),
        "the newer batch state must not expose signed queries",
      );
    } finally {
      c.dispose();
    }
  });

  it("keeps a queued acknowledgement accepted when reconnect happens during its refresh", async () => {
    const head = "https://example.com/head.bin?signature=head-secret";
    const tail = "https://example.com/tail.bin?signature=tail-secret";
    const addAck = deferred();
    const refreshStarted = deferred();
    const refreshGate = deferred();

    const { invoke, calls } = makeInvoke((command, args) => {
      if (command === "connect_surge") return [];
      if (command === "disconnect_surge") return { ok: true };
      if (command === "add_download") {
        if (args.url === head) return addAck.promise;
        throw new Error(`unexpected add url: ${args.url}`);
      }
      if (command === "list_downloads") {
        refreshStarted.resolve();
        return refreshGate.promise;
      }
      throw new Error(`unexpected command: ${command}`);
    });

    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      const batch = c.addBatch([head, tail].join("\n"));
      for (let i = 0; i < 5 && !calls.some((call) => call.command === "add_download"); i++) {
        await Promise.resolve();
      }
      assert.equal(calls.filter((call) => call.command === "add_download").length, 1);

      // The daemon definitively accepts the first link, then the follow-up
      // adoption refresh blocks. A reconnect during that refresh must not
      // reinterpret the queued acknowledgement as an uncertain in-flight add.
      addAck.resolve("queued");
      await refreshStarted.promise;
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);

      refreshGate.resolve([]);
      const result = await batch;
      assert.deepEqual(
        result.results.map((entry) => entry.status),
        ["queued", "unattempted"],
        "the acknowledged link stays queued; only the unsent tail is unattempted",
      );
      assert.deepEqual(
        result.recoverable,
        [tail],
        "the accepted link must not be offered for resend",
      );

      const recoveries = c.drainBatchRecoveries();
      assert.equal(recoveries.length, 1);
      assert.equal(recoveries[0].acceptedCount, 1, "the queued ack is recorded as accepted");
      assert.deepEqual(
        recoveries[0].uncertain,
        [],
        "a queued acknowledgement must never be recovered as uncertain",
      );
      assert.deepEqual(recoveries[0].unsent, [tail]);
      assert.equal(
        calls.filter((call) => call.command === "add_download").length,
        1,
        "nothing is retried automatically",
      );
    } finally {
      c.dispose();
    }
  });
});

describe("NativeQueueController recovery availability", () => {
  it("drains captured recovery synchronously while a reconnect is still blocked", async () => {
    const head = "https://example.com/head.bin?signature=head-secret";
    const inFlight = "https://example.com/a.bin?signature=in-flight-secret";
    const tail = "https://example.com/tail.bin?signature=tail-secret";
    const aStarted = deferred();
    const releaseA = deferred();
    const connectGate = deferred();
    let connects = 0;

    const { invoke } = makeInvoke((command, args) => {
      if (command === "connect_surge") {
        connects += 1;
        if (connects === 1) return [];
        return connectGate.promise;
      }
      if (command === "disconnect_surge") return { ok: true };
      if (command === "list_downloads") return [];
      if (command === "add_download") {
        if (args.url === head) return "queued";
        if (args.url === inFlight) {
          aStarted.resolve();
          return releaseA.promise;
        }
        throw new Error(`unexpected add url: ${args.url}`);
      }
      throw new Error(`unexpected command: ${command}`);
    });

    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      const batch = c.addBatch([head, inFlight, tail].join("\n"));
      await aStarted.promise;

      const reconnect = c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(c.state.phase, "connecting", "the reconnect is still blocked");
      const recoveries = c.drainBatchRecoveries();
      assert.equal(recoveries.length, 1, "recovery must be available before connect settles");
      assert.deepEqual(recoveries[0].uncertain, [inFlight]);
      assert.deepEqual(recoveries[0].unsent, [tail]);
      assert.equal(recoveries[0].results[0].line, redactUrlForDisplay(head));
      assert.ok(!JSON.stringify(recoveries[0].results).includes("signature="));
      assert.equal(c.drainBatchRecoveries().length, 0, "a drained redacted notice is not replayed");

      releaseA.resolve("queued");
      connectGate.resolve([]);
      await reconnect;
      await batch;
      assert.equal(c.state.phase, "connected");
      assert.equal(c.drainBatchRecoveries().length, 0, "stale settlement must not duplicate recovery");
    } finally {
      c.dispose();
    }
  });

  it("keeps captured recovery available when the reconnect fails", async () => {
    const head = "https://example.com/head.bin?signature=head-secret";
    const inFlight = "https://example.com/a.bin?signature=in-flight-secret";
    const tail = "https://example.com/tail.bin?signature=tail-secret";
    const aStarted = deferred();
    const releaseA = deferred();
    let connects = 0;

    const { invoke } = makeInvoke((command, args) => {
      if (command === "connect_surge") {
        connects += 1;
        if (connects === 1) return [];
        throw new Error("network down");
      }
      if (command === "disconnect_surge") return { ok: true };
      if (command === "list_downloads") return [];
      if (command === "add_download") {
        if (args.url === head) return "queued";
        if (args.url === inFlight) {
          aStarted.resolve();
          return releaseA.promise;
        }
        throw new Error(`unexpected add url: ${args.url}`);
      }
      throw new Error(`unexpected command: ${command}`);
    });

    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      const batch = c.addBatch([head, inFlight, tail].join("\n"));
      await aStarted.promise;

      const reconnect = c.connect(TEST_ENDPOINT, TEST_TOKEN);
      const recoveries = c.drainBatchRecoveries();
      assert.equal(recoveries.length, 1, "recovery is available while the reconnect is in flight");
      assert.deepEqual(recoveries[0].uncertain, [inFlight]);
      assert.deepEqual(recoveries[0].unsent, [tail]);

      await reconnect;
      assert.equal(c.state.phase, "error", "a failed connect never looks connected");
      assert.equal(c.drainBatchRecoveries().length, 0, "the same notice is never restored silently");

      releaseA.resolve("queued");
      await batch;
      assert.equal(c.drainBatchRecoveries().length, 0, "stale settlement must not duplicate recovery");
    } finally {
      c.dispose();
    }
  });
});

describe("recovered batch display projection", () => {
  it("keeps only redacted results and counts, handing raw lines off exactly once", () => {
    const secret = "https://example.com/a.bin?signature=SECRET123";
    const tail = "https://example.com/tail.bin?signature=SECRET456";
    const rejected = "mailto:alice:SECRET@example.com";
    const recovery = {
      id: 9,
      acceptedCount: 1,
      results: [
        {
          index: 0,
          line: redactUrlForDisplay(secret),
          status: "queued",
          message: "Daemon queued it.",
        },
      ],
      uncertain: [secret],
      unsent: [tail],
      rejected: [rejected],
    };

    const { view, lines } = projectBatchRecovery(recovery);
    assert.deepEqual(lines, [secret, tail, rejected], "raw lines are handed off for the draft");
    assert.deepEqual(Object.keys(view).sort(), [
      "acceptedCount",
      "id",
      "rejectedCount",
      "results",
      "uncertainCount",
      "unsentCount",
    ]);
    assert.equal(view.id, 9);
    assert.equal(view.acceptedCount, 1);
    assert.equal(view.uncertainCount, 1);
    assert.equal(view.unsentCount, 1);
    assert.equal(view.rejectedCount, 1);
    assert.ok(!JSON.stringify(view).includes("SECRET"), "the view never carries raw secrets");
    assert.ok(!JSON.stringify(view).includes("signature="), "the view never carries signed queries");

    recovery.uncertain.length = 0;
    recovery.unsent.length = 0;
    recovery.rejected.length = 0;
    recovery.results.length = 0;
    assert.deepEqual(lines, [secret, tail, rejected], "handed-off lines survive raw disposal");
    assert.equal(view.results.length, 1);
  });
});

describe("NativeQueueController max concurrent downloads setting", () => {
  it("loads and saves 1–10 on the managed connection and marks restart required", async () => {
    let saved = 3;
    const { invoke, calls } = makeInvoke((command, args) => {
      if (command === "start_managed_surge") return [];
      if (command === "get_max_concurrent_downloads") return saved;
      if (command === "set_max_concurrent_downloads") {
        assert.deepEqual(args, { before: 3, value: 5 });
        saved = args.value;
        return saved;
      }
      if (command === "disconnect_surge") return { ok: true };
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      assert.equal(await c.startManaged(), true);
      assert.equal(c.state.managed, true);
      await c.loadMaxConcurrent();
      assert.equal(c.state.maxConcurrent, 3);
      assert.equal(c.state.maxConcurrentLoading, false);
      assert.equal(await c.setMaxConcurrent(3, 5), true);
      assert.equal(c.state.maxConcurrent, 5);
      assert.equal(c.state.maxConcurrentRestartPending, true);
      assert.equal(
        calls.filter((call) => call.command === "set_max_concurrent_downloads").length,
        1,
      );
    } finally {
      c.dispose();
    }
  });

  it("rejects out-of-range choices locally and refuses writes on a manual connection", async () => {
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [];
      if (command === "disconnect_surge") return { ok: true };
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect(TEST_ENDPOINT, TEST_TOKEN);
      assert.equal(await c.setMaxConcurrent(0, 5), false);
      assert.equal(await c.setMaxConcurrent(3, 11), false);
      assert.equal(await c.setMaxConcurrent(3, 2.5), false);
      assert.equal(
        calls.filter((call) => call.command === "set_max_concurrent_downloads").length,
        0,
        "invalid values must not cross IPC",
      );
      assert.equal(await c.setMaxConcurrent(3, 5), false);
      assert.match(c.state.maxConcurrentError, /bundled daemon/i);
      assert.equal(
        calls.filter((call) => call.command === "set_max_concurrent_downloads").length,
        0,
        "manual connections must never receive the write",
      );
    } finally {
      c.dispose();
    }
  });

  it("surfaces a conflict without poisoning, and lets the user reload and retry", async () => {
    let setCalls = 0;
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "get_max_concurrent_downloads") return 4;
      if (command === "set_max_concurrent_downloads") {
        setCalls += 1;
        if (setCalls === 1) throw "settingsconflict";
        return 6;
      }
      if (command === "disconnect_surge") return { ok: true };
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadMaxConcurrent();
      assert.equal(await c.setMaxConcurrent(4, 6), false);
      assert.match(c.state.maxConcurrentError, /changed/i);
      assert.equal(c.state.maxConcurrentUnconfirmed, false, "a conflict is definitive");
      assert.equal(await c.setMaxConcurrent(4, 6), true);
      assert.equal(c.state.maxConcurrent, 6);
      assert.equal(setCalls, 2);
    } finally {
      c.dispose();
    }
  });

  it("poisons settings writes after an unconfirmed outcome, survives reconnect, but never poisons rate control", async () => {
    let setCalls = 0;
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "get_max_concurrent_downloads") return 3;
      if (command === "set_max_concurrent_downloads") {
        setCalls += 1;
        throw "settingsunconfirmed";
      }
      if (command === "get_global_rate_limit") return 0;
      if (command === "set_global_rate_limit") return 1_000_000;
      if (command === "disconnect_surge") return { ok: true };
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadMaxConcurrent();
      assert.equal(await c.setMaxConcurrent(3, 5), false);
      assert.equal(c.state.maxConcurrentUnconfirmed, true);
      assert.match(c.state.maxConcurrentError, /until the app restarts/i);
      assert.equal(setCalls, 1);

      await c.disconnect();
      assert.equal(await c.startManaged(), true);
      assert.equal(c.state.maxConcurrentUnconfirmed, true, "poison must survive reconnect");
      assert.equal(await c.setMaxConcurrent(3, 5), false);
      assert.equal(setCalls, 1, "a poisoned writer must not send another request");

      assert.equal(await c.setRateLimit(1_000_000), true);
      assert.equal(c.state.rateUnconfirmed, false, "settings poisoning must not touch rate control");
    } finally {
      c.dispose();
    }
  });

  it("discards an old settings save that completes after reconnect", async () => {
    const oldSave = deferred();
    let saved = 3;
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "get_max_concurrent_downloads") return saved;
      if (command === "set_max_concurrent_downloads") return oldSave.promise;
      if (command === "disconnect_surge") return { ok: true };
      throw new Error(`unexpected command: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadMaxConcurrent();
      assert.equal(c.state.maxConcurrent, 3);
      const pendingSave = c.setMaxConcurrent(3, 5);
      assert.equal(c.state.maxConcurrentSaving, true);

      await c.startManaged();
      assert.equal(c.state.maxConcurrent, null, "reconnect clears the old saved value");
      assert.equal(c.state.maxConcurrentSaving, false);

      oldSave.resolve(5);
      assert.equal(await pendingSave, false, "old save must not resolve true on a new epoch");
      assert.equal(c.state.maxConcurrent, null, "old save must not set the new connection value");
      assert.equal(c.state.maxConcurrentRestartPending, false);
      assert.equal(c.state.maxConcurrentError, null);
      assert.equal(
        calls.filter((call) => call.command === "set_max_concurrent_downloads").length,
        1,
      );

      saved = 4;
      await c.loadMaxConcurrent();
      assert.equal(c.state.maxConcurrent, 4, "new connection loads independently");
    } finally {
      c.dispose();
    }
  });
});
