// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Regression guard for the App.svelte hash auto-load effect.
// Snapshot cloning changes the rows identity on every emit; before the
// per-epoch claim, an empty or failed read re-triggered loadHashes forever.
// node --test cannot import .svelte, so the effect body is extracted from
// source and run against a real NativeQueueController behind a fake bridge.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { NativeQueueController } from "./native-controller.ts";

const here = path.dirname(fileURLToPath(import.meta.url));
const appSrc = fs.readFileSync(path.join(here, "..", "App.svelte"), "utf8");

function scanBalanced(src, openIndex, open, close) {
  let depth = 0;
  let quote = null;
  for (let i = openIndex; i < src.length; i++) {
    const ch = src[i];
    if (quote !== null) {
      if (ch === "\\") {
        i += 1;
        continue;
      }
      if (ch === quote) quote = null;
      continue;
    }
    if (ch === '"' || ch === "'" || ch === "`") {
      quote = ch;
      continue;
    }
    if (ch === "/" && src[i + 1] === "/") {
      while (i < src.length && src[i] !== "\n") i += 1;
      continue;
    }
    if (ch === open) depth += 1;
    else if (ch === close) {
      depth -= 1;
      if (depth === 0) return i;
    }
  }
  throw new Error(`unbalanced ${open}${close} block in App.svelte`);
}

function extractEffects(src) {
  const bodies = [];
  const re = /\$effect\(\(\)\s*=>\s*\{/g;
  let match;
  while ((match = re.exec(src)) !== null) {
    const open = src.indexOf("{", match.index);
    const close = scanBalanced(src, open, "{", "}");
    bodies.push(src.slice(open + 1, close));
  }
  return bodies;
}

const effectBodies = extractEffects(appSrc);
const hashLoadBody = effectBodies.find((body) => body.includes("tryClaimHashesAutoLoad"));
assert.ok(hashLoadBody, "missing per-epoch hash auto-load claim in App.svelte");
assert.ok(
  !hashLoadBody.includes("known.length > 0"),
  "hash auto-load must not depend on the rows identity",
);

const runHashLoad = new Function(
  "ctx",
  `with (ctx) { (() => { ${hashLoadBody} })(); }`,
);

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

const IDENTITY = (fn) => fn();
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function createHarness(handler) {
  const { invoke, calls } = makeInvoke(handler);
  let snapshot = null;
  const controller = new NativeQueueController(
    invoke,
    (next) => {
      // Snapshot cloning changes array identity even when the rows are equal.
      snapshot = { ...next, hashes: [...(next.hashes ?? [])] };
    },
    60_000,
  );
  snapshot = { ...controller.state, hashes: [] };

  const ctx = {
    isTauri: true,
    controller,
    untrack: IDENTITY,
    snapshot,
  };

  function runEffects() {
    ctx.snapshot = snapshot;
    runHashLoad(ctx);
  }

  async function settle(rounds = 8) {
    for (let i = 0; i < rounds; i += 1) {
      await delay(0);
      runEffects();
    }
  }

  return {
    controller,
    calls,
    ctx,
    runEffects,
    settle,
    get snapshot() {
      return snapshot;
    },
  };
}

function reads(calls, command) {
  return calls.filter((call) => call.command === command).length;
}

describe("App.svelte hash auto-load effect", () => {
  it("loads an empty hash list once per epoch without looping", async () => {
    const h = createHarness((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "list_downloads") return [];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      if (command === "get_download_hashes") return [];
      throw new Error(`unexpected command: ${command}`);
    });
    try {
      await h.controller.startManaged();
      await h.settle();
      assert.deepEqual(h.snapshot.hashes, []);
      assert.equal(h.snapshot.hashError, null);
      assert.equal(reads(h.calls, "get_download_hashes"), 1, "empty load runs once");

      await h.settle(12);
      await delay(40);
      assert.equal(
        reads(h.calls, "get_download_hashes"),
        1,
        "snapshot cloning must not re-trigger an empty load",
      );
    } finally {
      h.controller.dispose();
    }
  });

  it("does not loop after a failed load and recovers only on explicit retry", async () => {
    let failing = true;
    const h = createHarness((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "list_downloads") return [];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      if (command === "get_download_hashes") {
        if (failing) throw "network";
        return [{ id: "dl-1", state: "pending" }];
      }
      if (command === "refresh_download_hashes") return [{ id: "dl-1", state: "pending" }];
      throw new Error(`unexpected command: ${command}`);
    });
    try {
      await h.controller.startManaged();
      await h.settle();
      assert.equal(
        reads(h.calls, "get_download_hashes"),
        1,
        "the failed automatic load runs once",
      );
      assert.ok(h.snapshot.hashError, "the failure stays visible");

      await h.settle(12);
      await delay(40);
      assert.equal(
        reads(h.calls, "get_download_hashes"),
        1,
        "a failed load must not retry from an effect rerun",
      );

      failing = false;
      await h.controller.refreshHashes();
      await h.settle();
      assert.equal(
        reads(h.calls, "get_download_hashes"),
        1,
        "explicit refresh uses its own validated command",
      );
      assert.equal(h.snapshot.hashError, null);
      assert.deepEqual(
        (h.snapshot.hashes ?? []).map((row) => row.id),
        ["dl-1"],
        "explicit retry is the only recovery path",
      );
    } finally {
      h.controller.dispose();
    }
  });
});
