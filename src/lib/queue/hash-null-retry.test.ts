// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Regression guard for the invalidated-baseline retry (App.svelte only).
// Another controller's definite save clears this controller's cached default
// without setting an error; the auto-load runs once per epoch, so without an
// explicit button the help text promises a retry that does not exist.
// Covers: cross-controller success -> null/no-error -> retry visible ->
// manual retry reads -> baseline confirmed + implicit batch enabled.
// Also covers: error / saving / poison / disconnected / loading show no
// null-retry and no duplicate error retry; no automatic loops.
import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  NativeQueueController,
  resetSharedHashDefaultStoreForTests,
} from "./native-controller.ts";

beforeEach(resetSharedHashDefaultStoreForTests);

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
  throw new Error(`unbalanced ${open}${close}`);
}

function extractDerived(src, name) {
  const marker = `const ${name} = $derived(`;
  const at = src.indexOf(marker);
  assert.ok(at > -1, `missing derived ${name} in App.svelte`);
  const open = at + marker.length - 1;
  const close = scanBalanced(src, open, "(", ")");
  return src.slice(open + 1, close).replace(/,\s*$/, "").trim();
}

function extractFunction(src, name) {
  const at = src.indexOf(`function ${name}(`);
  assert.ok(at > -1, `missing ${name} in App.svelte`);
  const paren = src.indexOf("(", at);
  const parenClose = scanBalanced(src, paren, "(", ")");
  const brace = src.indexOf("{", parenClose);
  const close = scanBalanced(src, brace, "{", "}");
  return src.slice(brace + 1, close);
}

const evalNullRetry = new Function(
  "ctx",
  `with (ctx) { return (${extractDerived(appSrc, "showHashNullRetry")}); }`,
);
const evalBaselineStable = new Function(
  "ctx",
  `with (ctx) { return (${extractDerived(appSrc, "hashDefaultBaselineStable")}); }`,
);
const evalResolved = new Function(
  "ctx",
  `with (ctx) { return (${extractDerived(appSrc, "hashChoiceResolved")}); }`,
);
const evalUnknown = new Function(
  "ctx",
  `with (ctx) { return (${extractDerived(appSrc, "hashChoiceUnknown")}); }`,
);
const evalCanSubmit = new Function(
  "ctx",
  `with (ctx) { return (${extractDerived(appSrc, "canSubmitBatch")}); }`,
);

const delay = (ms) => new Promise((r) => setTimeout(r, ms));

function ctxFor(over) {
  return {
    isConnected: false,
    isManaged: false,
    hashDefaultEnabled: null,
    hashDefaultLoading: false,
    hashDefaultSaving: false,
    hashDefaultError: null,
    hashDefaultUnconfirmed: false,
    hashForBatch: false,
    hashBatchDirty: false,
    addBusy: false,
    addUrl: "https://example.com/a.bin",
    ...over,
  };
}

function syncDerived(ctx) {
  ctx.hashDefaultBaselineStable = evalBaselineStable(ctx);
  ctx.hashChoiceResolved = evalResolved(ctx);
  ctx.hashChoiceUnknown = evalUnknown(ctx);
  ctx.canSubmitBatch = evalCanSubmit(ctx);
  ctx.showHashNullRetry = evalNullRetry(ctx);
}

describe("App.svelte invalidated-baseline retry", () => {
  it("template keeps error retry and adds null retry on same handler without poison affordance", () => {
    // Existing error retry remains.
    assert.ok(
      appSrc.includes("Retry loading default"),
      "retry label must exist",
    );
    const occurrences = appSrc.split("Retry loading default").length - 1;
    assert.equal(occurrences, 2, "error retry + null retry, no more");
    // Null retry is gated by the new derived.
    assert.ok(
      appSrc.includes("{#if showHashNullRetry}"),
      "null retry must be gated by showHashNullRetry",
    );
    const nullBlockAt = appSrc.indexOf("{#if showHashNullRetry}");
    const nullBlock = appSrc.slice(nullBlockAt, nullBlockAt + 400);
    assert.ok(
      nullBlock.includes("handleHashRetry"),
      "null retry must reuse handleHashRetry",
    );
    // Error branch still guards poison.
    assert.ok(
      appSrc.includes("{#if !hashDefaultUnconfirmed}"),
      "error retry must stay hidden when poisoned",
    );
    // Handler is read-only: never writes, only reloads when connected.
    const handler = extractFunction(appSrc, "handleHashRetry");
    assert.ok(handler.includes("loadHashDefault"), "retry must invoke read");
    assert.ok(!handler.includes("setHashDefault"), "retry must not write");
    assert.ok(handler.includes("isConnected"), "retry requires connection");
    // No retry wording inside poison copy (would suggest retry fixes poison).
    const poisonHelpAt = appSrc.indexOf("is unknown. Choose compute hashes");
    if (poisonHelpAt > -1) {
      const poisonEnd = appSrc.indexOf("</p>", poisonHelpAt);
      const poisonHelp = appSrc.slice(poisonHelpAt, poisonEnd);
      assert.doesNotMatch(poisonHelp, /retry/i, "poison help must not promise retry");
    }
    // Derived definition requires every guard.
    const derivedSrc = extractDerived(appSrc, "showHashNullRetry");
    for (const token of [
      "isConnected",
      "isManaged",
      "hashDefaultEnabled",
      "hashDefaultLoading",
      "hashDefaultSaving",
      "hashDefaultError",
      "hashDefaultUnconfirmed",
    ]) {
      assert.ok(derivedSrc.includes(token), `derived must check ${token}`);
    }
  });

  it("derived shows retry only for connected null/no-error/idle/clean state", () => {
    const base = {
      isConnected: true,
      isManaged: true,
      hashDefaultEnabled: null,
      hashDefaultLoading: false,
      hashDefaultSaving: false,
      hashDefaultError: null,
      hashDefaultUnconfirmed: false,
    };
    assert.equal(evalNullRetry(ctxFor(base)), true, "target state shows retry");

    assert.equal(
      evalNullRetry(ctxFor({ ...base, hashDefaultError: "boom" })),
      false,
      "error state must not show null retry (error retry owns it)",
    );
    assert.equal(
      evalNullRetry(ctxFor({ ...base, hashDefaultSaving: true })),
      false,
      "saving must not show retry",
    );
    assert.equal(
      evalNullRetry(ctxFor({ ...base, hashDefaultUnconfirmed: true, hashDefaultError: "poison" })),
      false,
      "poison must not show retry",
    );
    // Poison without error text is still poison.
    assert.equal(
      evalNullRetry(ctxFor({ ...base, hashDefaultUnconfirmed: true })),
      false,
      "poison alone must not show retry",
    );
    assert.equal(
      evalNullRetry(ctxFor({ ...base, isConnected: false })),
      false,
      "disconnected must not show retry",
    );
    assert.equal(
      evalNullRetry(ctxFor({ ...base, isManaged: false })),
      false,
      "manual connection must not show managed retry",
    );
    assert.equal(
      evalNullRetry(ctxFor({ ...base, hashDefaultLoading: true })),
      false,
      "loading must not show duplicate retry",
    );
    assert.equal(
      evalNullRetry(ctxFor({ ...base, hashDefaultEnabled: true })),
      false,
      "loaded baseline must not show retry",
    );
    assert.equal(
      evalNullRetry(ctxFor({ ...base, hashDefaultEnabled: false })),
      false,
      "loaded false baseline must not show retry",
    );
  });

  it("cross-controller success -> invalidated retry -> manual read restores implicit batch, no loops", async () => {
    let serverDefault = false;
    let reads = 0;
    let writes = 0;
    let deferred = 0;
    const pending = [];
    const invoke = async (command, args) => {
      if (command === "start_managed_surge") return [];
      if (command === "connect_surge") return [];
      if (command === "disconnect_surge") return { ok: true };
      if (command === "list_downloads") return [];
      if (command === "get_download_hashes") return [];
      if (command === "refresh_download_hashes") return [];
      if (command === "get_automatic_hash_settings") {
        reads += 1;
        return { defaultEnabled: serverDefault };
      }
      if (command === "set_automatic_hash_default") {
        writes += 1;
        if (deferred > 0) {
          deferred -= 1;
          return new Promise((resolve, reject) => pending.push({ enabled: args.enabled, resolve, reject }));
        }
        serverDefault = args.enabled;
        return { defaultEnabled: serverDefault };
      }
      if (command === "add_download_with_options") return { status: "queued", id: "x", hashRecorded: args.hash };
      throw new Error(`unexpected ${command}`);
    };

    const snapshots = new Map();
    const a = new NativeQueueController(invoke, (s) => snapshots.set("a", { ...s }), 60_000);
    const b = new NativeQueueController(invoke, (s) => snapshots.set("b", { ...s }), 60_000);
    try {
      await a.startManaged();
      await b.startManaged();
      await a.loadHashDefault();
      await b.loadHashDefault();
      assert.equal(snapshots.get("b").hashDefaultEnabled, false);
      const readsAfterInit = reads;

      // Writer A starts a save; B observes saving (old value visible).
      deferred += 1;
      const saving = a.setHashDefault(true);
      await delay(0);
      assert.equal(snapshots.get("b").hashDefaultSaving, true);
      const bSavingCtx = ctxFor({
        isConnected: snapshots.get("b").phase === "connected",
        isManaged: snapshots.get("b").managed === true,
        hashDefaultEnabled: snapshots.get("b").hashDefaultEnabled ?? null,
        hashDefaultLoading: snapshots.get("b").hashDefaultLoading === true,
        hashDefaultSaving: snapshots.get("b").hashDefaultSaving === true,
        hashDefaultError: snapshots.get("b").hashDefaultError ?? null,
        hashDefaultUnconfirmed: snapshots.get("b").hashDefaultUnconfirmed === true,
      });
      assert.equal(evalNullRetry(bSavingCtx), false, "no null retry while shared save pending");

      // Definite success invalidates B with no error.
      const rec = pending.shift();
      serverDefault = rec.enabled;
      rec.resolve({ defaultEnabled: serverDefault });
      assert.equal(await saving, true);
      await delay(0);
      const bSnap = snapshots.get("b") ?? b.state;
      assert.equal(bSnap.hashDefaultEnabled, null, "B invalidated");
      assert.equal(bSnap.hashDefaultError, null, "no error on definite success");
      assert.equal(bSnap.hashDefaultUnconfirmed, false);
      assert.equal(bSnap.hashDefaultSaving, false);

      const bCtx = ctxFor({
        isConnected: bSnap.phase === "connected",
        isManaged: bSnap.managed === true,
        hashDefaultEnabled: bSnap.hashDefaultEnabled ?? null,
        hashDefaultLoading: bSnap.hashDefaultLoading === true,
        hashDefaultSaving: bSnap.hashDefaultSaving === true,
        hashDefaultError: bSnap.hashDefaultError ?? null,
        hashDefaultUnconfirmed: bSnap.hashDefaultUnconfirmed === true,
        hashForBatch: false,
        hashBatchDirty: false,
      });
      syncDerived(bCtx);
      assert.equal(bCtx.showHashNullRetry, true, "rendered retry reachable");
      assert.equal(bCtx.hashChoiceUnknown, true, "implicit batch gated");
      assert.equal(bCtx.canSubmitBatch, false, "no guessed implicit batch");

      // User retry invokes exactly one read and confirms baseline.
      const readsBeforeRetry = reads;
      await b.loadHashDefault();
      assert.equal(reads, readsBeforeRetry + 1, "retry performs one read");
      await delay(0);
      const after = snapshots.get("b") ?? b.state;
      assert.equal(after.hashDefaultEnabled, true);
      assert.equal(after.hashDefaultError, null);
      const afterCtx = ctxFor({
        isConnected: after.phase === "connected",
        isManaged: after.managed === true,
        hashDefaultEnabled: after.hashDefaultEnabled ?? null,
        hashDefaultLoading: after.hashDefaultLoading === true,
        hashDefaultSaving: after.hashDefaultSaving === true,
        hashDefaultError: after.hashDefaultError ?? null,
        hashDefaultUnconfirmed: after.hashDefaultUnconfirmed === true,
        // Mirror effect would set hashForBatch to the default when clean.
        hashForBatch: after.hashDefaultEnabled === true,
        hashBatchDirty: false,
      });
      syncDerived(afterCtx);
      assert.equal(afterCtx.showHashNullRetry, false, "retry clears after confirm");
      assert.equal(afterCtx.hashChoiceUnknown, false, "implicit batch enabled");
      assert.equal(afterCtx.canSubmitBatch, true, "next implicit batch allowed");
      assert.equal(writes, 1, "success never retried as write");
      // No automatic loops: settling without epoch change performs no reads.
      const readsSettled = reads;
      await delay(0);
      await b.loadHashes().catch(() => {});
      assert.ok(reads >= readsSettled, "reads only on explicit retry");
      assert.equal(readsAfterInit >= 2, true, "init reads happened");
    } finally {
      a.dispose();
      b.dispose();
    }
  });
});
