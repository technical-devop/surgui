// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Regression guards for App.svelte rate + max-concurrent saves.
// Before the fix the handlers cleared dirty before awaiting, so a disconnect
// discarded the submitted draft and a late confirmation could overwrite a
// newer edit. The component bodies are extracted from source and run against
// a real NativeQueueController behind a fake bridge.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { NativeQueueController } from "./native-controller.ts";
import { draftAfterConnectionLoss } from "./view-tabs.ts";
import {
  formatRateBpsToMb,
  isValidMaxConcurrentValue,
  parseRateMbToBps,
} from "./queue.ts";

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

function extractHandler(src, name) {
  const at = src.indexOf(`async function ${name}`);
  assert.ok(at > -1, `missing ${name} in App.svelte`);
  const paren = src.indexOf("(", at);
  const parenClose = scanBalanced(src, paren, "(", ")");
  const brace = src.indexOf("{", parenClose);
  const close = scanBalanced(src, brace, "{", "}");
  return src.slice(brace + 1, close);
}

const effectBodies = extractEffects(appSrc);
const rateAutoLoad = effectBodies.find((body) => body.includes("tryClaimRateAutoLoad"));
const rateMirror = effectBodies.find((body) => body.includes("rateDraft = formatRateBpsToMb(known)"));
const ratePreserve = effectBodies.find(
  (body) => body.includes("draftAfterConnectionLoss") && body.includes("rateDraft"),
);
const maxAutoLoad = effectBodies.find((body) => body.includes("tryClaimMaxConcurrentAutoLoad"));
const maxMirror = effectBodies.find((body) => body.includes("maxConcurrentDraft = String(known)"));
const maxPreserve = effectBodies.find(
  (body) => body.includes("draftAfterConnectionLoss") && body.includes("maxConcurrentDraft"),
);

assert.ok(rateAutoLoad, "missing rate auto-load effect in App.svelte");
assert.ok(rateMirror, "missing rate mirror effect in App.svelte");
assert.ok(ratePreserve, "missing rate preserve effect in App.svelte");
assert.ok(maxAutoLoad, "missing max auto-load effect in App.svelte");
assert.ok(maxMirror, "missing max mirror effect in App.svelte");
assert.ok(maxPreserve, "missing max preserve effect in App.svelte");
assert.ok(
  !rateAutoLoad.includes("rateDirty) return"),
  "the rate baseline must load even with a dirty draft",
);

const compileEffect = (body) =>
  new Function("ctx", `with (ctx) { (() => { ${body} })(); }`);
const compileHandler = (body) =>
  new Function("ctx", `with (ctx) { return (async () => { ${body} })(); }`);

const runRateEffects = [rateAutoLoad, rateMirror, ratePreserve].map(compileEffect);
const runMaxEffects = [maxAutoLoad, maxMirror, maxPreserve].map(compileEffect);
const runRateSave = compileHandler(extractHandler(appSrc, "handleRateSave"));
const runMaxSave = compileHandler(extractHandler(appSrc, "handleMaxConcurrentSave"));

const IDENTITY = (fn) => fn();
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function waitFor(predicate, message, timeout = 1500) {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeout;
    const tick = () => {
      if (predicate()) return resolve();
      if (Date.now() > deadline) return reject(new Error(message));
      setTimeout(tick, 2);
    };
    tick();
  });
}

function createRateHarness() {
  const daemon = { rate: 2_000_000 };
  const reads = [];
  const writes = [];
  let holdWrites = false;
  let releaseWrite = null;

  const invoke = async (command, args) => {
    if (command === "start_managed_surge") return [];
    if (command === "disconnect_surge") return { ok: true };
    if (command === "list_downloads") return [];
    if (command === "get_global_rate_limit") {
      reads.push(daemon.rate);
      return daemon.rate;
    }
    if (command === "set_global_rate_limit") {
      writes.push({ rate: args.rate });
      if (holdWrites) {
        return new Promise((resolve) => {
          releaseWrite = () => {
            daemon.rate = args.rate;
            resolve(daemon.rate);
          };
        });
      }
      daemon.rate = args.rate;
      return daemon.rate;
    }
    throw new Error(`unexpected command: ${command}`);
  };

  let snapshot = null;
  const controller = new NativeQueueController(
    invoke,
    (next) => {
      snapshot = { ...next, hashes: [...(next.hashes ?? [])] };
    },
    60_000,
  );
  snapshot = { ...controller.state, hashes: [] };

  const ctx = {
    event: { preventDefault() {} },
    isTauri: true,
    controller,
    snapshot,
    untrack: IDENTITY,
    draftAfterConnectionLoss,
    formatRateBpsToMb,
    parseRateMbToBps,
    rateDraft: "",
    rateDirty: false,
    rateTouched: false,
    rateSaveSeq: 0,
    isManaged: false,
    isConnected: false,
    rateSaving: false,
    rateLoading: false,
    rateUnconfirmed: false,
  };

  function syncDerived() {
    ctx.isManaged = snapshot.managed === true;
    ctx.isConnected = snapshot.phase === "connected";
    ctx.rateSaving = snapshot.rateSaving === true;
    ctx.rateLoading = snapshot.rateLoading === true;
    ctx.rateUnconfirmed = snapshot.rateUnconfirmed === true;
  }

  function runEffects() {
    ctx.snapshot = snapshot;
    syncDerived();
    for (const run of runRateEffects) run(ctx);
  }

  async function settle(rounds = 8) {
    for (let i = 0; i < rounds; i += 1) {
      await delay(0);
      runEffects();
    }
  }

  function editRate(value) {
    ctx.rateDraft = value;
    ctx.rateDirty = true;
    ctx.rateTouched = true;
    runEffects();
  }

  return {
    controller,
    daemon,
    reads,
    writes,
    ctx,
    runEffects,
    settle,
    editRate,
    save() {
      return runRateSave(ctx);
    },
    holdWrites() {
      holdWrites = true;
    },
    releaseWrite() {
      assert.ok(releaseWrite, "expected a pending rate write");
      releaseWrite();
    },
    hasPendingWrite() {
      return typeof releaseWrite === "function";
    },
    get snapshot() {
      return snapshot;
    },
  };
}

function createMaxHarness() {
  const daemon = { maxConcurrent: 3 };
  const reads = [];
  const writes = [];
  let holdWrites = false;
  let releaseWrite = null;

  const invoke = async (command, args) => {
    if (command === "start_managed_surge") return [];
    if (command === "disconnect_surge") return { ok: true };
    if (command === "list_downloads") return [];
    if (command === "get_max_concurrent_downloads") {
      reads.push(daemon.maxConcurrent);
      return daemon.maxConcurrent;
    }
    if (command === "set_max_concurrent_downloads") {
      writes.push({ before: args.before, value: args.value });
      if (holdWrites) {
        return new Promise((resolve) => {
          releaseWrite = () => {
            daemon.maxConcurrent = args.value;
            resolve(daemon.maxConcurrent);
          };
        });
      }
      daemon.maxConcurrent = args.value;
      return daemon.maxConcurrent;
    }
    throw new Error(`unexpected command: ${command}`);
  };

  let snapshot = null;
  const controller = new NativeQueueController(
    invoke,
    (next) => {
      snapshot = { ...next, hashes: [...(next.hashes ?? [])] };
    },
    60_000,
  );
  snapshot = { ...controller.state, hashes: [] };

  const ctx = {
    event: { preventDefault() {} },
    isTauri: true,
    controller,
    snapshot,
    untrack: IDENTITY,
    draftAfterConnectionLoss,
    isValidMaxConcurrentValue,
    maxConcurrentDraft: "",
    maxConcurrentDirty: false,
    maxConcurrentTouched: false,
    maxConcurrentSaveSeq: 0,
    isManaged: false,
    isConnected: false,
    maxConcurrent: null,
    maxConcurrentLoading: false,
    maxConcurrentSaving: false,
    maxConcurrentUnconfirmed: false,
    maxConcurrentParsed: null,
  };

  function syncDerived() {
    ctx.isManaged = snapshot.managed === true;
    ctx.isConnected = snapshot.phase === "connected";
    ctx.maxConcurrent = snapshot.maxConcurrent ?? null;
    ctx.maxConcurrentLoading = snapshot.maxConcurrentLoading === true;
    ctx.maxConcurrentSaving = snapshot.maxConcurrentSaving === true;
    ctx.maxConcurrentUnconfirmed = snapshot.maxConcurrentUnconfirmed === true;
    ctx.maxConcurrentParsed =
      ctx.maxConcurrentDraft === "" ? null : Number(ctx.maxConcurrentDraft);
  }

  function runEffects() {
    ctx.snapshot = snapshot;
    syncDerived();
    for (const run of runMaxEffects) run(ctx);
  }

  async function settle(rounds = 8) {
    for (let i = 0; i < rounds; i += 1) {
      await delay(0);
      runEffects();
    }
  }

  function editMax(value) {
    ctx.maxConcurrentDraft = value;
    ctx.maxConcurrentDirty = true;
    ctx.maxConcurrentTouched = true;
    runEffects();
  }

  return {
    controller,
    daemon,
    reads,
    writes,
    ctx,
    runEffects,
    settle,
    editMax,
    save() {
      return runMaxSave(ctx);
    },
    holdWrites() {
      holdWrites = true;
    },
    releaseWrite() {
      assert.ok(releaseWrite, "expected a pending max-concurrent write");
      releaseWrite();
    },
    hasPendingWrite() {
      return typeof releaseWrite === "function";
    },
    get snapshot() {
      return snapshot;
    },
  };
}

describe("App.svelte settings saves preserve submitted drafts", () => {
  it("rate: reloads the baseline across reconnect without overwriting a dirty draft", async () => {
    const h = createRateHarness();
    try {
      await h.controller.startManaged();
      await h.settle();
      assert.deepEqual(h.reads, [2_000_000]);
      assert.equal(h.ctx.rateDraft, "2");

      h.editRate("9");
      await h.controller.disconnect();
      await h.settle();
      assert.equal(h.ctx.rateDraft, "9", "dirty draft survives disconnect");
      assert.equal(h.ctx.rateDirty, true);

      h.daemon.rate = 5_000_000;
      await h.controller.startManaged();
      await h.settle();
      assert.deepEqual(
        h.reads,
        [2_000_000, 5_000_000],
        "reconnect still reads the authoritative baseline",
      );
      assert.equal(h.snapshot.rateLimitBps, 5_000_000);
      assert.equal(h.ctx.rateDraft, "9", "the baseline never clobbers the draft");
      assert.equal(h.ctx.rateDirty, true, "the draft is still unsaved");
      assert.deepEqual(h.writes, [], "no automatic write on reconnect");
    } finally {
      h.controller.dispose();
    }
  });

  it("rate: keeps the submitted draft and its uncertain outcome when disconnect races the save", async () => {
    const h = createRateHarness();
    try {
      await h.controller.startManaged();
      await h.settle();
      h.editRate("9");
      h.holdWrites();
      const saving = h.save();
      await waitFor(() => h.hasPendingWrite(), "expected an in-flight rate write");

      assert.equal(h.ctx.rateDraft, "9", "draft stays visible while saving");
      assert.equal(h.ctx.rateDirty, true, "dirty is not cleared before confirmation");

      await h.controller.disconnect();
      await h.settle();
      h.releaseWrite();
      await saving;
      await h.settle();

      assert.equal(h.ctx.rateDraft, "9", "a raced disconnect must not erase the submitted draft");
      assert.equal(h.ctx.rateDirty, true, "the uncertain draft remains unsaved");
      assert.equal(h.snapshot.phase, "offline");
      assert.equal(h.snapshot.rateLimitBps, null);
      assert.equal(h.writes.length, 1, "the write is never retried automatically");
    } finally {
      h.controller.dispose();
    }
  });

  it("rate: a late confirmation never clears a newer edit", async () => {
    const h = createRateHarness();
    try {
      await h.controller.startManaged();
      await h.settle();
      h.editRate("5");
      h.holdWrites();
      const saving = h.save();
      await waitFor(() => h.hasPendingWrite(), "expected an in-flight rate write");

      h.editRate("7");
      h.releaseWrite();
      await saving;
      await h.settle();

      assert.equal(h.ctx.rateDraft, "7", "the newer edit wins");
      assert.equal(h.ctx.rateDirty, true, "a late confirmation cannot clear the newer edit");
      assert.equal(h.snapshot.rateLimitBps, 5_000_000, "the daemon confirms the submitted value");
      assert.equal(h.writes.length, 1, "the write is never retried automatically");
    } finally {
      h.controller.dispose();
    }
  });

  it("rate: a same-epoch confirmation clears dirty only after the value is confirmed", async () => {
    const h = createRateHarness();
    try {
      await h.controller.startManaged();
      await h.settle();
      h.editRate("9");
      await h.save();
      await h.settle();

      assert.deepEqual(h.writes, [{ rate: 9_000_000 }]);
      assert.equal(h.snapshot.rateLimitBps, 9_000_000);
      assert.equal(h.ctx.rateDraft, "9");
      assert.equal(h.ctx.rateDirty, false, "confirmed save clears the dirty flag");
      assert.equal(h.ctx.rateTouched, false);
    } finally {
      h.controller.dispose();
    }
  });

  it("max: keeps the submitted draft when disconnect races the save", async () => {
    const h = createMaxHarness();
    try {
      await h.controller.startManaged();
      await h.settle();
      assert.deepEqual(h.reads, [3]);
      assert.equal(h.ctx.maxConcurrentDraft, "3");

      h.editMax("8");
      h.holdWrites();
      const saving = h.save();
      await waitFor(() => h.hasPendingWrite(), "expected an in-flight max-concurrent write");

      assert.equal(h.ctx.maxConcurrentDraft, "8");
      assert.equal(h.ctx.maxConcurrentDirty, true, "dirty is not cleared before confirmation");

      await h.controller.disconnect();
      await h.settle();
      h.releaseWrite();
      await saving;
      await h.settle();

      assert.equal(
        h.ctx.maxConcurrentDraft,
        "8",
        "a raced disconnect must not erase the submitted draft",
      );
      assert.equal(h.ctx.maxConcurrentDirty, true, "the uncertain draft remains unsaved");
      assert.equal(h.snapshot.maxConcurrent, null);
      assert.equal(h.writes.length, 1, "the write is never retried automatically");
    } finally {
      h.controller.dispose();
    }
  });

  it("max: a late confirmation never clears a newer edit", async () => {
    const h = createMaxHarness();
    try {
      await h.controller.startManaged();
      await h.settle();
      h.editMax("5");
      h.holdWrites();
      const saving = h.save();
      await waitFor(() => h.hasPendingWrite(), "expected an in-flight max-concurrent write");

      h.editMax("8");
      h.releaseWrite();
      await saving;
      await h.settle();

      assert.equal(h.ctx.maxConcurrentDraft, "8", "the newer edit wins");
      assert.equal(h.ctx.maxConcurrentDirty, true, "a late confirmation cannot clear the newer edit");
      assert.equal(h.snapshot.maxConcurrent, 5, "the daemon confirms the submitted value");
      assert.equal(h.writes.length, 1, "the write is never retried automatically");
    } finally {
      h.controller.dispose();
    }
  });

  it("max: clears a same-epoch confirmed unchanged draft only", async () => {
    const h = createMaxHarness();
    try {
      await h.controller.startManaged();
      await h.settle();
      h.editMax("8");
      await h.save();
      await h.settle();

      assert.deepEqual(h.writes, [{ before: 3, value: 8 }]);
      assert.equal(h.snapshot.maxConcurrent, 8);
      assert.equal(h.ctx.maxConcurrentDraft, "8");
      assert.equal(h.ctx.maxConcurrentDirty, false);
      assert.equal(h.ctx.maxConcurrentTouched, false);
    } finally {
      h.controller.dispose();
    }
  });
});
