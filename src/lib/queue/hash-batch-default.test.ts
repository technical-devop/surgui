// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Regression guard for App.svelte batch hash-default handling.
// A batch must not silently submit hashing disabled while the persisted
// default is unknown (still loading or failed to load). An explicit per-batch
// choice is required. A successful reload resolves only a load failure, never
// the uncertainty left by an ambiguous save. The component bodies are
// extracted from source and run against a real NativeQueueController.
import { beforeEach, describe, it } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  NativeQueueController,
  resetSharedHashDefaultStoreForTests,
} from "./native-controller.ts";
import { resolveBatchDraft } from "./queue.ts";

beforeEach(resetSharedHashDefaultStoreForTests);

const HASH_DEFAULT_UNCONFIRMED =
  "The saved hashing default could not be confirmed after a save. Changing it is disabled until the app restarts to avoid unsafe writes; a per-batch choice is still available.";

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

function extractDerived(src, name) {
  const marker = `const ${name} = $derived(`;
  const at = src.indexOf(marker);
  assert.ok(at > -1, `missing derived ${name} in App.svelte`);
  const open = at + marker.length - 1;
  const close = scanBalanced(src, open, "(", ")");
  return src.slice(open + 1, close).replace(/,\s*$/, "").trim();
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
const hashDefaultAutoLoad = effectBodies.find((body) =>
  body.includes("tryClaimHashDefaultAutoLoad"),
);
const hashDefaultMirror = effectBodies.find((body) => body.includes("hashForBatch = def"));
assert.ok(hashDefaultAutoLoad, "missing hash-default auto-load effect in App.svelte");
assert.ok(hashDefaultMirror, "missing hash-default mirror effect in App.svelte");

const runHashDefaultEffects = [hashDefaultAutoLoad, hashDefaultMirror].map(
  (body) => new Function("ctx", `with (ctx) { (() => { ${body} })(); }`),
);
const runAdd = new Function(
  "ctx",
  `with (ctx) { return (async () => { ${extractHandler(appSrc, "handleAdd")} })(); }`,
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

const IDENTITY = (fn) => fn();
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function createHarness() {
  let settingsFails = true;
  let defaultsEnabled = true;
  let settingsReads = 0;
  let settingsGate = null;
  let releaseSettingsGate = null;
  let saveCalls = 0;
  let nextSaveError = null;
  let nextSaveApplies = false;
  let deferredSaves = 0;
  const saveResolvers = [];
  const addCalls = [];
  let deferredAdds = 0;
  const addResolvers = [];

  const invoke = async (command, args) => {
    if (command === "start_managed_surge") return [];
    if (command === "connect_surge") return [];
    if (command === "disconnect_surge") return { ok: true };
    if (command === "list_downloads") return [];
    if (command === "get_automatic_hash_settings") {
      settingsReads += 1;
      if (settingsFails) throw "network";
      if (settingsGate !== null) await settingsGate;
      return { defaultEnabled: defaultsEnabled };
    }
    if (command === "set_automatic_hash_default") {
      saveCalls += 1;
      if (deferredSaves > 0) {
        deferredSaves -= 1;
        return new Promise((resolve, reject) =>
          saveResolvers.push({ enabled: args.enabled, resolve, reject }),
        );
      }
      if (nextSaveError !== null) {
        const error = nextSaveError;
        if (nextSaveApplies) defaultsEnabled = args.enabled;
        nextSaveError = null;
        nextSaveApplies = false;
        throw error;
      }
      defaultsEnabled = args.enabled;
      return { defaultEnabled: defaultsEnabled };
    }
    if (command === "get_download_hashes") return [];
    if (command === "refresh_download_hashes") return [];
    if (command === "add_download_with_options") {
      addCalls.push(args);
      if (deferredAdds > 0) {
        deferredAdds -= 1;
        return new Promise((resolve) => addResolvers.push(resolve));
      }
      return { status: "queued", id: "dl-1", hashRecorded: args.hash === true };
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
    resolveBatchDraft,
    pullRecoveries() {},
    restoreRecoveredDraft() {},
    isManaged: false,
    isConnected: false,
    addBusy: false,
    batchRunning: false,
    addUrl: "",
    addOperationSeq: 0,
    hashForBatch: false,
    hashBatchDirty: false,
    hashChoiceRevision: 0,
    hashDefaultEnabled: null,
    hashDefaultSaving: false,
    hashDefaultLoading: false,
    hashDefaultBaselineStable: false,
    hashChoiceResolved: false,
    hashChoiceUnknown: false,
    canSubmitBatch: false,
  };

  function syncDerived() {
    ctx.isManaged = snapshot.managed === true;
    ctx.isConnected = snapshot.phase === "connected";
    ctx.batchRunning = snapshot.batch?.running === true;
    ctx.addBusy = snapshot.adding === true || ctx.batchRunning;
    ctx.hashDefaultEnabled = snapshot.hashDefaultEnabled ?? null;
    ctx.hashDefaultSaving = snapshot.hashDefaultSaving === true;
    ctx.hashDefaultLoading = snapshot.hashDefaultLoading === true;
    ctx.hashDefaultBaselineStable = evalBaselineStable(ctx);
    ctx.hashChoiceResolved = evalResolved(ctx);
    ctx.hashChoiceUnknown = evalUnknown(ctx);
    ctx.canSubmitBatch = evalCanSubmit(ctx);
  }

  function runEffects() {
    ctx.snapshot = snapshot;
    syncDerived();
    for (const run of runHashDefaultEffects) run(ctx);
  }

  async function settle(rounds = 8) {
    for (let i = 0; i < rounds; i += 1) {
      await delay(0);
      runEffects();
    }
  }

  return {
    controller,
    ctx,
    addCalls,
    invoke,
    get settingsReads() {
      return settingsReads;
    },
    setSettingsFails(value) {
      settingsFails = value;
    },
    setDefault(value) {
      defaultsEnabled = value;
    },
    get saveCalls() {
      return saveCalls;
    },
    failNextSave(error, apply = false) {
      nextSaveError = error;
      nextSaveApplies = apply;
    },
    deferNextSave() {
      deferredSaves += 1;
    },
    releaseDeferredSave({ apply = true, error = null } = {}) {
      const pending = saveResolvers.shift();
      assert.ok(pending, "expected a deferred default save");
      if (apply) defaultsEnabled = pending.enabled;
      if (error !== null) pending.reject(error);
      else pending.resolve({ defaultEnabled: defaultsEnabled });
    },
    deferSettings() {
      settingsGate = new Promise((resolve) => {
        releaseSettingsGate = resolve;
      });
    },
    releaseSettings() {
      const release = releaseSettingsGate;
      settingsGate = null;
      releaseSettingsGate = null;
      release();
    },
    deferNextAdd() {
      deferredAdds += 1;
    },
    releaseDeferredAdd(result = { status: "queued", id: "dl-1", hashRecorded: false }) {
      const release = addResolvers.shift();
      if (release) release(result);
    },
    runEffects,
    settle,
    add() {
      ctx.event = { preventDefault() {} };
      return runAdd(ctx);
    },
    get snapshot() {
      return snapshot;
    },
  };
}

describe("App.svelte batch hashing default", () => {
  it("blocks submission while the default is unknown, then honors an explicit choice", async () => {
    const h = createHarness();
    try {
      await h.controller.startManaged();
      h.ctx.addUrl = "https://example.com/a.bin";
      h.runEffects();
      assert.equal(h.snapshot.hashDefaultEnabled, null);
      assert.equal(h.ctx.hashChoiceUnknown, true);
      assert.equal(h.ctx.canSubmitBatch, false, "unknown default must block submission");

      await h.add();
      assert.equal(h.addCalls.length, 0, "no batch may be submitted with a guessed hash choice");
      assert.equal(h.ctx.addUrl, "https://example.com/a.bin");

      // The automatic load fails: the default stays unknown and must not loop.
      await h.settle(10);
      assert.equal(h.settingsReads, 1, "a failed default load runs once");
      assert.equal(h.snapshot.hashDefaultEnabled, null);
      assert.equal(h.ctx.canSubmitBatch, false);
      await h.add();
      assert.equal(h.addCalls.length, 0, "a failed default load must not enable a guessed batch");

      // An explicit per-batch choice is intentional and may submit.
      h.ctx.hashBatchDirty = true;
      h.ctx.hashForBatch = true;
      h.runEffects();
      assert.equal(h.ctx.hashChoiceUnknown, false);
      assert.equal(h.ctx.canSubmitBatch, true);
      await h.add();
      assert.equal(h.addCalls.length, 1);
      assert.equal(h.addCalls[0].hash, true, "the explicit per-batch choice is submitted");
    } finally {
      h.controller.dispose();
    }
  });

  it("blocks implicit submission while a default save is in flight and allows an explicit override", async () => {
    const h = createHarness();
    try {
      h.setSettingsFails(false);
      h.setDefault(false);
      await h.controller.startManaged();
      await h.settle();
      assert.equal(h.snapshot.hashDefaultEnabled, false);

      h.ctx.addUrl = "https://example.com/saved.bin";
      h.runEffects();
      assert.equal(h.ctx.canSubmitBatch, true);

      h.deferNextSave();
      const saving = h.controller.setHashDefault(true);
      await delay(0);
      h.runEffects();

      assert.equal(h.snapshot.hashDefaultSaving, true);
      assert.equal(h.snapshot.hashDefaultEnabled, false, "the old default stays visible");
      assert.equal(h.ctx.hashChoiceUnknown, true);
      assert.equal(
        h.ctx.canSubmitBatch,
        false,
        "the old default must not drive a new batch while its save is in flight",
      );
      await h.add();
      assert.equal(h.addCalls.length, 0, "an implicit old-default batch never crosses IPC");

      h.ctx.hashBatchDirty = true;
      h.ctx.hashForBatch = true;
      h.ctx.hashChoiceRevision += 1;
      h.runEffects();
      assert.equal(h.ctx.hashChoiceUnknown, false);
      assert.equal(h.ctx.canSubmitBatch, true);
      await h.add();
      assert.equal(h.addCalls.length, 1);
      assert.equal(h.addCalls[0].hash, true, "the explicit per-batch override is allowed");

      h.releaseDeferredSave();
      assert.equal(await saving, true);
      await h.settle();

      assert.equal(h.snapshot.hashDefaultSaving, false);
      assert.equal(h.snapshot.hashDefaultEnabled, true);
      assert.equal(h.ctx.hashForBatch, true);

      h.ctx.addUrl = "https://example.com/next.bin";
      h.runEffects();
      assert.equal(h.ctx.canSubmitBatch, true);
      await h.add();
      assert.equal(h.addCalls.length, 2);
      assert.equal(
        h.addCalls[1].hash,
        true,
        "the next implicit batch uses the fresh confirmed baseline",
      );
      assert.equal(h.saveCalls, 1, "the save is never retried automatically");
    } finally {
      h.controller.dispose();
    }
  });

  it("uses the persisted default only after an explicit retry succeeds", async () => {
    const h = createHarness();
    try {
      await h.controller.startManaged();
      await h.settle(10);
      assert.equal(h.settingsReads, 1);
      assert.equal(h.snapshot.hashDefaultEnabled, null);

      h.setSettingsFails(false);
      h.setDefault(true);
      await h.controller.loadHashDefault();
      assert.equal(h.settingsReads, 2, "the explicit retry performs exactly one more read");
      h.ctx.addUrl = "https://example.com/b.bin";
      await h.settle();

      assert.equal(h.snapshot.hashDefaultEnabled, true);
      assert.equal(h.ctx.hashForBatch, true, "the loaded default mirrors into the batch choice");
      assert.equal(h.ctx.hashChoiceUnknown, false);
      assert.equal(h.ctx.canSubmitBatch, true);
      await h.add();
      assert.equal(h.addCalls.length, 1);
      assert.equal(h.addCalls[0].hash, true, "a loaded default is used, not guessed");
      assert.equal(h.settingsReads, 2, "no further automatic default read runs");
    } finally {
      h.controller.dispose();
    }
  });

  it("does not reuse a cached default across a reconnect before the new epoch read resolves", async () => {
    const h = createHarness();
    try {
      h.setSettingsFails(false);
      h.setDefault(true);
      await h.controller.startManaged();
      await h.settle();
      assert.equal(h.snapshot.hashDefaultEnabled, true);
      h.ctx.addUrl = "https://example.com/a.bin";
      h.runEffects();
      assert.equal(h.ctx.canSubmitBatch, true);

      await h.controller.disconnect();
      h.setDefault(false);
      h.deferSettings();
      await h.controller.startManaged();
      await h.settle(1);

      assert.equal(
        h.snapshot.hashDefaultEnabled,
        null,
        "the previous epoch's default is unconfirmed, not reused",
      );
      assert.equal(h.ctx.hashChoiceUnknown, true);
      assert.equal(h.ctx.canSubmitBatch, false, "a batch is gated until the new epoch read succeeds");
      await h.add();
      assert.equal(h.addCalls.length, 0, "the old cached default must not submit a batch");

      h.releaseSettings();
      await h.settle(12);
      assert.equal(h.snapshot.hashDefaultEnabled, false);
      assert.equal(h.ctx.hashForBatch, false, "the externally changed default resolves the batch choice");
      assert.equal(h.ctx.canSubmitBatch, true);
      await h.add();
      assert.equal(h.addCalls.length, 1);
      assert.equal(h.addCalls[0].hash, false, "the current epoch default is submitted");
    } finally {
      h.controller.dispose();
    }
  });

  it("keeps the default unconfirmed when the new epoch reload fails", async () => {
    const h = createHarness();
    try {
      h.setSettingsFails(false);
      h.setDefault(true);
      await h.controller.startManaged();
      await h.settle();
      assert.equal(h.snapshot.hashDefaultEnabled, true);

      await h.controller.disconnect();
      h.setSettingsFails(true);
      await h.controller.startManaged();
      await h.settle(10);

      assert.equal(h.settingsReads, 2, "the new epoch performs exactly one automatic read");
      assert.equal(h.snapshot.hashDefaultEnabled, null, "a failed reload must not keep the old value");
      assert.equal(h.ctx.hashChoiceUnknown, true);
      assert.equal(h.ctx.canSubmitBatch, false);
      h.ctx.addUrl = "https://example.com/b.bin";
      h.runEffects();
      await h.add();
      assert.equal(h.addCalls.length, 0, "a failed reload never infers the old choice");

      // An explicit override is still allowed and is the only way through.
      h.ctx.hashBatchDirty = true;
      h.ctx.hashForBatch = false;
      h.runEffects();
      assert.equal(h.ctx.canSubmitBatch, true);
      await h.add();
      assert.equal(h.addCalls.length, 1);
      assert.equal(h.addCalls[0].hash, false, "only the explicit choice is submitted");
    } finally {
      h.controller.dispose();
    }
  });

  it("treats the default as unconfirmed after a manual reconnect too", async () => {
    const h = createHarness();
    try {
      h.setSettingsFails(false);
      h.setDefault(true);
      await h.controller.startManaged();
      await h.settle();
      assert.equal(h.snapshot.hashDefaultEnabled, true);

      await h.controller.disconnect();
      h.setDefault(false);
      h.deferSettings();
      await h.controller.connect("http://127.0.0.1:9", "token");
      await h.settle(1);

      assert.equal(h.controller.connectionEpoch() > 1, true);
      assert.equal(h.snapshot.phase, "connected");
      assert.equal(h.snapshot.managed, false);
      assert.equal(
        h.snapshot.hashDefaultEnabled,
        null,
        "manual connect clears the earlier epoch's cached default",
      );

      h.releaseSettings();
      await h.settle(12);
      assert.equal(h.snapshot.hashDefaultEnabled, false);
    } finally {
      h.controller.dispose();
    }
  });

  it("does not let a stale batch completion reset a new connection's batch choice", async () => {
    const h = createHarness();
    try {
      h.setSettingsFails(false);
      h.setDefault(false);
      await h.controller.startManaged();
      await h.settle();
      assert.equal(h.snapshot.hashDefaultEnabled, false);

      h.ctx.addUrl = "https://example.com/old.bin";
      h.runEffects();
      h.deferNextAdd();
      const oldAdd = h.add();
      await delay(0);
      assert.equal(h.addCalls.length, 1);
      assert.equal(h.addCalls[0].hash, false);

      await h.controller.disconnect();
      h.setDefault(true);
      await h.controller.startManaged();
      await h.settle();
      assert.equal(h.snapshot.hashDefaultEnabled, true);

      // New connection, explicit per-batch override back to disabled.
      h.ctx.addUrl = "https://example.com/new.bin";
      h.ctx.hashBatchDirty = true;
      h.ctx.hashForBatch = false;
      h.ctx.hashChoiceRevision += 1;
      h.runEffects();

      h.releaseDeferredAdd({ status: "queued", id: "old-1", hashRecorded: false });
      await oldAdd;
      await h.settle();

      assert.equal(h.ctx.hashForBatch, false, "the new connection's override survives");
      assert.equal(h.ctx.hashBatchDirty, true, "the newer choice is still marked dirty");
      assert.equal(h.ctx.addUrl, "https://example.com/new.bin", "newer input edits survive");
    } finally {
      h.controller.dispose();
    }
  });

  it("does not let a stale batch completion reset a newer user edit on the same epoch", async () => {
    const h = createHarness();
    try {
      h.setSettingsFails(false);
      h.setDefault(false);
      await h.controller.startManaged();
      await h.settle();

      h.ctx.addUrl = "https://example.com/old.bin";
      h.runEffects();
      h.deferNextAdd();
      const oldAdd = h.add();
      await delay(0);
      assert.equal(h.addCalls.length, 1);

      // The user overrides the next batch's choice before the old one settles.
      h.ctx.hashBatchDirty = true;
      h.ctx.hashForBatch = true;
      h.ctx.hashChoiceRevision += 1;
      h.runEffects();

      h.releaseDeferredAdd({ status: "queued", id: "old-1", hashRecorded: false });
      await oldAdd;
      await h.settle();

      assert.equal(h.ctx.hashForBatch, true, "the newer edit is not overwritten by the default");
      assert.equal(h.ctx.hashBatchDirty, true, "the newer edit remains dirty");
    } finally {
      h.controller.dispose();
    }
  });

  it("does not reset the next batch when a newer operation owns the add surface", async () => {
    const h = createHarness();
    try {
      h.setSettingsFails(false);
      h.setDefault(false);
      await h.controller.startManaged();
      await h.settle();

      h.ctx.addUrl = "https://example.com/old.bin";
      h.runEffects();
      h.deferNextAdd();
      const oldAdd = h.add();
      await delay(0);
      assert.equal(h.addCalls.length, 1);

      // A newer submission has taken ownership of the operation sequence.
      h.ctx.addOperationSeq += 1;
      h.ctx.hashForBatch = true;
      h.ctx.hashBatchDirty = true;

      h.releaseDeferredAdd({ status: "queued", id: "old-1", hashRecorded: false });
      await oldAdd;
      await h.settle();

      assert.equal(h.ctx.hashForBatch, true, "a newer operation keeps the current choice");
      assert.equal(h.ctx.hashBatchDirty, true);
    } finally {
      h.controller.dispose();
    }
  });

  it("blocks a batch after an ambiguous default save even when reload fails, then honors an explicit override", async () => {
    const h = createHarness();
    try {
      h.setSettingsFails(false);
      h.setDefault(false);
      await h.controller.startManaged();
      await h.settle();
      assert.equal(h.snapshot.hashDefaultEnabled, false);

      // The user enables the saved default, the daemon applies it, but the
      // reply is lost. The old value must not remain a confirmed default.
      h.failNextSave("network", true);
      assert.equal(await h.controller.setHashDefault(true), false);
      h.runEffects();
      assert.equal(h.snapshot.hashDefaultEnabled, null, "the prior default is unconfirmed");
      assert.equal(h.snapshot.hashDefaultError, HASH_DEFAULT_UNCONFIRMED);
      assert.equal(h.snapshot.hashDefaultUnconfirmed, true);
      assert.equal(h.ctx.hashChoiceUnknown, true);
      assert.equal(h.ctx.canSubmitBatch, false, "no batch may auto-use the stale default");
      assert.equal(h.settingsReads, 1, "an ambiguous save does not auto-reload");
      assert.equal(h.saveCalls, 1, "the mutation is never retried automatically");

      h.ctx.addUrl = "https://example.com/blocked.bin";
      h.runEffects();
      await h.add();
      assert.equal(h.addCalls.length, 0);

      // A manual reload of the current epoch also fails: still unconfirmed.
      h.setSettingsFails(true);
      await h.controller.loadHashDefault();
      await h.settle();
      assert.equal(h.settingsReads, 2);
      assert.equal(h.snapshot.hashDefaultEnabled, null);
      assert.equal(h.ctx.canSubmitBatch, false);
      await h.add();
      assert.equal(h.addCalls.length, 0, "a failed reload cannot resolve the batch choice");

      // An explicit per-batch override is the only way through while unknown.
      h.ctx.hashBatchDirty = true;
      h.ctx.hashForBatch = true;
      h.ctx.hashChoiceRevision += 1;
      h.runEffects();
      assert.equal(h.ctx.canSubmitBatch, true);
      await h.add();
      assert.equal(h.addCalls.length, 1);
      assert.equal(h.addCalls[0].hash, true, "only the explicit choice is submitted");

      // The override belonged to that batch; the next one is gated again.
      h.ctx.addUrl = "https://example.com/next.bin";
      h.runEffects();
      assert.equal(h.ctx.canSubmitBatch, false);
      await h.add();
      assert.equal(h.addCalls.length, 1, "a new batch never reuses the stale default");

      // The fake daemon committed true, but a matching read is not a completion
      // fence; the timed-out write could still commit later. The default stays
      // unknown and the next batch still needs an explicit override.
      h.setSettingsFails(false);
      h.setDefault(true);
      await h.controller.loadHashDefault();
      await h.settle();
      assert.equal(h.settingsReads, 3);
      assert.equal(h.snapshot.hashDefaultEnabled, null);
      assert.equal(h.snapshot.hashDefaultUnconfirmed, true);
      assert.equal(h.ctx.hashChoiceUnknown, true);
      assert.equal(h.ctx.canSubmitBatch, false);
      await h.add();
      assert.equal(h.addCalls.length, 1, "a matching read must not enable a guessed batch");

      h.ctx.hashBatchDirty = true;
      h.ctx.hashForBatch = false;
      h.ctx.hashChoiceRevision += 1;
      h.runEffects();
      await h.add();
      assert.equal(h.addCalls.length, 2);
      assert.equal(h.addCalls[1].hash, false, "the explicit override is still usable");
      assert.equal(h.saveCalls, 1, "no default mutation was retried");
    } finally {
      h.controller.dispose();
    }
  });

  it("preserves a dirty per-batch override across an unconfirmed save and a later fresh read", async () => {
    const h = createHarness();
    try {
      h.setSettingsFails(false);
      h.setDefault(false);
      await h.controller.startManaged();
      await h.settle();
      h.ctx.addUrl = "https://example.com/override.bin";

      // The user explicitly turns hashing off for the next batch.
      h.ctx.hashBatchDirty = true;
      h.ctx.hashForBatch = false;
      h.ctx.hashChoiceRevision += 1;
      h.runEffects();
      assert.equal(h.ctx.canSubmitBatch, true);

      // A separate save-default change is ambiguous even though the server
      // applied it. It must not clobber the dirty batch choice.
      h.failNextSave("network", true);
      assert.equal(await h.controller.setHashDefault(true), false);
      h.runEffects();
      assert.equal(h.snapshot.hashDefaultEnabled, null);
      assert.equal(h.ctx.hashForBatch, false, "the dirty override is preserved");
      assert.equal(h.ctx.hashBatchDirty, true);
      assert.equal(h.ctx.canSubmitBatch, true, "the explicit override remains usable");

      // A matching fresh read cannot confirm the saved value after an ambiguous
      // write, but it must still leave the dirty override untouched.
      h.setSettingsFails(false);
      h.setDefault(true);
      await h.controller.loadHashDefault();
      await h.settle();
      assert.equal(h.snapshot.hashDefaultEnabled, null);
      assert.equal(h.snapshot.hashDefaultUnconfirmed, true);
      assert.equal(h.ctx.hashForBatch, false, "the fresh value never overwrites a dirty edit");
      assert.equal(h.ctx.hashBatchDirty, true);

      h.ctx.addUrl = "https://example.com/override.bin";
      h.runEffects();
      await h.add();
      assert.equal(h.addCalls.length, 1);
      assert.equal(h.addCalls[0].hash, false, "the preserved override is submitted");
    } finally {
      h.controller.dispose();
    }
  });

  it("notifies another controller's batch gate of a shared save and recovers on definite success", async () => {
    const h = createHarness();
    let otherSnapshot = null;
    const other = new NativeQueueController(
      h.invoke,
      (next) => {
        otherSnapshot = next;
      },
      60_000,
    );
    try {
      h.setSettingsFails(false);
      h.setDefault(false);
      // h.controller is the observing controller B; `other` is writer A.
      await h.controller.startManaged();
      await h.settle();
      await other.startManaged();
      await other.loadHashDefault();
      assert.equal(h.snapshot.hashDefaultEnabled, false);
      assert.equal(otherSnapshot.hashDefaultEnabled, false);

      h.ctx.addUrl = "https://example.com/pending.bin";
      h.runEffects();
      assert.equal(h.ctx.canSubmitBatch, true);

      h.deferNextSave();
      const saving = other.setHashDefault(true);
      await delay(0);
      h.runEffects();

      assert.equal(h.snapshot.hashDefaultSaving, true, "B observes A's shared save");
      assert.equal(
        h.snapshot.hashDefaultEnabled,
        false,
        "the old value stays visible while the shared save is unresolved",
      );
      assert.equal(h.ctx.hashChoiceUnknown, true);
      assert.equal(h.ctx.canSubmitBatch, false);
      await h.add();
      assert.equal(h.addCalls.length, 0, "B never submits an implicit stale default");

      // An explicit per-batch choice remains available while the shared write
      // is pending, exactly as for the initiating controller.
      h.ctx.hashBatchDirty = true;
      h.ctx.hashForBatch = true;
      h.ctx.hashChoiceRevision += 1;
      h.runEffects();
      assert.equal(h.ctx.hashChoiceUnknown, false);
      assert.equal(h.ctx.canSubmitBatch, true);
      await h.add();
      assert.equal(h.addCalls.length, 1);
      assert.equal(h.addCalls[0].hash, true, "the explicit override is still submitted");

      h.releaseDeferredSave({ apply: true });
      assert.equal(await saving, true, "the definite success is reported by A");
      await h.settle();

      assert.equal(h.snapshot.hashDefaultSaving, false);
      assert.equal(
        h.snapshot.hashDefaultEnabled,
        null,
        "B's cached baseline is invalidated by A's definite success",
      );
      assert.equal(h.snapshot.hashDefaultUnconfirmed, false);

      // The next implicit batch is still gated until B confirms the new value.
      h.ctx.addUrl = "https://example.com/after-success.bin";
      h.runEffects();
      assert.equal(h.ctx.canSubmitBatch, false);
      await h.add();
      assert.equal(h.addCalls.length, 1, "the stale baseline is not reused after success");

      await h.controller.loadHashDefault();
      await h.settle();
      assert.equal(h.snapshot.hashDefaultEnabled, true, "B recovers with a fresh read");
      assert.equal(h.snapshot.hashDefaultError, null);
      assert.equal(h.ctx.hashChoiceUnknown, false);
      assert.equal(h.ctx.canSubmitBatch, true);
      await h.add();
      assert.equal(h.addCalls.length, 2);
      assert.equal(h.addCalls[1].hash, true, "the recovered default drives the next batch");
    } finally {
      other.dispose();
      h.controller.dispose();
    }
  });
});
