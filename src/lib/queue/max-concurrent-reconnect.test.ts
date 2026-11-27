// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Regression guard: the App.svelte concurrency auto-load must run once per
// connection even when a dirty draft survived a reconnect. The old guard
// (`if (maxConcurrentDirty) return;`) skipped the read, so maxConcurrent
// stayed null and the select/Save stayed disabled forever.
// node --test cannot import .svelte, so the component's own $effect and save
// bodies are extracted from source and executed against a real
// NativeQueueController behind a fake invoke bridge (no daemon, no network).
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { NativeQueueController } from "./native-controller.ts";
import { draftAfterConnectionLoss } from "./view-tabs.ts";
import { isValidMaxConcurrentValue } from "./queue.ts";

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
  // $derived(...) may trail a comma; `(expr,)` is not valid to re-evaluate.
  return src.slice(open + 1, close).replace(/,\s*$/, "").trim();
}

function extractSaveHandler(src) {
  const at = src.indexOf("async function handleMaxConcurrentSave");
  assert.ok(at > -1, "missing handleMaxConcurrentSave in App.svelte");
  const paren = src.indexOf("(", at);
  const parenClose = scanBalanced(src, paren, "(", ")");
  const brace = src.indexOf("{", parenClose);
  const close = scanBalanced(src, brace, "{", "}");
  return src.slice(brace + 1, close);
}

const effects = extractEffects(appSrc);
const autoLoadBody = effects.find((body) => body.includes("tryClaimMaxConcurrentAutoLoad"));
const mirrorBody = effects.find((body) => body.includes("maxConcurrentDraft = String(known)"));
const preserveBody = effects.find(
  (body) => body.includes("draftAfterConnectionLoss") && body.includes("maxConcurrentDraft"),
);
assert.ok(autoLoadBody, "missing max-concurrent auto-load effect in App.svelte");
assert.ok(mirrorBody, "missing max-concurrent mirror effect in App.svelte");
assert.ok(preserveBody, "missing max-concurrent preserve effect in App.svelte");

const EFFECT_NAMES = [
  "snapshot",
  "controller",
  "isTauri",
  "untrack",
  "draftAfterConnectionLoss",
  "maxConcurrentDraft",
  "maxConcurrentDirty",
  "maxConcurrentTouched",
];

function compileEffect(body) {
  const names = EFFECT_NAMES;
  return new Function(...names, `(() => { ${body} })(); return { ${names.join(", ")} };`);
}

const runAutoLoad = compileEffect(autoLoadBody);
const runMirror = compileEffect(mirrorBody);
const runPreserve = compileEffect(preserveBody);

const SAVE_NAMES = [
  "event",
  "isTauri",
  "controller",
  "isManaged",
  "isConnected",
  "maxConcurrentSaving",
  "maxConcurrentLoading",
  "maxConcurrentUnconfirmed",
  "maxConcurrent",
  "maxConcurrentParsed",
  "maxConcurrentSaveSeq",
  "maxConcurrentDraft",
  "maxConcurrentDirty",
  "maxConcurrentTouched",
  "untrack",
  "isValidMaxConcurrentValue",
];
const runSave = new Function(
  ...SAVE_NAMES,
  `return (async () => { ${extractSaveHandler(appSrc)} })().then(() => ({ ${SAVE_NAMES.join(", ")} }));`,
);

const SAVE_STATE_NAMES = [
  "isManaged",
  "isConnected",
  "maxConcurrentLoading",
  "maxConcurrentSaving",
  "maxConcurrentUnconfirmed",
  "maxConcurrent",
  "maxConcurrentParsed",
  "isValidMaxConcurrentValue",
];
const canSave = new Function(
  ...SAVE_STATE_NAMES,
  `return (${extractDerived(appSrc, "maxConcurrentCanSave")});`,
);
const inputDisabled = new Function(
  ...SAVE_STATE_NAMES,
  `return (${extractDerived(appSrc, "maxConcurrentInputDisabled")});`,
);

const IDENTITY = (fn) => fn();

function createHarness() {
  const reads = [];
  const writes = [];
  const daemon = { maxConcurrent: 3 };
  const invoke = async (command, args) => {
    if (command === "start_managed_surge") return [];
    if (command === "disconnect_surge") return { ok: true };
    if (command === "get_max_concurrent_downloads") {
      reads.push(daemon.maxConcurrent);
      return daemon.maxConcurrent;
    }
    if (command === "set_max_concurrent_downloads") {
      writes.push({ before: args.before, value: args.value });
      daemon.maxConcurrent = args.value;
      return daemon.maxConcurrent;
    }
    throw new Error(`unexpected command: ${command}`);
  };

  let snapshot = null;
  const controller = new NativeQueueController(
    invoke,
    (next) => {
      snapshot = next;
    },
    60_000,
  );
  snapshot = { ...controller.state };

  const ui = {
    maxConcurrentDraft: "",
    maxConcurrentDirty: false,
    maxConcurrentTouched: false,
    maxConcurrentSaveSeq: 0,
  };

  function derived() {
    return {
      isManaged: snapshot.managed === true,
      isConnected: snapshot.phase === "connected",
      maxConcurrent: snapshot.maxConcurrent ?? null,
      maxConcurrentLoading: snapshot.maxConcurrentLoading === true,
      maxConcurrentSaving: snapshot.maxConcurrentSaving === true,
      maxConcurrentUnconfirmed: snapshot.maxConcurrentUnconfirmed === true,
      maxConcurrentParsed:
        ui.maxConcurrentDraft === "" ? null : Number(ui.maxConcurrentDraft),
    };
  }

  function runEffects() {
    const runners = [runAutoLoad, runMirror, runPreserve];
    for (const run of runners) {
      const out = run(
        snapshot,
        controller,
        true,
        IDENTITY,
        draftAfterConnectionLoss,
        ui.maxConcurrentDraft,
        ui.maxConcurrentDirty,
        ui.maxConcurrentTouched,
      );
      ui.maxConcurrentDraft = out.maxConcurrentDraft;
      ui.maxConcurrentDirty = out.maxConcurrentDirty;
      ui.maxConcurrentTouched = out.maxConcurrentTouched;
    }
  }

  // Svelte re-runs the effects whenever a value they read changes; repeating
  // the pass until the queue settles models that without a component mount.
  async function settle(rounds = 6) {
    for (let i = 0; i < rounds; i += 1) {
      await new Promise((resolve) => setTimeout(resolve, 0));
      runEffects();
    }
  }

  function editDraft(value) {
    // The select's onchange handler: mark the edit, never write it out.
    ui.maxConcurrentDraft = value;
    ui.maxConcurrentDirty = true;
    ui.maxConcurrentTouched = true;
    runEffects();
  }

  async function saveDraft() {
    const state = derived();
    const out = await runSave(
      { preventDefault() {} },
      true,
      controller,
      state.isManaged,
      state.isConnected,
      state.maxConcurrentSaving,
      state.maxConcurrentLoading,
      state.maxConcurrentUnconfirmed,
      state.maxConcurrent,
      state.maxConcurrentParsed,
      ui.maxConcurrentSaveSeq,
      ui.maxConcurrentDraft,
      ui.maxConcurrentDirty,
      ui.maxConcurrentTouched,
      IDENTITY,
      isValidMaxConcurrentValue,
    );
    ui.maxConcurrentSaveSeq = out.maxConcurrentSaveSeq;
    ui.maxConcurrentDraft = out.maxConcurrentDraft;
    ui.maxConcurrentDirty = out.maxConcurrentDirty;
    ui.maxConcurrentTouched = out.maxConcurrentTouched;
    await settle();
  }

  function controls() {
    const state = derived();
    const args = { ...state, isTauri: true, isValidMaxConcurrentValue };
    return {
      canSave: canSave(...SAVE_STATE_NAMES.map((name) => args[name])),
      inputDisabled: inputDisabled(...SAVE_STATE_NAMES.map((name) => args[name])),
    };
  }

  return {
    controller,
    daemon,
    reads,
    writes,
    ui,
    runEffects,
    settle,
    editDraft,
    saveDraft,
    controls,
    get snapshot() {
      return snapshot;
    },
    async dispose() {
      await controller.dispose();
    },
  };
}

describe("App.svelte concurrency setting across reconnect", () => {
  it("reads the setting once per connection with a dirty draft, keeps the draft, and saves only explicitly", async () => {
    const h = createHarness();
    try {
      // --- connect #1: clean draft mirrors the authoritative value ---------
      assert.equal(await h.controller.startManaged(), true);
      await h.settle();
      assert.deepEqual(h.reads, [3], "first connection reads the saved value once");
      assert.equal(h.ui.maxConcurrentDraft, "3", "clean draft mirrors the daemon value");
      assert.equal(h.controls().inputDisabled, false, "selector enabled after the read");
      assert.equal(h.controls().canSave, false, "nothing to save until the user edits");

      // --- the user edits the select, then the connection drops ------------
      h.editDraft("8");
      assert.equal(h.ui.maxConcurrentDirty, true, "the edit is a dirty draft");
      assert.deepEqual(h.writes, [], "editing never writes without an explicit save");
      await h.controller.disconnect();
      await h.settle();
      assert.equal(h.snapshot.maxConcurrent, null, "disconnect clears the read value");
      assert.equal(h.ui.maxConcurrentDraft, "8", "dirty draft survives the connection loss");
      assert.equal(h.ui.maxConcurrentDirty, true, "the draft is still unsaved");

      // --- reconnect: the authoritative read must happen anyway ------------
      h.daemon.maxConcurrent = 5;
      assert.equal(await h.controller.startManaged(), true);
      await h.settle();
      assert.deepEqual(
        h.reads,
        [3, 5],
        "reconnect reads the saved setting exactly once even with a dirty draft",
      );
      assert.equal(h.snapshot.maxConcurrent, 5, "maxConcurrent is no longer null");
      assert.equal(h.ui.maxConcurrentDraft, "8", "the loaded value never clobbers the draft");
      assert.equal(h.ui.maxConcurrentDirty, true, "the draft is still dirty after the read");
      assert.equal(h.controls().inputDisabled, false, "selector enabled after the reconnect read");
      assert.equal(h.controls().canSave, true, "Save enabled by the loaded value, not auto-applied");
      assert.deepEqual(h.writes, [], "the reconnect read must not mutate the daemon");

      // --- only an explicit save sends the draft ---------------------------
      await h.saveDraft();
      assert.deepEqual(h.writes, [{ before: 5, value: 8 }], "explicit save writes the draft");
      assert.equal(h.snapshot.maxConcurrent, 8, "confirmed value follows the save");
      assert.equal(h.ui.maxConcurrentDraft, "8", "draft stays on the saved choice");
      assert.equal(h.ui.maxConcurrentDirty, false, "save clears the dirty flag");
      assert.equal(h.controls().canSave, false, "no repeat save with nothing to send");
      assert.equal(h.reads.length, 2, "saving never triggers another read");
    } finally {
      await h.dispose();
    }
  });
});
