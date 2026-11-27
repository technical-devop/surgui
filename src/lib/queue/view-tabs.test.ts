// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Surgui GUI cleanup: tabs, hash toggles, settings nav, rebrand guards.
// Pure-logic tests + source guards (no DOM mount; node --test cannot import .svelte).
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolveAppTab,
  nextAppTab,
  getHashControlsState,
  getSettingsSaveState,
  draftAfterConnectionLoss,
} from "./view-tabs.ts";

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(here, "..", "..", "..");
const appPath = path.join(here, "..", "App.svelte");
const queuePath = path.join(here, "..", "QueueScreen.svelte");
const appSrc = fs.readFileSync(appPath, "utf8");
const queueSrc = fs.readFileSync(queuePath, "utf8");

describe("view tabs: downloads default + keyboard nav", () => {
  it("defaults unknown/empty to downloads", () => {
    assert.equal(resolveAppTab(undefined), "downloads");
    assert.equal(resolveAppTab(null), "downloads");
    assert.equal(resolveAppTab(""), "downloads");
    assert.equal(resolveAppTab("settings-extra"), "downloads");
    assert.equal(resolveAppTab("DOWNLOADS"), "downloads");
  });

  it("keeps explicit tabs", () => {
    assert.equal(resolveAppTab("downloads"), "downloads");
    assert.equal(resolveAppTab("settings"), "settings");
  });

  it("arrow keys toggle between the two tabs", () => {
    assert.equal(nextAppTab("downloads", "ArrowRight"), "settings");
    assert.equal(nextAppTab("settings", "ArrowRight"), "downloads");
    assert.equal(nextAppTab("settings", "ArrowLeft"), "downloads");
    assert.equal(nextAppTab("downloads", "ArrowLeft"), "settings");
  });

  it("Home/End jump to first/last", () => {
    assert.equal(nextAppTab("settings", "Home"), "downloads");
    assert.equal(nextAppTab("downloads", "End"), "settings");
  });
});

describe("hash toggles: per-batch + persist-as-default", () => {
  const base = {
    isManaged: true,
    isConnected: true,
    addBusy: false,
    hashForBatch: false,
    hashDefaultEnabled: false,
    hashDefaultLoading: false,
    hashDefaultSaving: false,
    hashDefaultError: null,
  };

  it("both toggles enabled when managed + connected + idle", () => {
    const s = getHashControlsState(base);
    assert.equal(s.perBatchDisabled, false);
    assert.equal(s.persistDisabled, false);
    assert.equal(s.showManagedOnlyNote, false);
    assert.equal(s.showDefaultStatus, true);
    assert.equal(s.defaultStatusLabel, "Default for future batches: off.");
  });

  it("per-batch disabled while a batch runs; persist disabled too", () => {
    const s = getHashControlsState({ ...base, addBusy: true });
    assert.equal(s.perBatchDisabled, true);
    assert.equal(s.persistDisabled, true);
  });

  it("managed-only note when unmanaged, and toggles disabled", () => {
    const s = getHashControlsState({ ...base, isManaged: false });
    assert.equal(s.showManagedOnlyNote, true);
    assert.equal(s.perBatchDisabled, true);
    assert.equal(s.persistDisabled, true);
  });

  it("offline disables both toggles", () => {
    const s = getHashControlsState({ ...base, isConnected: false });
    assert.equal(s.perBatchDisabled, true);
    assert.equal(s.persistDisabled, true);
  });

  it("persist disabled while loading or saving (failure/saving states)", () => {
    assert.equal(getHashControlsState({ ...base, hashDefaultLoading: true }).persistDisabled, true);
    assert.equal(getHashControlsState({ ...base, hashDefaultSaving: true }).persistDisabled, true);
  });

  it("dirty when per-batch choice differs from persisted default", () => {
    assert.equal(getHashControlsState({ ...base, hashForBatch: true }).persistDirty, true);
    assert.equal(getHashControlsState({ ...base, hashForBatch: false }).persistDirty, false);
    assert.equal(
      getHashControlsState({ ...base, hashForBatch: true, hashDefaultEnabled: true }).persistDirty,
      false,
    );
  });

  it("no default status line until the default loads", () => {
    const s = getHashControlsState({ ...base, hashDefaultEnabled: null });
    assert.equal(s.showDefaultStatus, false);
    assert.equal(s.defaultStatusLabel, null);
  });
});

describe("settings save matrix: failures never enable writes", () => {
  const ok = {
    isManaged: true,
    isConnected: true,
    loading: false,
    saving: false,
    unconfirmed: false,
    dirty: true,
    valid: true,
  };

  it("canSave only when managed + connected + dirty + valid", () => {
    assert.equal(getSettingsSaveState(ok).canSave, true);
    assert.equal(getSettingsSaveState({ ...ok, dirty: false }).canSave, false);
    assert.equal(getSettingsSaveState({ ...ok, valid: false }).canSave, false);
    assert.equal(getSettingsSaveState({ ...ok, isManaged: false }).canSave, false);
    assert.equal(getSettingsSaveState({ ...ok, isConnected: false }).canSave, false);
  });

  it("loading/saving/unconfirmed block saves but keep error visible", () => {
    for (const patch of [{ loading: true }, { saving: true }, { unconfirmed: true }]) {
      const s = getSettingsSaveState({ ...ok, ...patch });
      assert.equal(s.canSave, false);
      assert.equal(s.disabled, true);
    }
  });
});

describe("settings drafts across reconnect", () => {
  it("preserves dirty edits instead of applying them automatically", () => {
    assert.deepEqual(draftAfterConnectionLoss("2.5", true, true), {
      draft: "2.5",
      dirty: true,
      touched: true,
    });
    assert.deepEqual(draftAfterConnectionLoss("4", true, false), {
      draft: "4",
      dirty: true,
      touched: false,
    });
  });

  it("clears clean drafts so old daemon values cannot leak", () => {
    assert.deepEqual(draftAfterConnectionLoss("2.5", false, true), {
      draft: "",
      dirty: false,
      touched: false,
    });
    assert.deepEqual(draftAfterConnectionLoss("", false, false), {
      draft: "",
      dirty: false,
      touched: false,
    });
  });
});

describe("App.svelte source guards: tabs, toggles, settings nav, rebrand", () => {
  it("has an accessible tablist with Downloads default + Settings gear", () => {
    assert.ok(appSrc.includes('role="tablist"'), "missing tablist");
    assert.ok(appSrc.includes('role="tab"'), "missing tabs");
    assert.ok(appSrc.includes("Downloads"), "missing Downloads tab");
    assert.ok(appSrc.includes("Settings"), "missing Settings tab");
    assert.ok(appSrc.includes("tab-gear") || appSrc.includes("<svg"), "missing gear icon");
    assert.ok(appSrc.includes('aria-selected={activeTab === "downloads"'), "tabs must bind aria-selected");
    assert.ok(appSrc.includes('aria-controls="panel-downloads"'), "missing panel wiring");
    assert.ok(appSrc.includes('aria-controls="panel-settings"'), "missing settings panel wiring");
    assert.ok(appSrc.includes('id="panel-downloads"'), "missing downloads tabpanel");
    assert.ok(appSrc.includes('id="panel-settings"'), "missing settings tabpanel");
    assert.ok(appSrc.includes('hidden={activeTab !== "downloads"}'), "downloads panel must hide");
    assert.ok(appSrc.includes('hidden={activeTab !== "settings"}'), "settings panel must hide");
  });

  it("defaults to downloads and never restarts the daemon on tab switch", () => {
    assert.ok(
      appSrc.includes('let activeTab = $state<AppTab>("downloads")') ||
        appSrc.includes("activeTab") && appSrc.includes('"downloads"'),
      "downloads must be the default tab",
    );
    const selectTab = appSrc.slice(appSrc.indexOf("function selectTab"), appSrc.indexOf("function selectTab") + 400);
    assert.ok(!selectTab.includes("startManaged") && !selectTab.includes("disconnect"), "tab switch must not touch daemon");
  });

  it("keeps drafts across navigation (state lives outside tab conditionals)", () => {
    assert.ok(appSrc.includes("let rateDraft"), "rate draft missing");
    assert.ok(appSrc.includes("let maxConcurrentDraft"), "concurrency draft missing");
    assert.ok(appSrc.includes("let addUrl"), "add draft missing");
  });

  it("moves speed, concurrency and advanced connection into Settings", () => {
    const settingsAt = appSrc.indexOf('id="panel-settings"');
    assert.ok(settingsAt > -1, "no settings panel");
    const settingsSrc = appSrc.slice(settingsAt);
    assert.ok(settingsSrc.includes('id="global-rate"'), "speed control must live in Settings");
    assert.ok(settingsSrc.includes('id="max-concurrent"'), "concurrency control must live in Settings");
    assert.ok(settingsSrc.includes('id="daemon-endpoint"'), "advanced connection must live in Settings");
    const downloadsAt = appSrc.indexOf('id="panel-downloads"');
    const downloadsSrc = appSrc.slice(downloadsAt, settingsAt);
    assert.ok(!downloadsSrc.includes('id="global-rate"'), "speed must not remain in Downloads");
    assert.ok(!downloadsSrc.includes('id="max-concurrent"'), "concurrency must not remain in Downloads");
    assert.ok(downloadsSrc.includes('id="add-url"'), "add URLs must stay in Downloads");
  });

  it("uses a larger multiline URL paste area", () => {
    assert.ok(appSrc.includes('rows="8"'), "URL textarea must be larger (rows=8)");
    assert.ok(appSrc.includes("min-height: 12rem") || appSrc.includes("min-height:12rem"), "URL area needs multiline min-height");
  });

  it("has compact per-batch + persisted-default toggles with clear labels", () => {
    assert.ok(appSrc.includes('id="hash-batch"'), "missing per-batch toggle");
    assert.ok(appSrc.includes('id="hash-save-default"'), "missing persisted-default toggle");
    assert.ok(appSrc.includes("Compute hashes for this batch"), "per-batch label unclear");
    assert.ok(appSrc.includes("Enable hashing by default"), "persist label unclear");
    assert.ok(!appSrc.includes("Save as default for future batches"), "old default label must be gone");
    assert.ok(appSrc.includes("hash-controls"), "toggles must share a compact row");
    assert.ok(
      /input\.checked = confirmed|currentTarget\.checked = confirmed/.test(appSrc),
      "save failure must explicitly restore the confirmed checkbox value",
    );
    assert.ok(
      /const confirmed = activeController\.state\.hashDefaultEnabled === true/.test(appSrc),
      "restore must use the confirmed boolean, not the attempted toggle value",
    );
  });

  it("keeps Settings rendered but disabled offline with dirty drafts preserved", () => {
    const settingsAt = appSrc.indexOf('id="panel-settings"');
    const rateAt = appSrc.indexOf('id="global-rate"');
    assert.ok(settingsAt > -1 && rateAt > settingsAt, "speed control must live in Settings");
    assert.ok(
      !appSrc.slice(settingsAt, rateAt).includes("{#if isConnected}"),
      "Settings controls must stay rendered offline",
    );
    assert.ok(appSrc.includes("Settings stay visible but are disabled offline"), "offline note missing");
    const preservationCalls = appSrc.match(/draftAfterConnectionLoss\(/g) ?? [];
    assert.ok(
      preservationCalls.length >= 4,
      "both rate and concurrency drafts must use the preserve-on-reconnect helper",
    );
    assert.ok(!appSrc.includes("rateDraft = \"\";\n    rateDirty = false;"), "disconnect must not wipe dirty rate drafts");
  });

  it("uses a div tablist instead of a nav landmark", () => {
    assert.ok(appSrc.includes('<div class="tabs" role="tablist"'), "tablist must be a plain div");
    assert.ok(!appSrc.includes('<nav class="tabs" role="tablist"'), "nav + tablist landmark conflict");
  });

  it("removes the explanatory hashing sentence but keeps managed-only + status context", () => {
    assert.ok(!appSrc.includes("Hashes each queued download after it finishes"), "explanatory sentence must be removed");
    assert.ok(appSrc.includes("Available only on the bundled daemon connection"), "managed-only reason must stay");
    assert.ok(appSrc.includes("Default for future batches"), "default status must stay");
    assert.ok(appSrc.includes("Loading hashing default"), "loading status must stay");
    assert.ok(appSrc.includes("Retry loading default"), "retry must stay");
    assert.ok(appSrc.includes("Saving default"), "saving status must stay");
  });

  it("retains dirty-choice, batch snapshot, persistent-setting API and redacted warnings", () => {
    assert.ok(appSrc.includes("hashBatchDirty"), "dirty-choice tracking missing");
    assert.ok(appSrc.includes("hashSnapshot") || appSrc.includes("hashForBatch"), "batch snapshot missing");
    assert.ok(appSrc.includes("loadHashDefault") && appSrc.includes("setHashDefault"), "persistent-setting API missing");
    assert.ok(appSrc.includes("hashIntentWarnings"), "hash warnings missing");
    assert.ok(appSrc.includes("bulkUncertainties"), "bulk warnings missing");
  });

  it("rebrands visible Surge Desktop to Surgui without touching contracts", () => {
    assert.ok(appSrc.includes("Surgui"), "wordmark must read Surgui");
    assert.ok(!appSrc.includes("Surge Desktop"), "visible Surge Desktop must be gone from UI");
  });

  it("stays keyboard + screenreader accessible", () => {
    assert.ok(appSrc.includes("handleTabKeydown"), "arrow-key tab nav missing");
    assert.ok(appSrc.includes("ArrowRight") && appSrc.includes("ArrowLeft"), "left/right nav missing");
    assert.ok(appSrc.includes("aria-describedby"), "describedby missing");
    assert.ok(appSrc.includes(":focus-visible"), "focus ring missing");
  });
});

describe("QueueScreen source guards: rebrand + hash copy trim", () => {
  it("rebrands to Surgui", () => {
    assert.ok(queueSrc.includes("Surgui"), "QueueScreen must read Surgui");
    assert.ok(!queueSrc.includes("Surge Desktop"), "Surge Desktop must be gone from QueueScreen");
  });

  it("keeps managed-only + error/status context without the explanatory sentence", () => {
    assert.ok(!queueSrc.includes("Digests identify file content"), "explanatory sentence must be removed");
    assert.ok(queueSrc.includes("Hashing needs the bundled daemon connection"), "managed-only reason must stay");
  });
});

describe("product identity guards: visible name vs frozen contracts", () => {
  it("index.html titles the app Surgui", () => {
    const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
    assert.ok(html.includes("<title>Surgui</title>"), "title must be Surgui");
    assert.ok(!html.includes("Surge Desktop"), "old title must be gone");
  });

  it("tauri.conf renames display name but preserves identifier", () => {
    const conf = JSON.parse(fs.readFileSync(path.join(root, "src-tauri", "tauri.conf.json"), "utf8"));
    assert.equal(conf.productName, "surgui");
    assert.equal(conf.identifier, "com.surge.desktop");
    assert.equal(conf.app.windows[0].title, "Surgui");
  });

  it("README rebrands and flags NSIS upgrade risk (not tested)", () => {
    const readme = fs.readFileSync(path.join(root, "README.md"), "utf8");
    assert.ok(readme.startsWith("# Surgui"), "README title must be Surgui");
    assert.ok(!readme.includes("# Surge Desktop"), "old README title must be gone");
    assert.ok(readme.includes("surgui-windows-x64-exe"), "Windows artifact label must be surgui-");
    assert.ok(readme.includes("surgui-linux-x64-deb"), "deb artifact label must be surgui-");
    assert.ok(/NSIS upgrade.*not (been )?tested/i.test(readme) || /not claimed/i.test(readme), "must flag NSIS upgrade risk");
  });

  it("workflows rename artifact labels only", () => {
    const win = fs.readFileSync(path.join(root, ".github", "workflows", "windows-build.yml"), "utf8");
    const lin = fs.readFileSync(path.join(root, ".github", "workflows", "linux-build.yml"), "utf8");
    assert.ok(win.includes("surgui-windows-x64-exe"), "windows artifact must be surgui-");
    assert.ok(!win.includes("surge-desktop-windows-x64-exe"), "old windows artifact must be gone");
    assert.ok(lin.includes("surgui-linux-x64-deb"), "deb artifact must be surgui-");
    assert.ok(lin.includes("surgui-linux-x64-appimage"), "appimage artifact must be surgui-");
  });
});

describe("responsive + Hallmark floors: 320/375/414/768 without horizontal scroll", () => {
  it("clips horizontal overflow at the root", () => {
    const css = fs.readFileSync(path.join(root, "src", "app.css"), "utf8");
    assert.ok(css.includes("overflow-x: clip"), "root must clip horizontal overflow");
  });

  it("tab labels stay single-line; panels hide without layout shift", () => {
    assert.ok(appSrc.includes("white-space: nowrap"), "affordances must not wrap");
    assert.ok(appSrc.includes("#panel-downloads[hidden]"), "hidden panels must collapse");
  });
});
