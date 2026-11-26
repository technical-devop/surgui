// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Source/render wiring guard: NativeQueueController already exposes
// hashIntentWarnings / hashIntentWarningOverflow + dismiss handlers; App.svelte
// must surface them alongside the existing bulk uncertainty notices, visible
// offline/across reconnect, with the same design and accessible dismiss
// controls. No URLs or paths in the new UI; per-batch copy stays unchanged.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const appPath = path.join(here, "..", "App.svelte");
const screenPath = path.join(here, "..", "QueueScreen.svelte");

function templateOf(src) {
  return src.slice(src.indexOf("</script>"));
}

describe("hash intent warnings wiring", () => {
  it("App.svelte exposes controller warnings alongside bulk uncertainties", () => {
    const src = fs.readFileSync(appPath, "utf8");
    assert.match(src, /snapshot\.hashIntentWarnings \?\? \[\]/, "App must derive hashIntentWarnings from snapshot");
    assert.match(src, /snapshot\.hashIntentWarningOverflow \?\? 0/, "App must derive hashIntentWarningOverflow from snapshot");
    assert.match(src, /hashIntentWarnings=\{hashIntentWarnings\}/, "App must pass warnings to QueueScreen");
    assert.match(src, /hashIntentWarningOverflow=\{hashIntentWarningOverflow\}/, "App must pass overflow to QueueScreen");
    assert.match(
      src,
      /onDismissHashIntentWarning=\{handleDismissHashIntentWarning\}/,
      "App must pass per-warning dismiss handler",
    );
    assert.match(
      src,
      /onDismissHashIntentWarningOverflow=\{handleDismissHashIntentWarningOverflow\}/,
      "App must pass overflow dismiss handler",
    );
    assert.match(
      src,
      /function handleDismissHashIntentWarning\(id: number\)/,
      "App must define per-warning dismiss handler",
    );
    assert.match(
      src,
      /controller\?\.dismissHashIntentWarning\(id\)/,
      "dismiss handler must route through the controller",
    );
    assert.match(
      src,
      /controller\?\.dismissHashIntentWarningOverflow\(\)/,
      "overflow dismiss must route through the controller",
    );
  });

  it("QueueScreen renders persistent warnings with the same design and dismiss controls", () => {
    const src = fs.readFileSync(screenPath, "utf8");
    const template = templateOf(src);
    assert.match(src, /HashIntentWarning/, "QueueScreen must type the new warnings");
    assert.match(src, /hashIntentWarnings = \[\]/, "warnings prop must default to an empty list");
    assert.match(src, /hashIntentWarningOverflow = 0/, "overflow prop must default to zero");
    assert.match(
      template,
      /\{#if hashIntentWarnings\.length > 0 \|\| hashIntentWarningOverflow > 0\}/,
      "warnings region must render whenever warnings or overflow exist",
    );
    assert.match(
      template,
      /aria-label="Unresolved hash requests"/,
      "warnings region needs its own accessible label",
    );
    assert.match(
      template,
      /\{#each hashIntentWarnings as warning \(warning\.id\)\}/,
      "each warning must render independently",
    );
    assert.ok(
      template.includes("{warning.message}"),
      "warning text must come from the redacted controller message",
    );
    assert.match(
      template,
      /aria-label="Dismiss hash request warning"/,
      "each warning needs an accessible dismiss control",
    );
    assert.ok(
      template.includes("onDismissHashIntentWarning(warning.id)"),
      "dismiss control must target its own warning id",
    );
    assert.match(
      template,
      /\{#if hashIntentWarningOverflow > 0\}/,
      "overflow notice must render when bounded warnings are omitted",
    );
    assert.ok(
      template.includes("additional unresolved hash"),
      "overflow notice must name the omitted hash warnings",
    );
    assert.match(
      template,
      /aria-label="Dismiss overflowed hash warnings"/,
      "overflow notice needs its own accessible dismiss control",
    );
    assert.ok(
      template.includes("onDismissHashIntentWarningOverflow()"),
      "overflow dismiss must not remove retained warnings",
    );
    // Same design: reuse the bulk uncertainty classes, no new visual system.
    assert.ok(template.includes('class="bulk-uncertain"'), "warnings must reuse the bulk notice design");
    assert.ok(template.includes('class="bulk-uncertain-text"'), "warning text must reuse the bulk text style");
    assert.ok(
      template.includes('class="bulk-uncertain-overflow-row"'),
      "overflow must reuse the bulk overflow layout",
    );
  });

  it("warnings stay visible offline and keep per-batch copy unchanged", () => {
    const screen = fs.readFileSync(screenPath, "utf8");
    const screenTemplate = templateOf(screen);
    const bulkIndex = screenTemplate.indexOf("Unresolved bulk actions");
    const hashIndex = screenTemplate.indexOf("Unresolved hash requests");
    assert.ok(bulkIndex >= 0 && hashIndex >= 0, "both notice regions must exist");
    const bulkNativeGuard = screenTemplate.lastIndexOf('{#if mode === "native"}', hashIndex);
    const bulkRegionClose = screenTemplate.lastIndexOf("{/if}", hashIndex);
    assert.ok(
      bulkNativeGuard < 0 || bulkRegionClose > bulkNativeGuard,
      "hash warnings must render outside the native-only bulk bar so offline/reconnect still shows them",
    );
    const hashBlock = screenTemplate.slice(hashIndex, hashIndex + 4000);
    assert.ok(!hashBlock.includes("{warning.line}"), "warnings must never render raw link lines");
    assert.ok(!hashBlock.includes("{warning.url}"), "warnings must never render URLs");
    // No paths: the block renders only the redacted message plus counts.
    assert.ok(!/https?:\/\//.test(hashBlock), "warning UI must not hard-code URLs or paths");
    const app = fs.readFileSync(appPath, "utf8");
    assert.ok(
      app.includes("batch?.results.some((result) => result.status"),
      "per-batch outcome copy must remain wired as before",
    );
  });

  it("overflow dismissal keeps retained warnings", () => {
    const src = fs.readFileSync(screenPath, "utf8");
    const template = templateOf(src);
    const overflowIndex = template.indexOf("Dismiss overflowed hash warnings");
    assert.ok(overflowIndex > 0, "expected overflow dismiss control");
    assert.ok(
      !template.slice(overflowIndex - 1500, overflowIndex).includes("hashIntentWarnings = []"),
      "overflow dismissal must not clear the retained warning list",
    );
  });
});
