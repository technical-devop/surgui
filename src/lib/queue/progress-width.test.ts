// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Regression guard for QueueScreen.svelte progress-fill widths: live progress
// must move at subpercent resolution. The row and detail fills previously used
// Math.round(progressRatio*100) for the width, so small real byte updates
// snapped to whole percents. Numeric text and aria-valuenow stay rounded;
// only the fill width uses the unrounded clamped ratio.
// A full component mount is out of scope (node --test cannot import .svelte),
// so this test inspects the component source and evaluates the extracted
// width expressions against real progressRatio values.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { progressRatio } from "./queue.ts";

const here = path.dirname(fileURLToPath(import.meta.url));
const src = fs.readFileSync(path.join(here, "..", "QueueScreen.svelte"), "utf8");

// Both progress-fill spans (row list + detail pane) bind a width template.
const widthExprs = [...src.matchAll(/class="progress-fill"[^>]*?style=\{`width: \$\{([^}]+)\}%`\}/g)].map(
  (m) => m[1],
);

function evalWidth(expr, ratio) {
  // The extracted expression must be a pure arithmetic term over `ratio`.
  assert.match(
    expr,
    /^[\d\s*+/().-]*ratio[\d\s*+/().-]*$/,
    `width expression must be arithmetic over ratio, got: ${expr}`,
  );
  return Function("ratio", `"use strict"; return (${expr});`)(ratio);
}

describe("QueueScreen progress-fill fractional width", () => {
  it("row and detail both bind a width expression", () => {
    assert.equal(widthExprs.length, 2, `expected 2 progress-fill widths (row + detail), got ${widthExprs.length}`);
  });

  it("widths use unrounded ratio*100, not Math.round", () => {
    for (const expr of widthExprs) {
      assert.ok(!expr.includes("Math.round"), `width must not round: ${expr}`);
      assert.ok(!/\bpct\b/.test(expr), `width must not use the rounded pct: ${expr}`);
    }
  });

  it("small fractional real updates move the fill width below one percent", () => {
    // Two live updates inside the same whole percent (50.1% -> 50.2%).
    const before = { id: "f1", name: "live", status: "active", bytesDone: 501, bytesTotal: 1000, speedBps: 1, addedAt: 1 };
    const after = { ...before, bytesDone: 502 };
    const rBefore = progressRatio(before);
    const rAfter = progressRatio(after);
    assert.equal(Math.round(rBefore * 100), Math.round(rAfter * 100), "rounded text must still read 50 for both");
    for (const expr of widthExprs) {
      const wBefore = evalWidth(expr, rBefore);
      const wAfter = evalWidth(expr, rAfter);
      assert.equal(wBefore, 50.1, `expected unrounded width 50.1, got ${wBefore}`);
      assert.equal(wAfter, 50.2, `expected unrounded width 50.2, got ${wAfter}`);
      assert.notEqual(wBefore, wAfter, "width must change on subpercent progress");
    }
  });

  it("width equals clamped progressRatio*100 for row and detail sources", () => {
    const item = { id: "f2", name: "live", status: "active", bytesDone: 1234, bytesTotal: 10000, speedBps: 1, addedAt: 1 };
    const expected = progressRatio(item) * 100;
    assert.ok(Math.abs(expected - 12.34) < 1e-9, `expected 12.34, got ${expected}`);
    for (const expr of widthExprs) {
      assert.equal(evalWidth(expr, progressRatio(item)), expected);
    }
    // Clamping preserved: out-of-range ratios stay within 0..100.
    for (const expr of widthExprs) {
      assert.equal(evalWidth(expr, progressRatio({ ...item, bytesDone: -5 })), 0);
      assert.equal(evalWidth(expr, progressRatio({ ...item, bytesDone: 99999 })), 100);
      assert.equal(evalWidth(expr, progressRatio({ ...item, bytesTotal: 0 })), 0);
    }
  });

  it("numeric text and aria-valuenow stay rounded", () => {
    const pctDefs = [...src.matchAll(/\{@const pct = ([^}]+)\}/g)].map((m) => m[1]);
    assert.equal(pctDefs.length, 2, `expected rounded pct consts for row + detail, got ${pctDefs.length}`);
    for (const def of pctDefs) {
      assert.ok(def.includes("Math.round"), `pct must stay Math.round for display/aria: ${def}`);
    }
    const ariaNow = [...src.matchAll(/aria-valuenow=\{([^}]+)\}/g)].map((m) => m[1]);
    assert.equal(ariaNow.length, 2, "row and detail progressbars expose aria-valuenow");
    for (const v of ariaNow) assert.equal(v, "pct", "aria-valuenow must use the rounded pct");
    assert.ok(src.includes("<span>{pct}%</span>"), "row meta percent text must use rounded pct");
    assert.ok(src.includes("<div><dt>Progress</dt><dd>{pct}%</dd></div>"), "detail percent text must use rounded pct");
    assert.ok(
      src.includes("${pct} percent"),
      "row aria-label must keep the rounded percent",
    );
  });
});
