// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Focused frontend tests for automatic MD5 + SHA-256 display helpers.
// Mocked data only; no daemon, no paths, no URLs, no hook errors.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  findDownloadHash,
  hashCopyValue,
  hashStateLabel,
  sanitizeDownloadHashes,
} from "./hashes.ts";

const MD5 = "900150983cd24fb0d6963f7d28e17f72";
const SHA256 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

describe("hash sanitize", () => {
  it("rejects malformed top-level payloads instead of treating them as empty", () => {
    assert.throws(() => sanitizeDownloadHashes({ items: [] }), /invalidResponse/);
    assert.throws(() => sanitizeDownloadHashes("not an array"), /invalidResponse/);
    assert.throws(() => sanitizeDownloadHashes(null), /invalidResponse/);
    assert.deepEqual(sanitizeDownloadHashes([]), []);
  });

  it("keeps full digests verbatim with no truncation", () => {
    const rows = sanitizeDownloadHashes([
      { id: "d-1", state: "complete", md5: MD5, sha256: SHA256, bytes: 3, resultVersion: 1 },
    ]);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].md5, MD5);
    assert.equal(rows[0].sha256, SHA256);
    assert.equal(rows[0].md5.length, 32);
    assert.equal(rows[0].sha256.length, 64);
  });

  it("drops rows with bad digests or unknown states without leaking input", () => {
    const rows = sanitizeDownloadHashes([
      { id: "bad-md5", state: "complete", md5: "XYZ", sha256: SHA256 },
      { id: "bad-sha", state: "complete", md5: MD5, sha256: "short" },
      { id: "bad-state", state: "calculating", md5: MD5, sha256: SHA256 },
      { id: "", state: "pending" },
    ]);
    assert.equal(rows.length, 0);
  });

  it("keeps queued/failed/stale/disabled states without digests", () => {
    const rows = sanitizeDownloadHashes([
      { id: "a", state: "pending" },
      { id: "b", state: "failed" },
      { id: "c", state: "unavailable" },
      { id: "d", state: "stale" },
      { id: "e", state: "disabled" },
    ]);
    assert.deepEqual(
      rows.map((r) => r.state),
      ["pending", "failed", "unavailable", "stale", "disabled"],
    );
    for (const row of rows) {
      assert.equal(row.md5, undefined);
      assert.equal(row.sha256, undefined);
    }
  });
});

describe("hash labels do not imply security verification", () => {
  it("uses content-identification wording, never verify/secure/trusted", () => {
    for (const state of ["pending", "complete", "failed", "unavailable", "stale", "disabled"]) {
      const label = hashStateLabel(state).toLowerCase();
      assert.ok(!label.includes("verif"), `state ${state} must not claim verification`);
      assert.ok(!label.includes("secure"), `state ${state} must not claim security`);
      assert.ok(!label.includes("trust"), `state ${state} must not claim trust`);
      assert.ok(label.length > 0);
    }
  });

  it("distinguishes queued vs calculating vs failure vs stale", () => {
    assert.match(hashStateLabel("pending").toLowerCase(), /queue|calculat|wait|pending/);
    assert.match(
      (hashStateLabel("failed") + " " + hashStateLabel("unavailable")).toLowerCase(),
      /fail|unavailable|not/,
    );
    assert.match(hashStateLabel("stale").toLowerCase(), /stale/);
  });
});

describe("hash copy values are exact", () => {
  it("returns exact lowercase digests for complete rows", () => {
    const rows = sanitizeDownloadHashes([
      { id: "d-1", state: "complete", md5: MD5, sha256: SHA256 },
    ]);
    assert.equal(hashCopyValue(rows[0], "md5"), MD5);
    assert.equal(hashCopyValue(rows[0], "sha256"), SHA256);
  });

  it("returns empty for non-complete rows", () => {
    const rows = sanitizeDownloadHashes([
      { id: "a", state: "pending" },
      { id: "b", state: "failed" },
    ]);
    assert.equal(hashCopyValue(rows[0], "md5"), "");
    assert.equal(hashCopyValue(rows[0], "sha256"), "");
    assert.equal(hashCopyValue(rows[1], "md5"), "");
  });

  it("finds rows by daemon id only", () => {
    const rows = sanitizeDownloadHashes([
      { id: "d-1", state: "pending" },
      { id: "d-2", state: "complete", md5: MD5, sha256: SHA256 },
    ]);
    assert.equal(findDownloadHash(rows, "d-2")?.id, "d-2");
    assert.equal(findDownloadHash(rows, "missing"), null);
  });
});
