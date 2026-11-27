// @ts-nocheck — this file runs under node --test, which strips types rather
// than checking them; the assertions below cover runtime behavior only.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BATCH_STATUS_LABELS,
  INVALID_URL_DISPLAY,
  MAX_BATCH_LINES,
  MAX_CONCURRENT_DOWNLOADS_DEFAULT,
  MAX_CONCURRENT_DOWNLOADS_MAX,
  MAX_CONCURRENT_DOWNLOADS_MIN,
  MAX_DOWNLOAD_URL_BYTES,
  batchStatusMessage,
  consumeRecoveryDraft,
  isValidMaxConcurrentValue,
  mergeRecoverableLines,
  planUrlLines,
  redactUrlForDisplay,
  resolveBatchDraft,
  urlLineErrorMessage,
  validateDownloadUrl,
} from "./queue.ts";

const here = path.dirname(fileURLToPath(import.meta.url));
const appPath = path.join(here, "..", "App.svelte");

function deferred() {
  let resolve;
  const promise = new Promise((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

describe("validateDownloadUrl", () => {
  it("accepts http and https URLs with hosts", () => {
    assert.deepEqual(validateDownloadUrl("https://example.com/file.iso"), {
      ok: true,
      url: "https://example.com/file.iso",
    });
    assert.deepEqual(validateDownloadUrl("  http://127.0.0.1:8080/a?b=c  "), {
      ok: true,
      url: "http://127.0.0.1:8080/a?b=c",
    });
  });

  it("rejects empty, malformed, non-http and credential-bearing URLs", () => {
    assert.equal(validateDownloadUrl("").reason, "empty");
    assert.equal(validateDownloadUrl("not a url").reason, "whitespace");
    assert.equal(validateDownloadUrl("ftp://example.com/a").reason, "scheme");
    assert.equal(validateDownloadUrl("https://user:pass@example.com/a").reason, "credentials");
    assert.equal(validateDownloadUrl("https://").reason, "malformed");
  });

  it("rejects whitespace, control characters and oversized URLs", () => {
    assert.equal(validateDownloadUrl("https://example.com/a b").reason, "whitespace");
    assert.equal(validateDownloadUrl("https://example.com/\u0000").reason, "whitespace");
    const oversized = `https://example.com/${"a".repeat(MAX_DOWNLOAD_URL_BYTES)}`;
    assert.equal(validateDownloadUrl(oversized).reason, "too-long");
  });
});

describe("planUrlLines", () => {
  it("splits one URL per line, ignores blanks, and preserves order", () => {
    const plan = planUrlLines(
      "\nhttps://example.com/a\n  \nhttp://example.com/b\r\nhttps://example.com/c\n",
    );
    assert.deepEqual(
      plan.map((entry) => entry.line),
      ["https://example.com/a", "http://example.com/b", "https://example.com/c"],
    );
    assert.deepEqual(
      plan.map((entry) => entry.index),
      [0, 1, 2],
    );
    assert.ok(plan.every((entry) => entry.kind === "url"));
  });

  it("classifies invalid lines in place without dropping them", () => {
    const plan = planUrlLines(
      "https://example.com/ok\nftp://example.com/bad\nhttps://user@example.com/cred",
    );
    assert.equal(plan[0].kind, "url");
    assert.equal(plan[1].kind, "invalid");
    assert.equal(plan[1].reason, "scheme");
    assert.equal(plan[2].kind, "invalid");
    assert.equal(plan[2].reason, "credentials");
  });

  it("marks lines past the batch limit as over-limit and unsent", () => {
    const lines = Array.from(
      { length: MAX_BATCH_LINES + 2 },
      (_, index) => `https://example.com/${index}`,
    );
    const plan = planUrlLines(lines.join("\n"));
    assert.equal(plan.length, MAX_BATCH_LINES + 2);
    assert.equal(plan[MAX_BATCH_LINES - 1].kind, "url");
    assert.equal(plan[MAX_BATCH_LINES].kind, "over-limit");
    assert.equal(plan[MAX_BATCH_LINES + 1].kind, "over-limit");
  });
});

describe("max concurrent download boundaries", () => {
  it("matches the pinned daemon default and 1–10 range", () => {
    assert.equal(MAX_CONCURRENT_DOWNLOADS_DEFAULT, 3);
    assert.equal(MAX_CONCURRENT_DOWNLOADS_MIN, 1);
    assert.equal(MAX_CONCURRENT_DOWNLOADS_MAX, 10);
  });

  it("accepts only whole numbers from 1 through 10", () => {
    for (const value of [0, 11, -1, 2.5, Number.NaN, Number.POSITIVE_INFINITY, "3", null]) {
      assert.equal(isValidMaxConcurrentValue(value), false, `accepted ${String(value)}`);
    }
    for (const value of [1, 3, 10]) {
      assert.equal(isValidMaxConcurrentValue(value), true, `rejected ${value}`);
    }
  });
});

describe("batch outcome copy", () => {
  it("labels every outcome in words, not colour alone", () => {
    for (const status of [
      "queued",
      "pendingApproval",
      "duplicate",
      "duplicateInBatch",
      "invalid",
      "failed",
      "ambiguous",
      "unattempted",
    ]) {
      assert.ok(BATCH_STATUS_LABELS[status].length > 0);
      assert.ok(batchStatusMessage(status, "scheme").length > 0);
    }
    assert.equal(BATCH_STATUS_LABELS.ambiguous, "Unconfirmed");
    assert.match(batchStatusMessage("ambiguous"), /not retried/i);
    assert.match(batchStatusMessage("failed"), /not retried/i);
    assert.match(batchStatusMessage("unattempted"), /before this link was sent/i);
    assert.match(batchStatusMessage("duplicateInBatch"), /earlier in the batch/i);
  });

  it("maps invalid reasons to actionable copy", () => {
    for (const reason of [
      "empty",
      "too-long",
      "whitespace",
      "scheme",
      "host",
      "credentials",
      "malformed",
    ]) {
      assert.ok(urlLineErrorMessage(reason).length > 0);
    }
    assert.match(urlLineErrorMessage("scheme"), /http/i);
    assert.match(urlLineErrorMessage("credentials"), /username or password/i);
  });
});

describe("per-link display redaction", () => {
  it("never echoes credentials, query or fragment", () => {
    const display = redactUrlForDisplay(
      "https://user:pass@example.com/file.iso?sig=signed-secret#frag",
    );
    assert.ok(display.startsWith("https://example.com/file.iso"));
    assert.ok(!display.includes("user"));
    assert.ok(!display.includes("pass"));
    assert.ok(!display.includes("signed-secret"));
    assert.ok(!display.includes("#frag"));
    assert.ok(display.includes("?redacted"));
  });

  it("returns a generic non-sensitive label for unparseable input", () => {
    const display = redactUrlForDisplay(`${"x".repeat(200)}`);
    assert.equal(display, INVALID_URL_DISPLAY);
    assert.ok(!display.includes("x"), "no part of the original input may be shown");
  });

  it("never echoes opaque non-http or malformed URLs, including their prefixes", () => {
    const unsafe = [
      "mailto:alice:SECRET@example.com",
      "mailto:alice:SECRET@example.com?token=secret123",
      "ftp://alice:SECRET@example.com/file?token=secret123",
      "data:text/plain;base64,U0VDUkVU",
      "javascript:alert('SECRET')",
      "file:///home/alice/SECRET.txt",
      "custom-opaque:alice:SECRET@example.com",
      "https://[bad?token=secret123",
      "not-a-url?token=secret123#token=secret123",
    ];
    for (const input of unsafe) {
      const display = redactUrlForDisplay(input);
      assert.equal(display, INVALID_URL_DISPLAY, `expected generic label for ${input}`);
      for (const fragment of ["SECRET", "secret123", "mailto:", "ftp:", "data:", "javascript:", "custom-opaque:", "https:", "alice"]) {
        assert.ok(
          !display.toLowerCase().includes(fragment.toLowerCase()),
          `leaked ${fragment} for ${input}: ${display}`,
        );
      }
    }
  });

  it("fails closed and strips secrets from every malformed display output", () => {
    const malformed = [
      "https://[bad?token=secret123",
      "https://user:pass@[bad?token=secret123",
      "https://[bad#token=secret123",
      "https://user:pass@[bad#token=secret123",
      "not-a-url?token=secret123#token=secret123",
    ];
    for (const input of malformed) {
      const display = redactUrlForDisplay(input);
      assert.ok(!display.includes("secret123"), `leaked secret for ${input}: ${display}`);
      assert.ok(!display.includes("token="), `leaked query/fragment for ${input}: ${display}`);
      assert.ok(!display.includes("user"), `leaked userinfo for ${input}: ${display}`);
      assert.ok(!display.includes("pass"), `leaked userinfo for ${input}: ${display}`);
    }
  });
});

describe("recoverable draft merge", () => {
  it("returns recoverable lines when the input is empty", () => {
    assert.equal(
      mergeRecoverableLines("", ["https://example.com/a.bin", "https://example.com/b.bin"]),
      "https://example.com/a.bin\nhttps://example.com/b.bin",
    );
  });

  it("keeps a user draft and appends recoverable lines after it", () => {
    assert.equal(
      mergeRecoverableLines("https://draft.example/new.bin", [
        "https://example.com/failed.bin",
      ]),
      "https://draft.example/new.bin\nhttps://example.com/failed.bin",
    );
  });

  it("does not append an exact line already present in the draft", () => {
    assert.equal(
      mergeRecoverableLines("https://example.com/failed.bin\nhttps://draft.example/new.bin", [
        "https://example.com/failed.bin",
        "https://example.com/other.bin",
      ]),
      "https://example.com/failed.bin\nhttps://draft.example/new.bin\nhttps://example.com/other.bin",
    );
  });
});

describe("consumed recovery draft", () => {
  it("preserves the current draft and consumes pending lines exactly once", () => {
    const pending = [
      "https://example.com/uncertain.bin",
      "https://example.com/unsent.bin",
    ];
    const first = consumeRecoveryDraft("https://draft.example/keep.bin", pending);
    assert.equal(
      first.draft,
      "https://draft.example/keep.bin\nhttps://example.com/uncertain.bin\nhttps://example.com/unsent.bin",
    );
    assert.deepEqual(first.pending, []);
    const second = consumeRecoveryDraft(first.draft, first.pending);
    assert.equal(second.draft, first.draft, "no pending lines must leave the edited draft untouched");
  });

  it("does not resurrect a recovered line the user removed after it was consumed", () => {
    const pending = ["https://example.com/recovered.bin"];
    const consumed = consumeRecoveryDraft("", pending);
    const edited = consumed.draft.replace("https://example.com/recovered.bin", "").trim();
    const again = consumeRecoveryDraft(edited, consumed.pending);
    assert.equal(again.draft, "", "consumed recoveries must not be replayed on connect/disconnect");
  });
});

describe("deferred batch draft resolution", () => {
  const base = {
    draft: "",
    submitted: "https://example.com/a.bin",
    hasResults: true,
    recoverable: ["https://example.com/a.bin"],
    operationId: 1,
    currentOperationId: 1,
    epoch: 7,
    currentEpoch: 7,
    sameController: true,
  };

  it("applies recoverable lines and preserves a draft typed while the batch ran", async () => {
    const settled = deferred();
    const apply = settled.promise.then(() =>
      resolveBatchDraft({ ...base, draft: "https://draft.example/new.bin" }),
    );
    settled.resolve();
    assert.equal(
      await apply,
      "https://draft.example/new.bin\nhttps://example.com/a.bin",
    );
  });

  it("leaves the draft untouched when a deferred result settles after reconnect", async () => {
    const settled = deferred();
    const apply = settled.promise.then(() =>
      resolveBatchDraft({
        ...base,
        draft: "https://draft.example/still-here.bin",
        currentEpoch: 8,
      }),
    );
    settled.resolve();
    assert.equal(await apply, null);
  });

  it("leaves the draft untouched when a newer submission owns the operation", async () => {
    const settled = deferred();
    const apply = settled.promise.then(() =>
      resolveBatchDraft({
        ...base,
        draft: "https://draft.example/still-here.bin",
        currentOperationId: 2,
      }),
    );
    settled.resolve();
    assert.equal(await apply, null);
  });

  it("restores the submitted input without discarding a draft when no results exist", async () => {
    const settled = deferred();
    const apply = settled.promise.then(() =>
      resolveBatchDraft({
        ...base,
        draft: "https://draft.example/still-here.bin",
        hasResults: false,
        recoverable: [],
      }),
    );
    settled.resolve();
    assert.equal(
      await apply,
      "https://draft.example/still-here.bin\nhttps://example.com/a.bin",
    );
  });
});

describe("App recovery draft wiring", () => {
  it("restores only unconsumed lines into the current draft", () => {
    const src = fs.readFileSync(appPath, "utf8");
    const match = src.match(/function restoreRecoveredDraft\(\): void \{([\s\S]*?)\n  \}/);
    assert.ok(match, "expected restoreRecoveredDraft in App.svelte");
    const body = match[1];
    assert.ok(
      !/mergeRecoverableLines\(\s*""/.test(body),
      "connect/disconnect must merge into the current draft, never start from an empty base",
    );
    assert.ok(
      body.includes("pendingRecoveryLines"),
      "restore must consume pending recovery lines instead of replaying every historical batch",
    );
  });

  it("lists each recovered redacted link with its own outcome and never renders signed URLs", () => {
    const src = fs.readFileSync(appPath, "utf8");
    const template = src.slice(src.indexOf("</script>"));
    assert.match(template, /recovery\.results/, "recovered batches must list each redacted outcome");
    assert.match(
      template,
      /BATCH_STATUS_LABELS\[result\.status\]/,
      "each recovered link must carry its own status label",
    );
    assert.ok(
      !/recovery\.(uncertain|unsent|rejected)\b/.test(template),
      "full signed URLs must never be rendered; only counts and redacted results may appear",
    );
  });

  it("stores only redacted notices and moves raw lines into the editable draft", () => {
    const src = fs.readFileSync(appPath, "utf8");
    assert.ok(
      !/recoveredBatches\s*=\s*\$state<BatchRecovery\[\]>/.test(src),
      "the UI must not retain full BatchRecovery objects with raw signed arrays",
    );
    assert.match(src, /\$state<BatchRecoveryView\[\]>/, "notices must use the display projection type");
    assert.match(src, /projectBatchRecovery\(recovery\)/, "recoveries must be projected before display");
    assert.ok(
      !/recoveredBatches\s*=\s*\[\.\.\.recoveredBatches, \.\.\.drained\]/.test(src),
      "drained raw recoveries must never be stored directly",
    );
    assert.match(
      src,
      /pendingRecoveryLines = \[\.\.\.pendingRecoveryLines, \.\.\.lines\]/,
      "raw lines may live only in the pending editable-draft transfer",
    );
    assert.match(
      src,
      /recovery\.uncertain\.length = 0/,
      "raw arrays must be disposed after projection",
    );
  });

  it("offers explicit dismissal and an editable add surface while offline", () => {
    const src = fs.readFileSync(appPath, "utf8");
    const template = src.slice(src.indexOf("</script>"));
    assert.match(src, /function dismissRecoveredBatch\(id: number\): void \{/);
    assert.ok(
      template.includes("onclick={() => dismissRecoveredBatch(recovery.id)}"),
      "each notice needs an explicit dismiss control",
    );
    assert.match(template, /\{#if showAddPanel\}/, "the add panel must render outside connected-only UI");
    assert.ok(
      template.includes(
        "disabled={addBusy || addUrl.trim().length === 0 || !isConnected || hashChoiceUnknown}",
      ),
      "submission must stay blocked while disconnected or the hash default is unknown",
    );
    assert.ok(
      template.includes("this draft is preserved and editable"),
      "offline draft preservation must be surfaced to the user",
    );
  });
});
