// @ts-nocheck — exercised at runtime by node --test (type stripping).
// Focused frontend tests for automatic-hash IPC wiring.
// Fake invoke bridge only: no daemon, no paths, no URLs, no secrets.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { NativeQueueController } from "./native-controller.ts";

const MD5 = "900150983cd24fb0d6963f7d28e17f72";
const SHA256 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

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

describe("automatic hash default", () => {
  it("reloads persisted default (false first run) via get_automatic_hash_settings", async () => {
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: false };
      if (command === "get_download_hashes") return [];
      if (command === "refresh_download_hashes") return [];
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashDefault();
      const call = calls.find((x) => x.command === "get_automatic_hash_settings");
      assert.ok(call, "expected get_automatic_hash_settings");
      assert.equal(c.state.hashDefaultEnabled, false);
    } finally {
      c.dispose();
    }
  });

  it("persists default via set_automatic_hash_default without touching in-flight batch", async () => {
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: false };
      if (command === "set_automatic_hash_default") return { defaultEnabled: true };
      if (command === "get_download_hashes") return [];
      if (command === "refresh_download_hashes") return [];
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.loadHashDefault();
      const ok = await c.setHashDefault(true);
      assert.equal(ok, true);
      const call = calls.find((x) => x.command === "set_automatic_hash_default");
      assert.ok(call);
      assert.deepEqual(call.args, { enabled: true });
      assert.equal(c.state.hashDefaultEnabled, true);
    } finally {
      c.dispose();
    }
  });
});

describe("batch hash snapshot", () => {
  it("uses add_download_with_options with snapshotted hash for accepted batch", async () => {
    const { invoke, calls } = makeInvoke((command, args) => {
      if (command === "connect_surge") return [];
      if (command === "list_downloads") return [];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      if (command === "get_download_hashes") return [];
      if (command === "refresh_download_hashes") return [];
      if (command === "add_download_with_options") {
        return { status: "queued", id: "dl-1", hashRecorded: true };
      }
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect("http://127.0.0.1:9", "t");
      const result = await c.addBatch("https://example.com/a.bin", true);
      assert.equal(result.results[0].status, "queued");
      const addCalls = calls.filter((x) => x.command === "add_download_with_options");
      assert.equal(addCalls.length, 1);
      assert.deepEqual(addCalls[0].args, { url: "https://example.com/a.bin", hash: true });
    } finally {
      c.dispose();
    }
  });

  it("supports pending, duplicate and ambiguous without guessing or retries", async () => {
    const { invoke, calls } = makeInvoke((command, args) => {
      if (command === "connect_surge") return [];
      if (command === "list_downloads") return [];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: false };
      if (command === "get_download_hashes") return [];
      if (command === "refresh_download_hashes") return [];
      if (command === "add_download_with_options") {
        const url = args.url;
        if (url.endsWith("/a.bin")) return { status: "queued", id: "dl-a", hashRecorded: false };
        if (url.endsWith("/b.bin")) return { status: "pendingApproval" };
        if (url.endsWith("/c.bin")) return { status: "duplicate" };
        throw "network";
      }
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect("http://127.0.0.1:9", "t");
      const result = await c.addBatch(
        "https://example.com/a.bin\nhttps://example.com/b.bin\nhttps://example.com/c.bin\nhttps://example.com/d.bin",
        false,
      );
      const byStatus = result.results.map((r) => r.status);
      assert.deepEqual(byStatus, ["queued", "pendingApproval", "duplicate", "ambiguous"]);
      // No automatic retry: exactly one IPC per attempted link, none for the tail after ambiguous.
      const addCalls = calls.filter((x) => x.command === "add_download_with_options");
      assert.equal(addCalls.length, 4);
      for (const call of addCalls) assert.equal(call.args.hash, false);
      // Unconfirmed + unattempted stay recoverable.
      assert.ok(result.recoverable.length >= 1);
    } finally {
      c.dispose();
    }
  });

  it("fast completion still records hash intent via receipt id", async () => {
    const { invoke } = makeInvoke((command) => {
      if (command === "connect_surge") return [];
      if (command === "list_downloads") return [];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      if (command === "get_download_hashes") return [];
      if (command === "refresh_download_hashes") return [];
      if (command === "add_download_with_options") return { status: "queued", id: "fast-1", hashRecorded: true };
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect("http://127.0.0.1:9", "t");
      const result = await c.addBatch("https://example.com/fast.bin", true);
      assert.equal(result.results[0].status, "queued");
    } finally {
      c.dispose();
    }
  });
});

describe("hash visibility, manual and stale fencing", () => {
  it("manual connections show no hashes with disabled explanation", async () => {
    const { invoke } = makeInvoke((command) => {
      if (command === "connect_surge") return [];
      if (command === "list_downloads") return [];
      if (command === "get_download_hashes")
        return [{ id: "d-1", state: "complete", md5: MD5, sha256: SHA256, bytes: 3, resultVersion: 1 }];
      if (command === "refresh_download_hashes") return [];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: false };
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect("http://127.0.0.1:9", "t");
      assert.equal(c.state.managed, false);
      await c.loadHashes();
      // Manual never surfaces another daemon's digests.
      assert.deepEqual(c.state.hashes ?? [], []);
    } finally {
      c.dispose();
    }
  });

  it("discards stale hash responses after reconnect and reconciles on startup", async () => {
    let releaseHashes;
    let hashCalls = 0;
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "connect_surge") return [];
      if (command === "list_downloads") return [];
      if (command === "disconnect_surge") return { ok: true };
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      if (command === "get_download_hashes") {
        hashCalls += 1;
        if (hashCalls > 1) return [];
        return new Promise((resolve) => {
          releaseHashes = () =>
            resolve([{ id: "old", state: "complete", md5: MD5, sha256: SHA256, bytes: 3, resultVersion: 1 }]);
        });
      }
      if (command === "refresh_download_hashes") return [];
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      const pending = c.loadHashes();
      await c.disconnect();
      releaseHashes();
      await pending;
      // Stale generation must not populate the new (offline) connection.
      assert.deepEqual(c.state.hashes ?? [], []);
      // Restart reconciles fresh.
      await c.startManaged();
      await c.loadHashes();
      assert.ok(Array.isArray(c.state.hashes));
    } finally {
      c.dispose();
    }
  });

  it("renders full digests and surfaces queued/errors without truncation", async () => {
    const { invoke } = makeInvoke((command) => {
      if (command === "start_managed_surge") return [];
      if (command === "list_downloads") return [];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      if (command === "get_download_hashes")
        return [
          { id: "full", state: "complete", md5: MD5, sha256: SHA256, bytes: 3, resultVersion: 1 },
          { id: "q", state: "pending" },
          { id: "f", state: "failed" },
          { id: "s", state: "stale" },
        ];
      if (command === "refresh_download_hashes")
        return [
          { id: "full", state: "complete", md5: MD5, sha256: SHA256, bytes: 3, resultVersion: 1 },
          { id: "q", state: "pending" },
          { id: "f", state: "failed" },
          { id: "s", state: "stale" },
        ];
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.startManaged();
      await c.refreshHashes();
      const rows = c.state.hashes ?? [];
      assert.equal(rows.length, 4);
      const full = rows.find((r) => r.id === "full");
      assert.equal(full.md5, MD5);
      assert.equal(full.sha256, SHA256);
    } finally {
      c.dispose();
    }
  });
});

describe("hash request warnings", () => {
  const UNSAVED = /hash request was not saved/i;

  function addHandler(bySuffix) {
    return (command, args) => {
      if (command === "connect_surge") return [];
      if (command === "start_managed_surge") return [];
      if (command === "list_downloads") return [];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      if (command === "get_download_hashes") return [];
      if (command === "refresh_download_hashes") return [];
      if (command === "add_download_with_options") {
        for (const [suffix, receipt] of Object.entries(bySuffix)) {
          if (args.url.endsWith(suffix)) return receipt;
        }
        throw new Error(`unexpected add: ${command}`);
      }
      if (command === "add_download") return "queued";
      throw new Error(`unexpected: ${command}`);
    };
  }

  it("warns only for accepted queued items with a missing, false or invalid hashRecorded", async () => {
    const { invoke, calls } = makeInvoke(
      addHandler({
        "/saved.bin": { status: "queued", id: "d-saved", hashRecorded: true },
        "/missing.bin": { status: "queued", id: "d-missing" },
        "/invalid.bin": { status: "queued", id: "d-invalid", hashRecorded: "yes" },
        "/false.bin": { status: "queued", id: "d-false", hashRecorded: false },
        "/pending.bin": { status: "pendingApproval" },
        "/duplicate.bin": { status: "duplicate" },
      }),
    );
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect("http://127.0.0.1:9", "t");
      const result = await c.addBatch(
        [
          "https://example.com/saved.bin",
          "https://example.com/missing.bin",
          "https://example.com/invalid.bin",
          "https://example.com/false.bin",
          "https://example.com/pending.bin",
          "https://example.com/duplicate.bin",
        ].join("\n"),
        true,
      );
      assert.deepEqual(
        result.results.map((row) => row.status),
        ["queued", "queued", "queued", "queued", "pendingApproval", "duplicate"],
      );
      const warnings = c.state.hashIntentWarnings ?? [];
      assert.equal(warnings.length, 3, "only the unsaved queued adds warn");
      for (const warning of warnings) {
        assert.match(warning.message, UNSAVED);
        assert.doesNotMatch(warning.message, /example\.com|\.bin|\/saved|\/missing|\/invalid|\/false/);
      }
      const warned = result.results.filter((row) => row.status === "queued");
      assert.equal(warned.length, 4);
      assert.match(warned[1].message, UNSAVED);
      assert.match(warned[2].message, UNSAVED);
      assert.match(warned[3].message, UNSAVED);
      const addCalls = calls.filter((x) => x.command === "add_download_with_options");
      assert.equal(addCalls.length, 6, "no link is retried for a hash warning");
      assert.ok(addCalls.every((call) => call.args.hash === true));
    } finally {
      c.dispose();
    }
  });

  it("adds no warning for hash=false, pending or duplicate outcomes", async () => {
    const { invoke } = makeInvoke(
      addHandler({
        "/false.bin": { status: "queued", id: "d-1", hashRecorded: false },
        "/pending.bin": { status: "pendingApproval" },
        "/duplicate.bin": { status: "duplicate" },
      }),
    );
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect("http://127.0.0.1:9", "t");
      await c.addBatch(
        "https://example.com/false.bin\nhttps://example.com/pending.bin\nhttps://example.com/duplicate.bin",
        false,
      );
      assert.deepEqual(c.state.hashIntentWarnings ?? [], []);
    } finally {
      c.dispose();
    }
  });

  it("keeps the warning across a reconnect and never retries the download", async () => {
    let connects = 0;
    const { invoke, calls } = makeInvoke((command, args) => {
      if (command === "connect_surge") {
        connects += 1;
        return [];
      }
      if (command === "list_downloads") return [];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      if (command === "get_download_hashes") return [];
      if (command === "refresh_download_hashes") return [];
      if (command === "add_download_with_options")
        return { status: "queued", id: "d-1", hashRecorded: false };
      if (command === "disconnect_surge") return { ok: true };
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect("http://127.0.0.1:9", "t");
      await c.addBatch("https://example.com/lost.bin", true);
      assert.equal((c.state.hashIntentWarnings ?? []).length, 1);
      const warningId = c.state.hashIntentWarnings[0].id;

      await c.disconnect();
      assert.equal(
        c.state.hashIntentWarnings.length,
        1,
        "the warning survives a rapid reconnect",
      );
      await c.connect("http://127.0.0.1:9", "t");
      assert.equal(connects, 2);
      assert.equal(c.state.hashIntentWarnings.length, 1);
      assert.equal(
        calls.filter((x) => x.command === "add_download_with_options").length,
        1,
        "the accepted download must never be retried",
      );

      c.dismissHashIntentWarning(warningId);
      assert.equal(c.state.hashIntentWarnings.length, 0);
    } finally {
      c.dispose();
    }
  });

  it("bounds retained warnings and exposes a dismissible overflow count", async () => {
    const urls = Array.from({ length: 7 }, (_, index) => `https://example.com/lost-${index}.bin`);
    const { invoke } = makeInvoke((command) => {
      if (command === "connect_surge") return [];
      if (command === "list_downloads") return [];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      if (command === "get_download_hashes") return [];
      if (command === "refresh_download_hashes") return [];
      if (command === "add_download_with_options")
        return { status: "queued", id: "d", hashRecorded: false };
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect("http://127.0.0.1:9", "t");
      await c.addBatch(urls.join("\n"), true);
      assert.equal(c.state.hashIntentWarnings.length, 5);
      assert.equal(c.state.hashIntentWarningOverflow, 2);
      const retained = c.state.hashIntentWarnings.map((notice) => notice.id);

      c.dismissHashIntentWarningOverflow();
      assert.equal(c.state.hashIntentWarningOverflow, 0);
      assert.deepEqual(
        c.state.hashIntentWarnings.map((notice) => notice.id),
        retained,
      );

      c.dismissHashIntentWarning(retained[0]);
      assert.equal(c.state.hashIntentWarnings.length, 4);
    } finally {
      c.dispose();
    }
  });

  it("does not warn on the legacy manual add path that carries no hash flag", async () => {
    const { invoke, calls } = makeInvoke((command) => {
      if (command === "connect_surge") return [];
      if (command === "list_downloads") return [];
      if (command === "get_automatic_hash_settings") return { defaultEnabled: true };
      if (command === "get_download_hashes") return [];
      if (command === "refresh_download_hashes") return [];
      if (command === "add_download") return "queued";
      throw new Error(`unexpected: ${command}`);
    });
    const c = new NativeQueueController(invoke, () => {}, 60_000);
    try {
      await c.connect("http://127.0.0.1:9", "t");
      const result = await c.addBatch("https://example.com/manual.bin");
      assert.equal(result.results[0].status, "queued");
      assert.deepEqual(c.state.hashIntentWarnings ?? [], []);
      assert.equal(
        calls.filter((x) => x.command === "add_download").length,
        1,
        "the legacy path is used once",
      );
      assert.equal(
        calls.filter((x) => x.command === "add_download_with_options").length,
        0,
      );
    } finally {
      c.dispose();
    }
  });
});
