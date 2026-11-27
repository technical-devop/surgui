# Surgui

Unofficial desktop GUI for Surge, the download daemon. Built with Tauri + Svelte.

This is a **third-party controller**, not part of the Surge upstream project and not
affiliated with it. It targets the Surge v0.13.0 API. The app bundles a patched Surge v0.13.0 daemon for Windows x64 and Linux x64. It starts the daemon automatically on 127.0.0.1:1700 over TLS and stops its owned daemon on disconnect or exit. If the port is occupied, startup fails rather than using an unrelated listener.

## Scope

- Native Tauri app for Windows x64 and Linux x64. The bundled daemon starts automatically; advanced manual connection accepts an endpoint URL and bearer token.
- Add one or more URLs through the UI (one per line, up to 100 per batch); the app
  submits them strictly one at a time through the same single-add path. Pause,
  resume, and remove with an in-app confirmation step.
- Managed credentials are generated per launch and passed to the child process environment, not the UI, disk, arguments, or logs. Local processes with sufficient privileges may inspect that environment.
- Remote endpoints must use HTTPS. Plain HTTP is loopback-only (e.g. `localhost`, or
  an SSH tunnel to the daemon host).
- Browser preview is sample-only and never connects to a daemon.
- **Open a completed download's containing folder**: only on the owned bundled
  (managed) daemon connection. The daemon-reported destination is validated
  locally and rejected when it is unsupported, non-local, or unsafe, then the
  containing folder opens in the platform file manager. The check is best-effort
  and fail-closed; it does not guarantee the path is physically local.
- **Global download speed limit**: entered in decimal MB/s (1 MB = 1,000,000
  bytes), where `0` means unlimited. Managed bundled-daemon connection only; if a
  write's outcome cannot be confirmed, speed control stays disabled until the
  app restarts.
- **Automatic MD5 + SHA-256**: an optional per-batch checkbox (backed by an
  app-wide persistent default that starts `false`) hashes each positively
  acknowledged queued download after the managed daemon fully finalizes it and
  any post-download hook has finished. Both full digests are shown. This is
  available only on the owned bundled managed-daemon connection. See
  [Automatic MD5 + SHA-256](#automatic-md5--sha-256-managed-daemon-only).
- **Daemon max simultaneous downloads**: reads and saves
  `network.max_concurrent_downloads` (1–10; daemon default 3). Saving requires
  the bundled managed-daemon connection. The setting is persisted by the daemon,
  but Surge applies it only after a daemon restart; the app never restarts the
  daemon automatically and does not claim a live effect. The write uses the
  pinned `PATCH /settings` change contract with the loaded value as `before`, so
  a concurrent settings change is rejected instead of overwritten. An
  unconfirmed write disables this control until the app restarts to avoid unsafe
  retries.

## Advanced: Running the daemon (Windows)

Surge runs as a separate server process. Download `surge.exe` for Windows from the
[Surge v0.13.0 release](https://github.com/SurgeDM/Surge/releases/tag/v0.13.0),
then extract it to a folder.

A minimal local setup uses two PowerShell windows, both opened in that extracted
Surge directory. In the first window, start the server and leave it running:

```powershell
.\surge.exe server --port 1700
```

Then open a second PowerShell window in the same extracted Surge directory and
print a user token:

```powershell
.\surge.exe token
```

(If you separately configure a Windows service, `.\surge.exe service token` prints
the service token instead; installing a service is not required for this setup.)

Then in the app:

- URL: `http://127.0.0.1:1700`
- Token: paste the token printed by `.\surge.exe token`. Enter it **raw, without a
  `Bearer ` prefix** — the app adds the `Authorization: Bearer` header itself. A
  Surge service token works in the same field if you use one instead of a user
  token.

### Network exposure

Upstream Surge v0.13.0 still defaults its server to all interfaces
(`0.0.0.0`), not loopback-only. Set `network.bind_host = '127.0.0.1'` in the
Surge settings before starting a manually installed daemon. If you do not, block
inbound port 1700 in Windows Firewall (or your equivalent), or put the daemon
behind an HTTPS tunnel. The app requires HTTPS for anything that is not loopback
HTTP, but that is a client-side rule only; it does not restrict who can reach
the daemon.

## Advanced: Running the daemon (Linux)

The same Surge release ships Linux binaries. Download
`Surge_0.13.0_linux_amd64.tar.gz` (or `Surge_0.13.0_linux_arm64.tar.gz`) from the
[Surge v0.13.0 release](https://github.com/SurgeDM/Surge/releases/tag/v0.13.0),
then extract it to a folder.

A minimal local setup uses two terminals, both opened in that extracted Surge
directory. In the first terminal, start the server and leave it running:

```sh
./surge server --port 1700
```

Then open a second terminal in the same extracted Surge directory and print a
user token:

```sh
./surge token
```

Then in the app:

- URL: `http://127.0.0.1:1700`
- Token: paste the token printed by `./surge token`. Enter it **raw, without a
  `Bearer ` prefix** — the app adds the `Authorization: *** header itself.

The port-1700 exposure caveat from [Network exposure](#network-exposure) applies
on Linux too: firewall the port (e.g. `ufw deny 1700/tcp`) or tunnel the
connection before starting a broadly-listening server.

## What the GUI does

- **Connect / list**: authenticated `GET /list`, then the queue refreshes on a
  timer.
- **Live progress (optional)**: authenticated `GET /events` SSE adds
  near-real-time progress on top of polling. The snapshot cursor is captured
  before `/list` and every event is sequenced at client receipt, but receipt
  order cannot prove daemon order: the daemon stream carries no generation id,
  so the cursor is a conservative heuristic, not absolute ordering. Byte
  counters are kept monotonic for a download generation as a best-effort
  guard; a transient stale value can still appear. A
  snapshot that reports fewer bytes than the displayed value for the same
  id/addedAt is ambiguous: if a newer progress receipt arrived after the list
  request started, the live value is kept and a fresh authoritative follow-up
  read (single-flight, bounded backoff) conservatively confirms whether it was a real reset.
  Only a read that still reports the lower value after its cursor covers the
  newer receipt is treated as a likely reset; live events for that row are then
  quarantined because a delayed sample from the previous generation cannot be
  distinguished without a daemon generation id. Snapshots alone update the row
  until its `addedAt` changes, the record disappears, or it becomes
  `completed`/`failed` (paused/pausing do not release the quarantine). A lower snapshot with no
  newer in-flight progress is accepted as a reset through reconciliation, without
  claiming that a stale snapshot can never overwrite newer progress.
  The `/events` handshake is bounded but an established idle body is never cut,
  manual mode disables proxies, and repeated malformed or immediately-empty
  streams back off and fall back to polling. Reconnect backoff resets only after
  a sustained stream, so one valid frame followed by an immediate EOF keeps
  backing off instead of retrying forever at 500 ms. Listener registration is
  bounded, cleaned up on failure/timeout, and retried on the next reconnect; the
  native stream starts only after all three listeners are ready.
- **Add URLs**: accepts HTTP(S) URLs one per line. Blank lines are ignored;
  invalid or non-HTTP(S) lines are reported without being sent; up to 100 lines
  are attempted per batch. Valid lines are submitted strictly one at a time
  through the existing single-add route, so the daemon still manages download
  concurrency itself. Each line reports exactly one outcome: accepted/queued,
  pending approval, duplicate, invalid, failed, or unconfirmed (ambiguous).
  A disconnect, a definitive connection failure, or an unconfirmed request
  stops the batch; remaining links are not attempted, and
  invalid/failed/unconfirmed/unattempted lines stay in the input for the user
  to fix or resend deliberately. Nothing is retried automatically. Pending approval still happens in the Surge TUI on the daemon
  host; a duplicate URL is reported and not re-queued.
- **Pause / resume**: sends `/pause` and `/resume` for the selected record.
- **Pause all / Resume all**: sends the daemon's bulk `/pause-all` and
  `/resume-all` routes. These apply to the daemon's whole queue, not just the
  rows currently filtered in the UI. Pause all targets every item except
  completed, already-paused, and errored ones (queued and unknown included);
  Resume all targets every paused item, including individually paused ones.
  The UI reports accepted and failed counts from the bounded, validated
  per-item response. Per-item daemon error text is never forwarded: it can
  contain a token, URL, or local path, so failed rows show a fixed safe
  message and only the validated item id. The UI acknowledges the request,
  states that row changes are applied asynchronously, and keeps refreshing
  periodically. A transport or malformed/unconfirmed outcome is reported as
  uncertain, triggers a fresh daemon reconciliation, and is **never retried
  automatically**. If the connection changes while a bulk request is in
  flight, a persistent warning survives reconnect and clears only on explicit
  per-warning dismissal. It says the current connection **may be the same
  daemon**, so the unconfirmed action may still apply; it never claims the new
  connection is unaffected. A timeout on the connection that is still current
  says so directly instead of blaming a previous connection. Warnings stay
  visible while offline/connecting, are bounded in number, and any overflow is
  reported conservatively; the overflow summary has its own dismissal that
  leaves the individual warnings in place. A busy/rejected bulk attempt never
  turns a still-confirmed request into an uncertainty warning. Server ids and
  raw per-item error strings are never rendered.
- **Remove**: asks for confirmation, then calls `/delete`. It never calls `/purge`.
  Removing may delete the incomplete working file of a transfer still in progress;
  a completed final file is left on disk.
- **Disconnect**: stops the bundled managed daemon and clears the connection. An externally managed daemon is not stopped.
- **Open download location**: offered on completed rows. Sends only the row id;
  the backend refreshes `/list` on the owned bundled daemon, requires exactly one
  completed, non-moving, present file, validates the reported destination
  (unsupported, non-local, or unsafe locations fail closed), then opens the
  containing folder in the platform file manager. Manual/external connections are
  refused; the destination path is not returned to the app interface or logged by the app.
- **Global download speed limit**: reads `network.global_rate_limit` from
  `/settings` and saves a decimal MB/s value (`0` = unlimited) through
  `/rate-limit/global`, confirming the readback. Only the bundled managed daemon
  can be written; an unconfirmed write disables speed control until the app
  restarts.
- **Daemon max simultaneous downloads**: reads
  `network.max_concurrent_downloads` from `/settings` (1–10; default 3) and
  saves it with `PATCH /settings` using the change's `before` baseline. Only the
  bundled managed daemon can be written; manual connections are read-only. The
  app never restarts the daemon, so the UI presents every save as requiring a
  daemon restart and does not claim the running concurrency changed. A 409
  means the daemon value changed since it was loaded; reload and retry. An
  unconfirmed write disables this control until the app restarts, independent of
  the speed-limit control.

## Automatic MD5 + SHA-256 (managed daemon only)

The batch submission form has one optional hash checkbox. Its initial state is
the persisted app-wide default, which is `false` until the user changes it. The
default affects future batches only; it does not retroactively hash existing
rows. Per-batch choices are otherwise independent.

The backend binds hashing to the **confirmed queued download id**, not to a URL,
filename, or queue difference. It also persists a random per-owned-queue
namespace anchored to the authenticated app-private managed state directory,
plus the daemon-reported `added_at` (zero when v0.13 omits it) and a stable
queue fingerprint read back from that daemon. The namespace deliberately does
not depend on the ephemeral
connection id or process id, so the exact same persistent managed queue resumes
after an app restart. A `202 pending_approval` response carries an
approval-request id rather than a download id, and a duplicate has no new id, so
neither is bound. An ambiguous add is never retried automatically. If the association or provenance
cannot be proven, the intent is not recorded and `hashRecorded` is false instead
of guessing another download with the same id. If the app crashes after the
daemon acknowledges the add but before the intent file write is durable, the add
can succeed without a hash intent; this is an unavoidable two-party
acknowledgement window, and the feature does not claim exactly-once processing.
The add response reports whether the intent was durably recorded.

Only the owned bundled managed daemon is eligible. Manual/remote connections
are never hashed. A single bounded background worker makes an authenticated
`GET /list` and requires exactly one row with the bound id. It hashes only when
the daemon record is `completed`, its phase is not `verifying` or `finalizing`,
and it is not `moving`, `file_missing`, or `processing`. If a post-download hook
applies to the download (using the same released v0.13.0 category selection as
the daemon), the hook must also have finished (`processed`). Completed rows that
are still finalizing remain waiting. The daemon-provided destination is never
accepted from the UI.

The file is opened read-only and never written. On Linux each component is
opened relative to the previous directory with `O_NOFOLLOW`; the final regular
file is pinned before it is read and every ancestor-directory identity is
recorded. After the read, the path is reopened component-by-component from the
root, descriptor-relative and no-follow, and each ancestor identity plus the
final file identity, size, mtime, and change time (`ctime`) must match. This
rejects a replaced
ancestor even when the original inode is hard-linked into the new path. The
mount is checked against the same conservative local-filesystem allowlist used
by the open-location feature.

On Windows the path must be a drive-letter path on a fixed local drive.
UNC, verbatim, device, drive-relative, alternate-stream, reserved-device, and
trailing-dot/space names are rejected before any file is opened. The drive root
is pinned, then each component is opened with `NtCreateFile` relative to the
previously opened directory handle using `FILE_OPEN_REPARSE_POINT`. Every
opened component is checked for reparse tags (symbolic links, junctions, mount
points, or any other reparse point), directory/file type, disk-backed (non-
network) file type, and offline/recall attributes; any uncertainty fails
closed. The final regular file is read from that same handle. After the read,
the handle identity, size, mtime, and NTFS change time are rechecked, then the
drive root and every component are reopened and compared with the originally
pinned identities. A replaced root, ancestor, or final file fails even if the original
file object remains reachable through the old handle.

Both digests are streamed in one pass over the same opened handle with a fixed
64 KiB buffer. The daemon record (same id, `added_at`, destination, size,
completion state, hook state), managed daemon ownership, and connection
epoch/pid are rechecked before publication, and the post-download hook
configuration is read again after hashing. A changed or replaced file, replaced
ancestor, duplicate id, unsafe path, stale intent generation, removed record,
changed daemon, or malformed hook settings fails closed without publishing
digests. When completed rows are served, the authoritative `/list` record, the
same completion/hook rules, and the exact persisted file identity (including
change time) are re-verified first; a removed/replaced file or removed/changed
queue record is invalidated to `stale` before any digest is returned. Rows that
completed or changed while that validation was paused are not published with
digests, and the `set_download_hash_intents` response goes through the same
verified serve path instead of returning stored values directly.

Only sanitized states (`pending`, `complete`, `failed`, `unavailable`, `stale`,
`disabled`), whole lowercase hex digests, and byte counts cross IPC. Raw paths,
URLs, tokens, daemon errors, category commands, and hook errors never do.
Results are associated with the stable per-owned-queue namespace, not an
ephemeral connection id or process id. A replaced/new managed queue, a different
namespace, a reused id with different provenance, or missing/malformed
namespace metadata fails closed as `stale`; it is never rebound by id alone. A
new app lifecycle for the exact same persistent managed queue resumes saved
pending work and retains completed digests only after a fresh `/list` record and
the exact file identity are revalidated. A separate in-process lifecycle fence
still prevents old in-flight work from publishing after disconnect/reconnect.

State is stored in a bounded, versioned JSON file in the app-private managed
state directory, alongside a separate random namespace file created with
private permissions. Writes use a same-directory temporary file, fsync, an
atomic rename, and a directory fsync on Linux. Publication uses a two-phase
durable marker: `pending` is synced before the rename, `durable` after the new
state and directory entry are synced, and `acked` only after the caller proves
the lifecycle still authorizes the write. An unacknowledged `pending` or
`durable` marker makes the next owner fail closed; a stale lifecycle aborts
before `acked`, so a successor never accepts obsolete authority. An `acked`
marker records an authorized durable state whose unlink was not durable, and a
successor may accept that state. Consequently a lost reply after authorization
can make an operation visible after restart even though the caller saw an
unconfirmed error; this is deliberately not an exactly-once promise. A pending
marker is never treated as confirmation, and no manual removal is required or
recommended. The namespace is independent of the persisted default preference,
so a missing/corrupt queue namespace cannot erase the user's default. Missing
state means disabled and empty. Malformed or unsupported-version state fails
safe to disabled/empty rather than guessing. Duplicate ids in one binding call
keep the last value, and the record count is bounded.

The advisory lock protects only owners that honor it. An older build or another
writer that ignores the lock can interleave with a cooperating owner; a
check-then-rename cannot close that window. Mixed old/new versions running at
the same time are not claimed to be safe. Directory durability is implemented
for Linux; the Windows path is compiled but not runtime-validated in this
environment; macOS and other platforms have no implemented directory-durability
primitive, so automatic hash persistence fails closed there before creating
namespace metadata. macOS is therefore intentionally unsupported for this
feature rather than silently assuming directory durability. The helper fallback
that re-executes the main binary is also not packaging-validated. A
non-filesystem global initialization guard is taken before the helper performs
any path lookup, so successor helper processes, including lexical or symlink
aliases of the same directory, fail closed instead of accumulating behind an
uninterruptible preparation wait. This guard is not keyed by the supplied path
and does not extend across different Linux users or network namespaces, or
across Windows logon sessions. The per-directory advisory lock still serializes
cooperating owners that share the directory. An operation already stuck in such
a kernel wait cannot be cancelled and can keep the guard until the kernel
returns.

Metadata checks cannot prove immutability against a same-user actor that
mutates a file during the read, replaces an ancestor after the final descriptor
check, or deliberately restores its size, mtime, and change time afterwards.
Including ctime catches ordinary same-size rewrites that restore only mtime, but
a privileged or same-user actor with the tools and timing to restore all
metadata cannot be fully excluded. These checks reduce the race window and reject
every detectable change, but they are not a cryptographic guarantee. On Linux the pinned handle is reopened through `/proc/self/fd`; if
procfs is unavailable the hash fails closed. On Windows the pinned
`FILE_OPEN_REPARSE_POINT` component chain and the post-read re-walk narrow the
same race window; neither platform claims cryptographic immutability.

### IPC contract

- `add_download_with_options(url, hash)` → `{ status, id?, hashRecorded? }`.
  `id` and `hashRecorded` are present only for a confirmed queued add when
  `hash` was requested. `status` remains `queued`, `pendingApproval`, or
  `duplicate` for compatibility.
- `get_automatic_hash_settings()` → `{ defaultEnabled }`.
- `set_automatic_hash_default(enabled)` → `{ defaultEnabled }`.
- `set_download_hash_intents([{ id, enabled }])` → current sanitized rows,
  re-verified through the same serve path (never stored digests directly).
  Accepts only daemon ids and booleans; at most 100 intents per call.
- `get_download_hashes()` → current daemon's sanitized rows, re-verified
  against the authoritative queue record and exact file identity first.
- `refresh_download_hashes()` → asks the single worker for another pass and
  returns the re-verified currently visible rows.

### Where files land

Downloads are written on the **daemon host**, not the machine running the GUI. With
Surge's default settings that is usually the user's Downloads folder on the daemon
host, or wherever `--output`, config, or category settings point. The app does not
choose, change, or display the destination path; treat the daemon's own
configuration as the source of truth.

## Windows build (manual only)

- Workflow: `.github/workflows/windows-build.yml`
- Trigger: manual only, via the GitHub web UI:
  **Actions → Windows build (manual) → Run workflow**. No push, pull-request,
  schedule, or release triggers, and no release, deploy, or signing steps.
- Runner: `windows-latest`, with `contents: read` permission; concurrent runs on the
  same ref cancel in progress.
- Steps: frontend check / test / build, then Rust `fmt --check` / `test` /
  `clippy --all-targets -- -D warnings`, then
  `npm run tauri build -- --bundles nsis`.
- Output: unsigned NSIS setup EXE uploaded as artifact
  `surgui-windows-x64-exe` from `src-tauri/target/release/bundle/nsis/*.exe`,
  retained **7 days** then auto-deleted by Actions. NSIS upgrade compatibility
  from a previous install under the old product name is not tested and not claimed.

## Linux build (manual only)

- Workflow: `.github/workflows/linux-build.yml`
- Trigger: manual only, via the GitHub web UI:
  **Actions → Linux build (manual) → Run workflow**. No push, pull-request,
  schedule, or release triggers, and no release, deploy, or signing steps.
- Runner: `ubuntu-latest`, with `contents: read` permission; concurrent runs on the
  same ref cancel in progress. The job installs the Tauri v2 Linux prerequisites
  (`libwebkit2gtk-4.1-dev` and friends) before building.
- Steps: frontend check / test / build, then Rust `fmt --check` / `test` /
  `clippy --all-targets -- -D warnings`, then
  `npm run tauri build -- --bundles deb,appimage`.
- Output: unsigned `.deb` uploaded as artifact `surgui-linux-x64-deb` from
  `src-tauri/target/release/bundle/deb/*.deb`, and an AppImage uploaded as
  `surgui-linux-x64-appimage` from
  `src-tauri/target/release/bundle/appimage/*.AppImage`, both retained
  **7 days** then auto-deleted by Actions.

## Install notes

- The setup EXE is **unsigned**. Windows SmartScreen may warn about an unrecognized
  app or unknown publisher; that is expected. Only install artifacts built from a
  trusted commit in this repository.
- On Linux, install the `.deb` with `sudo apt install ./surgui_*.deb` (or
  `dpkg -i` plus `apt -f install`), or run the AppImage directly after
  `chmod +x`. Both are **unsigned**; this is expected, the same as the Windows
  EXE. The AppImage needs a FUSE-capable environment (or run it with
  `--appimage-extract-and-run`).
- No GitHub Release is published automatically. Publication happens by hand, only
  after an EXE smoke test passes.
- MSI installations are not upgraded in place by the setup EXE. If Surgui
  (previously installed under the old product name) is currently installed,
  uninstall it first (Windows → Apps & features, or accept any NSIS installer
  prompt to remove it) before running the setup EXE. NSIS upgrade
  compatibility from a previous install under the old product name has not been tested
  and is not claimed. This only removes the GUI install, not files downloaded by
  the separate Surge daemon.

## Verification

- Frontend: `npm run check` (svelte-check), `npm test` (node --test on
  `src/lib/queue/*.test.ts`), `npm run build` (vite build).
- Rust, from `src-tauri/`: `cargo fmt --check`, `cargo test`,
  `cargo clippy --all-targets -- -D warnings`.
- Automatic-hash Rust tests cover known MD5/SHA-256 vectors, zero-byte and
  multi-buffer files, unsafe paths, same-size modification and replacement,
  the v0.13 completion/finalizing/hook matrix, duplicate and missing ids,
  stale intent generations, stable per-queue namespace/provenance binding,
  pending resume after restart, fail-closed serving after file/queue removal
  or replacement, the disabled app default with fail-closed atomic
  persistence (including pending/durable/acked markers, stale-lifecycle abort,
  lost-reply durability, marker-cleanup failure, durable restart recovery,
  pre-lock helper blocking, and reaper wakeup/spawn-failure paths), malformed
  hook settings, cancellation, and the single bounded worker. Windows-cfg filesystem tests cover a nested
  regular local path, nonlocal/device/verbatim and dangerous names,
  symbolic-link, junction/reparse, replaced-ancestor, mutation/replacement
  during hashing, cancellation, and identity/descriptor stability. The Windows
  link tests explicitly skip only when the test account lacks symlink/junction
  creation privilege.
- Windows CI: the manual workflow above (`.github/workflows/windows-build.yml`)
  runs those same frontend and Rust checks on `windows-latest` and then builds
  the NSIS setup EXE, uploaded as a 7-day artifact. A green run covers checks
  and packaging only — it never installs or launches the app.
- Linux CI: the manual workflow above (`.github/workflows/linux-build.yml`)
  runs those same frontend and Rust checks on `ubuntu-latest` and then builds
  the `.deb` and AppImage, uploaded as 7-day artifacts. Same caveat: a green
  run covers checks and packaging only — it never installs or launches the app.
- The installed GUI is a separate manual smoke, done before any publication:
  install a fresh EXE (or `.deb`/AppImage on Linux) and exercise connect, add,
  pause, resume, remove, and disconnect against a real daemon.

## Development

On Linux, the Rust side needs the Tauri v2 system prerequisites before
`cargo`/`tauri dev` will build, e.g. on Debian/Ubuntu:
`sudo apt install libwebkit2gtk-4.1-dev build-essential curl wget file libxdo-dev libssl-dev libayatana-appindicator3-dev librsvg2-dev`.

```sh
npm ci
npm run check     # svelte-check
npm test          # node --test on src/lib/queue/*.test.ts
npm run build     # vite build
```

Rust, from `src-tauri/`:

```sh
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
```

No signing secrets, Surge tokens, or daemon setup are part of this repo or its CI.

## License

MIT — see [LICENSE](LICENSE).
