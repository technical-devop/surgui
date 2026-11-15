# Surge Desktop

Unofficial desktop GUI for Surge, the download daemon. Built with Tauri + Svelte.

This is a **third-party controller**, not part of the Surge upstream project and not
affiliated with it. It targets the Surge v0.12.4 API. The app bundles a patched Surge v0.12.4 daemon for Windows x64 and Linux x64. It starts the daemon automatically on 127.0.0.1:1700 over TLS and stops its owned daemon on disconnect or exit. If the port is occupied, startup fails rather than using an unrelated listener.

## Scope

- Native Tauri app for Windows x64 and Linux x64. The bundled daemon starts automatically; advanced manual connection accepts an endpoint URL and bearer token.
- Add URL through the UI; pause, resume, and remove with an in-app confirmation step.
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

## Advanced: Running the daemon (Windows)

Surge runs as a separate server process. Download `surge.exe` for Windows from the
[Surge v0.12.4 release](https://github.com/SurgeDM/Surge/releases/tag/v0.12.4),
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

Surge v0.12.4's server listens broadly on the host by default — not only on
loopback. Before starting it, block inbound port 1700 in Windows Firewall (or your
equivalent), or put the daemon behind an HTTPS tunnel. The app requires HTTPS for
anything that is not loopback HTTP, but that is a client-side rule only; it does
not restrict who can reach the daemon.

## Advanced: Running the daemon (Linux)

The same Surge release ships Linux binaries. Download
`Surge_0.12.4_linux_amd64.tar.gz` (or `Surge_0.12.4_linux_arm64.tar.gz`) from the
[Surge v0.12.4 release](https://github.com/SurgeDM/Surge/releases/tag/v0.12.4),
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
- **Add URL**: sends the URL to the daemon. The daemon may queue it immediately, or
  return "pending approval" — in that case approve it in the Surge TUI on the
  daemon host; the row is not queued until then. A duplicate URL is reported and
  not re-queued.
- **Pause / resume**: sends `/pause` and `/resume` for the selected record.
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
  `surge-desktop-windows-x64-exe` from `src-tauri/target/release/bundle/nsis/*.exe`,
  retained **7 days** then auto-deleted by Actions.

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
- Output: unsigned `.deb` uploaded as artifact `surge-desktop-linux-x64-deb` from
  `src-tauri/target/release/bundle/deb/*.deb`, and an AppImage uploaded as
  `surge-desktop-linux-x64-appimage` from
  `src-tauri/target/release/bundle/appimage/*.AppImage`, both retained
  **7 days** then auto-deleted by Actions.

## Install notes

- The setup EXE is **unsigned**. Windows SmartScreen may warn about an unrecognized
  app or unknown publisher; that is expected. Only install artifacts built from a
  trusted commit in this repository.
- On Linux, install the `.deb` with `sudo apt install ./surge-desktop_*.deb` (or
  `dpkg -i` plus `apt -f install`), or run the AppImage directly after
  `chmod +x`. Both are **unsigned**; this is expected, the same as the Windows
  EXE. The AppImage needs a FUSE-capable environment (or run it with
  `--appimage-extract-and-run`).
- No GitHub Release is published automatically. Publication happens by hand, only
  after an EXE smoke test passes.
- MSI installations are not upgraded in place by the setup EXE. If Surge
  Desktop is currently installed as an MSI, uninstall it first (Windows → Apps
  & features, or accept any NSIS installer prompt to remove it) before running
  the setup EXE. This only removes the GUI install, not files downloaded by
  the separate Surge daemon.

## Verification

- Frontend: `npm run check` (svelte-check), `npm test` (node --test on
  `src/lib/queue/*.test.ts`), `npm run build` (vite build).
- Rust, from `src-tauri/`: `cargo fmt --check`, `cargo test`,
  `cargo clippy --all-targets -- -D warnings`.
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
