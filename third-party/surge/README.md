# Pinned Surge source build (groundwork only)

This directory pins the Surge daemon source that the Tauri app bundles. It
covers **source fetch + verify + patch + compile + bundle** only. There is
no runtime lifecycle here: no GUI code, no managed process, no runtime
download.

## Pin

- Upstream: `https://github.com/SurgeDM/Surge`
- Pinned tag: contents of `SURGE_VERSION` (`v0.12.4`)
- Tag commit (audit reference): contents of `SURGE_COMMIT`
  (`0481d309a2e41ccc7a7bda3b2fc368fcb9c1d892`)
- Source archive (CI fetches this exact URL, never `git clone`):
  `https://codeload.github.com/SurgeDM/Surge/tar.gz/refs/tags/v0.12.4`
- Archive SHA-256 (contents of `SURGE_SHA256`):
  `97d34131f6a0aba56a048e45bb7a5162558bb38ae37c9f0f8ee6d09e5b0b14f6`

The codeload tarball carries no git metadata, so CI cannot re-derive the
commit hash from the archive. CI pins the **content hash** above instead,
which is stronger for reproducibility: any tag move or content change fails
the `sha256sum -c` / `Get-FileHash` gate. The commit hash is kept here for
human audit.

## Patch

`surge-bind-loopback.patch` touches `cmd/root_http_server.go` and
`cmd/cmd_test.go`: it adds a managed mode that binds `127.0.0.1` at a fixed
port with no fallback, authenticates via an ephemeral token, and uses
in-memory TLS PEM, plus managed-mode tests.

## Upstream license

`../../src-tauri/resources/Surge-LICENSE` is a byte-for-byte copy of the
upstream `LICENSE` at the pinned tag (MIT, Copyright (c) 2025 Junaid Islam).
Both build scripts `cmp` the archive's `LICENSE` against the committed copy
and abort on any difference. The file is bundled via Tauri `resources` (see
below), so every installer carries the upstream notice with its original
copyright.

## Reproducible builds

Toolchain: Go `1.26.0` (matches upstream `go.mod`'s `go 1.26.0`), set via
`actions/setup-go` in both manual workflows. Build flags are fixed:

- `CGO_ENABLED=0`
- `GOTOOLCHAIN=local` (refuse silent toolchain switching)
- `GOFLAGS=-trimpath`
- `go build -trimpath -ldflags="-s -w" -o <resource> .` from the extracted
  source root, with `GOOS`/`GOARCH` per target:
  - Linux x64: `GOOS=linux GOARCH=amd64` →
    `src-tauri/resources/surge-x86_64-unknown-linux-gnu`
  - Windows x64: `GOOS=windows GOARCH=amd64` →
    `src-tauri/resources/surge-x86_64-pc-windows-msvc.exe`

Temporary files live only in `./.surge-stage/` (gitignored, in-repo). The
scripts never touch `/tmp`, `$TMPDIR`, or anything outside the repo.

### Linux (manual workflow `.github/workflows/linux-build.yml`)

1. `actions/setup-go@v5` with `go-version: '1.26.0'`.
2. `bash scripts/build-surge-linux.sh` — fetch, SHA-256 gate, extract,
   license `cmp`, strict patch, `go build`, `chmod 0755`, resource checks.
3. Existing frontend checks, Rust checks, then
   `npm run tauri build -- --bundles deb,appimage`.
4. Pre-bundle resource check asserts the Linux binary exists and is
   executable (`test -x`).

### Windows (manual workflow `.github/workflows/windows-build.yml`)

1. `actions/setup-go@v5` with `go-version: '1.26.0'`.
2. `powershell scripts/build-surge-windows.ps1` — same gates as Linux
   (SHA-256 via `Get-FileHash`, `git apply --check`, license byte compare,
   `go build`), output `surge-x86_64-pc-windows-msvc.exe`.
3. Existing frontend checks, Rust checks, then
   `npm run tauri build -- --bundles nsis`.
4. Pre-bundle resource check asserts the `.exe` exists.

## Tauri resource destination and executability

`src-tauri/tauri.conf.json` declares:

```json
"resources": [
  "resources/Surge-LICENSE",
  "resources/surge-*"
]
```

Paths are relative to `src-tauri/`. The `surge-*` glob matches whichever
platform binary the current runner staged (Linux and Windows runners each
stage only their own binary), plus the always-present `Surge-LICENSE`.
Tauri copies these entries into the bundle at package time; the exact
installed on-disk paths are chosen by Tauri and the platform bundler and
are not asserted here.

Executability (checks cover the staged file only):

- Linux: the script runs `chmod 0755` on the staged
  `surge-x86_64-unknown-linux-gnu` and CI asserts `test -x` on that staged
  file before `tauri build`. The gate verifies the pre-bundle artifact; it
  does not assert the file mode of an installed copy.
- Windows: executability is by `.exe` extension; CI asserts the staged
  `.exe` exists. No `chmod` step.

## Explicit non-goals (not implemented here)

- No Tauri command, frontend UI, sidecar config, or daemon supervisor.
- No download of Surge at app runtime; the binary is baked in at CI time.
- No auto-update of the daemon, no version check phone-home.
- No `MSI` target change, no signing, no release publication.

## Re-verify locally (feasible without Rust/Go toolchains)

- `node --test` frontend tests, `npm run check`, `npm run build`.
- `node -e "JSON.parse(...tauri.conf.json)"` config syntax check.
- `git apply --check third-party/surge/surge-bind-loopback.patch`-style
  dry run needs the upstream tree; when it is unavailable, inspect the
  patch context against the documented upstream line instead.
- Full `go build` and `tauri build` require Go 1.26 and Rust respectively;
  those two steps run in CI.
