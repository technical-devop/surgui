//! Standalone Windows owned child-process lifetime.
//!
//! NOTE (scope): this file is platform-local. It does **not** depend on
//! `managed_process.rs`; the coordinator selects it only under `cfg(windows)`.
//! No `Cargo.toml` change is needed: the implementation uses only `std` plus
//! manually declared `extern "system"` imports from `kernel32.dll` (no
//! `windows-sys` / `libc` dependency).
//!
//! # Guarantees
//! - Starts exactly the absolute binary the caller passes, verbatim, with the
//!   supplied `args` / env. Bare filenames and relative paths are rejected, so
//!   `PATH` is never searched (`lpApplicationName` is the absolute path).
//! - Retains only real OS handles (`hProcess` + kill-on-close job handle).
//!   Termination and reaping use only the owned process handle
//!   (`TerminateProcess` + `WaitForSingleObject`); no pid-based
//!   `OpenProcess` / signalling, so PID reuse can never redirect a kill.
//! - Normal drop terminates and reaps only the owned child. It uses a fixed
//!   fast-path wait and, if the exit is not confirmed, retains the owned
//!   process handle and keeps waiting with error backoff rather than closing an
//!   unconfirmed handle. `Drop` never panics.
//! - Forced parent death (parent gets `TerminateProcess`, destructors never
//!   run) is mitigated best-effort: the child is created `CREATE_SUSPENDED`
//!   with a non-inheritable Job Object carrying
//!   `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` passed via
//!   `PROC_THREAD_ATTRIBUTE_JOB_LIST`, then verified with `IsProcessInJob`
//!   before `ResumeThread`. This narrows but does not formally eliminate the
//!   orphan window, is not documented as transactional, and does not guarantee
//!   termination of all descendants (e.g. nested jobs or processes that leave
//!   the job by other means are out of scope).
//!
//! # Race narrowing (orphan window is narrowed, not eliminated)
//! A naive `CreateProcess(CREATE_SUSPENDED)` + `AssignProcessToJobObject` +
//! `ResumeThread` sequence is racy: if the parent dies after `CreateProcess`
//! but before `AssignProcessToJobObject`, the suspended child is never in the
//! job and survives as an orphan. This module does **not** use that sequence
//! as its safety basis.
//!
//! Instead it passes the job handle through `STARTUPINFOEXW` /
//! `PROC_THREAD_ATTRIBUTE_JOB_LIST` (`UpdateProcThreadAttribute`) so the
//! kernel is asked to assign the new process to the job during `CreateProcessW`.
//! This is a best-effort narrowing step, not a formally documented
//! transactional guarantee: creation failure handling still assumes a child
//! may exist, verifies with `IsProcessInJob` while still suspended, and fails
//! closed on mismatch. Crash behavior:
//! - crash before `CreateProcessW`: no child exists;
//! - crash after `CreateProcessW` (before / during / after `ResumeThread`):
//!   the child is expected to be in a `KILL_ON_JOB_CLOSE` job whose last
//!   handle is held by the parent, so parent-handle closure asks the kernel to
//!   terminate processes currently in that job — typically the suspended child
//!   — but full coverage of every nested grandchild is explicitly not claimed.
//!
//! Defense in depth (still suspended, before any child code runs):
//! - `IsProcessInJob` is checked before `ResumeThread`; on mismatch the
//!   still-suspended child is `TerminateProcess`ed and creation fails closed
//!   with [`SpawnError::HardeningUnavailable`].
//! - `ResumeThread` failure likewise terminates the suspended child and fails
//!   closed.
//! - If job verification or resume fails after `CreateProcessW`, the created
//!   process handles are retained and the process is terminated and waited on
//!   until the OS confirms exit. The handles are never closed as if reaped
//!   while exit is unconfirmed, and no pid-based signal is used. The dedicated
//!   owner thread reports `SpawnFailed` promptly (bounded caller waiting) and
//!   only marks its owner lifetime reaped after that confirmed wait.
//! - The job is created unnamed with `NULL` security attributes, which yields
//!   a non-inheritable handle, and `bInheritHandles` is `FALSE`, so the child
//!   cannot inherit the job handle and keep the job alive after parent death.
//! - `LimitFlags` sets **only** `KILL_ON_JOB_CLOSE`; in particular
//!   `JOB_OBJECT_LIMIT_BREAKAWAY_OK` is never set, so children cannot escape
//!   via `CREATE_BREAKAWAY_FROM_JOB`, and `SILENT_BREAKAWAY_OK` is never set.
//!
//! # Test coverage and native Windows gaps
//! The post-`CreateProcessW` failure decision (job-verify / resume) and the
//! terminate+wait cleanup are factored into injectable pure helpers
//! (`created_process_needs_cleanup` and
//! `cleanup_created_process_until_confirmed`) whose failure paths are unit
//! tested on any host: persistent wait errors, wait timeouts, and rejected
//! termination never report `Confirmed`. They do **not** fake native results.
//! Real job-object, `ResumeThread`, `TerminateProcess`, and
//! `WaitForSingleObject` behavior is exercised only by the native Windows
//! integration tests on a Windows host. There is no native fault injection for
//! job-verification or `ResumeThread` failure, so those exact OS failure
//! branches are covered by the pure decision helper plus code inspection, not
//! by an executed Windows failure scenario. Cleanup that cannot confirm exit
//! keeps the handle and keeps waiting rather than closing it or claiming
//! success; that is a deliberate fail-closed hang of the owner thread, while
//! the async caller's spawn/readiness waits remain bounded.
//!
//! Minimum OS: supported configurations are Windows 10 / Server 2016 and
//! later. `PROC_THREAD_ATTRIBUTE_JOB_LIST` assignment is attempted via
//! `UpdateProcThreadAttribute`; if it is unavailable the spawn fails closed
//! with `HardeningUnavailable("windows-job-list")` without resuming a child.
//!
//! # Privacy
//! Neither [`ManagedChild`] nor [`SpawnError`] stores or prints the binary
//! path, arguments, or environment values. `Debug` / `Display` emit only
//! invariant codes, the child pid, and `io::ErrorKind` where relevant.

use std::ffi::OsStr;
#[cfg(any(windows, test))]
use std::ffi::OsString;
use std::fmt;
use std::io;
use std::path::Path;
use std::process::ExitStatus;
use std::time::Duration;
#[cfg(any(windows, test))]
use std::time::Instant;

/// Sanitized spawn failure.
///
/// Carries no binary path, argument, environment key/value, or OS message
/// text — only invariant codes and `io::ErrorKind` — so `Display` and `Debug`
/// are safe to log or return across IPC.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SpawnError {
    /// The binary path was not absolute. `PATH` is never probed.
    NotAbsolute,
    /// An OS call failed (or an input contained an interior NUL / a bad env
    /// key). Only the `ErrorKind` is retained.
    SpawnFailed { kind: io::ErrorKind },
    /// Orphan prevention could not be established, so spawning was refused
    /// rather than silently weakened. The payload names the missing step
    /// (`"windows-job-create"`, `"windows-job-limit"`, `"windows-job-list"`,
    /// `"windows-job-verify"`, or `"windows-only"` on non-Windows).
    HardeningUnavailable(&'static str),
}

impl fmt::Debug for SpawnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAbsolute => write!(f, "NotAbsolute"),
            Self::SpawnFailed { kind } => {
                f.debug_struct("SpawnFailed").field("kind", kind).finish()
            }
            Self::HardeningUnavailable(what) => {
                f.debug_tuple("HardeningUnavailable").field(what).finish()
            }
        }
    }
}

impl fmt::Display for SpawnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAbsolute => write!(f, "binary path must be absolute"),
            Self::SpawnFailed { .. } => write!(f, "process spawn failed"),
            Self::HardeningUnavailable(_) => {
                write!(f, "orphan prevention unavailable on this platform")
            }
        }
    }
}

impl std::error::Error for SpawnError {}

#[cfg(windows)]
#[inline]
fn last_spawn_error() -> SpawnError {
    SpawnError::SpawnFailed {
        kind: io::Error::last_os_error().kind(),
    }
}

#[cfg(any(windows, test))]
#[inline]
fn invalid_input() -> SpawnError {
    SpawnError::SpawnFailed {
        kind: io::ErrorKind::InvalidInput,
    }
}

/// Retry a blocking wait on the same owned handle until the OS confirms exit.
///
/// A transient wait error is never treated as success and the handle is never
/// discarded. The backoff prevents a busy loop; the caller retains ownership
/// for the whole loop.
#[cfg(any(windows, test))]
fn wait_until_confirmed<T, F>(mut wait: F) -> T
where
    F: FnMut() -> io::Result<T>,
{
    let mut delay = Duration::from_millis(10);
    loop {
        match wait() {
            Ok(value) => return value,
            Err(_) => {
                std::thread::sleep(delay);
                delay = delay.saturating_mul(2).min(Duration::from_secs(1));
            }
        }
    }
}

/// Result of cleaning up a process that was created but never handed to an
/// owner (for example after job verification or resume failed).
#[cfg(any(windows, test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CleanupOutcome {
    /// The OS wait reported the owned process exited. Handles may be closed.
    Confirmed,
    /// A bound expired with exit still unconfirmed. The handle must be kept.
    Unconfirmed,
}

/// Retry termination and a real owned wait until exit is confirmed.
///
/// `terminate` is invoked on every attempt and its boolean result is
/// deliberately *not* treated as confirmation: only `wait` returning
/// `Ok(true)` (the OS reported the process object signalled) proves exit.
/// `wait` returning `Ok(false)` (timeout) or `Err` is retried. With
/// `deadline == None` this never returns `Unconfirmed`; production cleanup
/// retains the handle and waits until the real exit. Tests pass a short
/// deadline to exercise persistent termination/wait failure without hanging.
#[cfg(any(windows, test))]
fn cleanup_created_process_until_confirmed<T, W>(
    deadline: Option<Instant>,
    mut terminate: T,
    mut wait: W,
) -> CleanupOutcome
where
    T: FnMut() -> bool,
    W: FnMut(Duration) -> io::Result<bool>,
{
    let mut delay = Duration::from_millis(10);
    loop {
        // A terminate failure is retried below; it is never success.
        let _terminate_accepted = terminate();
        if let Ok(true) = wait(Duration::from_millis(1_000)) {
            return CleanupOutcome::Confirmed;
        }
        if deadline.is_some_and(|d| Instant::now() >= d) {
            return CleanupOutcome::Unconfirmed;
        }
        std::thread::sleep(delay);
        delay = delay.saturating_mul(2).min(Duration::from_secs(1));
    }
}

/// Close the owned kill-on-close job exactly once before the indefinite
/// terminate+wait retry begins, then delegate to
/// [`cleanup_created_process_until_confirmed`].
///
/// `close_job` is invoked exactly once up front — before any `terminate` /
/// `wait` attempt — so a persistently failing termination cannot starve the
/// job backstop. The owned process handle stays with the caller and is only
/// closed after the OS confirms exit; only `wait` returning `Ok(true)` counts
/// as confirmation. With `deadline == None` this never returns `Unconfirmed`.
#[cfg(any(windows, test))]
fn cleanup_with_job_backstop_until_confirmed<C, T, W>(
    deadline: Option<Instant>,
    mut close_job: C,
    terminate: T,
    wait: W,
) -> CleanupOutcome
where
    C: FnMut(),
    T: FnMut() -> bool,
    W: FnMut(Duration) -> io::Result<bool>,
{
    // Backstop first: exactly once, before any terminate/wait retry.
    close_job();
    cleanup_created_process_until_confirmed(deadline, terminate, wait)
}

/// True when a process created by `CreateProcessW` still needs terminate+wait
/// cleanup. Both job verification and resume must have succeeded before the
/// child is considered safely owned; any failure keeps the process object.
#[cfg(any(windows, test))]
fn created_process_needs_cleanup(job_verified: bool, resumed: bool) -> bool {
    !(job_verified && resumed)
}

// ---------------------------------------------------------------------------
// UTF-16 helpers (cross-platform so quoting / env-block logic is unit
// testable anywhere; on Windows the encoding is exact via `OsStrExt`).
// ---------------------------------------------------------------------------

#[cfg(windows)]
fn encode_os_to_wide(s: &OsStr) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;
    s.encode_wide().collect()
}

#[cfg(all(not(windows), test))]
fn encode_os_to_wide(s: &OsStr) -> Vec<u16> {
    s.to_string_lossy().encode_utf16().collect()
}

#[cfg(any(windows, test))]
#[inline]
fn wide_has_nul(w: &[u16]) -> bool {
    w.contains(&0)
}

#[cfg(any(windows, test))]
const W_QUOTE: u16 = 0x22; // `"`
#[cfg(any(windows, test))]
const W_BSLASH: u16 = 0x5C; // `\`

#[cfg(any(windows, test))]
fn needs_quoting(arg: &[u16]) -> bool {
    if arg.is_empty() {
        return true;
    }
    for &c in arg {
        match c {
            0x20 | 0x09 | 0x0A | 0x0D | 0x0B | 0x22 => return true,
            _ => {}
        }
    }
    false
}

/// Append one securely-quoted argument following `CommandLineToArgvW`
/// quoting rules as consumed by the standard Go binary (Go runtime argument
/// parsing). Scope is intentionally narrow: only the standard Go binary's
/// parsing is supported here, not universal Windows command-line parsing
/// (MSVCRT/Cygwin/custom parsers differ).
#[cfg(any(windows, test))]
fn append_quoted_wide(arg: &[u16], out: &mut Vec<u16>) {
    if !needs_quoting(arg) {
        out.extend_from_slice(arg);
        return;
    }
    out.push(W_QUOTE);
    let mut i = 0usize;
    while i < arg.len() {
        let mut backslashes = 0usize;
        while i + backslashes < arg.len() && arg[i + backslashes] == W_BSLASH {
            backslashes += 1;
        }
        if i + backslashes == arg.len() {
            // Trailing backslashes: double them (they precede the closing quote).
            for _ in 0..(2 * backslashes) {
                out.push(W_BSLASH);
            }
            break;
        }
        if arg[i + backslashes] == W_QUOTE {
            // `n` backslashes + quote -> `2n+1` backslashes + quote.
            for _ in 0..(2 * backslashes) {
                out.push(W_BSLASH);
            }
            out.push(W_BSLASH);
            out.push(W_QUOTE);
            i += backslashes + 1;
        } else {
            for _ in 0..backslashes {
                out.push(W_BSLASH);
            }
            out.push(arg[i + backslashes]);
            i += backslashes + 1;
        }
    }
    out.push(W_QUOTE);
}

/// Build a NUL-terminated mutable command line: quoted exe + quoted args.
///
/// Does not check absoluteness (callers do); rejects interior NULs and
/// over-long command lines fail-closed.
#[cfg(any(windows, test))]
fn build_command_line(binary: &Path, args: &[impl AsRef<OsStr>]) -> Result<Vec<u16>, SpawnError> {
    let exe = encode_os_to_wide(binary.as_os_str());
    if wide_has_nul(&exe) {
        return Err(invalid_input());
    }
    let mut cmd: Vec<u16> = Vec::with_capacity(exe.len() + 16 + args.len() * 8);
    append_quoted_wide(&exe, &mut cmd);
    for a in args {
        let w = encode_os_to_wide(a.as_ref());
        if wide_has_nul(&w) {
            return Err(invalid_input());
        }
        cmd.push(0x20);
        append_quoted_wide(&w, &mut cmd);
    }
    cmd.push(0); // NUL terminator for `CreateProcessW` (which may modify it).
                 // Windows command-line limit is 32,767 characters including terminator.
    if cmd.len() > 32_767 {
        return Err(invalid_input());
    }
    Ok(cmd)
}

#[cfg(any(windows, test))]
fn wide_is_ascii(w: &[u16]) -> bool {
    w.iter().all(|&c| c < 0x80)
}

#[cfg(any(windows, test))]
#[inline]
fn wide_ascii_upper_byte(c: u16) -> u16 {
    if (0x61..=0x7A).contains(&c) {
        c - 32
    } else {
        c
    }
}

#[cfg(any(windows, test))]
fn keys_equal_ascii_ci(a: &OsStr, b: &OsStr) -> bool {
    // ASCII-only case-insensitive match on the UTF-16 encoding
    // (`PATH` vs `Path`). Non-ASCII keys are rejected elsewhere
    // (fail closed), so ASCII folding is the complete dedupe/sort key here
    // and never merges distinct ASCII names.
    let aw = encode_os_to_wide(a);
    let bw = encode_os_to_wide(b);
    if aw.len() != bw.len() {
        return false;
    }
    aw.iter()
        .zip(bw.iter())
        .all(|(&x, &y)| wide_ascii_upper_byte(x) == wide_ascii_upper_byte(y))
}

#[cfg(any(windows, test))]
fn cmp_env_key_ci(a: &OsStr, b: &OsStr) -> std::cmp::Ordering {
    let au: Vec<u16> = encode_os_to_wide(a)
        .iter()
        .map(|&c| wide_ascii_upper_byte(c))
        .collect();
    let bu: Vec<u16> = encode_os_to_wide(b)
        .iter()
        .map(|&c| wide_ascii_upper_byte(c))
        .collect();
    // `au`/`bu` are ASCII-only after validation; compare case-folded first,
    // then exact bytes for a deterministic tie-break.
    match au.cmp(&bu) {
        std::cmp::Ordering::Equal => encode_os_to_wide(a).cmp(&encode_os_to_wide(b)),
        ord => ord,
    }
}

/// Build a double-NUL-terminated Unicode environment block in the
/// Windows-required case-insensitive sorted order.
///
/// - `clear_env == true`: only `overrides` are present.
/// - otherwise: parent environment inherited, `overrides` replace/add
///   (ASCII case-insensitive name match, last wins).
/// - Non-ASCII names are rejected fail-closed: no reliable non-ASCII
///   casefold is implemented here, so merging them could produce an ambiguous
///   environment; keys containing any unit `> 0x7F` are an error. Unicode
///   values are retained.
///
/// Rejects empty names, `=` in names, and interior NULs fail-closed.
#[cfg(any(windows, test))]
fn build_env_block(
    clear_env: bool,
    overrides: &[(impl AsRef<OsStr>, impl AsRef<OsStr>)],
) -> Result<Vec<u16>, SpawnError> {
    let mut entries: Vec<(OsString, OsString)> = Vec::new();
    // Last wins under ASCII case-insensitive match.
    let push_dedup = |k: OsString, v: OsString, entries: &mut Vec<(OsString, OsString)>| {
        entries.retain(|(ek, _)| !keys_equal_ascii_ci(ek, &k));
        entries.push((k, v));
    };
    if !clear_env {
        for (k, v) in std::env::vars_os() {
            push_dedup(k, v, &mut entries);
        }
    }
    for (k, v) in overrides {
        let k = k.as_ref().to_os_string();
        let v = v.as_ref().to_os_string();
        push_dedup(k, v, &mut entries);
    }

    // Validate keys before sorting: empty / `=` / NUL / non-ASCII fail closed.
    // Values may carry any Unicode except interior NUL.
    for (k, v) in &entries {
        let kw = encode_os_to_wide(k);
        let vw = encode_os_to_wide(v);
        if kw.is_empty() || kw.contains(&0x3D) || wide_has_nul(&kw) || wide_has_nul(&vw) {
            return Err(invalid_input());
        }
        if !wide_is_ascii(&kw) {
            return Err(invalid_input());
        }
    }
    // Windows requires the block sorted case-insensitively by name.
    entries.sort_by(|a, b| cmp_env_key_ci(&a.0, &b.0));

    let mut block: Vec<u16> = Vec::with_capacity(256);
    for (k, v) in &entries {
        let kw = encode_os_to_wide(k);
        let vw = encode_os_to_wide(v);
        // Re-checked after sort; keys were validated ASCII above.
        debug_assert!(!kw.is_empty() && !kw.contains(&0x3D) && wide_is_ascii(&kw));
        block.extend_from_slice(&kw);
        block.push(0x3D); // `=`
        block.extend_from_slice(&vw);
        block.push(0);
    }
    if block.is_empty() {
        // Empty environment: double NUL.
        block.push(0);
        block.push(0);
    } else {
        // Terminate the block with the second NUL.
        block.push(0);
    }
    Ok(block)
}

// ---------------------------------------------------------------------------
// Windows kernel32 FFI (manual, std-only; no `windows-sys` dependency).
// ---------------------------------------------------------------------------

#[cfg(windows)]
#[allow(non_camel_case_types, non_snake_case, dead_code)]
mod win32 {
    use std::ffi::c_void;

    pub type HANDLE = *mut c_void;
    pub type BOOL = i32;
    pub type DWORD = u32;

    pub const CREATE_SUSPENDED: DWORD = 0x0000_0004;
    pub const CREATE_UNICODE_ENVIRONMENT: DWORD = 0x0000_0400;
    pub const EXTENDED_STARTUPINFO_PRESENT: DWORD = 0x0008_0000;
    pub const CREATE_NO_WINDOW: DWORD = 0x0800_0000;

    pub const JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: DWORD = 0x0000_2000;
    pub const JOB_OBJECT_EXTENDED_LIMIT_INFORMATION: i32 = 9;

    // `PROC_THREAD_ATTRIBUTE_JOB_LIST` (131085). Supported configs are
    // Windows 10 / Server 2016 and later.
    pub const PROC_THREAD_ATTRIBUTE_JOB_LIST: usize = 0x0002_000D;

    pub const INFINITE: DWORD = 0xFFFF_FFFF;
    pub const WAIT_OBJECT_0: DWORD = 0;
    pub const WAIT_TIMEOUT: DWORD = 258;

    #[repr(C)]
    pub struct StartupInfoW {
        pub cb: DWORD,
        pub lpReserved: *mut u16,
        pub lpDesktop: *mut u16,
        pub lpTitle: *mut u16,
        pub dwX: DWORD,
        pub dwY: DWORD,
        pub dwXSize: DWORD,
        pub dwYSize: DWORD,
        pub dwXCountChars: DWORD,
        pub dwYCountChars: DWORD,
        pub dwFillAttribute: DWORD,
        pub dwFlags: DWORD,
        pub wShowWindow: u16,
        pub cbReserved2: u16,
        pub lpReserved2: *mut u8,
        pub hStdInput: HANDLE,
        pub hStdOutput: HANDLE,
        pub hStdError: HANDLE,
    }

    #[repr(C)]
    pub struct StartupInfoExW {
        pub StartupInfo: StartupInfoW,
        pub AttributeList: *mut c_void,
    }

    #[repr(C)]
    pub struct ProcessInformation {
        pub hProcess: HANDLE,
        pub hThread: HANDLE,
        pub dwProcessId: DWORD,
        pub dwThreadId: DWORD,
    }

    #[repr(C)]
    pub struct JobObjectBasicLimitInformation {
        pub PerProcessUserTimeLimit: i64,
        pub PerJobUserTimeLimit: i64,
        pub LimitFlags: DWORD,
        pub MinimumWorkingSetSize: usize,
        pub MaximumWorkingSetSize: usize,
        pub ActiveProcessLimit: DWORD,
        pub Affinity: usize,
        pub PriorityClass: DWORD,
        pub SchedulingClass: DWORD,
    }

    #[repr(C)]
    pub struct IoCounters {
        pub ReadOperationCount: u64,
        pub WriteOperationCount: u64,
        pub OtherOperationCount: u64,
        pub ReadTransferCount: u64,
        pub WriteTransferCount: u64,
        pub OtherTransferCount: u64,
    }

    #[repr(C)]
    pub struct JobObjectExtendedLimitInformation {
        pub BasicLimitInformation: JobObjectBasicLimitInformation,
        pub IoInfo: IoCounters,
        pub ProcessMemoryLimit: usize,
        pub JobMemoryLimit: usize,
        pub PeakProcessMemoryUsed: usize,
        pub PeakJobMemoryUsed: usize,
    }

    #[link(name = "kernel32")]
    extern "system" {
        pub fn CreateJobObjectW(lpJobAttributes: *const c_void, lpName: *const u16) -> HANDLE;
        pub fn SetInformationJobObject(
            hJob: HANDLE,
            JobObjectInformationClass: i32,
            lpJobObjectInformation: *mut c_void,
            cbJobObjectInformationLength: DWORD,
        ) -> BOOL;
        pub fn InitializeProcThreadAttributeList(
            lpAttributeList: *mut c_void,
            dwAttributeCount: DWORD,
            dwFlags: DWORD,
            lpSize: *mut usize,
        ) -> BOOL;
        pub fn UpdateProcThreadAttribute(
            lpAttributeList: *mut c_void,
            dwFlags: DWORD,
            Attribute: usize,
            lpValue: *mut c_void,
            cbSize: usize,
            lpPreviousValue: *mut c_void,
            lpSizeReturned: *mut usize,
        ) -> BOOL;
        pub fn DeleteProcThreadAttributeList(lpAttributeList: *mut c_void);
        pub fn CreateProcessW(
            lpApplicationName: *const u16,
            lpCommandLine: *mut u16,
            lpProcessAttributes: *mut c_void,
            lpThreadAttributes: *mut c_void,
            bInheritHandles: BOOL,
            dwCreationFlags: DWORD,
            lpEnvironment: *mut c_void,
            lpCurrentDirectory: *const u16,
            lpStartupInfo: *const StartupInfoExW,
            lpProcessInformation: *mut ProcessInformation,
        ) -> BOOL;
        pub fn ResumeThread(hThread: HANDLE) -> DWORD;
        pub fn IsProcessInJob(ProcessHandle: HANDLE, JobHandle: HANDLE, Result: *mut BOOL) -> BOOL;
        pub fn TerminateProcess(hProcess: HANDLE, uExitCode: DWORD) -> BOOL;
        pub fn WaitForSingleObject(hHandle: HANDLE, dwMilliseconds: DWORD) -> DWORD;
        pub fn GetExitCodeProcess(hProcess: HANDLE, lpExitCode: *mut DWORD) -> BOOL;
        pub fn CloseHandle(hObject: HANDLE) -> BOOL;
        pub fn GetProcessHeap() -> HANDLE;
        pub fn HeapAlloc(hHeap: HANDLE, dwFlags: DWORD, dwBytes: usize) -> *mut c_void;
        pub fn HeapFree(hHeap: HANDLE, dwFlags: DWORD, lpMem: *mut c_void) -> BOOL;
    }
}

// ---------------------------------------------------------------------------
// Owned child (Windows).
// ---------------------------------------------------------------------------

/// Owned handle to exactly one Windows child process plus its kill-on-close
/// job.
///
/// Holds no path, args, or env. `kill` / `wait` act on the owned process
/// handle only — never on a bare pid — so PID reuse cannot redirect
/// termination at an unrelated process.
///
/// This type is intentionally neither `Send` nor `Sync` (it owns raw `HANDLE`s
/// with no `unsafe impl`); a caller needing cross-thread ownership should use
/// a dedicated owner thread and message passing instead of sharing this type.
#[cfg(windows)]
pub struct ManagedChild {
    // `None` once the kill-on-close backstop has been invoked (closed exactly
    // once); `Drop` closes only a still-owned `Some` handle, so the early
    // close in `terminate_and_reap_blocking` can never double-close.
    job: Option<win32::HANDLE>,
    process: win32::HANDLE,
    pid: u32,
}

/// Retained ownership of a process created by `CreateProcessW` whose spawn
/// path failed before handing it to a `ManagedChild`.
///
/// The raw handles stay on this thread (the type is not `Send`/`Sync`). The
/// process is terminated and the owned handle is waited on until the OS
/// confirms exit; an unconfirmed handle is never closed and no result is faked.
#[cfg(windows)]
struct PendingProcess {
    job: Option<win32::HANDLE>,
    process: Option<win32::HANDLE>,
    thread: Option<win32::HANDLE>,
    confirmed: bool,
}

#[cfg(windows)]
impl PendingProcess {
    fn new(job: win32::HANDLE, process: win32::HANDLE, thread: win32::HANDLE) -> Self {
        Self {
            job: Some(job),
            process: Some(process),
            thread: Some(thread),
            confirmed: false,
        }
    }

    fn close_thread(&mut self) {
        if let Some(thread) = self.thread.take() {
            // SAFETY: this handle is owned here and closed exactly once.
            unsafe {
                let _ = win32::CloseHandle(thread);
            }
        }
    }

    fn close_job(&mut self) {
        if let Some(job) = self.job.take() {
            // SAFETY: this handle is owned here and closed exactly once.
            // Closing it also asks kill-on-close to terminate current members.
            unsafe {
                let _ = win32::CloseHandle(job);
            }
        }
    }

    /// Retain the owned handles and block until the real exit is confirmed.
    ///
    /// Termination failures and wait timeouts/errors are retried with backoff;
    /// never treated as success. The process handle is closed only after the
    /// OS wait confirms the process object is signalled.
    fn wait_until_confirmed(&mut self) {
        if self.confirmed {
            return;
        }
        self.close_thread();
        let job = &mut self.job;
        let process = self.process;
        let _outcome = cleanup_created_process_until_confirmed(
            None,
            || {
                if let Some(job) = job.take() {
                    // SAFETY: this owned job handle is closed exactly once.
                    unsafe {
                        let _ = win32::CloseHandle(job);
                    }
                }
                if let Some(process) = process {
                    // SAFETY: owned process handle only; never a bare pid.
                    unsafe {
                        let _ = win32::TerminateProcess(process, 1);
                    }
                }
                true
            },
            |timeout| {
                let process = process.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "process handle closed")
                })?;
                let millis = timeout.as_millis().min((win32::INFINITE - 1) as u128) as win32::DWORD;
                // SAFETY: owned process handle only.
                unsafe {
                    match win32::WaitForSingleObject(process, millis) {
                        win32::WAIT_OBJECT_0 => Ok(true),
                        win32::WAIT_TIMEOUT => Ok(false),
                        _ => Err(io::Error::last_os_error()),
                    }
                }
            },
        );
        self.confirmed = true;
        if let Some(process) = self.process.take() {
            // SAFETY: owned handle closed exactly once after confirmed exit.
            unsafe {
                let _ = win32::CloseHandle(process);
            }
        }
    }
}

#[cfg(windows)]
impl Drop for PendingProcess {
    fn drop(&mut self) {
        if !self.confirmed {
            // Fail closed: keep the handle and wait rather than closing an
            // unconfirmed process.
            self.wait_until_confirmed();
        }
        self.close_job();
        self.close_thread();
        if let Some(process) = self.process.take() {
            // SAFETY: owned handle closed exactly once after confirmed exit.
            unsafe {
                let _ = win32::CloseHandle(process);
            }
        }
    }
}

/// Sanitized failed-spawn result that may still own a created process whose
/// exit has not been confirmed yet.
///
/// `spawn_with_cleanup` returns this so a dedicated owner thread can report
/// the spawn failure promptly (bounded caller waiting) and then retain the
/// handles and wait on the same thread until real exit. `spawn` instead
/// completes that wait before returning the sanitized [`SpawnError`].
#[cfg(windows)]
pub struct SpawnFailure {
    error: SpawnError,
    cleanup: Option<PendingProcess>,
}

#[cfg(windows)]
impl SpawnFailure {
    fn none(error: SpawnError) -> Self {
        Self {
            error,
            cleanup: None,
        }
    }

    fn with_cleanup(error: SpawnError, cleanup: PendingProcess) -> Self {
        Self {
            error,
            cleanup: Some(cleanup),
        }
    }

    /// True when a process was created and its handle is retained here.
    pub fn has_pending_cleanup(&self) -> bool {
        self.cleanup.is_some()
    }

    /// Block until the retained process exit is confirmed (no-op if none).
    pub fn wait_until_confirmed(&mut self) {
        if let Some(mut cleanup) = self.cleanup.take() {
            cleanup.wait_until_confirmed();
        }
    }

    /// Sanitized error for callers that do not need the cleanup ownership.
    pub fn error(&self) -> SpawnError {
        self.error
    }
}

#[cfg(windows)]
impl fmt::Debug for SpawnFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpawnFailure")
            .field("error", &self.error)
            .field("pending_cleanup", &self.cleanup.is_some())
            .finish()
    }
}

#[cfg(windows)]
impl fmt::Display for SpawnFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.error, f)
    }
}

#[cfg(windows)]
impl std::error::Error for SpawnFailure {}

#[cfg(windows)]
impl fmt::Debug for ManagedChild {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedChild")
            .field("pid", &self.pid)
            .finish()
    }
}

#[cfg(windows)]
impl ManagedChild {
    /// Spawn exactly `binary` with `args` and env.
    ///
    /// - `binary` must be absolute (e.g. `C:\Tools\surge.exe`, exact `.exe`
    ///   suffix); anything else is [`SpawnError::NotAbsolute`]. It is passed
    ///   as `lpApplicationName`, so no `PATH` lookup happens.
    /// - `args` are quoted into the mutable command line using
    ///   `CommandLineToArgvW` rules scoped to the standard Go binary's parsing;
    ///   never shelled out, never logged. Not claimed universal for all
    ///   Windows runtimes.
    /// - `clear_env == true` clears the parent environment first and only
    ///   `env_overrides` are set; otherwise the parent env is inherited and
    ///   `env_overrides` replace/add entries. Env block is case-insensitive
    ///   sorted; non-ASCII names fail closed, Unicode values retained. Values
    ///   are never logged.
    /// - The child gets no console window (`CREATE_NO_WINDOW`), inherits no
    ///   handles, and starts suspended with a non-inheritable
    ///   `KILL_ON_JOB_CLOSE` job passed via `JOB_LIST`, verified best-effort
    ///   with `IsProcessInJob` before `ResumeThread`. Assignment is not a
    ///   formal transaction and full descendant coverage is not guaranteed.
    pub fn spawn(
        binary: &Path,
        args: &[impl AsRef<OsStr>],
        env_overrides: &[(impl AsRef<OsStr>, impl AsRef<OsStr>)],
        clear_env: bool,
    ) -> Result<Self, SpawnError> {
        match Self::spawn_with_cleanup(binary, args, env_overrides, clear_env) {
            Ok(child) => Ok(child),
            Err(mut failure) => {
                // This entry point has no ownership slot to return, so retain
                // the possible child here and wait until its real exit is
                // confirmed before reporting the sanitized spawn error.
                failure.wait_until_confirmed();
                Err(failure.error())
            }
        }
    }

    /// Spawn exactly `binary`, returning retained cleanup ownership if a
    /// process was created but could not be safely handed off.
    ///
    /// The dedicated owner thread uses this so it can report `SpawnFailed`
    /// promptly (bounded caller wait) and only finish its owner lifetime after
    /// [`SpawnFailure::wait_until_confirmed`] observes the real process exit.
    pub(crate) fn spawn_with_cleanup(
        binary: &Path,
        args: &[impl AsRef<OsStr>],
        env_overrides: &[(impl AsRef<OsStr>, impl AsRef<OsStr>)],
        clear_env: bool,
    ) -> Result<Self, SpawnFailure> {
        if !binary.is_absolute() {
            return Err(SpawnFailure::none(SpawnError::NotAbsolute));
        }
        let mut app: Vec<u16> = encode_os_to_wide(binary.as_os_str());
        if wide_has_nul(&app) {
            return Err(SpawnFailure::none(invalid_input()));
        }
        app.push(0);
        let mut cmd = build_command_line(binary, args).map_err(SpawnFailure::none)?;
        let mut env = build_env_block(clear_env, env_overrides).map_err(SpawnFailure::none)?;

        // SAFETY: every OS call's return value is checked; every acquired
        // handle / allocation is released on all paths; buffers outlive the
        // `CreateProcessW` call; `STARTUPINFOEXW` / `PROCESS_INFORMATION` are
        // zero-initialized (all-zero is a valid initial state: NULL
        // pointers/handles, zero flags/ids).
        use std::ptr::{null, null_mut};
        unsafe {
            use win32::*;

            let job = CreateJobObjectW(null(), null());
            if job.is_null() {
                return Err(SpawnFailure::none(SpawnError::HardeningUnavailable(
                    "windows-job-create",
                )));
            }

            let mut limits: JobObjectExtendedLimitInformation = std::mem::zeroed();
            limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            let ok = SetInformationJobObject(
                job,
                JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
                &mut limits as *mut _ as *mut std::ffi::c_void,
                std::mem::size_of::<JobObjectExtendedLimitInformation>() as DWORD,
            );
            if ok == 0 {
                CloseHandle(job);
                return Err(SpawnFailure::none(SpawnError::HardeningUnavailable(
                    "windows-job-limit",
                )));
            }

            // Attribute list so the kernel is asked to assign the job during
            // creation (narrowing, not transactional).
            let mut attr_size: usize = 0;
            let _ = InitializeProcThreadAttributeList(null_mut(), 1, 0, &mut attr_size);
            if attr_size == 0 {
                CloseHandle(job);
                return Err(SpawnFailure::none(SpawnError::HardeningUnavailable(
                    "windows-job-list",
                )));
            }
            let heap = GetProcessHeap();
            if heap.is_null() {
                // Capture OS error before any cleanup which may clobber it.
                let e = last_spawn_error();
                CloseHandle(job);
                return Err(SpawnFailure::none(e));
            }
            let attr = HeapAlloc(heap, 0, attr_size);
            if attr.is_null() {
                let e = last_spawn_error();
                CloseHandle(job);
                return Err(SpawnFailure::none(e));
            }
            if InitializeProcThreadAttributeList(attr, 1, 0, &mut attr_size) == 0 {
                HeapFree(heap, 0, attr);
                CloseHandle(job);
                return Err(SpawnFailure::none(SpawnError::HardeningUnavailable(
                    "windows-job-list",
                )));
            }
            let mut job_for_attr: HANDLE = job;
            let updated = UpdateProcThreadAttribute(
                attr,
                0,
                PROC_THREAD_ATTRIBUTE_JOB_LIST,
                &mut job_for_attr as *mut _ as *mut std::ffi::c_void,
                std::mem::size_of::<HANDLE>(),
                null_mut(),
                null_mut(),
            );
            if updated == 0 {
                DeleteProcThreadAttributeList(attr);
                HeapFree(heap, 0, attr);
                CloseHandle(job);
                return Err(SpawnFailure::none(SpawnError::HardeningUnavailable(
                    "windows-job-list",
                )));
            }

            let mut siex: StartupInfoExW = std::mem::zeroed();
            siex.StartupInfo.cb = std::mem::size_of::<StartupInfoExW>() as DWORD;
            siex.AttributeList = attr;

            let mut pi: ProcessInformation = std::mem::zeroed();
            let flags: DWORD = EXTENDED_STARTUPINFO_PRESENT
                | CREATE_SUSPENDED
                | CREATE_UNICODE_ENVIRONMENT
                | CREATE_NO_WINDOW;
            let created = CreateProcessW(
                app.as_ptr(),
                cmd.as_mut_ptr(),
                null_mut(),
                null_mut(),
                0,
                flags,
                env.as_mut_ptr() as *mut std::ffi::c_void,
                null(),
                &siex,
                &mut pi,
            );
            // Capture `GetLastError` immediately; cleanup below may clobber it.
            let create_err = if created == 0 {
                Some(last_spawn_error())
            } else {
                None
            };
            // Attribute list is only needed for the creation call itself.
            DeleteProcThreadAttributeList(attr);
            HeapFree(heap, 0, attr);

            if let Some(e) = create_err {
                // No process was created, so there is nothing to wait for.
                CloseHandle(job);
                return Err(SpawnFailure::none(e));
            }

            // Still suspended: best-effort verify of job assignment before
            // running. Any failure below retains the owned process handles.
            let mut in_job: BOOL = 0;
            let queried = IsProcessInJob(pi.hProcess, job, &mut in_job);
            let job_verified = queried != 0 && in_job != 0;
            let mut resume_error: Option<SpawnError> = None;
            let mut resumed = false;
            if job_verified {
                if ResumeThread(pi.hThread) == u32::MAX {
                    resume_error = Some(last_spawn_error());
                } else {
                    resumed = true;
                }
            }

            if created_process_needs_cleanup(job_verified, resumed) {
                let error = if !job_verified {
                    SpawnError::HardeningUnavailable("windows-job-verify")
                } else {
                    resume_error.unwrap_or(SpawnError::SpawnFailed {
                        kind: io::ErrorKind::Other,
                    })
                };
                // Hand both the sanitized error and the still-owned handles to
                // the caller (owner thread), which must wait for real exit.
                let cleanup = PendingProcess::new(job, pi.hProcess, pi.hThread);
                return Err(SpawnFailure::with_cleanup(error, cleanup));
            }

            // Post-resume thread-handle cleanup is non-fatal: the child is
            // already correctly resumed inside the job.
            let _ = CloseHandle(pi.hThread);
            Ok(Self {
                job: Some(job),
                process: pi.hProcess,
                pid: pi.dwProcessId,
            })
        }
    }

    /// OS process id of the owned child. Safe to log.
    pub fn id(&self) -> u32 {
        self.pid
    }

    /// Non-blocking reap check on the owned handle only.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        use std::os::windows::process::ExitStatusExt;
        unsafe {
            // `WaitForSingleObject(process, 0)` avoids the `STILL_ACTIVE`
            // (259) ambiguity of `GetExitCodeProcess` alone.
            let r = win32::WaitForSingleObject(self.process, 0);
            if r == win32::WAIT_OBJECT_0 {
                let mut code: win32::DWORD = 0;
                if win32::GetExitCodeProcess(self.process, &mut code) == 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(Some(ExitStatus::from_raw(code)))
            } else if r == win32::WAIT_TIMEOUT {
                Ok(None)
            } else {
                Err(io::Error::last_os_error())
            }
        }
    }

    /// Blocking reap of the owned handle only.
    pub fn wait(&mut self) -> io::Result<ExitStatus> {
        use std::os::windows::process::ExitStatusExt;
        unsafe {
            let r = win32::WaitForSingleObject(self.process, win32::INFINITE);
            if r != win32::WAIT_OBJECT_0 {
                return Err(io::Error::last_os_error());
            }
            let mut code: win32::DWORD = 0;
            if win32::GetExitCodeProcess(self.process, &mut code) == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(ExitStatus::from_raw(code))
        }
    }

    /// Terminate the owned handle only (`TerminateProcess`, exit code 1).
    pub fn kill(&mut self) -> io::Result<()> {
        unsafe {
            if win32::TerminateProcess(self.process, 1) == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }
    }

    /// Force-terminate and reap only the owned handle, waiting at most
    /// `timeout`. Never signals a bare pid. Returns the exit code when the
    /// child is reaped before the bound; `None` means the bound elapsed. This
    /// bounded probe does not transfer ownership.
    pub fn force_kill_and_reap(&mut self, timeout: Duration) -> Option<i32> {
        let _ = self.kill();
        let deadline = Instant::now() + timeout;
        loop {
            match self.try_wait() {
                Ok(Some(status)) => return status.code(),
                Ok(None) => {
                    if Instant::now() >= deadline {
                        return None;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => return None,
            }
        }
    }

    /// Force-terminate the owned child and wait for confirmed exit on the
    /// calling thread after a short bounded fast path.
    ///
    /// Intended for the dedicated owner thread. After the bound expires that
    /// same thread closes the owned kill-on-close job handle exactly once
    /// (invoking the backstop up front) and then retains the process handle
    /// and blocks (retrying owned `TerminateProcess` plus a finite owned wait
    /// with capped backoff via [`cleanup_with_job_backstop_until_confirmed`])
    /// until the process object itself signals exit, instead of spawning
    /// another owner (the type is deliberately not `Send`). Only `Ok(true)`
    /// from the real wait (process object signalled) counts as confirmation;
    /// termination failures, timeouts, and wait errors are retried and never
    /// reported as success. The job slot becomes `None` after the early close
    /// so `Drop` cannot double-close; no bare pid is ever used. App-exit and
    /// async callers still wait only within their own fixed bounds and
    /// observe the real result through the owner lifetime.
    pub fn terminate_and_reap_blocking(&mut self, timeout: Duration) -> (Option<i32>, bool) {
        let deadline = Instant::now() + timeout;
        let mut killed = false;
        loop {
            match self.try_wait() {
                Ok(Some(status)) => return (status.code(), true),
                Ok(None) => {
                    if !killed {
                        let _ = self.kill();
                        killed = true;
                    }
                    if Instant::now() >= deadline {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => {
                    if !killed {
                        let _ = self.kill();
                    }
                    break;
                }
            }
        }
        // Bounded fast path expired without confirmed exit. Close the owned
        // kill-on-close job exactly once BEFORE the indefinite retry so the
        // backstop is invoked even if `TerminateProcess` keeps failing. The
        // owned process handle is retained (copied, not taken) and only
        // closed after the OS confirms exit; `deadline == None` never yields
        // `Unconfirmed`: this blocks rather than faking success or closing an
        // unconfirmed handle.
        let process = self.process;
        let job_slot = &mut self.job;
        let _outcome = cleanup_with_job_backstop_until_confirmed(
            None,
            || {
                if let Some(job) = job_slot.take() {
                    // SAFETY: owned job handle closed exactly once; the slot
                    // becomes `None` so `Drop` cannot close it again. Closing
                    // asks kill-on-close to terminate current job members.
                    unsafe {
                        let _ = win32::CloseHandle(job);
                    }
                }
            },
            || {
                // SAFETY: owned process handle only; never a bare pid. The
                // boolean is deliberately ignored: a rejected terminate is
                // retried below, never treated as success.
                unsafe {
                    let _ = win32::TerminateProcess(process, 1);
                }
                true
            },
            |wait_timeout| {
                let millis =
                    wait_timeout.as_millis().min((win32::INFINITE - 1) as u128) as win32::DWORD;
                // SAFETY: owned process handle only; finite wait (never
                // INFINITE) so a failed terminate cannot hang without retry.
                unsafe {
                    match win32::WaitForSingleObject(process, millis) {
                        win32::WAIT_OBJECT_0 => Ok(true),
                        win32::WAIT_TIMEOUT => Ok(false),
                        _ => Err(io::Error::last_os_error()),
                    }
                }
            },
        );
        // `Confirmed` is the only possible outcome with `None` deadline: the
        // process object signalled. Report its code; `None` code with
        // `true` confirmed still means "exited, code unknown", never a fake.
        match self.try_wait() {
            Ok(Some(status)) => (status.code(), true),
            Ok(None) => {
                // Extremely narrow: signalled but `try_wait` still reports
                // running (e.g. transient). Fall back to a direct exit-code
                // probe; unknown stays `None` but confirmed stays `true`.
                let code = unsafe {
                    let mut raw: win32::DWORD = 0;
                    if win32::GetExitCodeProcess(process, &mut raw) == 0 {
                        None
                    } else {
                        Some(raw as i32)
                    }
                };
                (code, true)
            }
            Err(_) => {
                let code = unsafe {
                    let mut raw: win32::DWORD = 0;
                    if win32::GetExitCodeProcess(process, &mut raw) == 0 {
                        None
                    } else {
                        Some(raw as i32)
                    }
                };
                (code, true)
            }
        }
    }
}

#[cfg(windows)]
impl Drop for ManagedChild {
    fn drop(&mut self) {
        unsafe {
            // Terminate only the owned process handle; "already exited" fails
            // are fine. Then close the owned job handle if still owned
            // (`terminate_and_reap_blocking` may have already closed it
            // exactly once via `take`, leaving `None`): kill-on-close remains
            // the backstop for processes currently in the job, with no
            // double-close of an unrelated handle.
            let _ = win32::TerminateProcess(self.process, 1);
            if let Some(job) = self.job.take() {
                // SAFETY: owned job handle closed exactly once.
                let _ = win32::CloseHandle(job);
            }
            // Keep the owned process handle until the OS confirms exit. A
            // timeout or transient wait error is retried with backoff rather
            // than closing the handle as if the child were reaped. This can
            // outlive the old fixed bound only for an unkillable/unconfirmable
            // process, which is reported honestly rather than faked.
            let mut delay = Duration::from_millis(10);
            loop {
                if win32::WaitForSingleObject(self.process, 5_000) == win32::WAIT_OBJECT_0 {
                    break;
                }
                std::thread::sleep(delay);
                delay = delay.saturating_mul(2).min(Duration::from_secs(1));
            }
            let _ = win32::CloseHandle(self.process);
        }
    }
}

// ---------------------------------------------------------------------------
// Non-Windows stub (fail closed; keeps the file compiling if ever wired on
// another platform; never spawns).
// ---------------------------------------------------------------------------

/// Stub owned child used only on non-Windows builds. `spawn` always fails
/// closed because the Windows Job Object mitigation cannot be provided.
///
/// Like the Windows type, this stub is neither `Send` nor `Sync` by design;
/// use a dedicated owner thread with message passing if cross-thread use is needed.
#[cfg(not(windows))]
pub struct ManagedChild {
    _private: (),
}

#[cfg(not(windows))]
impl fmt::Debug for ManagedChild {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedChild").field("pid", &0u32).finish()
    }
}

#[cfg(not(windows))]
impl ManagedChild {
    pub fn spawn(
        binary: &Path,
        args: &[impl AsRef<OsStr>],
        env_overrides: &[(impl AsRef<OsStr>, impl AsRef<OsStr>)],
        clear_env: bool,
    ) -> Result<Self, SpawnError> {
        if !binary.is_absolute() {
            return Err(SpawnError::NotAbsolute);
        }
        let _ = (args, env_overrides, clear_env);
        Err(SpawnError::HardeningUnavailable("windows-only"))
    }

    pub fn id(&self) -> u32 {
        0
    }

    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "windows-only"))
    }

    pub fn wait(&mut self) -> io::Result<ExitStatus> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "windows-only"))
    }

    pub fn kill(&mut self) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "windows-only"))
    }

    pub fn force_kill_and_reap(&mut self, _timeout: Duration) -> Option<i32> {
        None
    }
}

// ---------------------------------------------------------------------------
// Tests. Cross-platform unit tests run anywhere (quoting scoped to the
// standard Go binary, env block, sanitization); Windows integration tests run
// only on Windows.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    #[cfg(windows)]
    use std::time::{Duration, Instant};

    const SECRET: &str = "s3cret-t0ken-value-xyz";

    fn take_injected_failure(counter: &std::sync::atomic::AtomicUsize) -> bool {
        use std::sync::atomic::Ordering;
        let mut current = counter.load(Ordering::SeqCst);
        while current > 0 {
            match counter.compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
        false
    }

    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().collect()
    }

    fn cmd_string(cmd: &[u16]) -> String {
        // Test-only lossy view of an ASCII command line (strips terminator).
        let end = cmd.iter().position(|&c| c == 0).unwrap_or(cmd.len());
        String::from_utf16_lossy(&cmd[..end])
    }

    fn block_entries(block: &[u16]) -> Vec<String> {
        // Split a double-NUL block into `K=V` strings (test-only).
        let mut out = Vec::new();
        let mut cur: Vec<u16> = Vec::new();
        for &c in block {
            if c == 0 {
                if cur.is_empty() {
                    break;
                }
                out.push(String::from_utf16_lossy(&cur));
                cur.clear();
            } else {
                cur.push(c);
            }
        }
        out
    }

    #[test]
    fn rejects_bare_and_relative_paths() {
        for rel in ["", "sleep.exe", "./x.exe", "Tools/a.exe"] {
            let err = ManagedChild::spawn(
                Path::new(rel),
                &[] as &[&OsStr],
                &[] as &[(&OsStr, &OsStr)],
                true,
            )
            .unwrap_err();
            assert_eq!(err, SpawnError::NotAbsolute, "input: {rel:?}");
        }
    }

    #[test]
    fn spawn_error_display_carries_no_private_data() {
        let err = ManagedChild::spawn(
            Path::new("relative-with-secret"),
            &[SECRET],
            &[] as &[(&OsStr, &OsStr)],
            true,
        )
        .unwrap_err();
        let dbg = format!("{err:?}");
        let disp = format!("{err}");
        assert!(!dbg.contains(SECRET), "debug leaked: {dbg}");
        assert!(!disp.contains(SECRET), "display leaked: {disp}");
        assert!(!dbg.contains("relative-with-secret"));
        assert!(!disp.contains("relative-with-secret"));
        let hard = SpawnError::HardeningUnavailable("windows-job-list");
        let dbg = format!("{hard:?}");
        let disp = format!("{hard}");
        assert!(!dbg.contains(SECRET));
        assert!(!disp.contains(SECRET));
        assert!(disp.contains("orphan prevention"));
    }

    // Quoting scope: standard Go binary parsing only (not universal Windows
    // runtimes). These cases pin the Go-scoped `CommandLineToArgvW` behavior.
    #[test]
    fn quoting_plain_arg_is_unquoted() {
        let mut out = Vec::new();
        append_quoted_wide(&wide("abc"), &mut out);
        assert_eq!(String::from_utf16_lossy(&out), "abc");
    }

    #[test]
    fn quoting_empty_becomes_empty_quotes() {
        let mut out = Vec::new();
        append_quoted_wide(&[], &mut out);
        assert_eq!(String::from_utf16_lossy(&out), "\"\"");
    }

    #[test]
    fn quoting_space_wraps_in_quotes() {
        let mut out = Vec::new();
        append_quoted_wide(&wide("a b"), &mut out);
        assert_eq!(String::from_utf16_lossy(&out), "\"a b\"");
    }

    #[test]
    fn quoting_embedded_quote_is_escaped() {
        let mut out = Vec::new();
        append_quoted_wide(&wide("a\"b"), &mut out);
        assert_eq!(String::from_utf16_lossy(&out), "\"a\\\"b\"");
    }

    #[test]
    fn quoting_trailing_backslash_is_doubled() {
        let mut out = Vec::new();
        append_quoted_wide(&wide("C:\\path\\"), &mut out);
        // No spaces, so no quoting is needed and no doubling occurs.
        assert_eq!(String::from_utf16_lossy(&out), "C:\\path\\");
        let mut out = Vec::new();
        append_quoted_wide(&wide("C:\\my dir\\"), &mut out);
        assert_eq!(String::from_utf16_lossy(&out), "\"C:\\my dir\\\\\"");
    }

    #[test]
    fn command_line_joins_quoted_exe_and_args() {
        let cmd = build_command_line(Path::new("/bin/echo"), &["a b", "c"]).unwrap();
        assert_eq!(cmd_string(&cmd), "/bin/echo \"a b\" c");
        assert_eq!(*cmd.last().unwrap(), 0);
    }

    #[test]
    fn command_line_rejects_interior_nul() {
        let err = build_command_line(Path::new("/bin/echo"), &["a\0b"]).unwrap_err();
        assert_eq!(
            err,
            SpawnError::SpawnFailed {
                kind: io::ErrorKind::InvalidInput
            }
        );
    }

    #[test]
    fn env_block_is_double_nul_terminated() {
        let block = build_env_block(true, &[("A", "1"), ("B", "2")]).unwrap();
        let entries = block_entries(&block);
        assert_eq!(entries.len(), 2);
        assert!(entries.contains(&"A=1".to_string()));
        assert!(entries.contains(&"B=2".to_string()));
        assert_eq!(*block.last().unwrap(), 0);
        assert_eq!(block[block.len() - 2], 0);
    }

    #[test]
    fn env_block_empty_is_double_nul() {
        let block = build_env_block(true, &[] as &[(&str, &str)]).unwrap();
        assert_eq!(block, vec![0u16, 0u16]);
    }

    #[test]
    fn env_override_wins_case_insensitively() {
        let block = build_env_block(true, &[("Path", "a"), ("PATH", "b")]).unwrap();
        let entries = block_entries(&block);
        assert_eq!(entries, vec!["PATH=b".to_string()]);
    }

    #[test]
    fn env_block_is_sorted_case_insensitively() {
        // Windows requires the block sorted case-insensitively by name.
        let block =
            build_env_block(true, &[("zebra", "1"), ("Apple", "2"), ("mango", "3")]).unwrap();
        let entries = block_entries(&block);
        assert_eq!(
            entries,
            vec![
                "Apple=2".to_string(),
                "mango=3".to_string(),
                "zebra=1".to_string()
            ]
        );
    }

    #[test]
    fn env_duplicate_cases_last_wins_and_sorted() {
        let block = build_env_block(
            true,
            &[
                ("Path", "a"),
                ("ZED", "1"),
                ("PATH", "b"),
                ("pAtH", "c"),
                ("apple", "x"),
            ],
        )
        .unwrap();
        let entries = block_entries(&block);
        // Last `pAtH=c` wins among the case variants; block stays sorted.
        assert_eq!(
            entries,
            vec![
                "apple=x".to_string(),
                "pAtH=c".to_string(),
                "ZED=1".to_string()
            ]
        );
    }

    #[test]
    fn env_rejects_non_ascii_keys_fail_closed_retains_unicode_values() {
        // Non-ASCII names cannot be casefolded reliably here: reject rather
        // than risk an ambiguous merged environment.
        assert!(build_env_block(true, &[("PÄTH", "1")]).is_err());
        assert!(build_env_block(true, &[("日本語", "1")]).is_err());
        // Unicode values are retained.
        let block = build_env_block(true, &[("GREETING", "héllo✓")]).unwrap();
        let entries = block_entries(&block);
        assert_eq!(entries, vec!["GREETING=héllo✓".to_string()]);
        // Parent-inherited non-ASCII names also fail closed when inherited.
        // (Uses `clear_env == false` path; at minimum overrides with ASCII
        // keys must not mask a validation failure for non-ASCII keys.)
        assert!(build_env_block(true, &[("A", "1"), ("Ä", "2")]).is_err());
    }

    #[test]
    fn env_rejects_bad_keys_and_nul_values() {
        assert!(build_env_block(true, &[("A=B", "1")]).is_err());
        assert!(build_env_block(true, &[("", "1")]).is_err());
        assert!(build_env_block(true, &[("A", "x\0y")]).is_err());
    }

    #[test]
    fn wait_error_is_retried_until_confirmed_without_busy_loop() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Instant;

        let failures = AtomicUsize::new(1);
        let start = Instant::now();
        let value = wait_until_confirmed(|| {
            if take_injected_failure(&failures) {
                Err(io::Error::other("injected transient wait failure"))
            } else {
                Ok(7u32)
            }
        });
        assert_eq!(value, 7);
        assert_eq!(failures.load(Ordering::SeqCst), 0);
        assert!(start.elapsed() >= Duration::from_millis(5));
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn created_process_cleanup_required_for_verify_or_resume_failure() {
        // Only a fully verified and resumed child is safely owned.
        assert!(!created_process_needs_cleanup(true, true));
        assert!(created_process_needs_cleanup(false, false));
        assert!(created_process_needs_cleanup(false, true));
        assert!(created_process_needs_cleanup(true, false));
    }

    #[test]
    fn cleanup_created_process_confirms_only_after_real_wait() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Instant;

        let waits = AtomicUsize::new(0);
        let terminations = AtomicUsize::new(0);
        let start = Instant::now();
        let outcome = cleanup_created_process_until_confirmed(
            Some(Instant::now() + Duration::from_secs(2)),
            || {
                terminations.fetch_add(1, Ordering::SeqCst);
                true
            },
            |_| {
                let n = waits.fetch_add(1, Ordering::SeqCst) + 1;
                match n {
                    1 => Err(io::Error::other("injected wait failure")),
                    2 => Ok(false),
                    _ => Ok(true),
                }
            },
        );
        assert_eq!(outcome, CleanupOutcome::Confirmed);
        assert!(terminations.load(Ordering::SeqCst) >= 3);
        assert_eq!(waits.load(Ordering::SeqCst), 3);
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn cleanup_created_process_never_confirms_on_persistent_wait_failure() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Instant;

        let waits = AtomicUsize::new(0);
        let outcome = cleanup_created_process_until_confirmed(
            Some(Instant::now() + Duration::from_millis(50)),
            || true,
            |_| {
                waits.fetch_add(1, Ordering::SeqCst);
                Err(io::Error::other("injected persistent wait failure"))
            },
        );
        assert_eq!(outcome, CleanupOutcome::Unconfirmed);
        assert!(waits.load(Ordering::SeqCst) > 0);
    }

    #[test]
    fn cleanup_created_process_never_confirms_when_termination_fails() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Instant;

        let waits = AtomicUsize::new(0);
        let outcome = cleanup_created_process_until_confirmed(
            Some(Instant::now() + Duration::from_millis(50)),
            // Termination rejected: this must never be interpreted as success.
            || false,
            |_| {
                waits.fetch_add(1, Ordering::SeqCst);
                Ok(false)
            },
        );
        assert_eq!(outcome, CleanupOutcome::Unconfirmed);
        assert!(waits.load(Ordering::SeqCst) > 0);
    }

    // NOTE (no Windows native verification): the two tests below exercise
    // only the injectable `cleanup_created_process_until_confirmed` helper on
    // any host. Real `TerminateProcess` / `WaitForSingleObject` / job
    // kill-on-close behavior is verified solely by native Windows integration
    // tests on a Windows host; there is no native fault injection here.

    #[test]
    fn cleanup_terminate_failure_then_recovery_confirms_no_native_verification() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Instant;

        // First two terminate attempts rejected (`false`), then accepted.
        // Wait fails once, times out once, then signals exit. Proves the
        // blocking owner path retries owned termination (never a bare pid)
        // and only `Ok(true)` from the real wait confirms.
        let terminations = AtomicUsize::new(0);
        let waits = AtomicUsize::new(0);
        let outcome = cleanup_created_process_until_confirmed(
            Some(Instant::now() + Duration::from_secs(2)),
            || {
                let n = terminations.fetch_add(1, Ordering::SeqCst) + 1;
                // Injected failure then recovery: rejected twice, then accepted.
                n > 2
            },
            |_| {
                let n = waits.fetch_add(1, Ordering::SeqCst) + 1;
                match n {
                    1 => Err(io::Error::other("injected transient wait failure")),
                    2 => Ok(false),
                    _ => Ok(true),
                }
            },
        );
        assert_eq!(outcome, CleanupOutcome::Confirmed);
        // Terminate retried past the initial failures; wait retried past
        // failure/timeout until the signalled object.
        assert!(terminations.load(Ordering::SeqCst) >= 3);
        assert_eq!(waits.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn cleanup_persistent_terminate_and_wait_failure_never_confirms_no_native_verification() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Instant;

        // Persistent failure on both sides must never report `Confirmed`
        // (no false success) and must keep the owned handle (caller keeps
        // waiting with bounded deadline in tests, unbounded in production).
        let terminations = AtomicUsize::new(0);
        let waits = AtomicUsize::new(0);
        let outcome = cleanup_created_process_until_confirmed(
            Some(Instant::now() + Duration::from_millis(60)),
            || {
                terminations.fetch_add(1, Ordering::SeqCst);
                false
            },
            |_| {
                waits.fetch_add(1, Ordering::SeqCst);
                Err(io::Error::other("injected persistent wait failure"))
            },
        );
        assert_eq!(outcome, CleanupOutcome::Unconfirmed);
        assert!(terminations.load(Ordering::SeqCst) > 0);
        assert!(waits.load(Ordering::SeqCst) > 0);
    }

    #[test]
    fn job_backstop_closes_exactly_once_before_retry_on_persistent_failure() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Instant;

        // Models the owned job slot (`Some` handle taken exactly once). The
        // close closure mirrors `terminate_and_reap_blocking`: `take()` the
        // owned token, counting a real close only when a handle was present,
        // so a second call or `Drop` cannot close an unrelated handle.
        let mut slot: Option<u32> = Some(7);
        let closes = AtomicUsize::new(0);
        let terminations = AtomicUsize::new(0);
        let waits = AtomicUsize::new(0);
        let seq = AtomicUsize::new(0);
        let close_seq = AtomicUsize::new(usize::MAX);
        let first_terminate_seq = AtomicUsize::new(usize::MAX);
        let outcome = cleanup_with_job_backstop_until_confirmed(
            Some(Instant::now() + Duration::from_millis(60)),
            || {
                let s = seq.fetch_add(1, Ordering::SeqCst);
                if slot.take().is_some() {
                    closes.fetch_add(1, Ordering::SeqCst);
                    close_seq.store(s, Ordering::SeqCst);
                }
            },
            || {
                let s = seq.fetch_add(1, Ordering::SeqCst);
                first_terminate_seq
                    .compare_exchange(usize::MAX, s, Ordering::SeqCst, Ordering::SeqCst)
                    .ok();
                terminations.fetch_add(1, Ordering::SeqCst);
                // Injected persistent `TerminateProcess` failure: never success.
                false
            },
            |_| {
                waits.fetch_add(1, Ordering::SeqCst);
                Err(io::Error::other("injected persistent wait failure"))
            },
        );
        // Persistent failure on both sides must never report `Confirmed`
        // (no false success), yet the backstop must have run exactly once
        // and strictly before the first terminate retry.
        assert_eq!(outcome, CleanupOutcome::Unconfirmed);
        assert_eq!(
            closes.load(Ordering::SeqCst),
            1,
            "job backstop must close once"
        );
        assert!(terminations.load(Ordering::SeqCst) > 0);
        assert!(waits.load(Ordering::SeqCst) > 0);
        assert!(
            close_seq.load(Ordering::SeqCst) < first_terminate_seq.load(Ordering::SeqCst),
            "job close must precede terminate retry"
        );
        // Slot is now `None`: a repeated backstop (second blocking call or
        // `Drop`) must not close again.
        let outcome2 = cleanup_with_job_backstop_until_confirmed(
            Some(Instant::now() + Duration::from_millis(20)),
            || {
                if slot.take().is_some() {
                    closes.fetch_add(1, Ordering::SeqCst);
                }
            },
            || {
                terminations.fetch_add(1, Ordering::SeqCst);
                false
            },
            |_| Err(io::Error::other("injected persistent wait failure")),
        );
        assert_eq!(outcome2, CleanupOutcome::Unconfirmed);
        assert_eq!(
            closes.load(Ordering::SeqCst),
            1,
            "duplicate close of job handle is forbidden"
        );
    }

    #[test]
    fn job_backstop_failure_recovery_confirms() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Instant;

        // Terminate rejected twice then accepted; wait errors once, times out
        // once, then signals. Proves the indefinite phase retries owned
        // termination (never a bare pid) after the single backstop close and
        // only `Ok(true)` from the real wait confirms.
        let mut slot: Option<u32> = Some(9);
        let closes = AtomicUsize::new(0);
        let terminations = AtomicUsize::new(0);
        let waits = AtomicUsize::new(0);
        let outcome = cleanup_with_job_backstop_until_confirmed(
            Some(Instant::now() + Duration::from_secs(2)),
            || {
                if slot.take().is_some() {
                    closes.fetch_add(1, Ordering::SeqCst);
                }
            },
            || {
                let n = terminations.fetch_add(1, Ordering::SeqCst) + 1;
                n > 2
            },
            |_| {
                let n = waits.fetch_add(1, Ordering::SeqCst) + 1;
                match n {
                    1 => Err(io::Error::other("injected transient wait failure")),
                    2 => Ok(false),
                    _ => Ok(true),
                }
            },
        );
        assert_eq!(outcome, CleanupOutcome::Confirmed);
        assert_eq!(
            closes.load(Ordering::SeqCst),
            1,
            "backstop closes exactly once"
        );
        assert!(terminations.load(Ordering::SeqCst) >= 3);
        assert_eq!(waits.load(Ordering::SeqCst), 3);
        assert!(slot.is_none(), "owned job token consumed exactly once");
    }

    #[test]
    fn debug_helper_carries_no_secret() {
        let _p: PathBuf = Path::new("/bin/echo").to_path_buf();
        let dbg = format!("{:?}", SpawnError::NotAbsolute);
        assert!(!dbg.contains(SECRET));
    }

    // ---- Windows-only integration tests (need a real Windows host) ----

    #[cfg(windows)]
    fn windows_cmd() -> PathBuf {
        PathBuf::from(r"C:\Windows\System32\cmd.exe")
    }

    #[cfg(windows)]
    fn wait_for_exit(child: &mut ManagedChild, timeout: Duration) -> Option<ExitStatus> {
        let start = Instant::now();
        while start.elapsed() < timeout {
            match child.try_wait() {
                Ok(Some(status)) => return Some(status),
                Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                Err(_) => return None,
            }
        }
        None
    }

    #[cfg(windows)]
    #[test]
    fn windows_missing_absolute_binary_fails_closed() {
        let err = ManagedChild::spawn(
            Path::new(r"C:\nonexistent-absolute-9f2c4d\binary.exe"),
            &[] as &[&OsStr],
            &[] as &[(&OsStr, &OsStr)],
            true,
        )
        .unwrap_err();
        assert!(
            matches!(
                err,
                SpawnError::SpawnFailed { .. } | SpawnError::HardeningUnavailable(_)
            ),
            "got {err:?}"
        );
        let dbg = format!("{err:?}");
        assert!(!dbg.contains("nonexistent-absolute"));
    }

    #[cfg(windows)]
    #[test]
    fn windows_exit_code_is_forwarded() {
        let mut child = ManagedChild::spawn(
            &windows_cmd(),
            &["/D", "/c", "exit", "42"],
            &[] as &[(&OsStr, &OsStr)],
            true,
        )
        .expect("spawn cmd");
        let status = wait_for_exit(&mut child, Duration::from_secs(15)).expect("cmd exits fast");
        assert_eq!(status.code(), Some(42));
    }

    #[cfg(windows)]
    #[test]
    fn windows_env_override_reaches_child() {
        // `exit %VAR%` proves the supplied env (not ambient) reached the child.
        let mut child = ManagedChild::spawn(
            &windows_cmd(),
            &["/D", "/c", "exit %MANAGED_WIN_SECRET%"],
            &[("MANAGED_WIN_SECRET", "13")],
            true,
        )
        .expect("spawn cmd with env");
        let status = wait_for_exit(&mut child, Duration::from_secs(15)).expect("cmd exits fast");
        assert_eq!(status.code(), Some(13));
    }

    #[cfg(windows)]
    #[test]
    fn windows_spawned_process_is_in_job() {
        let mut child = ManagedChild::spawn(
            &windows_cmd(),
            &["/D", "/c", "ping -n 5 127.0.0.1 >NUL"],
            &[] as &[(&OsStr, &OsStr)],
            true,
        )
        .expect("spawn ping-ish sleep");
        unsafe {
            let mut in_job: super::win32::BOOL = 0;
            let job = child.job.expect("job still owned before blocking reap");
            let ok = super::win32::IsProcessInJob(child.process, job, &mut in_job);
            assert_eq!(ok, 1, "IsProcessInJob call failed");
            assert_ne!(in_job, 0, "child is not in the kill-on-close job");
        }
        child.kill().ok();
        let _ = wait_for_exit(&mut child, Duration::from_secs(15));
    }

    #[cfg(windows)]
    #[test]
    fn windows_kill_reaps_owned_child() {
        let ping = PathBuf::from(r"C:\Windows\System32\ping.exe");
        let mut child = ManagedChild::spawn(
            &ping,
            &["-n", "30", "127.0.0.1"],
            &[] as &[(&OsStr, &OsStr)],
            true,
        )
        .expect("spawn ping");
        assert!(child.try_wait().unwrap().is_none());
        child.kill().expect("kill owned ping");
        let status = wait_for_exit(&mut child, Duration::from_secs(15)).expect("killed ping reaps");
        assert!(!status.success());
    }

    #[cfg(windows)]
    #[test]
    fn windows_blocking_owner_reap_confirms_owned_child() {
        let ping = PathBuf::from(r"C:\Windows\System32\ping.exe");
        let mut child = ManagedChild::spawn(
            &ping,
            &["-n", "30", "127.0.0.1"],
            &[] as &[(&OsStr, &OsStr)],
            true,
        )
        .expect("spawn ping");
        assert!(child.try_wait().unwrap().is_none());
        let (_code, confirmed) = child.terminate_and_reap_blocking(Duration::from_millis(100));
        assert!(confirmed, "owner-thread wait must confirm the owned child");
    }

    #[cfg(windows)]
    #[test]
    fn windows_drop_preserves_sibling() {
        let ping = PathBuf::from(r"C:\Windows\System32\ping.exe");
        let mut survivor = ManagedChild::spawn(
            &ping,
            &["-n", "30", "127.0.0.1"],
            &[] as &[(&OsStr, &OsStr)],
            true,
        )
        .expect("spawn survivor");
        let victim_pid = {
            let victim = ManagedChild::spawn(
                &ping,
                &["-n", "30", "127.0.0.1"],
                &[] as &[(&OsStr, &OsStr)],
                true,
            )
            .expect("spawn victim");
            let pid = victim.id();
            assert_ne!(pid, survivor.id());
            drop(victim);
            pid
        };
        std::thread::sleep(Duration::from_millis(500));
        // Victim reaped via its own handle; sibling must still run.
        assert!(
            survivor.try_wait().unwrap().is_none(),
            "drop killed sibling (victim pid {victim_pid})"
        );
        survivor.kill().ok();
        let _ = wait_for_exit(&mut survivor, Duration::from_secs(15));
    }

    #[cfg(windows)]
    #[test]
    fn windows_debug_never_leaks_private_data() {
        let ping = PathBuf::from(r"C:\Windows\System32\ping.exe");
        let mut child = ManagedChild::spawn(
            &ping,
            &["-n", "30", "127.0.0.1"],
            &[("MANAGED_WIN_SECRET", SECRET)],
            true,
        )
        .expect("spawn ping");
        let dbg = format!("{child:?}");
        assert!(!dbg.contains(SECRET), "debug leaked env: {dbg}");
        assert!(!dbg.contains("MANAGED_WIN_SECRET"));
        assert!(!dbg.contains("ping.exe"));
        assert!(dbg.contains(&child.id().to_string()));
        child.kill().ok();
        let _ = wait_for_exit(&mut child, Duration::from_secs(15));
    }
}
