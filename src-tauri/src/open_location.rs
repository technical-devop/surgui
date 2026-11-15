//! Local download-location validation and file-manager launching.
//!
//! The only path input is the server-owned `dest_path` from a freshly fetched
//! `/list` response; no IPC caller or UI value is ever used as a filesystem
//! path. The validated value is reduced to the containing directory before it
//! leaves this module, and the directory is passed directly to a platform file
//! manager with a separate argument (never through a shell).
//!
//! # Locality is best-effort, not a proof
//!
//! On Linux every prefix from `/` through the final target is matched against
//! a `/proc/self/mountinfo` snapshot before any metadata call, reusing the
//! parsed entries and the longest matching mount point for each prefix. Every
//! encountered mount must have an allowlisted local filesystem type, so any
//! prefix on a network or unknown filesystem fails closed. Mountinfo parsing
//! and unescaping are strict, and malformed data fails closed. This avoids
//! `symlink_metadata` traversing a network mount just to discover its
//! filesystem type. Bind mounts inherit their underlying filesystem type, and
//! overlay mounts are rejected because their backing storage cannot be
//! established here. A mount can also change after the mountinfo snapshot and
//! before the file manager opens the directory.
//!
//! On Windows the drive root is checked with `GetDriveTypeW`, UNC/device/verbatim
//! prefixes are rejected, and every path component is checked for reparse
//! points; mapped network drives may still race or be remapped by the operating
//! system.
//!
//! There is also an unavoidable time-of-check/time-of-use window: the file can
//! be moved, replaced with a symlink/reparse point, or its mount can change
//! after validation and before the file manager opens the directory. This
//! module narrows that window and fails closed on every check it can perform,
//! but it does not claim a race-free or physically-local outcome.
//!
//! On Linux the file manager is resolved only from trusted absolute paths
//! (`/usr/bin/xdg-open` or `/bin/xdg-open`), never from inherited `PATH`, and
//! the validated directory is passed as a direct argument without a shell
//! command string. The helper also receives a fixed trusted `PATH`. Its status
//! is observed for a bounded window so an immediate failure is reported; a
//! helper still running at the deadline is unconfirmed and reported as a
//! failure, then reaped by a detached thread. A process-wide permit admits only
//! one outstanding helper at a time, so at most one such reaper can exist. The
//! permit is released only after exit is confirmed; a failed wait or an
//! inability to start the reaper permanently poisons it, so another helper is
//! never started while an earlier child may still exist. `xdg-open` itself may
//! be a shell script, which is a normal OS helper rather than data passed
//! through a shell command string.

use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::process::Child;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Conservative upper bound for a server-provided destination path.
const MAX_PATH_BYTES: usize = 32 * 1024;

/// Bounded window for confirming that the platform file manager did not exit
/// immediately with an error. A helper that is still running after this window
/// is unconfirmed, so the launch fails and is reaped in the background.
pub(crate) const LAUNCH_STATUS_TIMEOUT: Duration = Duration::from_secs(2);

/// Process-wide single-launch permit. It is acquired before spawning a file
/// manager and released only when the child's exit has been confirmed, even if
/// the command caller is dropped. A failed reaping attempt poisons the permit
/// for the rest of the process, so another helper can never be started while an
/// earlier child might still exist.
struct LaunchPermit {
    release_on_drop: Arc<AtomicBool>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LaunchPermitSlot {
    Free,
    Held,
    Poisoned,
}

fn launch_permit_slot() -> &'static Mutex<LaunchPermitSlot> {
    static SLOT: OnceLock<Mutex<LaunchPermitSlot>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(LaunchPermitSlot::Free))
}

fn lock_launch_permit_slot() -> std::sync::MutexGuard<'static, LaunchPermitSlot> {
    match launch_permit_slot().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn try_acquire_launch_permit() -> Option<LaunchPermit> {
    let mut slot = lock_launch_permit_slot();
    if *slot != LaunchPermitSlot::Free {
        return None;
    }
    *slot = LaunchPermitSlot::Held;
    Some(LaunchPermit {
        release_on_drop: Arc::new(AtomicBool::new(true)),
    })
}

impl LaunchPermit {
    /// Until the child is confirmed reaped, dropping this permit poisons the
    /// process-wide slot instead of making it available again.
    fn defer_release(&self) {
        self.release_on_drop.store(false, Ordering::SeqCst);
    }

    fn confirm_reaped(&self) {
        self.release_on_drop.store(true, Ordering::SeqCst);
    }
}

impl Drop for LaunchPermit {
    fn drop(&mut self) {
        let mut slot = lock_launch_permit_slot();
        if self.release_on_drop.load(Ordering::SeqCst) {
            if *slot == LaunchPermitSlot::Held {
                *slot = LaunchPermitSlot::Free;
            }
        } else {
            *slot = LaunchPermitSlot::Poisoned;
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
fn launch_permit_is_held() -> bool {
    matches!(
        *lock_launch_permit_slot(),
        LaunchPermitSlot::Held | LaunchPermitSlot::Poisoned
    )
}

#[cfg(all(test, target_os = "linux"))]
fn launch_permit_is_poisoned() -> bool {
    *lock_launch_permit_slot() == LaunchPermitSlot::Poisoned
}

#[cfg(all(test, target_os = "linux"))]
fn reset_launch_permit_for_test() {
    *lock_launch_permit_slot() = LaunchPermitSlot::Free;
}

/// Sanitized failure for local-location validation or launch.
///
/// No variant carries the path, filename, mount name, OS error, or raw process
/// output. The command layer maps these to stable IPC codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LocationError {
    InvalidPath,
    NonLocal,
    Symlink,
    #[cfg(not(any(target_os = "linux", windows)))]
    UnsupportedPlatform,
    Missing,
    Unavailable,
    LaunchFailed,
}

impl fmt::Display for LocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidPath => "invalid download location",
            Self::NonLocal => "download location is not local",
            Self::Symlink => "download location is a link",
            #[cfg(not(any(target_os = "linux", windows)))]
            Self::UnsupportedPlatform => "download location is unsupported",
            Self::Missing => "download file is missing",
            Self::Unavailable => "download location is unavailable",
            Self::LaunchFailed => "download location could not be opened",
        };
        f.write_str(message)
    }
}

impl std::error::Error for LocationError {}

/// Seam for opening a validated directory without a shell.
pub(crate) trait LocationLauncher: Send + Sync {
    fn launch_directory(&self, directory: &Path) -> Result<LocationLaunch, LocationError>;
}

/// A spawned file-manager helper that has not necessarily exited yet.
pub(crate) struct LocationLaunch {
    child: Option<Child>,
    permit: Option<LaunchPermit>,
    #[cfg(test)]
    result: Option<Result<(), LocationError>>,
}

impl LocationLaunch {
    fn from_child(child: Child, permit: LaunchPermit) -> Self {
        Self {
            child: Some(child),
            permit: Some(permit),
            #[cfg(test)]
            result: None,
        }
    }

    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn test_result(result: Result<(), LocationError>) -> Self {
        Self {
            child: None,
            permit: None,
            result: Some(result),
        }
    }

    /// Wait for one bounded confirmation window, then reap in the background.
    ///
    /// An immediate nonzero exit is surfaced as a sanitized launch failure. If
    /// the helper is still running at the deadline, the launch is unconfirmed
    /// and this returns a sanitized failure instead of claiming success. The
    /// child, together with the single process-wide launch permit, is moved to
    /// a detached reaper that releases the permit only after the child's exit
    /// is confirmed; a failed wait or reaper start poisons the permit instead.
    /// Dropping this value before a confirmed exit takes the same reaper path.
    pub(crate) fn wait_bounded(mut self, timeout: Duration) -> Result<(), LocationError> {
        #[cfg(test)]
        if let Some(result) = self.result.take() {
            return result;
        }
        let mut child = self.child.take().ok_or(LocationError::LaunchFailed)?;
        let deadline = Instant::now() + timeout;
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    return if status.success() {
                        Ok(())
                    } else {
                        Err(LocationError::LaunchFailed)
                    };
                }
                Ok(None) => {
                    let now = Instant::now();
                    if now >= deadline {
                        spawn_detached_reaper(child, self.permit.take());
                        return Err(LocationError::LaunchFailed);
                    }
                    std::thread::sleep((deadline - now).min(Duration::from_millis(10)));
                }
                Err(_) => {
                    spawn_detached_reaper(child, self.permit.take());
                    return Err(LocationError::LaunchFailed);
                }
            }
        }
    }
}

impl Drop for LocationLaunch {
    fn drop(&mut self) {
        if let Some(child) = self.child.take() {
            spawn_detached_reaper(child, self.permit.take());
        }
    }
}

/// One child that can be reaped by the detached reaper.
trait ReapableChild: Send {
    fn wait_for_exit(&mut self) -> std::io::Result<std::process::ExitStatus>;
}

impl ReapableChild for Child {
    fn wait_for_exit(&mut self) -> std::io::Result<std::process::ExitStatus> {
        Child::wait(self)
    }
}

/// Reap a helper without blocking the caller and release its launch permit only
/// after the child is confirmed to have exited. A failed wait or a failure to
/// start the reaper poisons the permit permanently; it is never released on an
/// unconfirmed exit, so a second child cannot be started.
fn spawn_detached_reaper(child: Child, permit: Option<LaunchPermit>) {
    spawn_detached_reaper_with(child, permit, |task| {
        std::thread::Builder::new()
            .name("open-location-reaper".to_owned())
            .spawn(task)
            .map(|_| ())
    });
}

fn spawn_detached_reaper_with<C, S>(child: C, permit: Option<LaunchPermit>, spawn: S)
where
    C: ReapableChild + 'static,
    S: FnOnce(Box<dyn FnOnce() + Send>) -> std::io::Result<()>,
{
    let permit = permit.inspect(|permit| {
        permit.defer_release();
    });
    let task: Box<dyn FnOnce() + Send> = Box::new(move || {
        let mut child = child;
        let permit = permit;
        if child.wait_for_exit().is_ok() {
            if let Some(permit) = permit.as_ref() {
                permit.confirm_reaped();
            }
        }
    });
    // If the reaper thread cannot be started, dropping the task drops the
    // still-deferred permit, which poisons it instead of allowing another
    // child. The child handle is dropped without being killed.
    let _ = spawn(task);
}

/// Platform file-manager launcher used by the Tauri command.
pub(crate) struct SystemLocationLauncher;

impl LocationLauncher for SystemLocationLauncher {
    fn launch_directory(&self, directory: &Path) -> Result<LocationLaunch, LocationError> {
        platform::spawn_directory(directory)
    }
}

/// Validate a completed download's destination and return its containing
/// directory.
///
/// The input must be absolute, bounded, NUL-free and free of parent-directory
/// traversal components. The existing file must be a regular file, and neither
/// it nor any ancestor may be a symlink/reparse point. The platform mount check
/// then fails closed for unrecognized/network filesystems.
pub(crate) fn containing_directory_for_completed_file(
    path: &str,
) -> Result<PathBuf, LocationError> {
    if path.is_empty()
        || path.len() > MAX_PATH_BYTES
        || path.as_bytes().contains(&0)
        || path.chars().any(char::is_control)
    {
        return Err(LocationError::InvalidPath);
    }
    let path = Path::new(path);
    if !path.is_absolute() {
        return Err(LocationError::InvalidPath);
    }
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(LocationError::InvalidPath);
    }
    platform::validate_file_and_parent(path)
}

#[cfg(target_os = "linux")]
mod platform {
    use super::*;
    use std::os::unix::ffi::OsStrExt;
    use std::process::{Command, Stdio};

    // Linux magic numbers not exported by the pinned libc crate.
    const JFS_SUPER_MAGIC: u64 = 0x3153_464a;
    const EXFAT_SUPER_MAGIC: u64 = 0x2011_bab0;
    const NTFS_SB_MAGIC: u64 = 0x5346_544e;
    const ZFS_SUPER_MAGIC: u64 = 0x2fc1_2fc1;

    // Only trusted absolute locations. The inherited PATH is never consulted,
    // so an attacker-controlled executable cannot receive the private path.
    const XDG_OPEN_CANDIDATES: [&str; 2] = ["/usr/bin/xdg-open", "/bin/xdg-open"];

    #[derive(Debug)]
    struct MountEntry {
        mount_point: Vec<u8>,
        filesystem_type: Vec<u8>,
    }

    pub(super) fn validate_file_and_parent(path: &Path) -> Result<PathBuf, LocationError> {
        let mut components = path.components();
        match components.next() {
            Some(Component::RootDir) => {}
            _ => return Err(LocationError::InvalidPath),
        }

        let mut parts = Vec::new();
        for component in components {
            match component {
                Component::Normal(part) => parts.push(part),
                Component::CurDir => {}
                _ => return Err(LocationError::InvalidPath),
            }
        }
        if parts.is_empty() {
            return Err(LocationError::Unavailable);
        }

        // Classify the containing mount from `/proc/self/mountinfo` before any
        // metadata call. Doing this first means a network mount is rejected
        // without `symlink_metadata` traversing it or touching the target file.
        let mut target = PathBuf::from("/");
        for part in &parts {
            target.push(part);
        }
        validate_local_mount(&target)?;

        // Walk every component without following links. Rejecting links at
        // every level prevents a linked parent from escaping the checked path.
        let mut current = PathBuf::from("/");
        for (index, part) in parts.iter().enumerate() {
            current.push(part);
            let metadata = match std::fs::symlink_metadata(&current) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(LocationError::Missing)
                }
                Err(_) => return Err(LocationError::Unavailable),
            };
            if metadata.file_type().is_symlink() {
                return Err(LocationError::Symlink);
            }
            if index + 1 == parts.len() {
                if !metadata.file_type().is_file() {
                    return Err(LocationError::Unavailable);
                }
            } else if !metadata.file_type().is_dir() {
                return Err(LocationError::Unavailable);
            }
        }

        current
            .parent()
            .filter(|parent| parent.is_absolute() && parent.is_dir())
            .map(Path::to_path_buf)
            .ok_or(LocationError::Unavailable)
    }

    fn validate_local_mount(path: &Path) -> Result<(), LocationError> {
        let mountinfo =
            std::fs::read("/proc/self/mountinfo").map_err(|_| LocationError::NonLocal)?;
        validate_mountinfo_for_path(&mountinfo, path)
    }

    pub(super) fn validate_mountinfo_for_path(
        mountinfo: &[u8],
        path: &Path,
    ) -> Result<(), LocationError> {
        let path = normalized_path_bytes(path).ok_or(LocationError::InvalidPath)?;
        let entries = parse_mountinfo(mountinfo).ok_or(LocationError::NonLocal)?;
        // Require every mount encountered from `/` through the final target
        // to be allowlisted local. Each prefix is classified with its longest
        // matching mount before any `symlink_metadata` call, so a local
        // nested mount under a network parent still fails closed.
        let mount = deepest_mount_for_path(&entries, b"/").ok_or(LocationError::NonLocal)?;
        if !linux_mount_type_is_local(&mount.filesystem_type) {
            return Err(LocationError::NonLocal);
        }
        if path.len() > 1 {
            for (index, byte) in path.iter().enumerate().skip(1) {
                if *byte != b'/' {
                    continue;
                }
                let prefix = &path[..index];
                let mount =
                    deepest_mount_for_path(&entries, prefix).ok_or(LocationError::NonLocal)?;
                if !linux_mount_type_is_local(&mount.filesystem_type) {
                    return Err(LocationError::NonLocal);
                }
            }
            let mount = deepest_mount_for_path(&entries, &path).ok_or(LocationError::NonLocal)?;
            if !linux_mount_type_is_local(&mount.filesystem_type) {
                return Err(LocationError::NonLocal);
            }
        }
        Ok(())
    }

    fn normalized_path_bytes(path: &Path) -> Option<Vec<u8>> {
        let mut out = Vec::new();
        let mut rooted = false;
        for component in path.components() {
            match component {
                Component::RootDir => {
                    if rooted {
                        return None;
                    }
                    rooted = true;
                }
                Component::Normal(part) => {
                    out.push(b'/');
                    out.extend_from_slice(part.as_bytes());
                }
                Component::CurDir => {}
                Component::ParentDir | Component::Prefix(_) => return None,
            }
        }
        if !rooted {
            return None;
        }
        if out.is_empty() {
            out.push(b'/');
        }
        Some(out)
    }

    fn parse_mountinfo(mountinfo: &[u8]) -> Option<Vec<MountEntry>> {
        let mut entries = Vec::new();
        for line in mountinfo.split(|byte| *byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            let fields: Vec<&[u8]> = line
                .split(|byte| *byte == b' ')
                .filter(|field| !field.is_empty())
                .collect();
            // mount ID, parent ID, major:minor, root, mount point, options,
            // optional fields..., "-", filesystem type, source, super options.
            let separator = fields.iter().position(|field| *field == b"-")?;
            if separator < 6 || fields.len() <= separator + 2 {
                return None;
            }
            let mount_point = unescape_mountinfo_field(fields[4])?;
            let filesystem_type = fields[separator + 1].to_vec();
            if filesystem_type.is_empty() {
                return None;
            }
            entries.push(MountEntry {
                mount_point,
                filesystem_type,
            });
        }
        Some(entries)
    }

    fn unescape_mountinfo_field(field: &[u8]) -> Option<Vec<u8>> {
        let mut out = Vec::with_capacity(field.len());
        let mut index = 0;
        while index < field.len() {
            if field[index] != b'\\' {
                out.push(field[index]);
                index += 1;
                continue;
            }
            if index + 3 >= field.len() {
                return None;
            }
            let digits = &field[index + 1..index + 4];
            if !digits.iter().all(|digit| matches!(digit, b'0'..=b'7')) {
                return None;
            }
            let value = ((digits[0] - b'0') as u32) << 6
                | ((digits[1] - b'0') as u32) << 3
                | (digits[2] - b'0') as u32;
            if value > u8::MAX as u32 {
                return None;
            }
            out.push(value as u8);
            index += 4;
        }
        Some(out)
    }

    fn deepest_mount_for_path<'a>(
        entries: &'a [MountEntry],
        path: &[u8],
    ) -> Option<&'a MountEntry> {
        let mut best: Option<&MountEntry> = None;
        for entry in entries {
            if !mount_point_matches(&entry.mount_point, path) {
                continue;
            }
            if best.is_none_or(|current| entry.mount_point.len() >= current.mount_point.len()) {
                best = Some(entry);
            }
        }
        best
    }

    fn mount_point_matches(mount_point: &[u8], path: &[u8]) -> bool {
        if mount_point == b"/" {
            return path.starts_with(b"/");
        }
        path == mount_point
            || (path.starts_with(mount_point) && path.get(mount_point.len()) == Some(&b'/'))
    }

    fn linux_mount_type_is_local(filesystem_type: &[u8]) -> bool {
        // Overlay is rejected below: its backing storage cannot be established.
        let magic = match filesystem_type {
            b"ext2" => libc::EXT2_SUPER_MAGIC as u64,
            b"ext3" => libc::EXT3_SUPER_MAGIC as u64,
            b"ext4" => libc::EXT4_SUPER_MAGIC as u64,
            b"xfs" => libc::XFS_SUPER_MAGIC as u64,
            b"btrfs" => libc::BTRFS_SUPER_MAGIC as u64,
            b"f2fs" => libc::F2FS_SUPER_MAGIC as u64,
            b"msdos" | b"vfat" | b"fat" => libc::MSDOS_SUPER_MAGIC as u64,
            b"reiserfs" => libc::REISERFS_SUPER_MAGIC as u64,
            b"tmpfs" => libc::TMPFS_MAGIC as u64,
            b"udf" => libc::UDF_SUPER_MAGIC as u64,
            b"iso9660" | b"isofs" => libc::ISOFS_SUPER_MAGIC as u64,
            b"nilfs2" => libc::NILFS_SUPER_MAGIC as u64,
            b"ocfs2" => libc::OCFS2_SUPER_MAGIC as u64,
            b"bcachefs" => libc::BCACHEFS_SUPER_MAGIC as u64,
            b"jfs" => JFS_SUPER_MAGIC,
            b"exfat" => EXFAT_SUPER_MAGIC,
            b"ntfs" | b"ntfs3" => NTFS_SB_MAGIC,
            b"zfs" => ZFS_SUPER_MAGIC,
            _ => return false,
        };
        linux_filesystem_is_local(magic)
    }

    /// Conservative local-filesystem allowlist. Everything else (including
    /// NFS, SMB/CIFS, AFS, FUSE, overlay, and unknown magics) fails closed.
    /// Overlay is rejected because its backing storage cannot be established.
    pub(super) fn linux_filesystem_is_local(magic: u64) -> bool {
        [
            libc::EXT2_SUPER_MAGIC as u64,
            libc::EXT3_SUPER_MAGIC as u64,
            libc::EXT4_SUPER_MAGIC as u64,
            libc::XFS_SUPER_MAGIC as u64,
            libc::BTRFS_SUPER_MAGIC as u64,
            libc::F2FS_SUPER_MAGIC as u64,
            libc::MSDOS_SUPER_MAGIC as u64,
            libc::REISERFS_SUPER_MAGIC as u64,
            libc::TMPFS_MAGIC as u64,
            libc::UDF_SUPER_MAGIC as u64,
            libc::ISOFS_SUPER_MAGIC as u64,
            libc::NILFS_SUPER_MAGIC as u64,
            libc::OCFS2_SUPER_MAGIC as u64,
            libc::BCACHEFS_SUPER_MAGIC as u64,
            JFS_SUPER_MAGIC,
            EXFAT_SUPER_MAGIC,
            NTFS_SB_MAGIC,
            ZFS_SUPER_MAGIC,
        ]
        .contains(&magic)
    }

    pub(super) fn resolve_executable(candidates: &[&Path]) -> Result<PathBuf, LocationError> {
        use std::os::unix::fs::PermissionsExt;

        for candidate in candidates {
            if !candidate.is_absolute() {
                continue;
            }
            if let Ok(metadata) = std::fs::metadata(candidate) {
                if metadata.is_file() && metadata.permissions().mode() & 0o111 != 0 {
                    return Ok(candidate.to_path_buf());
                }
            }
        }
        Err(LocationError::LaunchFailed)
    }

    pub(super) fn spawn_directory(directory: &Path) -> Result<LocationLaunch, LocationError> {
        if !directory.is_absolute() {
            return Err(LocationError::InvalidPath);
        }
        let candidates: Vec<&Path> = XDG_OPEN_CANDIDATES.iter().map(Path::new).collect();
        let program = resolve_executable(&candidates)?;
        spawn_with_program(&program, directory)
    }

    pub(super) fn spawn_with_program(
        program: &Path,
        directory: &Path,
    ) -> Result<LocationLaunch, LocationError> {
        if !program.is_absolute() || !directory.is_absolute() {
            return Err(LocationError::InvalidPath);
        }
        // A validated directory is absolute, so it starts with `/` and can
        // never be parsed by xdg-open as an option. Pass it as one distinct
        // argument and never build a shell command or consult inherited PATH.
        let mut command = Command::new(program);
        command.arg(directory);
        // The helper itself is absolute, and its child lookups must not inherit
        // an attacker-controlled PATH either.
        command.env("PATH", "/usr/bin:/bin");
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let permit = try_acquire_launch_permit().ok_or(LocationError::LaunchFailed)?;
        let child = command.spawn().map_err(|_| LocationError::LaunchFailed)?;
        Ok(LocationLaunch::from_child(child, permit))
    }
}

#[cfg(windows)]
mod platform {
    use super::*;
    use std::os::windows::ffi::{OsStrExt, OsStringExt};
    use std::os::windows::fs::MetadataExt;
    use std::process::{Command, Stdio};
    use windows_sys::Win32::Storage::FileSystem::{GetDriveTypeW, FILE_ATTRIBUTE_REPARSE_POINT};
    use windows_sys::Win32::System::SystemInformation::GetWindowsDirectoryW;

    const DRIVE_REMOVABLE: u32 = 2;
    const DRIVE_FIXED: u32 = 3;

    pub(super) fn validate_file_and_parent(path: &Path) -> Result<PathBuf, LocationError> {
        let mut components = path.components();
        let drive = match components.next() {
            Some(Component::Prefix(prefix)) => match prefix.kind() {
                std::path::Prefix::Disk(letter) => letter,
                // UNC, verbatim and device prefixes are rejected outright.
                _ => return Err(LocationError::NonLocal),
            },
            _ => return Err(LocationError::InvalidPath),
        };
        match components.next() {
            Some(Component::RootDir) => {}
            _ => return Err(LocationError::InvalidPath),
        }

        let mut parts = Vec::new();
        for component in components {
            match component {
                Component::Normal(part) => parts.push(part),
                Component::CurDir => {}
                _ => return Err(LocationError::InvalidPath),
            }
        }
        if parts.is_empty() {
            return Err(LocationError::Unavailable);
        }

        let root = PathBuf::from(format!("{}:\\", (drive as char).to_ascii_uppercase()));
        if !windows_drive_is_local(&root) {
            return Err(LocationError::NonLocal);
        }

        let mut current = root;
        for (index, part) in parts.iter().enumerate() {
            if part.to_string_lossy().contains(':') {
                return Err(LocationError::InvalidPath);
            }
            current.push(part);
            let metadata = match std::fs::symlink_metadata(&current) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(LocationError::Missing)
                }
                Err(_) => return Err(LocationError::Unavailable),
            };
            if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
                || metadata.file_type().is_symlink()
            {
                return Err(LocationError::Symlink);
            }
            if index + 1 == parts.len() {
                if !metadata.is_file() {
                    return Err(LocationError::Unavailable);
                }
            } else if !metadata.is_dir() {
                return Err(LocationError::Unavailable);
            }
        }

        current
            .parent()
            .filter(|parent| parent.is_absolute() && parent.is_dir())
            .map(Path::to_path_buf)
            .ok_or(LocationError::Unavailable)
    }

    fn windows_drive_is_local(root: &Path) -> bool {
        let mut wide: Vec<u16> = root.as_os_str().encode_wide().collect();
        if wide.contains(&0) {
            return false;
        }
        wide.push(0);
        // SAFETY: the pointer refers to a NUL-terminated UTF-16 buffer that
        // outlives the call and is not retained by the operating system.
        let drive_type = unsafe { GetDriveTypeW(wide.as_ptr()) };
        matches!(drive_type, DRIVE_FIXED | DRIVE_REMOVABLE)
    }

    pub(super) fn spawn_directory(directory: &Path) -> Result<LocationLaunch, LocationError> {
        if !directory.is_absolute() {
            return Err(LocationError::InvalidPath);
        }
        let program = windows_explorer_path()?;
        let mut command = Command::new(program);
        command.arg(directory);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let permit = try_acquire_launch_permit().ok_or(LocationError::LaunchFailed)?;
        let child = command.spawn().map_err(|_| LocationError::LaunchFailed)?;
        Ok(LocationLaunch::from_child(child, permit))
    }

    /// Use the Windows-directory `explorer.exe`, never a PATH lookup or a
    /// caller-supplied executable.
    fn windows_explorer_path() -> Result<PathBuf, LocationError> {
        let mut buffer = vec![0u16; 260];
        // SAFETY: the buffer is writable for `buffer.len()` UTF-16 units and
        // the returned length is checked before the buffer is read.
        let len = unsafe { GetWindowsDirectoryW(buffer.as_mut_ptr(), buffer.len() as u32) };
        if len == 0 || len as usize >= buffer.len() {
            return Err(LocationError::LaunchFailed);
        }
        buffer.truncate(len as usize);
        let windows = std::ffi::OsString::from_wide(&buffer);
        Ok(PathBuf::from(windows).join("explorer.exe"))
    }
}

#[cfg(not(any(target_os = "linux", windows)))]
mod platform {
    use super::*;

    pub(super) fn validate_file_and_parent(_path: &Path) -> Result<PathBuf, LocationError> {
        Err(LocationError::UnsupportedPlatform)
    }

    pub(super) fn spawn_directory(_directory: &Path) -> Result<LocationLaunch, LocationError> {
        Err(LocationError::UnsupportedPlatform)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(any(target_os = "linux", windows))]
    use std::sync::atomic::{AtomicU64, Ordering};

    #[cfg(any(target_os = "linux", windows))]
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    #[cfg(target_os = "linux")]
    static LAUNCH_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[cfg(target_os = "linux")]
    fn lock_launch_test() -> std::sync::MutexGuard<'static, ()> {
        let guard = match LAUNCH_TEST_LOCK.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        reset_launch_permit_for_test();
        guard
    }

    #[cfg(any(target_os = "linux", windows))]
    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "surge-open-location-{tag}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[cfg(any(target_os = "linux", windows))]
    fn remove_all_best_effort(path: &Path) {
        if path.symlink_metadata().is_ok() {
            if path.is_dir() && !path.is_symlink() {
                let _ = std::fs::remove_dir_all(path);
            } else {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    #[test]
    fn relative_malformed_and_traversing_paths_are_rejected() {
        for path in [
            "",
            "relative/file.bin",
            "./file.bin",
            "/tmp/../file.bin",
            "/tmp/\0file.bin",
            "/tmp/bad\nname.bin",
        ] {
            assert_eq!(
                containing_directory_for_completed_file(path),
                Err(LocationError::InvalidPath),
                "path must be rejected: {path:?}"
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn local_completed_file_with_shell_metacharacters_is_literal() {
        let root = temp_dir("metacharacters");
        let parent = root.join("-dir ; $(echo pwn) `id` & 'quote'");
        std::fs::create_dir_all(&parent).expect("create parent");
        let file = parent.join("-file \"double\" 'single' ; $HOME `touch pwned` & !.bin");
        std::fs::write(&file, b"completed").expect("write fixture");
        let result = containing_directory_for_completed_file(&file.to_string_lossy());
        assert_eq!(result, Ok(parent.clone()));
        assert!(!parent.join("pwned").exists());
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn missing_non_file_and_symlink_paths_fail_closed() {
        let root = temp_dir("rejections");
        let missing = root.join("missing.bin");
        assert_eq!(
            containing_directory_for_completed_file(&missing.to_string_lossy()),
            Err(LocationError::Missing)
        );

        let directory = root.join("directory.bin");
        std::fs::create_dir_all(&directory).expect("create directory fixture");
        assert_eq!(
            containing_directory_for_completed_file(&directory.to_string_lossy()),
            Err(LocationError::Unavailable)
        );

        let file = root.join("real.bin");
        std::fs::write(&file, b"completed").expect("write fixture");
        let file_link = root.join("file-link.bin");
        let parent_link = root.join("parent-link");
        std::os::unix::fs::symlink(&file, &file_link).expect("create file symlink");
        std::os::unix::fs::symlink(&root, &parent_link).expect("create parent symlink");
        assert_eq!(
            containing_directory_for_completed_file(&file_link.to_string_lossy()),
            Err(LocationError::Symlink)
        );
        assert_eq!(
            containing_directory_for_completed_file(
                &parent_link.join("real.bin").to_string_lossy()
            ),
            Err(LocationError::Symlink)
        );

        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_mount_classification_fails_closed_for_network_and_fuse() {
        assert!(platform::linux_filesystem_is_local(
            libc::EXT4_SUPER_MAGIC as u64
        ));
        assert!(platform::linux_filesystem_is_local(
            libc::TMPFS_MAGIC as u64
        ));
        assert!(!platform::linux_filesystem_is_local(
            libc::NFS_SUPER_MAGIC as u64
        ));
        assert!(!platform::linux_filesystem_is_local(
            libc::SMB_SUPER_MAGIC as u64
        ));
        assert!(!platform::linux_filesystem_is_local(
            libc::FUSE_SUPER_MAGIC as u64
        ));
        assert!(!platform::linux_filesystem_is_local(
            libc::OVERLAYFS_SUPER_MAGIC as u64
        ));
        assert!(!platform::linux_filesystem_is_local(u64::MAX));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_mountinfo_uses_deepest_mount_and_fails_closed() {
        const MOUNTINFO: &str = concat!(
            "36 35 0:32 / / rw,relatime - ext4 /dev/sda1 rw\n",
            "40 36 0:40 / /home rw,relatime shared:1 - nfs4 server:/export rw\n",
            "41 36 0:41 / /mnt/local rw,relatime - ext4 /dev/sdb1 rw\n",
            "42 36 0:42 / /mnt/remote rw,relatime - cifs //server/share rw\n",
            "43 36 0:43 / /mnt/fuse rw,relatime - fuse.sshfs sshfs rw\n",
            "44 36 0:44 / /mnt/odd\\040mount rw,relatime - ext4 /dev/sdc1 rw\n",
            "45 40 0:45 / /home/local rw,relatime - ext4 /dev/sdd1 rw\n",
            "46 36 0:46 / /mnt/mystery rw,relatime - mysteryfs none rw\n",
        );
        let info = MOUNTINFO.as_bytes();
        let classify = |path: &str| platform::validate_mountinfo_for_path(info, Path::new(path));

        assert_eq!(classify("/var/tmp/file.bin"), Ok(()));
        assert_eq!(
            classify("/home/other/file.bin"),
            Err(LocationError::NonLocal)
        );
        assert_eq!(
            classify("/home/local/file.bin"),
            Err(LocationError::NonLocal)
        );
        assert_eq!(classify("/home2/file.bin"), Ok(()));
        assert_eq!(
            classify("/mnt/remote/file.bin"),
            Err(LocationError::NonLocal)
        );
        assert_eq!(classify("/mnt/fuse/file.bin"), Err(LocationError::NonLocal));
        assert_eq!(
            classify("/mnt/mystery/file.bin"),
            Err(LocationError::NonLocal)
        );
        assert_eq!(classify("/mnt/odd mount/file.bin"), Ok(()));

        assert_eq!(
            platform::validate_mountinfo_for_path(b"", Path::new("/tmp/file.bin")),
            Err(LocationError::NonLocal)
        );
        assert_eq!(
            platform::validate_mountinfo_for_path(b"malformed\n", Path::new("/tmp/file.bin")),
            Err(LocationError::NonLocal)
        );
        assert_eq!(
            platform::validate_mountinfo_for_path(
                b"36 35 0:32 / / rw - ext4 /dev/sda1 rw\n47 36 0:47 / /mnt/bad\\09x rw - ext4 /dev/sde1 rw\n",
                Path::new("/tmp/file.bin")
            ),
            Err(LocationError::NonLocal)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_mountinfo_rejects_network_parent_and_overlay() {
        const MOUNTINFO: &str = concat!(
            "36 35 0:32 / / rw,relatime - ext4 /dev/sda1 rw\n",
            "40 36 0:40 / /home rw,relatime shared:1 - nfs4 server:/export rw\n",
            "45 40 0:45 / /home/local rw,relatime - ext4 /dev/sdd1 rw\n",
            "46 36 0:46 / /overlay rw,relatime - overlay overlay rw\n",
            "47 46 0:47 / /overlay/local rw,relatime - ext4 /dev/sdb1 rw\n",
        );
        let info = MOUNTINFO.as_bytes();
        let classify = |path: &str| platform::validate_mountinfo_for_path(info, Path::new(path));

        // A local mount nested beneath a network parent still traverses the
        // network prefix, so every prefix check must fail closed.
        assert_eq!(
            classify("/home/local/file.bin"),
            Err(LocationError::NonLocal)
        );
        assert_eq!(classify("/home/local"), Err(LocationError::NonLocal));
        // Overlay backing storage cannot be established, so any overlay
        // prefix fails closed, even under or above a local mount.
        assert_eq!(classify("/overlay/file.bin"), Err(LocationError::NonLocal));
        assert_eq!(
            classify("/overlay/local/file.bin"),
            Err(LocationError::NonLocal)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_launcher_uses_checked_absolute_program_and_bounded_status() {
        use std::os::unix::fs::PermissionsExt;

        let _serial = lock_launch_test();
        let root = temp_dir("launcher");
        let directory = root.join("target dir");
        std::fs::create_dir(&directory).expect("create launch target");

        let ok = root.join("ok-launcher");
        std::fs::write(&ok, b"#!/bin/sh\nexit 0\n").expect("write ok helper");
        std::fs::set_permissions(&ok, std::fs::Permissions::from_mode(0o700))
            .expect("chmod ok helper");

        let failing = root.join("failing-launcher");
        std::fs::write(&failing, b"#!/bin/sh\nexit 7\n").expect("write failing helper");
        std::fs::set_permissions(&failing, std::fs::Permissions::from_mode(0o700))
            .expect("chmod failing helper");

        let non_executable = root.join("non-executable");
        std::fs::write(&non_executable, b"#!/bin/sh\nexit 0\n").expect("write non-exec helper");
        std::fs::set_permissions(&non_executable, std::fs::Permissions::from_mode(0o600))
            .expect("chmod non-exec helper");

        let missing = root.join("missing-launcher");
        let candidates = [non_executable.as_path(), missing.as_path(), ok.as_path()];
        assert_eq!(platform::resolve_executable(&candidates), Ok(ok.clone()));
        assert_eq!(
            platform::resolve_executable(&[non_executable.as_path()]),
            Err(LocationError::LaunchFailed)
        );

        platform::spawn_with_program(&ok, &directory)
            .expect("spawn ok helper")
            .wait_bounded(std::time::Duration::from_secs(2))
            .expect("successful helper must be accepted");
        assert_eq!(
            platform::spawn_with_program(&failing, &directory)
                .expect("spawn failing helper")
                .wait_bounded(std::time::Duration::from_secs(2)),
            Err(LocationError::LaunchFailed)
        );

        let slow = root.join("slow-launcher");
        std::fs::write(&slow, b"#!/bin/sh\nexec /bin/sleep 1\n").expect("write slow helper");
        std::fs::set_permissions(&slow, std::fs::Permissions::from_mode(0o700))
            .expect("chmod slow helper");
        let started = Instant::now();
        assert_eq!(
            platform::spawn_with_program(&slow, &directory)
                .expect("spawn slow helper")
                .wait_bounded(std::time::Duration::from_millis(50)),
            Err(LocationError::LaunchFailed),
            "an unconfirmed helper must not be reported as success"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_millis(800),
            "the status window must stay bounded"
        );
        assert!(
            launch_permit_is_held(),
            "the late helper must retain the single launch permit"
        );
        assert_eq!(
            platform::spawn_with_program(&ok, &directory).err(),
            Some(LocationError::LaunchFailed),
            "a second launch must fail while the helper is outstanding"
        );
        let deadline = Instant::now() + std::time::Duration::from_secs(3);
        while launch_permit_is_held() && Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            !launch_permit_is_held(),
            "the late reaper must release the permit after exit"
        );
        platform::spawn_with_program(&ok, &directory)
            .expect("spawn after reaper")
            .wait_bounded(std::time::Duration::from_secs(2))
            .expect("the next launch is accepted once the helper exits");

        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_pending_helper_holds_permit_and_blocks_second_launch_until_reaped() {
        use std::os::unix::fs::PermissionsExt;

        let _serial = lock_launch_test();
        let root = temp_dir("launch-permit");
        let directory = root.join("target dir");
        std::fs::create_dir(&directory).expect("create launch target");

        let slow = root.join("slow-launcher");
        std::fs::write(&slow, b"#!/bin/sh\nexec /bin/sleep 1\n").expect("write slow helper");
        std::fs::set_permissions(&slow, std::fs::Permissions::from_mode(0o700))
            .expect("chmod slow helper");

        let quick = root.join("quick-launcher");
        std::fs::write(&quick, b"#!/bin/sh\nexit 0\n").expect("write quick helper");
        std::fs::set_permissions(&quick, std::fs::Permissions::from_mode(0o700))
            .expect("chmod quick helper");

        let launch = platform::spawn_with_program(&slow, &directory).expect("spawn slow helper");
        assert!(launch_permit_is_held(), "spawn must take the permit");
        assert_eq!(
            launch.wait_bounded(std::time::Duration::from_millis(25)),
            Err(LocationError::LaunchFailed)
        );
        assert!(
            launch_permit_is_held(),
            "the detached reaper must own the permit until exit"
        );
        assert_eq!(
            platform::spawn_with_program(&quick, &directory).err(),
            Some(LocationError::LaunchFailed),
            "a second launch must fail while the first helper is pending"
        );

        let deadline = Instant::now() + std::time::Duration::from_secs(3);
        while launch_permit_is_held() && Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            !launch_permit_is_held(),
            "the late reaper must release the permit after confirmed exit"
        );
        platform::spawn_with_program(&quick, &directory)
            .expect("spawn after confirmed exit")
            .wait_bounded(std::time::Duration::from_secs(2))
            .expect("a new launch is accepted after the prior helper is reaped");

        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    struct FailingWaitChild;

    #[cfg(target_os = "linux")]
    impl ReapableChild for FailingWaitChild {
        fn wait_for_exit(&mut self) -> std::io::Result<std::process::ExitStatus> {
            Err(std::io::Error::from(std::io::ErrorKind::Other))
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reaper_poisons_permit_when_wait_does_not_confirm_exit() {
        let _serial = lock_launch_test();
        let permit = try_acquire_launch_permit().expect("acquire the launch permit");

        spawn_detached_reaper_with(FailingWaitChild, Some(permit), |task| {
            task();
            Ok(())
        });

        assert!(
            launch_permit_is_poisoned(),
            "an unconfirmed wait must poison the permit"
        );
        assert!(
            try_acquire_launch_permit().is_none(),
            "no second child may be admitted after an unconfirmed reap"
        );
        reset_launch_permit_for_test();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reaper_poisons_permit_when_thread_cannot_start() {
        let _serial = lock_launch_test();
        let permit = try_acquire_launch_permit().expect("acquire the launch permit");

        spawn_detached_reaper_with(FailingWaitChild, Some(permit), |task| {
            drop(task);
            Err(std::io::Error::from(std::io::ErrorKind::Other))
        });

        assert!(
            launch_permit_is_poisoned(),
            "a reaper that cannot start must poison the permit"
        );
        assert!(
            try_acquire_launch_permit().is_none(),
            "no second child may be admitted without a reaper"
        );
        reset_launch_permit_for_test();
    }

    #[cfg(windows)]
    #[test]
    fn windows_rejects_unc_device_verbatim_and_drive_relative_paths() {
        for path in [
            r"\\server\share\file.bin",
            r"\\.\C:\file.bin",
            r"\\?\C:\file.bin",
            r"\\?\UNC\server\share\file.bin",
            r"C:file.bin",
        ] {
            let result = containing_directory_for_completed_file(path);
            assert!(
                matches!(
                    result,
                    Err(LocationError::NonLocal) | Err(LocationError::InvalidPath)
                ),
                "path must be rejected: {path:?}"
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_local_completed_file_is_validated() {
        let root = temp_dir("windows-local");
        let file = root.join("completed.bin");
        std::fs::write(&file, b"completed").expect("write fixture");
        let result = containing_directory_for_completed_file(&file.to_string_lossy());
        assert_eq!(result, Ok(root.clone()));
        remove_all_best_effort(&root);
    }
}
