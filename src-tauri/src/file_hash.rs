//! Safe, read-only MD5 + SHA-256 hashing of a completed download file.
//!
//! The daemon-owned destination is the only path input. The file is opened
//! read-only and never written. On Linux every component is opened relative to
//! the previous directory with `O_NOFOLLOW`; the final component is pinned and
//! checked to be a regular file before it is read. After hashing, the ancestor
//! directory chain is reopened descriptor-relative and compared with the
//! originally opened identities, so a replaced ancestor cannot redirect the
//! served result even when the final inode is hard-linked into the new path.
//!
//! On Windows the path must be a drive-letter path on a fixed local drive.
//! UNC, verbatim, device, drive-relative, alternate-stream, reserved-device,
//! and trailing-dot/space names are rejected before any open. The volume root
//! is pinned with `CreateFileW`, then every component is opened with
//! `NtCreateFile` using one-component `OBJECT_ATTRIBUTES` rooted at the
//! previously opened directory handle. Each component is opened with
//! `FILE_OPEN_REPARSE_POINT` and its attribute/reparse tag, object type, file
//! type, and volume identity are checked before it is used as the next root.
//! Symlinks, junctions, mount points, any other reparse point, remote/offline
//! files, and non-disk handles fail closed. The final regular file is read from
//! that same handle; afterwards the handle identity, size, and mtime are
//! rechecked and the whole path is reopened from the drive root component by
//! component and compared with the originally pinned root and ancestor
//! identities. A replaced root, ancestor, or final file therefore fails even
//! when the original file object is still reachable through the old handle.
//!
//! Both digests are calculated in one streaming pass over the same opened
//! handle with a fixed 64 KiB buffer. Identity, size, mtime, and change time
//! (Linux `ctime`, Windows `ChangeTime`) are compared before and after hashing,
//! and the path is reopened without following links to detect a replaced file.
//! The change time catches an ordinary same-size rewrite even when the actor
//! restores last-write time. These checks are best-effort against a same-user
//! actor that can mutate a file during the read or restore metadata afterwards;
//! they do not provide a cryptographic proof of immutability.

use std::fmt;
use std::path::{Component, Path};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const MAX_PATH_BYTES: usize = 32 * 1024;
const HASH_BUFFER_BYTES: usize = 64 * 1024;

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FileHashError {
    InvalidPath,
    UnsupportedPlatform,
    NonLocal,
    Symlink,
    Missing,
    NotRegular,
    Changed,
    Io,
}

impl fmt::Display for FileHashError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidPath => "download file path is invalid",
            Self::UnsupportedPlatform => "download file hashing is unsupported on this platform",
            Self::NonLocal => "download file is not on local storage",
            Self::Symlink => "download file path contains a link",
            Self::Missing => "download file is missing",
            Self::NotRegular => "download file is not a regular file",
            Self::Changed => "download file changed while it was being hashed",
            Self::Io => "download file could not be read",
        };
        f.write_str(message)
    }
}

impl std::error::Error for FileHashError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FileDigests {
    pub md5: String,
    pub sha256: String,
    pub size: u64,
    pub mtime_sec: i64,
    pub mtime_nsec: i64,
    pub ctime_sec: i64,
    pub ctime_nsec: i64,
    pub file_dev: u64,
    pub file_ino: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FileIdentity {
    pub dev: u64,
    pub ino: u64,
    pub size: u64,
    pub mtime_sec: i64,
    pub mtime_nsec: i64,
    pub ctime_sec: i64,
    pub ctime_nsec: i64,
}

pub(crate) fn hash_file(
    path: &str,
    expected_size: Option<u64>,
    cancel: &Arc<AtomicBool>,
) -> Result<FileDigests, FileHashError> {
    let path = validate_path(path)?;
    #[cfg(target_os = "linux")]
    {
        linux::hash(path, expected_size, cancel)
    }
    #[cfg(windows)]
    {
        windows::hash(path, expected_size, cancel)
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        let _ = (path, expected_size, cancel);
        Err(FileHashError::UnsupportedPlatform)
    }
}

/// Re-validate that an already-hashed file still has the exact recorded
/// identity. Linux reopens every path component descriptor-relative without
/// following links; Windows reopens the same handle-relative component chain
/// and rejects any reparse point or changed identity.
pub(crate) fn verify_file_identity(
    path: &str,
    expected: &FileIdentity,
) -> Result<(), FileHashError> {
    let path = validate_path(path)?;
    #[cfg(target_os = "linux")]
    {
        linux::verify_identity(path, expected)
    }
    #[cfg(windows)]
    {
        windows::verify_identity(path, expected)
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        let _ = (path, expected);
        Err(FileHashError::UnsupportedPlatform)
    }
}

#[cfg(all(test, any(target_os = "linux", windows)))]
fn hash_file_with_hook(
    path: &str,
    expected_size: Option<u64>,
    cancel: &Arc<AtomicBool>,
    hook: impl FnOnce(&Path),
) -> Result<FileDigests, FileHashError> {
    let path = validate_path(path)?;
    #[cfg(target_os = "linux")]
    {
        linux::hash_with_hook_for_test(path, expected_size, cancel, hook)
    }
    #[cfg(windows)]
    {
        windows::hash_with_hook_for_test(path, expected_size, cancel, hook)
    }
}

fn validate_path(path: &str) -> Result<&Path, FileHashError> {
    if path.is_empty()
        || path.len() > MAX_PATH_BYTES
        || path.as_bytes().contains(&0)
        || path.chars().any(char::is_control)
    {
        return Err(FileHashError::InvalidPath);
    }
    let path = Path::new(path);
    if !path.is_absolute() {
        return Err(FileHashError::InvalidPath);
    }
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(FileHashError::InvalidPath);
    }
    Ok(path)
}

fn stream_digests<F>(
    mut read: F,
    cancel: &Arc<AtomicBool>,
) -> Result<(String, String, u64), FileHashError>
where
    F: FnMut(&mut [u8]) -> std::io::Result<usize>,
{
    use sha2::{Digest as _, Sha256};

    let mut md5 = Md5::new();
    let mut sha256 = Sha256::new();
    let mut buffer = vec![0u8; HASH_BUFFER_BYTES];
    let mut total = 0u64;
    loop {
        if cancel.load(Ordering::SeqCst) {
            return Err(FileHashError::Changed);
        }
        let read_len = read(&mut buffer).map_err(|_| FileHashError::Io)?;
        if read_len == 0 {
            break;
        }
        let chunk = &buffer[..read_len];
        md5.update(chunk);
        sha256.update(chunk);
        total = total.saturating_add(read_len as u64);
    }
    Ok((md5.finish_hex(), format!("{:x}", sha256.finalize()), total))
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use crate::open_location::{self, LocationError};
    use std::ffi::{CStr, CString};
    use std::fs::File;
    use std::io::Read;
    use std::os::fd::{AsRawFd, FromRawFd, RawFd};
    use std::os::unix::ffi::OsStrExt;

    pub(super) fn hash(
        path: &Path,
        expected_size: Option<u64>,
        cancel: &Arc<AtomicBool>,
    ) -> Result<FileDigests, FileHashError> {
        hash_with_hook(path, expected_size, cancel, |_| {})
    }

    fn hash_with_hook(
        path: &Path,
        expected_size: Option<u64>,
        cancel: &Arc<AtomicBool>,
        hook: impl FnOnce(&Path),
    ) -> Result<FileDigests, FileHashError> {
        open_location::validate_local_mount_for_hash(path).map_err(map_location)?;
        let opened = OpenedFile::open(path)?;
        let before = opened.before;
        if let Some(expected) = expected_size {
            if expected != before.size {
                return Err(FileHashError::Changed);
            }
        }
        hook(path);

        let (md5, sha256, total) = stream_digests(|buffer| (&opened.reader).read(buffer), cancel)?;
        if total != before.size || cancel.load(Ordering::SeqCst) {
            return Err(FileHashError::Changed);
        }
        opened.verify_unchanged()?;
        Ok(FileDigests {
            md5,
            sha256,
            size: total,
            mtime_sec: before.mtime_sec,
            mtime_nsec: before.mtime_nsec,
            ctime_sec: before.ctime_sec,
            ctime_nsec: before.ctime_nsec,
            file_dev: before.dev,
            file_ino: before.ino,
        })
    }

    pub(super) fn verify_identity(
        path: &Path,
        expected: &FileIdentity,
    ) -> Result<(), FileHashError> {
        open_location::validate_local_mount_for_hash(path).map_err(map_location)?;
        let opened = OpenedFile::open(path)?;
        if opened.before != *expected {
            return Err(FileHashError::Changed);
        }
        Ok(())
    }

    fn map_location(error: LocationError) -> FileHashError {
        match error {
            LocationError::InvalidPath => FileHashError::InvalidPath,
            LocationError::NonLocal => FileHashError::NonLocal,
            LocationError::Symlink => FileHashError::Symlink,
            LocationError::Missing => FileHashError::Missing,
            LocationError::Unavailable => FileHashError::NotRegular,
            LocationError::LaunchFailed => FileHashError::Io,
            #[cfg(not(any(target_os = "linux", windows)))]
            LocationError::UnsupportedPlatform => FileHashError::UnsupportedPlatform,
        }
    }

    struct OpenedFile {
        reader: File,
        names: Vec<CString>,
        ancestors: Vec<(u64, u64)>,
        before: FileIdentity,
    }

    impl OpenedFile {
        fn open(path: &Path) -> Result<Self, FileHashError> {
            let mut parts = Vec::new();
            let mut rooted = false;
            for component in path.components() {
                match component {
                    Component::RootDir if !rooted => rooted = true,
                    Component::Normal(part) => parts.push(c_name(part)?),
                    Component::CurDir => {}
                    Component::ParentDir | Component::Prefix(_) | Component::RootDir => {
                        return Err(FileHashError::InvalidPath)
                    }
                }
            }
            if !rooted || parts.is_empty() {
                return Err(FileHashError::InvalidPath);
            }

            let mut parent = open_root()?;
            let mut ancestors = vec![dir_identity(&parent)?];
            for name in &parts[..parts.len() - 1] {
                parent = open_at(
                    parent.as_raw_fd(),
                    name,
                    libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )?;
                ancestors.push(dir_identity(&parent)?);
            }
            let name = parts[parts.len() - 1].clone();
            let pinned = open_at(
                parent.as_raw_fd(),
                &name,
                libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )?;
            let path_stat = stat_fd(pinned.as_raw_fd())?;
            if path_stat.st_mode & libc::S_IFMT == libc::S_IFLNK {
                return Err(FileHashError::Symlink);
            }
            if path_stat.st_mode & libc::S_IFMT != libc::S_IFREG {
                return Err(FileHashError::NotRegular);
            }
            let before = identity_from_stat(&path_stat);
            // Reopen the pinned regular file through procfs so the read handle
            // refers to exactly the inode that was checked above.
            let reader = File::open(format!("/proc/self/fd/{}", pinned.as_raw_fd()))
                .map_err(|_| FileHashError::Io)?;
            let read_stat = stat_fd(reader.as_raw_fd())?;
            if identity_from_stat(&read_stat) != before {
                return Err(FileHashError::Changed);
            }
            Ok(Self {
                reader,
                names: parts,
                ancestors,
                before,
            })
        }

        fn verify_unchanged(&self) -> Result<(), FileHashError> {
            let after = identity_from_stat(&stat_fd(self.reader.as_raw_fd())?);
            if after != self.before {
                return Err(FileHashError::Changed);
            }
            // Re-walk the exact path from the root using descriptor-relative
            // no-follow components. Comparing every ancestor identity rejects
            // a renamed/replaced directory even when the final inode is
            // hard-linked into the new path.
            let mut parent = open_root()?;
            if dir_identity(&parent)? != self.ancestors[0] {
                return Err(FileHashError::Changed);
            }
            for (index, name) in self.names[..self.names.len() - 1].iter().enumerate() {
                parent = open_at(
                    parent.as_raw_fd(),
                    name,
                    libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )?;
                if dir_identity(&parent)? != self.ancestors[index + 1] {
                    return Err(FileHashError::Changed);
                }
            }
            let name = &self.names[self.names.len() - 1];
            let path_fd = open_at(
                parent.as_raw_fd(),
                name,
                libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )?;
            let path_after = identity_from_stat(&stat_fd(path_fd.as_raw_fd())?);
            if path_after != self.before {
                return Err(FileHashError::Changed);
            }
            Ok(())
        }
    }

    fn c_name(part: &std::ffi::OsStr) -> Result<CString, FileHashError> {
        CString::new(part.as_bytes()).map_err(|_| FileHashError::InvalidPath)
    }

    fn open_root() -> Result<File, FileHashError> {
        let fd = unsafe {
            libc::open(
                c"/".as_ptr(),
                libc::O_PATH | libc::O_DIRECTORY | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(FileHashError::Io);
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    fn open_at(parent: RawFd, name: &CStr, flags: i32) -> Result<File, FileHashError> {
        let fd = unsafe { libc::openat(parent, name.as_ptr(), flags) };
        if fd < 0 {
            let code = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            return Err(match code {
                libc::ENOENT => FileHashError::Missing,
                libc::ELOOP | libc::ENOTDIR => FileHashError::Symlink,
                _ => FileHashError::Io,
            });
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    fn stat_fd(fd: RawFd) -> Result<libc::stat, FileHashError> {
        let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
        if unsafe { libc::fstat(fd, &mut stat) } != 0 {
            return Err(FileHashError::Io);
        }
        Ok(stat)
    }

    fn dir_identity(file: &File) -> Result<(u64, u64), FileHashError> {
        let stat = stat_fd(file.as_raw_fd())?;
        if stat.st_mode & libc::S_IFMT != libc::S_IFDIR {
            return Err(FileHashError::NotRegular);
        }
        Ok((stat.st_dev, stat.st_ino))
    }

    fn identity_from_stat(stat: &libc::stat) -> FileIdentity {
        FileIdentity {
            dev: stat.st_dev,
            ino: stat.st_ino,
            size: stat.st_size.max(0) as u64,
            mtime_sec: stat.st_mtime,
            mtime_nsec: stat.st_mtime_nsec,
            ctime_sec: stat.st_ctime,
            ctime_nsec: stat.st_ctime_nsec,
        }
    }

    #[cfg(test)]
    pub(super) fn hash_with_hook_for_test(
        path: &Path,
        expected_size: Option<u64>,
        cancel: &Arc<AtomicBool>,
        hook: impl FnOnce(&Path),
    ) -> Result<FileDigests, FileHashError> {
        hash_with_hook(path, expected_size, cancel, hook)
    }
}

#[cfg(windows)]
mod windows {
    use super::*;
    use std::ffi::OsStr;
    use std::os::windows::ffi::OsStrExt;
    use std::path::Prefix;
    use std::ptr::{null, null_mut};
    use windows_sys::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows_sys::Wdk::Storage::FileSystem::{
        NtCreateFile, FILE_NON_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_REPARSE_POINT,
        FILE_SEQUENTIAL_ONLY, FILE_SYNCHRONOUS_IO_NONALERT,
    };
    use windows_sys::Win32::Foundation::{
        CloseHandle, HANDLE, INVALID_HANDLE_VALUE, NTSTATUS, OBJ_CASE_INSENSITIVE,
        STATUS_DELETE_PENDING, STATUS_FILE_IS_A_DIRECTORY, STATUS_INVALID_PARAMETER,
        STATUS_IO_REPARSE_TAG_INVALID, STATUS_IO_REPARSE_TAG_MISMATCH,
        STATUS_IO_REPARSE_TAG_NOT_HANDLED, STATUS_NOT_A_DIRECTORY, STATUS_OBJECT_NAME_INVALID,
        STATUS_OBJECT_NAME_NOT_FOUND, STATUS_OBJECT_PATH_NOT_FOUND, STATUS_OBJECT_PATH_SYNTAX_BAD,
        STATUS_REPARSE, STATUS_REPARSE_OBJECT, STATUS_REPARSE_POINT_ENCOUNTERED,
        STATUS_REPARSE_POINT_NOT_RESOLVED, UNICODE_STRING,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FileAttributeTagInfo, FileBasicInfo, GetDriveTypeW,
        GetFileInformationByHandle, GetFileInformationByHandleEx, GetFileType, ReadFile,
        BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_DEVICE, FILE_ATTRIBUTE_DIRECTORY,
        FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_OFFLINE, FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS,
        FILE_ATTRIBUTE_RECALL_ON_OPEN, FILE_ATTRIBUTE_REPARSE_POINT, FILE_ATTRIBUTE_TAG_INFO,
        FILE_BASIC_INFO, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_READ_ATTRIBUTES, FILE_READ_DATA, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        FILE_TRAVERSE, FILE_TYPE_DISK, FILE_TYPE_REMOTE, OPEN_EXISTING, SYNCHRONIZE,
    };
    use windows_sys::Win32::System::IO::IO_STATUS_BLOCK;

    const DRIVE_FIXED: u32 = 3;
    const MAX_COMPONENT_UNITS: usize = 255;
    const WINDOWS_TICKS_PER_SECOND: u64 = 10_000_000;
    const WINDOWS_TO_UNIX_EPOCH_SECONDS: i64 = 11_644_473_600;

    pub(super) fn hash(
        path: &Path,
        expected_size: Option<u64>,
        cancel: &Arc<AtomicBool>,
    ) -> Result<FileDigests, FileHashError> {
        hash_with_hook(path, expected_size, cancel, |_| {})
    }

    fn hash_with_hook(
        path: &Path,
        expected_size: Option<u64>,
        cancel: &Arc<AtomicBool>,
        hook: impl FnOnce(&Path),
    ) -> Result<FileDigests, FileHashError> {
        let opened = OpenedFile::open(path)?;
        let before = opened.before;
        if let Some(expected) = expected_size {
            if expected != before.size {
                return Err(FileHashError::Changed);
            }
        }
        hook(path);

        let (md5, sha256, total) =
            stream_digests(|buffer| read_handle(opened.file.raw(), buffer), cancel)?;
        if total != before.size || cancel.load(Ordering::SeqCst) {
            return Err(FileHashError::Changed);
        }
        opened.verify_unchanged()?;
        Ok(FileDigests {
            md5,
            sha256,
            size: total,
            mtime_sec: before.mtime_sec,
            mtime_nsec: before.mtime_nsec,
            ctime_sec: before.ctime_sec,
            ctime_nsec: before.ctime_nsec,
            file_dev: before.dev,
            file_ino: before.ino,
        })
    }

    pub(super) fn verify_identity(
        path: &Path,
        expected: &FileIdentity,
    ) -> Result<(), FileHashError> {
        let opened = OpenedFile::open(path)?;
        if opened.before != *expected {
            return Err(FileHashError::Changed);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn hash_with_hook_for_test(
        path: &Path,
        expected_size: Option<u64>,
        cancel: &Arc<AtomicBool>,
        hook: impl FnOnce(&Path),
    ) -> Result<FileDigests, FileHashError> {
        hash_with_hook(path, expected_size, cancel, hook)
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct ObjectIdentity {
        volume: u64,
        index: u64,
    }

    struct OpenedFile {
        file: Handle,
        drive: char,
        names: Vec<Vec<u16>>,
        root_identity: ObjectIdentity,
        dir_identities: Vec<ObjectIdentity>,
        before: FileIdentity,
        _ancestors: Vec<Handle>,
    }

    impl OpenedFile {
        fn open(path: &Path) -> Result<Self, FileHashError> {
            let (drive, names) = parse_local_path(path)?;
            let (root, root_info) = open_root(drive)?;
            let root_identity = object_identity(&root_info);
            let mut ancestors = vec![root];
            let mut dir_identities = Vec::new();
            for name in &names[..names.len() - 1] {
                let parent = ancestors.last().ok_or(FileHashError::InvalidPath)?.raw();
                let (handle, info) = open_component(parent, name, true)?;
                if info.dwVolumeSerialNumber as u64 != root_identity.volume {
                    return Err(FileHashError::NonLocal);
                }
                dir_identities.push(object_identity(&info));
                ancestors.push(handle);
            }
            let parent = ancestors.last().ok_or(FileHashError::InvalidPath)?.raw();
            let last = names.last().ok_or(FileHashError::InvalidPath)?;
            let (file, info) = open_component(parent, last, false)?;
            if info.dwVolumeSerialNumber as u64 != root_identity.volume {
                return Err(FileHashError::NonLocal);
            }
            let before = identity_from_info(file.raw(), &info)?;
            Ok(Self {
                file,
                drive,
                names,
                root_identity,
                dir_identities,
                before,
                _ancestors: ancestors,
            })
        }

        fn verify_unchanged(&self) -> Result<(), FileHashError> {
            let after_info = query_info(self.file.raw()).map_err(|_| FileHashError::Changed)?;
            if verify_object(self.file.raw(), &after_info, false).is_err() {
                return Err(FileHashError::Changed);
            }
            if identity_from_info(self.file.raw(), &after_info)? != self.before {
                return Err(FileHashError::Changed);
            }

            // Re-walk the exact path from a freshly opened drive root. Every
            // retained original handle remains open, but this second walk is
            // what proves that the path still resolves to the same root,
            // ancestors, and final file object.
            let (fresh_root, fresh_root_info) =
                open_root(self.drive).map_err(|_| FileHashError::Changed)?;
            if object_identity(&fresh_root_info) != self.root_identity {
                return Err(FileHashError::Changed);
            }
            let mut parent = fresh_root;
            for (index, name) in self.names[..self.names.len() - 1].iter().enumerate() {
                let (handle, info) =
                    open_component(parent.raw(), name, true).map_err(|_| FileHashError::Changed)?;
                if object_identity(&info) != self.dir_identities[index] {
                    return Err(FileHashError::Changed);
                }
                parent = handle;
            }
            let last = self.names.last().ok_or(FileHashError::Changed)?;
            let (final_handle, final_info) =
                open_component(parent.raw(), last, false).map_err(|_| FileHashError::Changed)?;
            if identity_from_info(final_handle.raw(), &final_info)? != self.before {
                return Err(FileHashError::Changed);
            }
            Ok(())
        }
    }

    struct Handle {
        raw: HANDLE,
    }

    impl Handle {
        fn raw(&self) -> HANDLE {
            self.raw
        }
    }

    impl Drop for Handle {
        fn drop(&mut self) {
            if !self.raw.is_null() && self.raw != INVALID_HANDLE_VALUE {
                // SAFETY: this handle was returned by CreateFileW or
                // NtCreateFile and is owned exclusively by this object.
                unsafe {
                    CloseHandle(self.raw);
                }
            }
        }
    }

    fn parse_local_path(path: &Path) -> Result<(char, Vec<Vec<u16>>), FileHashError> {
        let mut components = path.components();
        let drive = match components.next() {
            Some(Component::Prefix(prefix)) => match prefix.kind() {
                Prefix::Disk(letter) => letter,
                Prefix::UNC(_, _) | Prefix::VerbatimUNC(_, _) | Prefix::DeviceNS(_) => {
                    return Err(FileHashError::NonLocal)
                }
                _ => return Err(FileHashError::InvalidPath),
            },
            _ => return Err(FileHashError::InvalidPath),
        };
        if !drive.is_ascii_alphabetic() {
            return Err(FileHashError::InvalidPath);
        }
        let drive = char::from(drive).to_ascii_uppercase();
        if !matches!(components.next(), Some(Component::RootDir)) {
            return Err(FileHashError::InvalidPath);
        }
        let mut names = Vec::new();
        for component in components {
            match component {
                Component::Normal(part) => names.push(validate_component(part)?),
                _ => return Err(FileHashError::InvalidPath),
            }
        }
        if names.is_empty() {
            return Err(FileHashError::InvalidPath);
        }
        Ok((drive, names))
    }

    fn validate_component(part: &OsStr) -> Result<Vec<u16>, FileHashError> {
        let wide: Vec<u16> = part.encode_wide().collect();
        if wide.is_empty() || wide.len() > MAX_COMPONENT_UNITS {
            return Err(FileHashError::InvalidPath);
        }
        for &unit in &wide {
            if unit == 0 || unit < 0x20 || unit == 0x7f {
                return Err(FileHashError::InvalidPath);
            }
            // / \ : * ? " < > | are not valid in a normal Win32 file name.
            if matches!(
                unit,
                0x2f | 0x5c | 0x3a | 0x2a | 0x3f | 0x22 | 0x3c | 0x3e | 0x7c
            ) {
                return Err(FileHashError::InvalidPath);
            }
        }
        if matches!(wide.last().copied(), Some(0x2e | 0x20)) {
            return Err(FileHashError::InvalidPath);
        }

        let base_end = wide
            .iter()
            .position(|&unit| unit == 0x2e)
            .unwrap_or(wide.len());
        let mut base = &wide[..base_end];
        while matches!(base.last().copied(), Some(0x20)) {
            base = &base[..base.len() - 1];
        }
        if !base.is_empty() && base.iter().all(|&unit| unit <= 0x7f) {
            let mut ascii = String::with_capacity(base.len());
            for &unit in base {
                ascii.push((unit as u8).to_ascii_uppercase() as char);
            }
            let reserved = matches!(
                ascii.as_str(),
                "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$" | "CLOCK$"
            ) || matches!(
                ascii.as_str(),
                "COM1" | "COM2" | "COM3" | "COM4" | "COM5" | "COM6" | "COM7" | "COM8" | "COM9"
            ) || matches!(
                ascii.as_str(),
                "LPT1" | "LPT2" | "LPT3" | "LPT4" | "LPT5" | "LPT6" | "LPT7" | "LPT8" | "LPT9"
            );
            if reserved {
                return Err(FileHashError::InvalidPath);
            }
        }
        Ok(wide)
    }

    fn open_root(drive: char) -> Result<(Handle, BY_HANDLE_FILE_INFORMATION), FileHashError> {
        let root = [drive as u16, ':' as u16, '\\' as u16, 0];
        // SAFETY: `root` is a NUL-terminated UTF-16 drive-root string that
        // outlives the call and is not retained by the operating system.
        let drive_type = unsafe { GetDriveTypeW(root.as_ptr()) };
        if drive_type != DRIVE_FIXED {
            return Err(FileHashError::NonLocal);
        }
        // SAFETY: the path is NUL-terminated and the optional arguments are
        // null. The returned handle is checked before it is wrapped.
        let raw = unsafe {
            CreateFileW(
                root.as_ptr(),
                FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                null(),
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
                null_mut(),
            )
        };
        let handle = checked_handle(raw)?;
        let info = query_info(handle.raw())?;
        verify_object(handle.raw(), &info, true)?;
        Ok((handle, info))
    }

    fn open_component(
        parent: HANDLE,
        name: &[u16],
        directory: bool,
    ) -> Result<(Handle, BY_HANDLE_FILE_INFORMATION), FileHashError> {
        if name.is_empty() || name.len() > MAX_COMPONENT_UNITS {
            return Err(FileHashError::InvalidPath);
        }
        let length = name
            .len()
            .checked_mul(2)
            .and_then(|bytes| u16::try_from(bytes).ok())
            .ok_or(FileHashError::InvalidPath)?;
        let mut buffer = name.to_vec();
        let unicode = UNICODE_STRING {
            Length: length,
            MaximumLength: length,
            Buffer: buffer.as_mut_ptr(),
        };
        let attributes = OBJECT_ATTRIBUTES {
            Length: std::mem::size_of::<OBJECT_ATTRIBUTES>() as u32,
            RootDirectory: parent,
            ObjectName: &unicode,
            Attributes: OBJ_CASE_INSENSITIVE,
            SecurityDescriptor: null(),
            SecurityQualityOfService: null(),
        };
        let (access, options) = if directory {
            // Do not combine FILE_DIRECTORY_FILE with FILE_OPEN_REPARSE_POINT:
            // the documented compatible-option list does not cover that pair.
            // Open without a directory-only flag, then verify the attributes.
            (
                FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
                FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
            )
        } else {
            (
                FILE_READ_DATA | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
                FILE_NON_DIRECTORY_FILE
                    | FILE_OPEN_REPARSE_POINT
                    | FILE_SEQUENTIAL_ONLY
                    | FILE_SYNCHRONOUS_IO_NONALERT,
            )
        };
        let mut raw: HANDLE = INVALID_HANDLE_VALUE;
        let mut io_status = IO_STATUS_BLOCK::default();
        // SAFETY: all pointers refer to live stack values for the duration of
        // the call. The root handle is owned by the caller and remains valid.
        let status = unsafe {
            NtCreateFile(
                &mut raw,
                access,
                &attributes,
                &mut io_status,
                null(),
                FILE_ATTRIBUTE_NORMAL,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                FILE_OPEN,
                options,
                null(),
                0,
            )
        };
        if !nt_success(status) {
            return Err(map_status(status));
        }
        let handle = checked_handle(raw)?;
        let info = query_info(handle.raw())?;
        verify_object(handle.raw(), &info, directory)?;
        Ok((handle, info))
    }

    fn checked_handle(raw: HANDLE) -> Result<Handle, FileHashError> {
        if raw.is_null() || raw == INVALID_HANDLE_VALUE {
            return Err(FileHashError::Io);
        }
        Ok(Handle { raw })
    }

    fn read_handle(handle: HANDLE, buffer: &mut [u8]) -> std::io::Result<usize> {
        let requested = buffer.len().min(u32::MAX as usize) as u32;
        let mut read = 0u32;
        // SAFETY: the buffer is writable for `requested` bytes, the handle was
        // opened for synchronous FILE_READ_DATA, and the overlap pointer is
        // null because synchronous I/O does not use OVERLAPPED.
        let ok = unsafe {
            ReadFile(
                handle,
                buffer.as_mut_ptr(),
                requested,
                &mut read,
                null_mut(),
            )
        };
        if ok == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(read as usize)
    }

    fn query_info(handle: HANDLE) -> Result<BY_HANDLE_FILE_INFORMATION, FileHashError> {
        let mut info = BY_HANDLE_FILE_INFORMATION::default();
        // SAFETY: `info` is a valid writable structure for this query.
        if unsafe { GetFileInformationByHandle(handle, &mut info) } == 0 {
            return Err(FileHashError::Io);
        }
        Ok(info)
    }

    fn query_basic_info(handle: HANDLE) -> Result<FILE_BASIC_INFO, FileHashError> {
        let mut info = FILE_BASIC_INFO::default();
        // SAFETY: `info` is a valid writable structure for this query.
        let ok = unsafe {
            GetFileInformationByHandleEx(
                handle,
                FileBasicInfo,
                &mut info as *mut FILE_BASIC_INFO as *mut core::ffi::c_void,
                std::mem::size_of::<FILE_BASIC_INFO>() as u32,
            )
        };
        if ok == 0 {
            return Err(FileHashError::Io);
        }
        Ok(info)
    }

    fn query_attribute_tag(handle: HANDLE) -> Result<FILE_ATTRIBUTE_TAG_INFO, FileHashError> {
        let mut tag = FILE_ATTRIBUTE_TAG_INFO::default();
        // SAFETY: `tag` is writable for exactly its own size.
        let ok = unsafe {
            GetFileInformationByHandleEx(
                handle,
                FileAttributeTagInfo,
                &mut tag as *mut FILE_ATTRIBUTE_TAG_INFO as *mut core::ffi::c_void,
                std::mem::size_of::<FILE_ATTRIBUTE_TAG_INFO>() as u32,
            )
        };
        if ok == 0 {
            return Err(FileHashError::Io);
        }
        Ok(tag)
    }

    fn verify_object(
        handle: HANDLE,
        info: &BY_HANDLE_FILE_INFORMATION,
        expect_directory: bool,
    ) -> Result<(), FileHashError> {
        if info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(FileHashError::Symlink);
        }
        let tag = query_attribute_tag(handle)?;
        if tag.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 || tag.ReparseTag != 0 {
            return Err(FileHashError::Symlink);
        }
        if info.dwFileAttributes & FILE_ATTRIBUTE_DEVICE != 0 {
            return Err(FileHashError::NotRegular);
        }
        let nonlocal = FILE_ATTRIBUTE_OFFLINE
            | FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS
            | FILE_ATTRIBUTE_RECALL_ON_OPEN;
        if info.dwFileAttributes & nonlocal != 0 {
            return Err(FileHashError::NonLocal);
        }
        let is_directory = info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY != 0;
        if is_directory != expect_directory {
            return Err(FileHashError::NotRegular);
        }
        // SAFETY: the handle is a valid file-object handle.
        let file_type = unsafe { GetFileType(handle) };
        if file_type & FILE_TYPE_REMOTE != 0 {
            return Err(FileHashError::NonLocal);
        }
        if file_type != FILE_TYPE_DISK {
            return Err(FileHashError::NotRegular);
        }
        Ok(())
    }

    fn object_identity(info: &BY_HANDLE_FILE_INFORMATION) -> ObjectIdentity {
        ObjectIdentity {
            volume: info.dwVolumeSerialNumber as u64,
            index: ((info.nFileIndexHigh as u64) << 32) | info.nFileIndexLow as u64,
        }
    }

    fn identity_from_info(
        handle: HANDLE,
        info: &BY_HANDLE_FILE_INFORMATION,
    ) -> Result<FileIdentity, FileHashError> {
        let ticks = ((info.ftLastWriteTime.dwHighDateTime as u64) << 32)
            | info.ftLastWriteTime.dwLowDateTime as u64;
        let seconds = ticks / WINDOWS_TICKS_PER_SECOND;
        let nanos = (ticks % WINDOWS_TICKS_PER_SECOND) * 100;
        let identity = object_identity(info);
        // FILE_BASIC_INFO exposes the NTFS change time, which catches an
        // ordinary content rewrite even when the caller restores last-write
        // time. It is best-effort, not a cryptographic immutability proof.
        let basic = query_basic_info(handle)?;
        let change_ticks = basic.ChangeTime.max(0) as u64;
        let change_seconds = change_ticks / WINDOWS_TICKS_PER_SECOND;
        let change_nanos = (change_ticks % WINDOWS_TICKS_PER_SECOND) * 100;
        Ok(FileIdentity {
            dev: identity.volume,
            ino: identity.index,
            size: ((info.nFileSizeHigh as u64) << 32) | info.nFileSizeLow as u64,
            mtime_sec: seconds as i64 - WINDOWS_TO_UNIX_EPOCH_SECONDS,
            mtime_nsec: nanos as i64,
            ctime_sec: change_seconds as i64 - WINDOWS_TO_UNIX_EPOCH_SECONDS,
            ctime_nsec: change_nanos as i64,
        })
    }

    fn nt_success(status: NTSTATUS) -> bool {
        status >= 0
    }

    fn map_status(status: NTSTATUS) -> FileHashError {
        match status {
            STATUS_OBJECT_NAME_NOT_FOUND | STATUS_OBJECT_PATH_NOT_FOUND | STATUS_DELETE_PENDING => {
                FileHashError::Missing
            }
            STATUS_REPARSE
            | STATUS_REPARSE_OBJECT
            | STATUS_REPARSE_POINT_ENCOUNTERED
            | STATUS_REPARSE_POINT_NOT_RESOLVED
            | STATUS_IO_REPARSE_TAG_INVALID
            | STATUS_IO_REPARSE_TAG_MISMATCH
            | STATUS_IO_REPARSE_TAG_NOT_HANDLED => FileHashError::Symlink,
            STATUS_OBJECT_NAME_INVALID
            | STATUS_OBJECT_PATH_SYNTAX_BAD
            | STATUS_INVALID_PARAMETER => FileHashError::InvalidPath,
            STATUS_NOT_A_DIRECTORY | STATUS_FILE_IS_A_DIRECTORY => FileHashError::NotRegular,
            _ => FileHashError::Io,
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn parse_local_path_returns_uppercase_ascii_drive_char() {
            let (drive, names) =
                parse_local_path(Path::new(r"c:\Windows\win.ini")).expect("local drive path");
            assert_eq!(drive, 'C');
            assert_eq!(names.len(), 2);
            assert_eq!(
                parse_local_path(Path::new(r"Z:\temp\file.bin"))
                    .expect("local drive path")
                    .0,
                'Z'
            );
        }
    }
}

// ---------------------------------------------------------------------------
// MD5 (RFC 1321). Implemented locally so the only crypto dependency added is
// the already-vendored SHA-256 crate.
// ---------------------------------------------------------------------------

struct Md5 {
    state: [u32; 4],
    length: u64,
    buffer: [u8; 64],
    buffer_len: usize,
}

impl Md5 {
    fn new() -> Self {
        Self {
            state: [0x6745_2301, 0xefcd_ab89, 0x98ba_dcfe, 0x1032_5476],
            length: 0,
            buffer: [0u8; 64],
            buffer_len: 0,
        }
    }

    fn update(&mut self, mut input: &[u8]) {
        self.length = self.length.wrapping_add(input.len() as u64);
        if self.buffer_len > 0 {
            let take = (64 - self.buffer_len).min(input.len());
            self.buffer[self.buffer_len..self.buffer_len + take].copy_from_slice(&input[..take]);
            self.buffer_len += take;
            input = &input[take..];
            if self.buffer_len == 64 {
                let block = self.buffer;
                self.compress(&block);
                self.buffer_len = 0;
            }
        }
        while input.len() >= 64 {
            let mut block = [0u8; 64];
            block.copy_from_slice(&input[..64]);
            self.compress(&block);
            input = &input[64..];
        }
        if !input.is_empty() {
            self.buffer[..input.len()].copy_from_slice(input);
            self.buffer_len = input.len();
        }
    }

    fn finish_hex(mut self) -> String {
        let bit_len = self.length.wrapping_mul(8);
        self.update(&[0x80]);
        while self.buffer_len != 56 {
            self.update(&[0]);
        }
        self.update(&bit_len.to_le_bytes());
        debug_assert_eq!(self.buffer_len, 0);
        let mut out = String::with_capacity(32);
        for word in self.state {
            for byte in word.to_le_bytes() {
                out.push_str(&format!("{byte:02x}"));
            }
        }
        out
    }

    fn compress(&mut self, block: &[u8; 64]) {
        const S: [u32; 64] = [
            7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20,
            5, 9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
            6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
        ];
        const K: [u32; 64] = [
            0xd76a_a478,
            0xe8c7_b756,
            0x2420_70db,
            0xc1bd_ceee,
            0xf57c_0faf,
            0x4787_c62a,
            0xa830_4613,
            0xfd46_9501,
            0x6980_98d8,
            0x8b44_f7af,
            0xffff_5bb1,
            0x895c_d7be,
            0x6b90_1122,
            0xfd98_7193,
            0xa679_438e,
            0x49b4_0821,
            0xf61e_2562,
            0xc040_b340,
            0x265e_5a51,
            0xe9b6_c7aa,
            0xd62f_105d,
            0x0244_1453,
            0xd8a1_e681,
            0xe7d3_fbc8,
            0x21e1_cde6,
            0xc337_07d6,
            0xf4d5_0d87,
            0x455a_14ed,
            0xa9e3_e905,
            0xfcef_a3f8,
            0x676f_02d9,
            0x8d2a_4c8a,
            0xfffa_3942,
            0x8771_f681,
            0x6d9d_6122,
            0xfde5_380c,
            0xa4be_ea44,
            0x4bde_cfa9,
            0xf6bb_4b60,
            0xbebf_bc70,
            0x289b_7ec6,
            0xeaa1_27fa,
            0xd4ef_3085,
            0x0488_1d05,
            0xd9d4_d039,
            0xe6db_99e5,
            0x1fa2_7cf8,
            0xc4ac_5665,
            0xf429_2244,
            0x432a_ff97,
            0xab94_23a7,
            0xfc93_a039,
            0x655b_59c3,
            0x8f0c_cc92,
            0xffef_f47d,
            0x8584_5dd1,
            0x6fa8_7e4f,
            0xfe2c_e6e0,
            0xa301_4314,
            0x4e08_11a1,
            0xf753_7e82,
            0xbd3a_f235,
            0x2ad7_d2bb,
            0xeb86_d391,
        ];
        let mut m = [0u32; 16];
        for (index, chunk) in block.as_chunks::<4>().0.iter().enumerate() {
            m[index] = u32::from_le_bytes(*chunk);
        }
        let [mut a, mut b, mut c, mut d] = self.state;
        for i in 0..64 {
            let (f, g) = match i {
                0..=15 => ((b & c) | (!b & d), i),
                16..=31 => ((d & b) | (!d & c), (5 * i + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | !d), (7 * i) % 16),
            };
            let next = a.wrapping_add(f).wrapping_add(K[i]).wrapping_add(m[g]);
            a = d;
            d = c;
            c = b;
            b = b.wrapping_add(next.rotate_left(S[i]));
        }
        self.state[0] = self.state[0].wrapping_add(a);
        self.state[1] = self.state[1].wrapping_add(b);
        self.state[2] = self.state[2].wrapping_add(c);
        self.state[3] = self.state[3].wrapping_add(d);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "surge-file-hash-{tag}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn remove_all_best_effort(path: &std::path::Path) {
        if path.symlink_metadata().is_ok() {
            if path.is_dir() && !path.is_symlink() {
                let _ = std::fs::remove_dir_all(path);
            } else {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    fn cancel() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }

    #[cfg(windows)]
    fn windows_link_error_is_privilege(error: &std::io::Error) -> bool {
        matches!(error.raw_os_error(), Some(5) | Some(1314))
            || error.kind() == std::io::ErrorKind::PermissionDenied
    }

    #[cfg(windows)]
    fn windows_rename_denied_by_open_handle(error: &std::io::Error) -> bool {
        // ERROR_ACCESS_DENIED, ERROR_SHARING_VIOLATION, ERROR_LOCK_VIOLATION.
        matches!(error.raw_os_error(), Some(5) | Some(32) | Some(33))
            || error.kind() == std::io::ErrorKind::PermissionDenied
    }

    #[cfg(windows)]
    fn windows_symlink_or_skip(
        link: &std::path::Path,
        target: &std::path::Path,
        directory: bool,
    ) -> bool {
        let result = if directory {
            std::os::windows::fs::symlink_dir(target, link)
        } else {
            std::os::windows::fs::symlink_file(target, link)
        };
        match result {
            Ok(()) => true,
            Err(error) if windows_link_error_is_privilege(&error) => {
                eprintln!(
                    "skipping Windows {} symlink test: creation privilege unavailable ({error})",
                    if directory { "directory" } else { "file" }
                );
                false
            }
            Err(error) => panic!("create Windows test symlink failed: {error}"),
        }
    }

    #[cfg(windows)]
    fn windows_junction_or_skip(link: &std::path::Path, target: &std::path::Path) -> bool {
        use std::process::Command;
        let output = match Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(link)
            .arg(target)
            .output()
        {
            Ok(output) => output,
            Err(error) => {
                eprintln!("skipping Windows junction test: could not run mklink ({error})");
                return false;
            }
        };
        if output.status.success() {
            return true;
        }
        let detail = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let lower = detail.to_ascii_lowercase();
        if lower.contains("privilege")
            || lower.contains("access is denied")
            || lower.contains("administrator")
        {
            eprintln!(
                "skipping Windows junction test: mklink /J lacks creation privilege ({}): {}",
                output.status,
                detail.trim()
            );
            return false;
        }
        panic!(
            "create Windows test junction failed ({}): {}",
            output.status,
            detail.trim()
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn known_vectors_and_zero_file() {
        let root = temp_dir("vectors");
        let abc = root.join("abc.bin");
        std::fs::write(&abc, b"abc").expect("write abc");
        let digests = hash_file(&abc.to_string_lossy(), Some(3), &cancel()).expect("hash abc");
        assert_eq!(digests.md5, "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(
            digests.sha256,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );

        let empty = root.join("empty.bin");
        std::fs::write(&empty, b"").expect("write empty");
        let digests = hash_file(&empty.to_string_lossy(), Some(0), &cancel()).expect("hash empty");
        assert_eq!(digests.md5, "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(
            digests.sha256,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(digests.size, 0);
        remove_all_best_effort(&root);
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn multi_buffer_file_is_hashed_in_one_pass() {
        let root = temp_dir("multi-buffer");
        let file = root.join("pattern.bin");
        let data: Vec<u8> = (0u8..=255).cycle().take(262_144).collect();
        std::fs::write(&file, &data).expect("write pattern");
        let digests = hash_file(&file.to_string_lossy(), Some(262_144), &cancel())
            .expect("hash multi-buffer file");
        assert_eq!(digests.md5, "d19215b1d714757e1fdb0060c52fd4c8");
        assert_eq!(
            digests.sha256,
            "2312394bd99545d9de131c24efb781e765ac1aec243f2ed9347597a793a415e9"
        );
        assert_eq!(digests.size, 262_144);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn unsafe_paths_fail_closed() {
        let root = temp_dir("unsafe");
        let file = root.join("real.bin");
        std::fs::write(&file, b"data").expect("write file");
        let directory = root.join("dir.bin");
        std::fs::create_dir(&directory).expect("create dir");
        let missing = root.join("missing.bin");

        assert_eq!(
            hash_file("relative/file.bin", None, &cancel()),
            Err(FileHashError::InvalidPath)
        );
        assert_eq!(
            hash_file(&directory.to_string_lossy(), None, &cancel()),
            Err(FileHashError::NotRegular)
        );
        assert_eq!(
            hash_file(&missing.to_string_lossy(), None, &cancel()),
            Err(FileHashError::Missing)
        );

        #[cfg(unix)]
        {
            let file_link = root.join("file-link.bin");
            std::os::unix::fs::symlink(&file, &file_link).expect("file symlink");
            assert_eq!(
                hash_file(&file_link.to_string_lossy(), None, &cancel()),
                Err(FileHashError::Symlink)
            );
            let parent_link = root.join("parent-link");
            std::os::unix::fs::symlink(&root, &parent_link).expect("parent symlink");
            assert_eq!(
                hash_file(
                    &parent_link.join("real.bin").to_string_lossy(),
                    None,
                    &cancel()
                ),
                Err(FileHashError::Symlink)
            );
        }
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn detects_same_size_modification_and_replacement() {
        let root = temp_dir("race");
        let file = root.join("live.bin");
        let data = vec![b'a'; 1_048_576];
        std::fs::write(&file, &data).expect("write live file");

        let result = hash_file_with_hook(
            &file.to_string_lossy(),
            Some(data.len() as u64),
            &cancel(),
            |path| {
                let mut changed = std::fs::read(path).expect("read before modification");
                changed[1234] = b'b';
                std::fs::write(path, changed).expect("modify in place");
            },
        );
        assert_eq!(result, Err(FileHashError::Changed));

        std::fs::write(&file, &data).expect("reset file");
        let replacement = root.join("replacement.bin");
        std::fs::write(&replacement, vec![b'c'; data.len()]).expect("write replacement");
        let result = hash_file_with_hook(
            &file.to_string_lossy(),
            Some(data.len() as u64),
            &cancel(),
            |path| {
                std::fs::rename(&replacement, path).expect("replace path");
            },
        );
        assert_eq!(result, Err(FileHashError::Changed));
        remove_all_best_effort(&root);
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn detects_same_size_rewrite_with_restored_mtime() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt as _;

        let root = temp_dir("rewrite-ctime");
        let file = root.join("live.bin");
        std::fs::write(&file, b"abc").expect("write live file");
        let digests = hash_file(&file.to_string_lossy(), Some(3), &cancel()).expect("hash abc");
        let expected = FileIdentity {
            dev: digests.file_dev,
            ino: digests.file_ino,
            size: digests.size,
            mtime_sec: digests.mtime_sec,
            mtime_nsec: digests.mtime_nsec,
            ctime_sec: digests.ctime_sec,
            ctime_nsec: digests.ctime_nsec,
        };

        std::fs::write(&file, b"xyz").expect("rewrite same-size content");
        let path = CString::new(file.as_os_str().as_bytes()).expect("path");
        let times = [
            libc::timespec {
                tv_sec: 0,
                tv_nsec: libc::UTIME_OMIT,
            },
            libc::timespec {
                tv_sec: digests.mtime_sec as _,
                tv_nsec: digests.mtime_nsec as _,
            },
        ];
        let restored = unsafe { libc::utimensat(libc::AT_FDCWD, path.as_ptr(), times.as_ptr(), 0) };
        assert_eq!(restored, 0, "restore mtime for the regression");

        assert_eq!(
            verify_file_identity(&file.to_string_lossy(), &expected),
            Err(FileHashError::Changed),
            "a same-size rewrite with a restored mtime must still fail identity verification"
        );
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn detects_replaced_ancestor_even_when_final_inode_is_hard_linked() {
        let root = temp_dir("ancestor-race");
        let ancestor = root.join("ancestor");
        std::fs::create_dir(&ancestor).expect("create ancestor");
        let file = ancestor.join("live.bin");
        let data = vec![b'a'; 4096];
        std::fs::write(&file, &data).expect("write live file");
        let moved = root.join("ancestor-old");
        let replacement_dir = root.join("ancestor-new");

        let result = hash_file_with_hook(
            &file.to_string_lossy(),
            Some(data.len() as u64),
            &cancel(),
            |path| {
                std::fs::rename(&ancestor, &moved).expect("rename ancestor");
                std::fs::create_dir(&replacement_dir).expect("create replacement ancestor");
                std::fs::hard_link(moved.join("live.bin"), replacement_dir.join("live.bin"))
                    .expect("hard-link the same inode into the replacement path");
                std::fs::rename(&replacement_dir, &ancestor).expect("restore ancestor path");
                assert_eq!(
                    path,
                    ancestor.join("live.bin").as_path(),
                    "the hook must still target the original path"
                );
            },
        );
        assert_eq!(
            result,
            Err(FileHashError::Changed),
            "a replaced ancestor must fail even when the final inode is unchanged"
        );
        remove_all_best_effort(&root);
    }

    #[cfg(windows)]
    #[test]
    fn windows_known_vectors_empty_and_multibuffer() {
        let root = temp_dir("windows-vectors");
        let abc = root.join("abc.bin");
        std::fs::write(&abc, b"abc").expect("write abc");
        let digests =
            hash_file(&abc.to_string_lossy(), Some(3), &cancel()).expect("hash abc on Windows");
        assert_eq!(digests.md5, "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(
            digests.sha256,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );

        let empty = root.join("empty.bin");
        std::fs::write(&empty, b"").expect("write empty");
        let digests =
            hash_file(&empty.to_string_lossy(), Some(0), &cancel()).expect("hash empty on Windows");
        assert_eq!(digests.md5, "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(
            digests.sha256,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(digests.size, 0);

        let file = root.join("pattern.bin");
        let data: Vec<u8> = (0u8..=255).cycle().take(262_144).collect();
        std::fs::write(&file, &data).expect("write pattern");
        let digests = hash_file(&file.to_string_lossy(), Some(262_144), &cancel())
            .expect("hash multi-buffer file on Windows");
        assert_eq!(digests.md5, "d19215b1d714757e1fdb0060c52fd4c8");
        assert_eq!(
            digests.sha256,
            "2312394bd99545d9de131c24efb781e765ac1aec243f2ed9347597a793a415e9"
        );
        assert_eq!(digests.size, 262_144);
        remove_all_best_effort(&root);
    }

    #[cfg(windows)]
    #[test]
    fn windows_regular_nested_path_identity_and_descriptor_stability() {
        let root = temp_dir("windows-regular");
        let nested = root.join("one").join("two");
        std::fs::create_dir_all(&nested).expect("create nested dirs");
        let file = nested.join("abc.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let digests = hash_file(&file.to_string_lossy(), Some(3), &cancel())
            .expect("hash regular nested file on Windows");
        assert_eq!(digests.size, 3);
        assert_eq!(digests.md5, "900150983cd24fb0d6963f7d28e17f72");

        let expected = FileIdentity {
            dev: digests.file_dev,
            ino: digests.file_ino,
            size: digests.size,
            mtime_sec: digests.mtime_sec,
            mtime_nsec: digests.mtime_nsec,
            ctime_sec: digests.ctime_sec,
            ctime_nsec: digests.ctime_nsec,
        };
        verify_file_identity(&file.to_string_lossy(), &expected)
            .expect("same open file identity must verify");

        let replacement = root.join("replacement.bin");
        std::fs::write(&replacement, b"abc").expect("write replacement");
        std::fs::remove_file(&file).expect("remove original file");
        std::fs::rename(&replacement, &file).expect("replace file path");
        assert_eq!(
            verify_file_identity(&file.to_string_lossy(), &expected),
            Err(FileHashError::Changed),
            "a new file object at the same path must not inherit the old identity"
        );
        remove_all_best_effort(&root);
    }

    #[cfg(windows)]
    #[test]
    fn windows_rejects_nonlocal_and_dangerous_namespaces() {
        assert_eq!(
            hash_file(r"\\server\share\alpha.bin", None, &cancel()),
            Err(FileHashError::NonLocal)
        );
        assert_eq!(
            hash_file(r"\\?\UNC\server\share\alpha.bin", None, &cancel()),
            Err(FileHashError::NonLocal)
        );
        assert!(matches!(
            hash_file(r"\\.\C:\Windows\win.ini", None, &cancel()),
            Err(FileHashError::NonLocal) | Err(FileHashError::InvalidPath)
        ));
        assert_eq!(
            hash_file(r"\\?\C:\Windows\win.ini", None, &cancel()),
            Err(FileHashError::InvalidPath)
        );
        assert_eq!(
            hash_file(r"C:relative.bin", None, &cancel()),
            Err(FileHashError::InvalidPath)
        );

        let root = temp_dir("windows-names");
        assert_eq!(
            hash_file(&root.join("CON").to_string_lossy(), None, &cancel()),
            Err(FileHashError::InvalidPath)
        );
        assert_eq!(
            hash_file(
                &root.join("file.bin:stream").to_string_lossy(),
                None,
                &cancel()
            ),
            Err(FileHashError::InvalidPath)
        );
        assert_eq!(
            hash_file(&root.join("trailing.").to_string_lossy(), None, &cancel()),
            Err(FileHashError::InvalidPath)
        );
        remove_all_best_effort(&root);
    }

    #[cfg(windows)]
    #[test]
    fn windows_rejects_symlinks_junctions_and_reparse_ancestors() {
        let root = temp_dir("windows-links");
        let target_dir = root.join("target");
        std::fs::create_dir(&target_dir).expect("create target dir");
        let target_file = target_dir.join("payload.bin");
        std::fs::write(&target_file, b"abc").expect("write payload");

        let file_link = root.join("file-link.bin");
        if windows_symlink_or_skip(&file_link, &target_file, false) {
            assert_eq!(
                hash_file(&file_link.to_string_lossy(), None, &cancel()),
                Err(FileHashError::Symlink)
            );
            let _ = std::fs::remove_file(&file_link);
        }

        let dir_link = root.join("dir-link");
        if windows_symlink_or_skip(&dir_link, &target_dir, true) {
            assert_eq!(
                hash_file(
                    &dir_link.join("payload.bin").to_string_lossy(),
                    None,
                    &cancel()
                ),
                Err(FileHashError::Symlink)
            );
            let _ = std::fs::remove_dir(&dir_link);
        }

        let junction = root.join("junction");
        if windows_junction_or_skip(&junction, &target_dir) {
            assert_eq!(
                hash_file(
                    &junction.join("payload.bin").to_string_lossy(),
                    None,
                    &cancel()
                ),
                Err(FileHashError::Symlink)
            );
            let _ = std::fs::remove_dir(&junction);
        }
        remove_all_best_effort(&root);
    }

    #[cfg(windows)]
    #[test]
    fn windows_detects_replaced_ancestor_with_hard_linked_final_file() {
        let root = temp_dir("windows-ancestor-race");
        let ancestor = root.join("ancestor");
        std::fs::create_dir(&ancestor).expect("create ancestor");
        let file = ancestor.join("live.bin");
        let data = vec![b'a'; 4096];
        std::fs::write(&file, &data).expect("write live file");
        let moved = root.join("ancestor-old");

        let mut replaced_during_hash = false;
        let result = hash_file_with_hook(
            &file.to_string_lossy(),
            Some(data.len() as u64),
            &cancel(),
            |observed| {
                assert_eq!(observed, file.as_path());
                match std::fs::rename(&ancestor, &moved) {
                    Ok(()) => {
                        std::fs::create_dir(&ancestor).expect("create replacement ancestor");
                        std::fs::hard_link(moved.join("live.bin"), ancestor.join("live.bin"))
                            .expect("hard-link the same file into the replacement ancestor");
                        replaced_during_hash = true;
                    }
                    Err(error) if windows_rename_denied_by_open_handle(&error) => {
                        eprintln!(
                            "rename of the open Windows ancestor was denied ({error}); \
                             verifying the open handles are what pin it"
                        );
                    }
                    Err(error) => panic!(
                        "rename ancestor failed unexpectedly while hash handles were open: \
                         {error} (os error {:?})",
                        error.raw_os_error()
                    ),
                }
            },
        );

        if replaced_during_hash {
            assert_eq!(
                result,
                Err(FileHashError::Changed),
                "a replaced Windows ancestor must fail even when the final file object is unchanged"
            );
        } else {
            let digests = result
                .expect("hash must succeed when Windows denied renaming the still-open ancestor");
            assert_eq!(digests.size, data.len() as u64);
            std::fs::rename(&ancestor, &moved).expect(
                "the ancestor must be renameable after all hash handles close; otherwise the \
                 earlier denial was not caused by those handles",
            );
        }
        remove_all_best_effort(&root);
    }

    #[cfg(windows)]
    #[test]
    fn windows_detects_mutation_and_replacement_during_hash() {
        let root = temp_dir("windows-race");
        let file = root.join("live.bin");
        let data = vec![b'a'; 1_048_576];
        std::fs::write(&file, &data).expect("write live file");

        let result = hash_file_with_hook(
            &file.to_string_lossy(),
            Some(data.len() as u64),
            &cancel(),
            |path| {
                let mut changed = std::fs::read(path).expect("read before modification");
                changed[1234] = b'b';
                std::fs::write(path, changed).expect("modify in place");
            },
        );
        assert_eq!(result, Err(FileHashError::Changed));

        std::fs::write(&file, &data).expect("reset file");
        let replacement = root.join("replacement.bin");
        std::fs::write(&replacement, vec![b'c'; data.len()]).expect("write replacement");
        let result = hash_file_with_hook(
            &file.to_string_lossy(),
            Some(data.len() as u64),
            &cancel(),
            |path| {
                std::fs::rename(&replacement, path).expect("replace path");
            },
        );
        assert_eq!(result, Err(FileHashError::Changed));
        remove_all_best_effort(&root);
    }

    #[cfg(windows)]
    #[test]
    fn windows_cancellation_fails_closed_without_publishing() {
        let root = temp_dir("windows-cancel");
        let file = root.join("cancel.bin");
        std::fs::write(&file, b"data").expect("write fixture");
        let cancelling = Arc::new(AtomicBool::new(true));
        assert_eq!(
            hash_file(&file.to_string_lossy(), Some(4), &cancelling),
            Err(FileHashError::Changed)
        );
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cancellation_fails_closed_without_publishing() {
        let root = temp_dir("cancel");
        let file = root.join("cancel.bin");
        std::fs::write(&file, b"data").expect("write fixture");
        let cancelling = Arc::new(AtomicBool::new(true));
        assert_eq!(
            hash_file(&file.to_string_lossy(), Some(4), &cancelling),
            Err(FileHashError::Changed)
        );
        remove_all_best_effort(&root);
    }
}
