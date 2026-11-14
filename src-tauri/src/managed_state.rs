//! Persistent app-private state directory preparation (Surge queue/settings).
//!
//! This module only prepares a persistent, app-private directory. It is
//! separate from ephemeral TLS secrets (see `managed_credentials.rs`, which
//! stages in-memory PEMs into a unique child that is deleted on drop). This
//! module never deletes existing data: neither `Drop` nor any failure path
//! removes the prepared directory or files inside it.
//!
//! # Intended caller
//!
//! Callers pass an absolute app-owned base path obtained from a trusted Tauri
//! resource (e.g. an app-data directory resolved by Tauri). That string is
//! treated as the directory to prepare: if missing it is created, if present
//! it is reused after verification. Relative paths are always rejected.
//!
//! # Security properties (Linux)
//!
//! * The final component is never followed if it is a symlink: the directory
//!   is opened with `O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC` and any symlink at
//!   the final component fails closed (`ELOOP`) without traversal.
//! * An existing directory is reused only if it is a real directory owned by
//!   the current effective UID with exact mode `0700`. Anything else fails
//!   closed and is left untouched.
//! * An existing directory is never `chmod`-ed. Changing permissions on an
//!   arbitrary pre-existing path would be dangerous (it might be a user
//!   home, a shared ancestor, or attacker-planted). Only a directory this
//!   call just created via `mkdir(0700)` is forced to `0700` with `fchmod(2)`
//!   through its held file descriptor (to neutralize umask), then re-verified.
//! * Creation races are handled: `mkdir` failure with `EEXIST` retries the
//!   open-and-verify path, so two processes preparing the same missing path
//!   converge on reuse rather than erroring or replacing. The retry loop is
//!   bounded.
//! * Ancestor limitations: only the final directory itself is held open and
//!   verified. Ancestor components of `base` are resolved once by the OS
//!   during `open(2)`/`mkdir(2)` and are neither verified nor pinned. A
//!   same-user actor with write permission on an ancestor (or on the
//!   directory itself) can rename or replace paths around the daemon. This
//!   module does not defend against a malicious same-user racing path reads;
//!   the returned [`ManagedStateDir`] only carries the path string for
//!   accessors, and callers must not assume ancestors are stable.
//! * Missing parents fail closed: this module creates at most the final
//!   component (`mkdir`, not `mkdir -p`). Recursive creation would traverse
//!   unverified ancestors and symlinks; callers must ensure the parent of
//!   `base` already exists.
//! * No secrets, environment variables, authentication files, or system
//!   configuration are read or modified. No data is written outside the
//!   `base` path supplied by the caller.
//!
//! # Security properties (Windows)
//!
//! * Paths must be absolute and contain no `.` / `..` components. Device
//!   namespace prefixes are rejected; disk and UNC paths are accepted. A NUL
//!   in the path is rejected before conversion to UTF-16.
//! * The final component is opened with `FILE_FLAG_OPEN_REPARSE_POINT` and
//!   rejected if it carries `FILE_ATTRIBUTE_REPARSE_POINT`; a symlink,
//!   junction, or mount point is therefore never traversed.
//! * A new directory is created with a security descriptor containing a
//!   protected DACL and inheritable ACEs (`OI | CI`) for the current user,
//!   LocalSystem, and Builtin Administrators. Full control is granted only
//!   to those three SIDs. The owner is the current user SID read from the
//!   process token (`OpenProcessToken` + `TokenUser`). After creation the
//!   same owner/DACL/protection is re-applied and then re-read through the
//!   held directory handle; any mismatch fails closed.
//! * An existing directory is never modified or deleted. It is reused only
//!   if its owner is the current user, its DACL is present/non-NULL,
//!   `SE_DACL_PROTECTED` is set, it is not DACL-defaulted, and every ACE is
//!   an `ACCESS_ALLOWED_ACE_TYPE` with full control + `OI | CI` for exactly
//!   the current user, LocalSystem, or Builtin Administrators. A NULL DACL,
//!   an unknown/broad SID (e.g. Everyone, Users, Authenticated Users), a
//!   deny/audit/object ACE, or an inherited ACE fails closed. At least the
//!   current user's allow ACE must be present.
//! * A bounded `CreateDirectory` / open loop absorbs a concurrent creator or
//!   a disappearing path. Handles are closed with RAII and allocated
//!   security descriptors are freed with `LocalFree`.
//! * As on Linux, only the final directory is pinned and verified. Ancestor
//!   components are resolved by the OS; callers must supply a trusted
//!   absolute app-data base path and must not assume ancestors are stable.
//! * No secrets, environment variables, authentication files, or system
//!   configuration are read or modified. No data is written outside the
//!   caller-supplied `base` path.
//!
//! Windows tests cover ACL inspection, NULL/broad DACL rejection, reparse
//! rejection, and persistence. They compile for Windows but are not executed
//! here (no native Windows runtime), so runtime behavior is unverified in
//! this environment.
//!
//! # Errors
//!
//! `Display`/`Debug` never include paths, file contents, environment
//! contents, or OS error strings.

use std::fmt;
use std::path::{Path, PathBuf};

#[cfg(target_os = "linux")]
use std::fs::{File, OpenOptions};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;
#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt;
#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;

#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
#[cfg(windows)]
use std::path::Prefix;
#[cfg(windows)]
use windows_sys::Win32::Foundation::{
    CloseHandle, LocalFree, ERROR_ACCESS_DENIED, ERROR_ALREADY_EXISTS, ERROR_BAD_PATHNAME,
    ERROR_BUFFER_OVERFLOW, ERROR_DIRECTORY, ERROR_FILENAME_EXCED_RANGE, ERROR_FILE_EXISTS,
    ERROR_FILE_NOT_FOUND, ERROR_INVALID_NAME, ERROR_INVALID_PARAMETER, ERROR_PATH_NOT_FOUND,
    ERROR_PRIVILEGE_NOT_HELD, ERROR_REPARSE, ERROR_REPARSE_OBJECT, ERROR_REPARSE_POINT_ENCOUNTERED,
    ERROR_REPARSE_TAG_INVALID, ERROR_REPARSE_TAG_MISMATCH, ERROR_SHARING_VIOLATION,
    ERROR_TOO_MANY_LINKS, HANDLE, HLOCAL, INVALID_HANDLE_VALUE,
};
#[cfg(windows)]
use windows_sys::Win32::Security::Authorization::{GetSecurityInfo, SE_FILE_OBJECT};
#[cfg(windows)]
use windows_sys::Win32::Security::{
    AclSizeInformation, AddAccessAllowedAceEx, CreateWellKnownSid, EqualSid, GetAce,
    GetAclInformation, GetLengthSid, GetSecurityDescriptorControl, GetSecurityDescriptorDacl,
    GetTokenInformation, InitializeAcl, InitializeSecurityDescriptor, IsValidSid,
    SetSecurityDescriptorControl, SetSecurityDescriptorDacl, SetSecurityDescriptorOwner, TokenUser,
    WinBuiltinAdministratorsSid, WinLocalSystemSid, ACCESS_ALLOWED_ACE, ACE_HEADER, ACL,
    ACL_REVISION, ACL_SIZE_INFORMATION, CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION,
    INHERITED_ACE, OBJECT_INHERIT_ACE, OWNER_SECURITY_INFORMATION,
    PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SECURITY_ATTRIBUTES,
    SECURITY_DESCRIPTOR, SECURITY_MAX_SID_SIZE, SE_DACL_DEFAULTED, SE_DACL_PRESENT,
    SE_DACL_PROTECTED, TOKEN_QUERY, WELL_KNOWN_SID_TYPE,
};
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    CreateDirectoryW, CreateFileW, GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
    FILE_ALL_ACCESS, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT,
    FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_READ_ATTRIBUTES,
    FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING, READ_CONTROL, WRITE_DAC, WRITE_OWNER,
};
#[cfg(windows)]
use windows_sys::Win32::System::Memory::{LocalAlloc, LPTR};
#[cfg(windows)]
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

/// Prepared persistent app-private state directory.
///
/// Holds only the verified path. Unlike ephemeral credential staging, this
/// object never deletes anything: `Drop` is a no-op and there is no `cleanup`
/// method. Test teardown removes sandbox directories explicitly; the library
/// itself preserves user data.
pub struct ManagedStateDir {
    path: PathBuf,
}

/// Sanitized error for state-directory preparation.
pub struct ManagedStateError {
    kind: ErrorKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
enum ErrorKind {
    UnsupportedPlatform,
    InvalidInput,
    Insecure,
    Symlink,
    Storage,
}

impl ManagedStateDir {
    /// Prepare the persistent directory at `base`.
    ///
    /// `base` must be absolute. If missing, the final component is created
    /// with a protected current-user/SYSTEM/Administrators DACL (`0700` on
    /// Linux); if present, it is reused only after verifying its owner,
    /// DACL/mode, and that it is not a reparse point/symlink. Never deletes
    /// data. On other platforms this always fails closed without filesystem
    /// I/O.
    pub fn prepare(base: &Path) -> Result<Self, ManagedStateError> {
        #[cfg(target_os = "linux")]
        {
            let path = prepare_linux(base)?;
            Ok(Self { path })
        }
        #[cfg(target_os = "windows")]
        {
            let path = prepare_windows(base)?;
            Ok(Self { path })
        }
        #[cfg(not(any(target_os = "linux", target_os = "windows")))]
        {
            let _ = base;
            Err(ManagedStateError {
                kind: ErrorKind::UnsupportedPlatform,
            })
        }
    }

    /// Verified path of the persistent directory.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(target_os = "linux")]
fn prepare_linux(base: &Path) -> Result<PathBuf, ManagedStateError> {
    if !base.is_absolute() {
        return Err(ManagedStateError {
            kind: ErrorKind::InvalidInput,
        });
    }
    // Reject interior NUL early so `CString` conversion below cannot panic
    // and so error mapping stays deterministic.
    if base.as_os_str().as_bytes().contains(&0) {
        return Err(ManagedStateError {
            kind: ErrorKind::InvalidInput,
        });
    }

    // Bounded open-or-create loop to absorb an EEXIST race between the
    // failing open and our mkdir (concurrent creator).
    for _ in 0..4 {
        match open_dir_nofollow(base) {
            Ok(file) => {
                verify_existing(&file)?;
                return Ok(base.to_path_buf());
            }
            Err(err) => {
                let code = err.raw_os_error().unwrap_or(0);
                if code == libc::ENOENT {
                    match mkdir_0700(base) {
                        Ok(created) => {
                            if created {
                                // We created it: open (no-follow), force 0700
                                // via fd to neutralize umask, verify.
                                let file = open_dir_nofollow(base).map_err(map_open_error)?;
                                // SAFETY: fchmod on a valid held fd.
                                let rc = unsafe { libc::fchmod(file.as_raw_fd(), 0o700) };
                                if rc != 0 {
                                    return Err(ManagedStateError {
                                        kind: ErrorKind::Storage,
                                    });
                                }
                                verify_existing(&file)?;
                                return Ok(base.to_path_buf());
                            }
                            // mkdir reported already-exists: retry open.
                            continue;
                        }
                        Err(kind) => return Err(ManagedStateError { kind }),
                    }
                } else {
                    return Err(ManagedStateError {
                        kind: map_errno_to_kind(code),
                    });
                }
            }
        }
    }
    Err(ManagedStateError {
        kind: ErrorKind::Storage,
    })
}

#[cfg(target_os = "linux")]
fn map_errno_to_kind(code: i32) -> ErrorKind {
    if code == libc::ELOOP {
        ErrorKind::Symlink
    } else if code == libc::ENOTDIR {
        ErrorKind::InvalidInput
    } else if code == libc::EACCES || code == libc::EPERM {
        // Exists but not accessible (e.g. owned by another user with 0700):
        // fail closed as insecure rather than leaking access details.
        ErrorKind::Insecure
    } else {
        ErrorKind::Storage
    }
}

#[cfg(target_os = "linux")]
fn map_open_error(e: std::io::Error) -> ManagedStateError {
    ManagedStateError {
        kind: map_errno_to_kind(e.raw_os_error().unwrap_or(0)),
    }
}

#[cfg(target_os = "linux")]
fn open_dir_nofollow(path: &Path) -> Result<File, std::io::Error> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_CLOEXEC)
        .open(path)
}

#[cfg(target_os = "linux")]
fn verify_existing(file: &File) -> Result<(), ManagedStateError> {
    let meta = file.metadata().map_err(|_| ManagedStateError {
        kind: ErrorKind::Insecure,
    })?;
    if !meta.is_dir() {
        return Err(ManagedStateError {
            kind: ErrorKind::InvalidInput,
        });
    }
    // SAFETY: geteuid always succeeds.
    let euid = unsafe { libc::geteuid() };
    if meta.uid() != euid {
        return Err(ManagedStateError {
            kind: ErrorKind::Insecure,
        });
    }
    if meta.mode() & 0o777 != 0o700 {
        return Err(ManagedStateError {
            kind: ErrorKind::Insecure,
        });
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn mkdir_0700(path: &Path) -> Result<bool, ErrorKind> {
    use std::ffi::CString;
    let bytes = path.as_os_str().as_bytes();
    let c = CString::new(bytes).map_err(|_| ErrorKind::InvalidInput)?;
    // SAFETY: mkdir with a valid NUL-terminated path.
    let rc = unsafe { libc::mkdir(c.as_ptr(), 0o700) };
    if rc == 0 {
        return Ok(true);
    }
    let err = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
    if err == libc::EEXIST {
        return Ok(false);
    }
    if err == libc::ELOOP {
        return Err(ErrorKind::Symlink);
    }
    if err == libc::ENOTDIR {
        return Err(ErrorKind::InvalidInput);
    }
    if err == libc::ENOENT {
        // Parent missing: fail closed, do not create parents recursively.
        return Err(ErrorKind::Storage);
    }
    if err == libc::EACCES || err == libc::EPERM || err == libc::EROFS {
        return Err(ErrorKind::Storage);
    }
    Err(ErrorKind::Storage)
}

#[cfg(windows)]
const SECURITY_DESCRIPTOR_REVISION: u32 = 1;
#[cfg(windows)]
const INHERIT_FLAGS: u32 = OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE;
#[cfg(windows)]
const INHERIT_FLAGS_U8: u8 = (OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE) as u8;
#[cfg(windows)]
const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;

#[cfg(windows)]
#[repr(C, align(8))]
struct SidBuf {
    bytes: [u8; SECURITY_MAX_SID_SIZE as usize],
    len: u32,
}

#[cfg(windows)]
impl SidBuf {
    fn zeroed() -> Self {
        Self {
            bytes: [0u8; SECURITY_MAX_SID_SIZE as usize],
            len: 0,
        }
    }

    fn copy_from(sid: PSID) -> Result<Self, ManagedStateError> {
        if sid.is_null() {
            return Err(insecure());
        }
        // SAFETY: `sid` is non-NULL and came from a Win32 security API.
        if unsafe { IsValidSid(sid) } == 0 {
            return Err(insecure());
        }
        // SAFETY: valid SID.
        let len = unsafe { GetLengthSid(sid) };
        if len == 0 || len > SECURITY_MAX_SID_SIZE {
            return Err(insecure());
        }
        let mut out = Self::zeroed();
        // SAFETY: source is a valid SID of `len` bytes; destination is the
        // fixed-size aligned buffer.
        unsafe {
            std::ptr::copy_nonoverlapping(sid as *const u8, out.bytes.as_mut_ptr(), len as usize);
        }
        out.len = len;
        Ok(out)
    }

    fn well_known(kind: WELL_KNOWN_SID_TYPE) -> Result<Self, ManagedStateError> {
        let mut out = Self::zeroed();
        let mut len = SECURITY_MAX_SID_SIZE;
        // SAFETY: destination buffer is at least SECURITY_MAX_SID_SIZE.
        let ok = unsafe {
            CreateWellKnownSid(
                kind,
                std::ptr::null_mut(),
                out.bytes.as_mut_ptr() as PSID,
                &mut len,
            )
        };
        if ok == 0 || len == 0 || len > SECURITY_MAX_SID_SIZE {
            return Err(storage());
        }
        out.len = len;
        Ok(out)
    }

    fn as_psid(&self) -> PSID {
        self.bytes.as_ptr() as PSID
    }

    fn equals(&self, other: PSID) -> bool {
        if other.is_null() {
            return false;
        }
        // SAFETY: both SIDs are valid by construction / Win32 API contract.
        unsafe { EqualSid(self.as_psid(), other) != 0 }
    }
}

#[cfg(windows)]
#[repr(C, align(8))]
struct AclStorage {
    bytes: [u8; 512],
}

#[cfg(windows)]
impl AclStorage {
    fn new() -> Self {
        Self { bytes: [0u8; 512] }
    }

    fn as_mut(&mut self) -> *mut ACL {
        self.bytes.as_mut_ptr() as *mut ACL
    }

    fn as_ref(&self) -> *const ACL {
        self.bytes.as_ptr() as *const ACL
    }

    fn len(&self) -> u32 {
        self.bytes.len() as u32
    }
}

#[cfg(windows)]
struct Handle(HANDLE);

#[cfg(windows)]
impl Drop for Handle {
    fn drop(&mut self) {
        if self.0 != INVALID_HANDLE_VALUE && !self.0.is_null() {
            // SAFETY: handle is owned by this guard.
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
}

#[cfg(windows)]
struct LocalPtr(HLOCAL);

#[cfg(windows)]
impl Drop for LocalPtr {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: allocation is owned by this guard.
            unsafe {
                LocalFree(self.0);
            }
        }
    }
}

#[cfg(windows)]
fn insecure() -> ManagedStateError {
    ManagedStateError {
        kind: ErrorKind::Insecure,
    }
}

#[cfg(windows)]
fn storage() -> ManagedStateError {
    ManagedStateError {
        kind: ErrorKind::Storage,
    }
}

#[cfg(windows)]
fn last_win_error() -> u32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0) as u32
}

#[cfg(windows)]
fn map_win_error(code: u32) -> ErrorKind {
    match code {
        ERROR_ACCESS_DENIED | ERROR_PRIVILEGE_NOT_HELD | ERROR_SHARING_VIOLATION => {
            ErrorKind::Insecure
        }
        ERROR_FILE_NOT_FOUND | ERROR_PATH_NOT_FOUND => ErrorKind::Storage,
        ERROR_INVALID_NAME
        | ERROR_BAD_PATHNAME
        | ERROR_INVALID_PARAMETER
        | ERROR_DIRECTORY
        | ERROR_FILENAME_EXCED_RANGE
        | ERROR_BUFFER_OVERFLOW => ErrorKind::InvalidInput,
        ERROR_REPARSE
        | ERROR_REPARSE_OBJECT
        | ERROR_REPARSE_POINT_ENCOUNTERED
        | ERROR_REPARSE_TAG_INVALID
        | ERROR_REPARSE_TAG_MISMATCH
        | ERROR_TOO_MANY_LINKS => ErrorKind::Symlink,
        _ => ErrorKind::Storage,
    }
}

#[cfg(windows)]
fn validate_windows_path(path: &Path) -> Result<(), ManagedStateError> {
    if !path.is_absolute() {
        return Err(ManagedStateError {
            kind: ErrorKind::InvalidInput,
        });
    }
    if path.as_os_str().encode_wide().any(|c| c == 0) {
        return Err(ManagedStateError {
            kind: ErrorKind::InvalidInput,
        });
    }
    let mut have_prefix = false;
    let mut have_root = false;
    for component in path.components() {
        match component {
            std::path::Component::Prefix(prefix) => {
                have_prefix = true;
                match prefix.kind() {
                    Prefix::Disk(_)
                    | Prefix::UNC(_, _)
                    | Prefix::VerbatimDisk(_)
                    | Prefix::VerbatimUNC(_, _) => {}
                    _ => {
                        return Err(ManagedStateError {
                            kind: ErrorKind::InvalidInput,
                        })
                    }
                }
            }
            std::path::Component::RootDir => have_root = true,
            std::path::Component::CurDir | std::path::Component::ParentDir => {
                return Err(ManagedStateError {
                    kind: ErrorKind::InvalidInput,
                })
            }
            std::path::Component::Normal(_) => {}
        }
    }
    if !have_prefix || !have_root {
        return Err(ManagedStateError {
            kind: ErrorKind::InvalidInput,
        });
    }
    Ok(())
}

#[cfg(windows)]
fn wide_path(path: &Path) -> Result<Vec<u16>, ManagedStateError> {
    let mut wide: Vec<u16> = path.as_os_str().encode_wide().collect();
    if wide.contains(&0) {
        return Err(ManagedStateError {
            kind: ErrorKind::InvalidInput,
        });
    }
    wide.push(0);
    Ok(wide)
}

#[cfg(windows)]
fn current_user_sid() -> Result<SidBuf, ManagedStateError> {
    let mut token: HANDLE = std::ptr::null_mut();
    // SAFETY: GetCurrentProcess is pseudo-handle; token is written on success.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(insecure());
    }
    let token = Handle(token);
    let mut needed = 0u32;
    // SAFETY: first call sizes the buffer; expected insufficient-buffer error.
    unsafe {
        GetTokenInformation(token.0, TokenUser, std::ptr::null_mut(), 0, &mut needed);
    }
    if needed == 0 {
        return Err(insecure());
    }
    // SAFETY: LMEM fixed allocation for the exact required size.
    let raw = unsafe { LocalAlloc(LPTR, needed as usize) };
    if raw.is_null() {
        return Err(storage());
    }
    let raw = LocalPtr(raw);
    // SAFETY: buffer is at least `needed` bytes.
    if unsafe { GetTokenInformation(token.0, TokenUser, raw.0, needed, &mut needed) } == 0 {
        return Err(insecure());
    }
    // SAFETY: buffer holds TOKEN_USER as returned by the API.
    let token_user = unsafe { &*(raw.0 as *const windows_sys::Win32::Security::TOKEN_USER) };
    SidBuf::copy_from(token_user.User.Sid)
}

#[cfg(windows)]
fn add_allow_ace(acl: *mut ACL, sid: PSID) -> bool {
    if acl.is_null() || sid.is_null() {
        return false;
    }
    // SAFETY: ACL was initialized in the aligned storage; SID is valid.
    unsafe { AddAccessAllowedAceEx(acl, ACL_REVISION, INHERIT_FLAGS, FILE_ALL_ACCESS, sid) != 0 }
}

#[cfg(windows)]
fn open_windows_dir_raw(wide: &[u16], write_security: bool) -> Result<Handle, u32> {
    let mut access = READ_CONTROL | FILE_READ_ATTRIBUTES;
    if write_security {
        access |= WRITE_DAC | WRITE_OWNER;
    }
    // SAFETY: NUL-terminated UTF-16 path; no security attributes and no
    // template handle. OPEN_REPARSE_POINT prevents following the final link.
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            access,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE || handle.is_null() {
        return Err(last_win_error());
    }
    Ok(Handle(handle))
}

#[cfg(windows)]
fn verify_directory_handle(handle: &Handle) -> Result<(), ManagedStateError> {
    let mut info = BY_HANDLE_FILE_INFORMATION::default();
    // SAFETY: valid held file handle.
    if unsafe { GetFileInformationByHandle(handle.0, &mut info) } == 0 {
        return Err(storage());
    }
    if info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(ManagedStateError {
            kind: ErrorKind::Symlink,
        });
    }
    if info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY == 0 {
        return Err(ManagedStateError {
            kind: ErrorKind::InvalidInput,
        });
    }
    Ok(())
}

#[cfg(windows)]
fn verify_dacl(
    dacl: *const ACL,
    user: &SidBuf,
    system: &SidBuf,
    admins: &SidBuf,
) -> Result<(), ManagedStateError> {
    if dacl.is_null() {
        return Err(insecure());
    }
    let mut info = ACL_SIZE_INFORMATION::default();
    // SAFETY: non-NULL DACL from GetSecurityInfo; output struct is correct.
    let ok = unsafe {
        GetAclInformation(
            dacl,
            &mut info as *mut ACL_SIZE_INFORMATION as *mut core::ffi::c_void,
            std::mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
            AclSizeInformation,
        )
    };
    if ok == 0 {
        return Err(insecure());
    }

    // SAFETY: DACL is owned by the security descriptor for the duration of
    // this function.
    let acl_size = unsafe { (*dacl).AclSize as usize };
    if acl_size < std::mem::size_of::<ACL>() {
        return Err(insecure());
    }
    let acl_start = dacl as usize;
    let acl_end = acl_start.checked_add(acl_size).ok_or_else(insecure)?;

    let mut user_seen = false;
    for index in 0..info.AceCount {
        let mut ace_ptr: *mut core::ffi::c_void = std::ptr::null_mut();
        // SAFETY: index is within AceCount from the ACL itself.
        if unsafe { GetAce(dacl, index, &mut ace_ptr) } == 0 || ace_ptr.is_null() {
            return Err(insecure());
        }
        let ace_start = ace_ptr as usize;
        // SAFETY: ACE pointer was returned by GetAce.
        let header = unsafe { &*(ace_ptr as *const ACE_HEADER) };
        let ace_size = header.AceSize as usize;
        if ace_size < std::mem::size_of::<ACE_HEADER>() {
            return Err(insecure());
        }
        let ace_end = ace_start.checked_add(ace_size).ok_or_else(insecure)?;
        if ace_start < acl_start || ace_end > acl_end {
            return Err(insecure());
        }
        if header.AceType != ACCESS_ALLOWED_ACE_TYPE {
            return Err(insecure());
        }
        if header.AceFlags & (INHERITED_ACE as u8) != 0 {
            return Err(insecure());
        }
        if header.AceFlags & INHERIT_FLAGS_U8 != INHERIT_FLAGS_U8 {
            return Err(insecure());
        }

        // SAFETY: type was checked and the ACE fits within the ACL.
        let ace = unsafe { &*(ace_ptr as *const ACCESS_ALLOWED_ACE) };
        if ace.Mask != FILE_ALL_ACCESS {
            return Err(insecure());
        }
        let sid = &ace.SidStart as *const u32 as PSID;
        // SAFETY: SID lies inside the validated ACE.
        if unsafe { IsValidSid(sid) } == 0 {
            return Err(insecure());
        }
        // SAFETY: valid SID.
        let sid_len = unsafe { GetLengthSid(sid) } as usize;
        let sid_offset = std::mem::offset_of!(ACCESS_ALLOWED_ACE, SidStart);
        match sid_offset.checked_add(sid_len) {
            Some(end) if sid_len != 0 && end <= ace_size => {}
            _ => return Err(insecure()),
        }

        if user.equals(sid) {
            user_seen = true;
        } else if !system.equals(sid) && !admins.equals(sid) {
            return Err(insecure());
        }
    }

    if !user_seen {
        return Err(insecure());
    }
    Ok(())
}

#[cfg(windows)]
fn verify_security_handle(
    handle: &Handle,
    user: &SidBuf,
    system: &SidBuf,
    admins: &SidBuf,
) -> Result<(), ManagedStateError> {
    let mut owner: PSID = std::ptr::null_mut();
    let mut dacl: *mut ACL = std::ptr::null_mut();
    let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    // SAFETY: valid directory handle; outputs are owned by `sd`.
    let rc = unsafe {
        GetSecurityInfo(
            handle.0,
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            &mut owner,
            std::ptr::null_mut(),
            &mut dacl,
            std::ptr::null_mut(),
            &mut sd,
        )
    };
    if rc != 0 || sd.is_null() {
        return Err(insecure());
    }
    let _sd_guard = LocalPtr(sd as HLOCAL);

    if owner.is_null() || !user.equals(owner) {
        return Err(insecure());
    }

    let mut control = 0u16;
    let mut revision = 0u32;
    // SAFETY: `sd` is a valid self-relative descriptor.
    if unsafe { GetSecurityDescriptorControl(sd, &mut control, &mut revision) } == 0 {
        return Err(insecure());
    }
    if control & SE_DACL_PROTECTED == 0
        || control & SE_DACL_PRESENT == 0
        || control & SE_DACL_DEFAULTED != 0
    {
        return Err(insecure());
    }

    let mut present: i32 = 0;
    let mut dacl_from_sd: *mut ACL = std::ptr::null_mut();
    let mut defaulted: i32 = 0;
    // SAFETY: valid descriptor and output pointers.
    if unsafe { GetSecurityDescriptorDacl(sd, &mut present, &mut dacl_from_sd, &mut defaulted) }
        == 0
    {
        return Err(insecure());
    }
    if present == 0 || dacl_from_sd.is_null() || defaulted != 0 {
        return Err(insecure());
    }

    verify_dacl(dacl_from_sd as *const ACL, user, system, admins)
}

#[cfg(windows)]
fn prepare_windows(base: &Path) -> Result<PathBuf, ManagedStateError> {
    validate_windows_path(base)?;
    let wide = wide_path(base)?;

    let user = current_user_sid()?;
    let system = SidBuf::well_known(WinLocalSystemSid)?;
    let admins = SidBuf::well_known(WinBuiltinAdministratorsSid)?;

    let mut acl_store = AclStorage::new();
    let acl = acl_store.as_mut();
    // SAFETY: ACL storage is aligned and at least `len` bytes.
    if unsafe { InitializeAcl(acl, acl_store.len(), ACL_REVISION) } == 0 {
        return Err(storage());
    }
    if !add_allow_ace(acl, user.as_psid()) {
        return Err(storage());
    }
    if !system.equals(user.as_psid()) && !add_allow_ace(acl, system.as_psid()) {
        return Err(storage());
    }
    if !admins.equals(user.as_psid())
        && !admins.equals(system.as_psid())
        && !add_allow_ace(acl, admins.as_psid())
    {
        return Err(storage());
    }

    let mut sd = SECURITY_DESCRIPTOR::default();
    let sd_ptr = &mut sd as *mut SECURITY_DESCRIPTOR as PSECURITY_DESCRIPTOR;
    // SAFETY: all referenced SIDs/ACL outlive `sd` for this function.
    unsafe {
        if InitializeSecurityDescriptor(sd_ptr, SECURITY_DESCRIPTOR_REVISION) == 0 {
            return Err(storage());
        }
        if SetSecurityDescriptorOwner(sd_ptr, user.as_psid(), 0) == 0 {
            return Err(storage());
        }
        if SetSecurityDescriptorDacl(sd_ptr, 1, acl_store.as_ref(), 0) == 0 {
            return Err(storage());
        }
        if SetSecurityDescriptorControl(sd_ptr, SE_DACL_PROTECTED, SE_DACL_PROTECTED) == 0 {
            return Err(storage());
        }
    }

    let attributes = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: sd_ptr,
        bInheritHandle: 0,
    };

    for _ in 0..4 {
        // SAFETY: NUL-terminated path and valid security descriptor.
        let created = unsafe { CreateDirectoryW(wide.as_ptr(), &attributes) } != 0;

        if created {
            match open_windows_dir_raw(&wide, false) {
                Ok(handle) => {
                    verify_directory_handle(&handle)?;
                    verify_security_handle(&handle, &user, &system, &admins)?;
                    return Ok(base.to_path_buf());
                }
                Err(code) if code == ERROR_FILE_NOT_FOUND || code == ERROR_PATH_NOT_FOUND => {
                    continue;
                }
                Err(code) => {
                    return Err(ManagedStateError {
                        kind: map_win_error(code),
                    })
                }
            }
        }

        let code = last_win_error();
        if code == ERROR_ALREADY_EXISTS || code == ERROR_FILE_EXISTS {
            match open_windows_dir_raw(&wide, false) {
                Ok(handle) => {
                    verify_directory_handle(&handle)?;
                    verify_security_handle(&handle, &user, &system, &admins)?;
                    return Ok(base.to_path_buf());
                }
                Err(open_code)
                    if open_code == ERROR_FILE_NOT_FOUND || open_code == ERROR_PATH_NOT_FOUND =>
                {
                    continue;
                }
                Err(open_code) => {
                    return Err(ManagedStateError {
                        kind: map_win_error(open_code),
                    })
                }
            }
        }

        return Err(ManagedStateError {
            kind: map_win_error(code),
        });
    }

    Err(storage())
}

impl fmt::Display for ManagedStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            ErrorKind::UnsupportedPlatform => {
                f.write_str("managed state unavailable on this platform")
            }
            _ => f.write_str("managed state preparation failed"),
        }
    }
}

impl fmt::Debug for ManagedStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _ = self.kind;
        f.write_str("ManagedStateError { ..redacted.. }")
    }
}

impl fmt::Debug for ManagedStateDir {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ManagedStateDir { ..redacted.. }")
    }
}

impl std::error::Error for ManagedStateError {}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "linux")]
    use std::os::unix::fs::symlink;
    #[cfg(target_os = "linux")]
    use std::os::unix::fs::PermissionsExt;
    use std::sync::atomic::AtomicU64;
    #[cfg(any(target_os = "linux", windows))]
    use std::sync::atomic::Ordering;

    #[allow(dead_code)]
    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[cfg(any(target_os = "linux", windows))]
    fn sandbox_base() -> PathBuf {
        use std::sync::OnceLock;

        static SANDBOX_BASE: OnceLock<PathBuf> = OnceLock::new();

        SANDBOX_BASE
            .get_or_init(|| {
                let unique = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|elapsed| elapsed.as_nanos())
                    .unwrap_or(0);
                let base = std::env::temp_dir().join(format!(
                    "surge-managed-state-tests-{}-{}",
                    std::process::id(),
                    unique
                ));

                let mut builder = std::fs::DirBuilder::new();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::DirBuilderExt;
                    builder.mode(0o700);
                }
                if let Err(err) = builder.create(&base) {
                    panic!(
                        "sandbox_base: exclusive create of {} failed: {err}",
                        base.display()
                    );
                }

                let meta = std::fs::symlink_metadata(&base).unwrap_or_else(|err| {
                    panic!("sandbox_base: lstat of {} failed: {err}", base.display())
                });
                let file_type = meta.file_type();
                if file_type.is_symlink() || !file_type.is_dir() {
                    panic!(
                        "sandbox_base: {} is not a private real directory",
                        base.display()
                    );
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o700))
                        .unwrap_or_else(|err| {
                            panic!(
                                "sandbox_base: chmod 0700 on {} failed: {err}",
                                base.display()
                            )
                        });
                }
                base
            })
            .clone()
    }

    #[cfg(target_os = "linux")]
    fn hex_encode(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0x0f) as usize] as char);
        }
        out
    }

    #[cfg(target_os = "linux")]
    fn unique_tag(tag: &str) -> String {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let mut raw = [0u8; 8];
        // Best-effort OS randomness for uniqueness; fall back to counter mix
        // if getrandom is unavailable (test-only).
        let rc = unsafe { libc::getrandom(raw.as_mut_ptr() as *mut libc::c_void, raw.len(), 0) };
        if rc != raw.len() as isize {
            let pid = std::process::id() as u64;
            raw = (pid.wrapping_mul(0x9E3779B1).wrapping_add(id * 0x85EBCA6B)).to_le_bytes();
        }
        format!("{}-{}-{}-{}", tag, std::process::id(), id, hex_encode(&raw))
    }

    #[cfg(windows)]
    fn unique_tag(tag: &str) -> String {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let pid = std::process::id();
        format!("{}-{}-{}", tag, pid, id)
    }

    #[cfg(any(target_os = "linux", windows))]
    fn fresh_missing_path(tag: &str) -> PathBuf {
        std::fs::create_dir_all(sandbox_base()).ok();
        sandbox_base().join(unique_tag(tag))
    }

    #[cfg(target_os = "linux")]
    fn mode_of(path: &Path) -> u32 {
        path.symlink_metadata().expect("stat").mode() & 0o777
    }

    #[allow(dead_code)]
    fn remove_all_best_effort(path: &Path) {
        if let Ok(meta) = path.symlink_metadata() {
            let file_type = meta.file_type();
            if file_type.is_symlink() {
                if std::fs::remove_file(path).is_err() {
                    let _ = std::fs::remove_dir(path);
                }
            } else if file_type.is_dir() {
                let _ = std::fs::remove_dir_all(path);
            } else {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    #[test]
    fn errors_are_redacted_and_platform_clear() {
        let e = ManagedStateError {
            kind: ErrorKind::Storage,
        };
        assert_eq!(format!("{e}"), "managed state preparation failed");
        assert!(format!("{e:?}").contains("redacted"));
        assert!(!format!("{e:?}").contains("surge"));
        let u = ManagedStateError {
            kind: ErrorKind::UnsupportedPlatform,
        };
        // Windows path must be a clear fail-closed message, still redacted.
        assert_eq!(format!("{u}"), "managed state unavailable on this platform");
        assert!(format!("{u:?}").contains("redacted"));
        // Debug of the dir itself must not leak paths.
        #[cfg(target_os = "linux")]
        {
            let dir = fresh_missing_path("redact-dbg");
            let st = ManagedStateDir::prepare(&dir).expect("prepare");
            let dbg = format!("{:?}", st);
            assert!(dbg.contains("redacted"));
            assert!(!dbg.contains(&dir.to_string_lossy().into_owned()));
            remove_all_best_effort(&dir);
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn creates_missing_dir_with_exact_0700() {
        let target = fresh_missing_path("create");
        assert!(!target.symlink_metadata().is_ok());
        let st = ManagedStateDir::prepare(&target).expect("create must succeed");
        assert_eq!(st.path(), target.as_path());
        assert!(target.symlink_metadata().expect("stat").is_dir());
        assert!(!target.is_symlink());
        assert_eq!(mode_of(&target), 0o700);
        let euid = unsafe { libc::geteuid() };
        let uid = target.symlink_metadata().expect("stat").uid();
        assert_eq!(uid, euid);
        remove_all_best_effort(&target);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn reuses_existing_and_persists_files() {
        let target = fresh_missing_path("reuse");
        let first = ManagedStateDir::prepare(&target).expect("first prepare");
        let marker = first.path().join("queue.json");
        std::fs::write(&marker, b"{\"q\":1}").expect("write marker");
        // Second prepare must reuse the same path and preserve the file.
        let second = ManagedStateDir::prepare(&target).expect("second prepare");
        assert_eq!(first.path(), second.path());
        assert_eq!(second.path(), target.as_path());
        assert_eq!(std::fs::read(&marker).expect("read"), b"{\"q\":1}");
        assert_eq!(mode_of(&target), 0o700);
        // Drop must not delete (persistent, never deletes).
        let path_buf = second.path().to_path_buf();
        drop(first);
        drop(second);
        assert!(
            path_buf.symlink_metadata().is_ok(),
            "Drop must preserve dir"
        );
        assert!(
            marker.symlink_metadata().is_ok(),
            "Drop must preserve files"
        );
        assert_eq!(
            std::fs::read(&marker).expect("read after drop"),
            b"{\"q\":1}"
        );
        let _ = std::fs::remove_file(&marker);
        remove_all_best_effort(&target);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn rejects_insecure_modes_without_chmod_or_delete() {
        for mode in [0o755u32, 0o777, 0o750, 0o711, 0o720, 0o701, 0o707] {
            let target = fresh_missing_path("insecure");
            std::fs::create_dir_all(&target).expect("mkdir");
            let sentinel = target.join("settings.json");
            std::fs::write(&sentinel, b"keep").expect("sentinel");
            std::fs::set_permissions(&target, std::fs::Permissions::from_mode(mode))
                .expect("chmod insecure");
            let err = ManagedStateDir::prepare(&target).expect_err("must fail closed");
            assert_eq!(format!("{err}"), "managed state preparation failed");
            // Must not have chmod-ed the arbitrary existing directory.
            assert_eq!(
                mode_of(&target),
                mode,
                "existing dir permissions must be left untouched"
            );
            // Must not have deleted data.
            assert!(sentinel.symlink_metadata().is_ok(), "sentinel must survive");
            assert_eq!(std::fs::read(&sentinel).expect("read"), b"keep");
            let _ = std::fs::remove_file(&sentinel);
            std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o700)).ok();
            remove_all_best_effort(&target);
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn rejects_symlink_final_without_following() {
        std::fs::create_dir_all(sandbox_base()).ok();
        let real = fresh_missing_path("real");
        std::fs::create_dir_all(&real).expect("real dir");
        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o700)).ok();
        let link = sandbox_base().join(format!(
            "link-{}-{}",
            std::process::id(),
            TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        symlink(&real, &link).expect("symlink");
        let err = ManagedStateDir::prepare(&link).expect_err("symlink must fail");
        assert_eq!(format!("{err}"), "managed state preparation failed");
        // Nothing created through the link.
        let entries: Vec<_> = std::fs::read_dir(&real).expect("read real").collect();
        assert!(entries.is_empty(), "must not write through symlink");
        remove_all_best_effort(&link);
        remove_all_best_effort(&real);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn rejects_relative_file_and_missing_parent() {
        let rel = PathBuf::from("relative/state");
        assert!(ManagedStateDir::prepare(&rel).is_err());
        // Base is a regular file.
        let target = fresh_missing_path("is-file");
        std::fs::create_dir_all(sandbox_base()).ok();
        std::fs::write(&target, b"x").expect("file");
        assert!(ManagedStateDir::prepare(&target).is_err());
        assert_eq!(std::fs::read(&target).expect("read"), b"x");
        remove_all_best_effort(&target);
        // Parent missing: fail closed, do not create parents.
        let deep = sandbox_base().join(format!(
            "no-parent-{}-{}/child/state",
            std::process::id(),
            TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        assert!(!deep.parent().expect("parent").symlink_metadata().is_ok());
        assert!(ManagedStateDir::prepare(&deep).is_err());
        assert!(!deep.symlink_metadata().is_ok());
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn concurrent_prepare_converges_on_reuse() {
        use std::thread;
        let target = fresh_missing_path("concurrent");
        let mut handles = Vec::new();
        for _ in 0..8 {
            let p = target.clone();
            handles.push(thread::spawn(move || {
                ManagedStateDir::prepare(&p).map(|d| d.path().to_path_buf())
            }));
        }
        let mut ok = 0;
        for h in handles {
            let r = h.join().expect("thread");
            let p = r.expect("concurrent prepare must succeed");
            assert_eq!(p, target);
            ok += 1;
        }
        assert_eq!(ok, 8);
        assert_eq!(mode_of(&target), 0o700);
        remove_all_best_effort(&target);
    }

    #[cfg(windows)]
    fn test_set_dacl(path: &Path, null_dacl: bool) {
        use windows_sys::Win32::Security::Authorization::SetNamedSecurityInfoW;

        let wide = wide_path(path).expect("wide path");
        let mut acl_store = AclStorage::new();
        let acl = acl_store.as_mut();
        let dacl: *const ACL = if null_dacl {
            std::ptr::null()
        } else {
            let user = current_user_sid().expect("user sid");
            let world =
                SidBuf::well_known(windows_sys::Win32::Security::WinWorldSid).expect("world sid");
            // SAFETY: aligned ACL storage.
            assert_ne!(
                unsafe { InitializeAcl(acl, acl_store.len(), ACL_REVISION) },
                0
            );
            assert!(add_allow_ace(acl, user.as_psid()));
            assert!(add_allow_ace(acl, world.as_psid()));
            acl_store.as_ref()
        };
        // SAFETY: valid path, SID/ACL pointers live through the call. A NULL
        // DACL with these flags installs an unprotected-everyone ACL, which
        // the preparation path must reject.
        let rc = unsafe {
            SetNamedSecurityInfoW(
                wide.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                dacl,
                std::ptr::null(),
            )
        };
        assert_eq!(rc, 0, "test DACL setup failed");
    }

    #[cfg(windows)]
    fn assert_hardened_acl(path: &Path) {
        use windows_sys::Win32::Security::Authorization::GetNamedSecurityInfoW;

        let wide = wide_path(path).expect("wide path");
        let mut owner: PSID = std::ptr::null_mut();
        let mut dacl: *mut ACL = std::ptr::null_mut();
        let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        // SAFETY: valid path and output pointers.
        let rc = unsafe {
            GetNamedSecurityInfoW(
                wide.as_ptr(),
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                &mut owner,
                std::ptr::null_mut(),
                &mut dacl,
                std::ptr::null_mut(),
                &mut sd,
            )
        };
        assert_eq!(rc, 0, "read security");
        assert!(!sd.is_null());
        let _guard = LocalPtr(sd as HLOCAL);
        assert!(!owner.is_null());
        assert!(!dacl.is_null());

        let user = current_user_sid().expect("user sid");
        let system = SidBuf::well_known(WinLocalSystemSid).expect("system sid");
        let admins = SidBuf::well_known(WinBuiltinAdministratorsSid).expect("admin sid");
        assert!(user.equals(owner), "owner must be current user");

        let mut control = 0u16;
        let mut revision = 0u32;
        // SAFETY: valid descriptor.
        assert_ne!(
            unsafe { GetSecurityDescriptorControl(sd, &mut control, &mut revision,) },
            0
        );
        assert_ne!(control & SE_DACL_PROTECTED, 0, "DACL must be protected");
        assert_eq!(control & SE_DACL_DEFAULTED, 0, "DACL must not be defaulted");

        let mut present: i32 = 0;
        let mut dacl_from_sd: *mut ACL = std::ptr::null_mut();
        let mut defaulted: i32 = 0;
        // SAFETY: valid descriptor and output pointers.
        assert_ne!(
            unsafe {
                GetSecurityDescriptorDacl(sd, &mut present, &mut dacl_from_sd, &mut defaulted)
            },
            0
        );
        assert_ne!(present, 0);
        assert!(!dacl_from_sd.is_null());
        assert_eq!(defaulted, 0);

        let mut info = ACL_SIZE_INFORMATION::default();
        // SAFETY: DACL is owned by `sd`.
        assert_ne!(
            unsafe {
                GetAclInformation(
                    dacl_from_sd as *const ACL,
                    &mut info as *mut ACL_SIZE_INFORMATION as *mut core::ffi::c_void,
                    std::mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
                    AclSizeInformation,
                )
            },
            0
        );

        let mut user_seen = false;
        for index in 0..info.AceCount {
            let mut ace_ptr: *mut core::ffi::c_void = std::ptr::null_mut();
            // SAFETY: index within the ACL's ACE count.
            assert_ne!(unsafe { GetAce(dacl_from_sd, index, &mut ace_ptr) }, 0);
            assert!(!ace_ptr.is_null());
            // SAFETY: ACE pointer returned by GetAce.
            let header = unsafe { &*(ace_ptr as *const ACE_HEADER) };
            assert_eq!(header.AceType, ACCESS_ALLOWED_ACE_TYPE);
            assert_eq!(header.AceFlags & INHERIT_FLAGS_U8, INHERIT_FLAGS_U8);
            assert_eq!(header.AceFlags & (INHERITED_ACE as u8), 0);
            // SAFETY: allow ACE type checked above.
            let ace = unsafe { &*(ace_ptr as *const ACCESS_ALLOWED_ACE) };
            assert_eq!(ace.Mask, FILE_ALL_ACCESS);
            let sid = &ace.SidStart as *const u32 as PSID;
            let known = user.equals(sid) || system.equals(sid) || admins.equals(sid);
            assert!(known, "unexpected SID in hardened DACL");
            if user.equals(sid) {
                user_seen = true;
            }
        }
        assert!(user_seen, "current-user ACE missing");
    }

    #[cfg(windows)]
    fn assert_test_dacl_preserved(path: &Path, null_dacl: bool) {
        use windows_sys::Win32::Security::Authorization::GetNamedSecurityInfoW;

        let wide = wide_path(path).expect("wide path");
        let mut owner: PSID = std::ptr::null_mut();
        let mut dacl: *mut ACL = std::ptr::null_mut();
        let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        // SAFETY: valid path and output pointers.
        let rc = unsafe {
            GetNamedSecurityInfoW(
                wide.as_ptr(),
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                &mut owner,
                std::ptr::null_mut(),
                &mut dacl,
                std::ptr::null_mut(),
                &mut sd,
            )
        };
        assert_eq!(rc, 0, "read security after rejection");
        assert!(!sd.is_null());
        let _guard = LocalPtr(sd as HLOCAL);
        if null_dacl {
            assert!(dacl.is_null(), "NULL DACL must be left untouched");
            return;
        }
        assert!(!dacl.is_null());

        let world =
            SidBuf::well_known(windows_sys::Win32::Security::WinWorldSid).expect("world sid");
        let mut info = ACL_SIZE_INFORMATION::default();
        // SAFETY: DACL is owned by `sd`.
        assert_ne!(
            unsafe {
                GetAclInformation(
                    dacl as *const ACL,
                    &mut info as *mut ACL_SIZE_INFORMATION as *mut core::ffi::c_void,
                    std::mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
                    AclSizeInformation,
                )
            },
            0
        );
        let mut world_seen = false;
        for index in 0..info.AceCount {
            let mut ace_ptr: *mut core::ffi::c_void = std::ptr::null_mut();
            // SAFETY: index within the ACL's ACE count.
            assert_ne!(unsafe { GetAce(dacl, index, &mut ace_ptr) }, 0);
            assert!(!ace_ptr.is_null());
            // SAFETY: ACE pointer returned by GetAce.
            let header = unsafe { &*(ace_ptr as *const ACE_HEADER) };
            if header.AceType == ACCESS_ALLOWED_ACE_TYPE {
                // SAFETY: allow ACE type checked above.
                let ace = unsafe { &*(ace_ptr as *const ACCESS_ALLOWED_ACE) };
                let sid = &ace.SidStart as *const u32 as PSID;
                if world.equals(sid) {
                    world_seen = true;
                }
            }
        }
        assert!(world_seen, "broad DACL must be left untouched");
    }

    #[test]
    #[cfg(windows)]
    fn windows_creates_dir_with_protected_inheritable_dacl() {
        let target = fresh_missing_path("win-create");
        let state = ManagedStateDir::prepare(&target).expect("create must succeed");
        assert_eq!(state.path(), target.as_path());
        assert!(target.symlink_metadata().expect("stat").is_dir());
        assert!(!target.is_symlink());
        assert_hardened_acl(&target);
        drop(state);
        assert!(target.symlink_metadata().is_ok(), "Drop must preserve dir");
        remove_all_best_effort(&target);
    }

    #[test]
    #[cfg(windows)]
    fn windows_rejects_null_and_broad_dacl_without_modifying() {
        for null_dacl in [true, false] {
            let target = fresh_missing_path(if null_dacl {
                "win-null-dacl"
            } else {
                "win-broad-dacl"
            });
            std::fs::create_dir_all(&target).expect("mkdir");
            let sentinel = target.join("settings.json");
            std::fs::write(&sentinel, b"keep").expect("sentinel");
            test_set_dacl(&target, null_dacl);

            let err = ManagedStateDir::prepare(&target)
                .expect_err("insecure existing DACL must fail closed");
            assert_eq!(format!("{err}"), "managed state preparation failed");
            assert!(format!("{err:?}").contains("redacted"));
            assert!(sentinel.symlink_metadata().is_ok(), "data must survive");
            assert_eq!(std::fs::read(&sentinel).expect("read"), b"keep");
            assert_test_dacl_preserved(&target, null_dacl);

            let _ = std::fs::remove_file(&sentinel);
            remove_all_best_effort(&target);
        }
    }

    #[test]
    #[cfg(windows)]
    fn windows_rejects_reparse_final_without_following() {
        let real = fresh_missing_path("win-reparse-real");
        let state = ManagedStateDir::prepare(&real).expect("prepare real dir");
        let link = sandbox_base().join(unique_tag("win-reparse-link"));
        std::os::windows::fs::symlink_dir(&real, &link).expect("create directory symlink");
        assert!(link
            .symlink_metadata()
            .expect("lstat")
            .file_type()
            .is_symlink());

        let err =
            ManagedStateDir::prepare(&link).expect_err("reparse final component must fail closed");
        assert_eq!(format!("{err}"), "managed state preparation failed");
        assert!(
            link.symlink_metadata()
                .expect("lstat")
                .file_type()
                .is_symlink(),
            "link must not be replaced or followed"
        );

        remove_all_best_effort(&link);
        drop(state);
        remove_all_best_effort(&real);
    }

    #[test]
    #[cfg(windows)]
    fn windows_reuses_existing_and_preserves_files() {
        let target = fresh_missing_path("win-reuse");
        let first = ManagedStateDir::prepare(&target).expect("first prepare");
        let marker = first.path().join("queue.json");
        std::fs::write(&marker, b"{\"q\":1}").expect("write marker");

        let second = ManagedStateDir::prepare(&target).expect("second prepare");
        assert_eq!(first.path(), second.path());
        assert_eq!(second.path(), target.as_path());
        assert_eq!(std::fs::read(&marker).expect("read"), b"{\"q\":1}");
        assert_hardened_acl(&target);

        let path_buf = second.path().to_path_buf();
        drop(first);
        drop(second);
        assert!(
            path_buf.symlink_metadata().is_ok(),
            "Drop must preserve dir"
        );
        assert!(
            marker.symlink_metadata().is_ok(),
            "Drop must preserve files"
        );
        assert_eq!(
            std::fs::read(&marker).expect("read after drop"),
            b"{\"q\":1}"
        );

        let _ = std::fs::remove_file(&marker);
        remove_all_best_effort(&target);
    }

    #[test]
    #[cfg(windows)]
    fn windows_rejects_relative_and_traversal_paths() {
        assert!(ManagedStateDir::prepare(Path::new("relative\\state")).is_err());
        let absolute = sandbox_base().join("..").join(unique_tag("escape-state"));
        let err =
            ManagedStateDir::prepare(&absolute).expect_err("parent traversal must fail closed");
        assert_eq!(format!("{err}"), "managed state preparation failed");
        assert!(!absolute.symlink_metadata().is_ok());
        assert!(ManagedStateDir::prepare(Path::new(
            "\\\\.\\GLOBALROOT\\Device\\HarddiskVolume1\\state"
        ))
        .is_err());
    }
}
