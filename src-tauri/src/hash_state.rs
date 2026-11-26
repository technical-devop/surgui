//! Versioned, bounded, app-private persistence for automatic hash intents and
//! results. Intents are fenced by a mutable generation and bound to a stable
//! per-owned-queue namespace plus the authoritative per-download `added_at`
//! and queue fingerprint provenance. Ephemeral connection ids and daemon pids
//! are deliberately not persisted: they change on every app restart.

use std::path::{Path, PathBuf};

use sha2::{Digest as _, Sha256};

pub(crate) const HASH_STATE_VERSION: u32 = 2;
pub(crate) const HASH_RESULT_VERSION: u32 = 3;
pub(crate) const MAX_HASH_RECORDS: usize = 4096;
pub(crate) const MAX_HASH_ID_BYTES: usize = 4 * 1024;
pub(crate) const MAX_HASH_INTENTS_PER_CALL: usize = 100;
pub(crate) const HASH_NAMESPACE_FILE: &str = "automatic-hash-namespace";
pub(crate) const HASH_NAMESPACE_BYTES: usize = 16;
pub(crate) const HASH_NAMESPACE_HEX_LEN: usize = HASH_NAMESPACE_BYTES * 2;
pub(crate) const HASH_FINGERPRINT_HEX_LEN: usize = 64;
const MAX_NAMESPACE_FILE_BYTES: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum HashStatus {
    Pending,
    Complete,
    Failed,
    Unavailable,
    Disabled,
    Stale,
}

impl HashStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Unavailable => "unavailable",
            Self::Disabled => "disabled",
            Self::Stale => "stale",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HashIntent {
    pub id: String,
    pub enabled: bool,
}

/// Transient identity for one managed lifecycle. This is never persisted; it
/// only fences operations within the current process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DaemonBinding {
    pub connection_id: String,
    pub daemon_pid: u32,
}

/// Stable per-owned-queue provenance observed when an add is acknowledged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HashProvenance {
    pub namespace: String,
    pub added_at: i64,
    pub fingerprint: String,
}

/// One authoritative queue item re-read from `/list` for reconciliation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QueueItemIdentity {
    pub id: String,
    pub added_at: i64,
    pub fingerprint: String,
}

impl QueueItemIdentity {
    pub(crate) fn from_download(download: &crate::surge_client::Download) -> Self {
        Self {
            id: download.id.clone(),
            added_at: download.added_at,
            fingerprint: queue_fingerprint(&download.id, download.added_at, &download.url),
        }
    }
}

/// Hash stable, non-secret queue fields. The raw URL is never persisted; only
/// this digest binds the intent to the queue item that acknowledged it.
pub(crate) fn queue_fingerprint(id: &str, added_at: i64, url: &str) -> String {
    let mut digest = Sha256::new();
    for field in [id, &added_at.to_string(), url] {
        let len = field.len() as u64;
        digest.update(len.to_le_bytes());
        digest.update(field.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

/// One record selected for work, fenced by its mutable intent generation and
/// the current in-process lifecycle fence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HashCandidate {
    pub id: String,
    pub generation: u64,
    pub added_at: i64,
    pub fingerprint: String,
    pub fence: u64,
}

/// Snapshot of the exact records eligible for one `/list` reconciliation.
/// Records created or modified after this snapshot are intentionally excluded
/// so an older response cannot invalidate newer work.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ReconcileCohort {
    entries: Vec<(String, u64)>,
}

impl ReconcileCohort {
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn extend(&mut self, other: Self) {
        self.entries.extend(other.entries);
    }

    fn contains(&self, record: &HashRecord) -> bool {
        self.entries
            .iter()
            .any(|(id, generation)| id == &record.id && *generation == record.generation)
    }
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadHashDto {
    pub id: String,
    pub state: String,
    pub md5: Option<String>,
    pub sha256: Option<String>,
    pub bytes: Option<u64>,
    pub result_version: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HashCompleted {
    pub added_at: i64,
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

/// Persisted identity of a completed digest, used to re-verify the current
/// authoritative queue record and file before a digest is served.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CompletedHash {
    pub id: String,
    pub namespace: String,
    pub generation: u64,
    pub added_at: i64,
    pub fingerprint: String,
    pub size: u64,
    pub mtime_sec: i64,
    pub mtime_nsec: i64,
    pub ctime_sec: i64,
    pub ctime_nsec: i64,
    pub file_dev: u64,
    pub file_ino: u64,
}

pub(crate) struct HashStateError;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct HashRecord {
    id: String,
    enabled: bool,
    status: HashStatus,
    #[serde(default)]
    generation: u64,
    #[serde(default)]
    namespace: Option<String>,
    #[serde(default)]
    added_at: Option<i64>,
    #[serde(default)]
    fingerprint: Option<String>,
    #[serde(default)]
    file_size: Option<u64>,
    #[serde(default)]
    mtime_sec: Option<i64>,
    #[serde(default)]
    mtime_nsec: Option<i64>,
    #[serde(default)]
    ctime_sec: Option<i64>,
    #[serde(default)]
    ctime_nsec: Option<i64>,
    #[serde(default)]
    file_dev: Option<u64>,
    #[serde(default)]
    file_ino: Option<u64>,
    #[serde(default)]
    md5: Option<String>,
    #[serde(default)]
    sha256: Option<String>,
    #[serde(default)]
    result_version: Option<u32>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct HashStateFile {
    version: u32,
    default_enabled: bool,
    records: Vec<HashRecord>,
}

impl Default for HashStateFile {
    fn default() -> Self {
        Self {
            version: HASH_STATE_VERSION,
            default_enabled: false,
            records: Vec::new(),
        }
    }
}

impl HashRecord {
    fn new_pending(id: String, enabled: bool) -> Self {
        Self {
            id,
            enabled,
            status: if enabled {
                HashStatus::Pending
            } else {
                HashStatus::Disabled
            },
            generation: 1,
            namespace: None,
            added_at: None,
            fingerprint: None,
            file_size: None,
            mtime_sec: None,
            mtime_nsec: None,
            ctime_sec: None,
            ctime_nsec: None,
            file_dev: None,
            file_ino: None,
            md5: None,
            sha256: None,
            result_version: None,
        }
    }

    fn bump_generation(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.generation = 1;
        }
    }

    fn clear_result(&mut self) {
        self.namespace = None;
        self.added_at = None;
        self.fingerprint = None;
        self.file_size = None;
        self.mtime_sec = None;
        self.mtime_nsec = None;
        self.ctime_sec = None;
        self.ctime_nsec = None;
        self.file_dev = None;
        self.file_ino = None;
        self.md5 = None;
        self.sha256 = None;
        self.result_version = None;
    }

    fn clear_digest(&mut self) {
        self.file_size = None;
        self.mtime_sec = None;
        self.mtime_nsec = None;
        self.ctime_sec = None;
        self.ctime_nsec = None;
        self.file_dev = None;
        self.file_ino = None;
        self.md5 = None;
        self.sha256 = None;
        self.result_version = None;
    }

    fn clear_stale_provenance(&mut self) {
        self.added_at = None;
        self.fingerprint = None;
        self.clear_digest();
    }

    fn reset_pending(&mut self) {
        self.status = HashStatus::Pending;
        self.bump_generation();
        self.clear_result();
    }

    fn apply_provenance(&mut self, provenance: &HashProvenance) {
        self.namespace = Some(provenance.namespace.clone());
        self.added_at = Some(provenance.added_at);
        self.fingerprint = Some(provenance.fingerprint.clone());
    }

    fn provenance_matches(&self, provenance: &HashProvenance) -> bool {
        self.namespace.as_deref() == Some(provenance.namespace.as_str())
            && self.added_at == Some(provenance.added_at)
            && self.fingerprint.as_deref() == Some(provenance.fingerprint.as_str())
    }
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= MAX_HASH_ID_BYTES && !id.chars().any(char::is_control)
}

fn valid_namespace(value: &str) -> bool {
    value.len() == HASH_NAMESPACE_HEX_LEN
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_fingerprint(value: &str) -> bool {
    valid_hash(value, HASH_FINGERPRINT_HEX_LEN)
}

fn valid_hash(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

impl HashStateFile {
    fn validate(&self) -> bool {
        if self.version != HASH_STATE_VERSION || self.records.len() > MAX_HASH_RECORDS {
            return false;
        }
        self.records.iter().all(|record| {
            if !valid_id(&record.id) {
                return false;
            }
            let namespace_ok = record.namespace.as_deref().is_none_or(valid_namespace);
            let fingerprint_ok = record.fingerprint.as_deref().is_none_or(valid_fingerprint);
            match record.status {
                HashStatus::Disabled => {
                    !record.enabled
                        && record.md5.is_none()
                        && record.sha256.is_none()
                        && record.result_version.is_none()
                        && record.namespace.is_none()
                        && record.added_at.is_none()
                        && record.fingerprint.is_none()
                        && record.file_size.is_none()
                        && record.mtime_sec.is_none()
                        && record.mtime_nsec.is_none()
                        && record.ctime_sec.is_none()
                        && record.ctime_nsec.is_none()
                        && record.file_dev.is_none()
                        && record.file_ino.is_none()
                }
                HashStatus::Complete => {
                    record.enabled
                        && namespace_ok
                        && fingerprint_ok
                        && record.added_at.is_some()
                        && record.file_size.is_some()
                        && record.mtime_sec.is_some()
                        && record.mtime_nsec.is_some()
                        && record.ctime_sec.is_some()
                        && record.ctime_nsec.is_some()
                        && record.file_dev.is_some()
                        && record.file_ino.is_some()
                        && record.result_version == Some(HASH_RESULT_VERSION)
                        && record
                            .md5
                            .as_deref()
                            .is_some_and(|value| valid_hash(value, 32))
                        && record
                            .sha256
                            .as_deref()
                            .is_some_and(|value| valid_hash(value, 64))
                }
                HashStatus::Failed | HashStatus::Unavailable => {
                    record.enabled
                        && namespace_ok
                        && fingerprint_ok
                        && record.md5.is_none()
                        && record.sha256.is_none()
                        && record.result_version.is_none()
                        && record.file_size.is_none()
                        && record.mtime_sec.is_none()
                        && record.mtime_nsec.is_none()
                        && record.ctime_sec.is_none()
                        && record.ctime_nsec.is_none()
                        && record.file_dev.is_none()
                        && record.file_ino.is_none()
                }
                HashStatus::Pending => {
                    record.enabled
                        && namespace_ok
                        && fingerprint_ok
                        && record.md5.is_none()
                        && record.sha256.is_none()
                        && record.result_version.is_none()
                        && record.file_size.is_none()
                        && record.mtime_sec.is_none()
                        && record.mtime_nsec.is_none()
                        && record.ctime_sec.is_none()
                        && record.ctime_nsec.is_none()
                        && record.file_dev.is_none()
                        && record.file_ino.is_none()
                }
                HashStatus::Stale => {
                    record.enabled
                        && namespace_ok
                        && record.fingerprint.is_none()
                        && record.md5.is_none()
                        && record.sha256.is_none()
                        && record.result_version.is_none()
                        && record.added_at.is_none()
                        && record.file_size.is_none()
                        && record.mtime_sec.is_none()
                        && record.mtime_nsec.is_none()
                        && record.ctime_sec.is_none()
                        && record.ctime_nsec.is_none()
                        && record.file_dev.is_none()
                        && record.file_ino.is_none()
                }
            }
        })
    }
}

pub(crate) struct HashStore {
    path: Option<PathBuf>,
    data: HashStateFile,
    fence: u64,
}

impl HashStore {
    pub(crate) fn load(path: Option<PathBuf>) -> Self {
        let data = path.as_deref().and_then(load_state).unwrap_or_default();
        Self {
            path,
            data,
            fence: 0,
        }
    }

    pub(crate) fn default_enabled(&self) -> bool {
        self.data.default_enabled
    }

    pub(crate) fn fence(&self) -> u64 {
        self.fence
    }

    pub(crate) fn set_fence(&mut self, fence: u64) {
        self.fence = if fence == 0 { 1 } else { fence };
    }

    pub(crate) fn fence_in_flight(&mut self) {
        self.fence = self.fence.wrapping_add(1);
        if self.fence == 0 {
            self.fence = 1;
        }
    }

    pub(crate) fn set_default_enabled(&mut self, enabled: bool) -> Result<(), HashStateError> {
        let mut data = self.data.clone();
        data.default_enabled = enabled;
        self.commit(data)
    }

    #[cfg(test)]
    pub(crate) fn record_intents(&mut self, intents: &[HashIntent]) -> Result<(), HashStateError> {
        self.record_intents_with(intents, &[]).map(|_| ())
    }

    /// Persist the supplied intents and atomically return the exact identity
    /// and generation of every touched record as written. Callers use this
    /// cohort to reconcile a `/list` response that is older than any later
    /// replacement of the same id.
    pub(crate) fn record_intents_with(
        &mut self,
        intents: &[HashIntent],
        provenance: &[(String, HashProvenance)],
    ) -> Result<ReconcileCohort, HashStateError> {
        if intents.len() > MAX_HASH_INTENTS_PER_CALL {
            return Err(HashStateError);
        }
        if intents.iter().any(|intent| !valid_id(&intent.id)) {
            return Err(HashStateError);
        }
        if provenance.iter().any(|(id, binding)| {
            !valid_id(id)
                || !valid_namespace(&binding.namespace)
                || !valid_fingerprint(&binding.fingerprint)
        }) {
            return Err(HashStateError);
        }
        let mut data = self.data.clone();
        for intent in intents {
            let binding = provenance
                .iter()
                .find(|(id, _)| id == &intent.id)
                .map(|(_, binding)| binding);
            if let Some(existing) = data
                .records
                .iter_mut()
                .find(|record| record.id == intent.id)
            {
                if !intent.enabled {
                    existing.enabled = false;
                    existing.status = HashStatus::Disabled;
                    existing.clear_result();
                    existing.bump_generation();
                    continue;
                }
                let needs_reset = !existing.enabled
                    || matches!(
                        existing.status,
                        HashStatus::Failed | HashStatus::Unavailable | HashStatus::Stale
                    )
                    || binding.is_some_and(|binding| {
                        matches!(existing.status, HashStatus::Pending | HashStatus::Complete)
                            && !existing.provenance_matches(binding)
                    });
                if needs_reset {
                    existing.reset_pending();
                }
                existing.enabled = true;
                if let Some(binding) = binding {
                    existing.apply_provenance(binding);
                }
            } else {
                let mut record = HashRecord::new_pending(intent.id.clone(), intent.enabled);
                if intent.enabled {
                    if let Some(binding) = binding {
                        record.apply_provenance(binding);
                    }
                }
                data.records.push(record);
            }
        }
        while data.records.len() > MAX_HASH_RECORDS {
            data.records.remove(0);
        }
        let mut cohort = ReconcileCohort::default();
        for intent in intents {
            let Some(record) = data.records.iter().find(|record| record.id == intent.id) else {
                continue;
            };
            if !cohort.entries.iter().any(|(id, _)| id == &record.id) {
                cohort.entries.push((record.id.clone(), record.generation));
            }
        }
        self.commit(data)?;
        Ok(cohort)
    }

    pub(crate) fn candidates(&self, namespace: &str) -> Vec<HashCandidate> {
        self.data
            .records
            .iter()
            .filter(|record| {
                record.enabled
                    && record.status == HashStatus::Pending
                    && record.namespace.as_deref() == Some(namespace)
                    && record.added_at.is_some()
                    && record.fingerprint.is_some()
            })
            .map(|record| HashCandidate {
                id: record.id.clone(),
                generation: record.generation,
                added_at: record.added_at.expect("candidate provenance is checked"),
                fingerprint: record
                    .fingerprint
                    .clone()
                    .expect("candidate provenance is checked"),
                fence: self.fence,
            })
            .collect()
    }

    /// Capture the exact bound records eligible for the next `/list`
    /// reconciliation before the request starts.
    pub(crate) fn reconcile_cohort(&self) -> ReconcileCohort {
        let entries = self
            .data
            .records
            .iter()
            .filter(|record| {
                record.enabled
                    && matches!(
                        record.status,
                        HashStatus::Pending
                            | HashStatus::Complete
                            | HashStatus::Failed
                            | HashStatus::Unavailable
                    )
            })
            .map(|record| (record.id.clone(), record.generation))
            .collect();
        ReconcileCohort { entries }
    }

    /// Revalidate every bound record against the fresh authoritative queue
    /// before retaining/resuming it. A missing, duplicate, reused-id,
    /// different-namespace, or changed-provenance record fails closed as
    /// stale. Matching pending intents are returned for the current fence.
    #[cfg(test)]
    pub(crate) fn reconcile_and_candidates(
        &mut self,
        namespace: &str,
        items: &[QueueItemIdentity],
    ) -> Result<Vec<HashCandidate>, HashStateError> {
        let cohort = self.reconcile_cohort();
        self.reconcile_cohort_and_candidates(namespace, items, &cohort)
    }

    /// Reconcile only records whose id and generation were captured before the
    /// `/list` request. Newer or modified records are left untouched.
    pub(crate) fn reconcile_cohort_and_candidates(
        &mut self,
        namespace: &str,
        items: &[QueueItemIdentity],
        cohort: &ReconcileCohort,
    ) -> Result<Vec<HashCandidate>, HashStateError> {
        let mut data = self.data.clone();
        let mut changed = false;
        for record in &mut data.records {
            if !record.enabled
                || !matches!(
                    record.status,
                    HashStatus::Pending
                        | HashStatus::Complete
                        | HashStatus::Failed
                        | HashStatus::Unavailable
                )
                || !cohort.contains(record)
            {
                continue;
            }
            let mut matches = items.iter().filter(|item| item.id == record.id);
            let unique = matches.next().filter(|_| matches.next().is_none());
            let provable = unique.is_some_and(|item| {
                record.namespace.as_deref() == Some(namespace)
                    && record.added_at == Some(item.added_at)
                    && record.fingerprint.as_deref() == Some(item.fingerprint.as_str())
            });
            if !provable {
                // An unbound UI intent belongs to this queue's failed lookup;
                // keep it visible as stale. A record from another namespace
                // stays associated with that namespace and remains hidden.
                if record.namespace.is_none() {
                    record.namespace = Some(namespace.to_owned());
                }
                record.status = HashStatus::Stale;
                record.bump_generation();
                record.clear_stale_provenance();
                changed = true;
            }
        }
        if changed {
            self.commit(data)?;
        }
        Ok(self.candidates(namespace))
    }

    /// Fail closed for records whose persisted namespace cannot be proven to
    /// be the current owned queue. This runs at load time, before any daemon
    /// query, so a replaced/removed namespace never resurrects old results.
    pub(crate) fn invalidate_unprovable(
        &mut self,
        current_namespace: Option<&str>,
    ) -> Result<(), HashStateError> {
        let mut data = self.data.clone();
        let mut changed = false;
        for record in &mut data.records {
            if !record.enabled || matches!(record.status, HashStatus::Disabled | HashStatus::Stale)
            {
                continue;
            }
            if record.namespace.as_deref() != current_namespace {
                record.status = HashStatus::Stale;
                record.bump_generation();
                record.namespace = None;
                record.clear_stale_provenance();
                changed = true;
            }
        }
        if changed {
            self.commit(data)?;
        }
        Ok(())
    }

    pub(crate) fn visible(&self, namespace: &str) -> Vec<DownloadHashDto> {
        self.data
            .records
            .iter()
            .filter(|record| {
                record.status == HashStatus::Disabled
                    || record.namespace.as_deref() == Some(namespace)
            })
            .map(|record| DownloadHashDto {
                id: record.id.clone(),
                state: record.status.as_str().to_owned(),
                md5: (record.status == HashStatus::Complete)
                    .then(|| record.md5.clone())
                    .flatten(),
                sha256: (record.status == HashStatus::Complete)
                    .then(|| record.sha256.clone())
                    .flatten(),
                bytes: (record.status == HashStatus::Complete)
                    .then_some(record.file_size)
                    .flatten(),
                result_version: (record.status == HashStatus::Complete)
                    .then_some(record.result_version)
                    .flatten(),
            })
            .collect()
    }

    pub(crate) fn completed_for(&self, namespace: &str) -> Vec<CompletedHash> {
        self.data
            .records
            .iter()
            .filter(|record| {
                record.status == HashStatus::Complete
                    && record.namespace.as_deref() == Some(namespace)
            })
            .filter_map(|record| {
                Some(CompletedHash {
                    id: record.id.clone(),
                    namespace: record.namespace.clone()?,
                    generation: record.generation,
                    added_at: record.added_at?,
                    fingerprint: record.fingerprint.clone()?,
                    size: record.file_size?,
                    mtime_sec: record.mtime_sec?,
                    mtime_nsec: record.mtime_nsec?,
                    ctime_sec: record.ctime_sec?,
                    ctime_nsec: record.ctime_nsec?,
                    file_dev: record.file_dev?,
                    file_ino: record.file_ino?,
                })
            })
            .collect()
    }

    /// Build the IPC rows immediately before publication. A completed row is
    /// returned with digests only when it is one of the exact records verified
    /// in this request (id, generation, and file identity all match). A row
    /// that completed or changed while validation was in flight is downgraded
    /// to `pending` without digests, and never leaks an unverified value.
    pub(crate) fn visible_verified(
        &self,
        namespace: &str,
        verified: &[CompletedHash],
        expected_fence: u64,
    ) -> Option<Vec<DownloadHashDto>> {
        if self.fence != expected_fence {
            return None;
        }
        let rows = self
            .data
            .records
            .iter()
            .filter(|record| {
                record.status == HashStatus::Disabled
                    || record.namespace.as_deref() == Some(namespace)
            })
            .map(|record| {
                let verified = record.status == HashStatus::Complete
                    && verified.iter().any(|expected| {
                        expected.id == record.id
                            && expected.generation == record.generation
                            && expected.namespace.as_str() == namespace
                            && record.namespace.as_deref() == Some(namespace)
                            && record.added_at == Some(expected.added_at)
                            && record.fingerprint.as_deref() == Some(expected.fingerprint.as_str())
                            && record.file_size == Some(expected.size)
                            && record.mtime_sec == Some(expected.mtime_sec)
                            && record.mtime_nsec == Some(expected.mtime_nsec)
                            && record.ctime_sec == Some(expected.ctime_sec)
                            && record.ctime_nsec == Some(expected.ctime_nsec)
                            && record.file_dev == Some(expected.file_dev)
                            && record.file_ino == Some(expected.file_ino)
                    });
                if record.status == HashStatus::Complete && !verified {
                    return DownloadHashDto {
                        id: record.id.clone(),
                        state: HashStatus::Pending.as_str().to_owned(),
                        md5: None,
                        sha256: None,
                        bytes: None,
                        result_version: None,
                    };
                }
                DownloadHashDto {
                    id: record.id.clone(),
                    state: record.status.as_str().to_owned(),
                    md5: (record.status == HashStatus::Complete)
                        .then(|| record.md5.clone())
                        .flatten(),
                    sha256: (record.status == HashStatus::Complete)
                        .then(|| record.sha256.clone())
                        .flatten(),
                    bytes: (record.status == HashStatus::Complete)
                        .then_some(record.file_size)
                        .flatten(),
                    result_version: (record.status == HashStatus::Complete)
                        .then_some(record.result_version)
                        .flatten(),
                }
            })
            .collect();
        Some(rows)
    }

    pub(crate) fn mark_complete(
        &mut self,
        id: &str,
        generation: u64,
        fence: u64,
        completed: HashCompleted,
    ) -> Result<bool, HashStateError> {
        if fence != self.fence {
            return Ok(false);
        }
        let mut data = self.data.clone();
        let Some(record) = data.records.iter_mut().find(|record| record.id == id) else {
            return Ok(false);
        };
        if !record.enabled
            || record.status != HashStatus::Pending
            || record.generation != generation
            || record.added_at != Some(completed.added_at)
        {
            return Ok(false);
        }
        record.status = HashStatus::Complete;
        record.added_at = Some(completed.added_at);
        record.file_size = Some(completed.size);
        record.mtime_sec = Some(completed.mtime_sec);
        record.mtime_nsec = Some(completed.mtime_nsec);
        record.ctime_sec = Some(completed.ctime_sec);
        record.ctime_nsec = Some(completed.ctime_nsec);
        record.file_dev = Some(completed.file_dev);
        record.file_ino = Some(completed.file_ino);
        record.md5 = Some(completed.md5);
        record.sha256 = Some(completed.sha256);
        record.result_version = Some(HASH_RESULT_VERSION);
        record.bump_generation();
        self.commit(data)?;
        Ok(true)
    }

    pub(crate) fn mark_failed(
        &mut self,
        id: &str,
        generation: u64,
        fence: u64,
    ) -> Result<bool, HashStateError> {
        self.mark_without_result(id, generation, fence, HashStatus::Failed)
    }

    pub(crate) fn mark_unavailable(
        &mut self,
        id: &str,
        generation: u64,
        fence: u64,
    ) -> Result<bool, HashStateError> {
        self.mark_without_result(id, generation, fence, HashStatus::Unavailable)
    }

    pub(crate) fn mark_stale(
        &mut self,
        id: &str,
        generation: u64,
        fence: u64,
    ) -> Result<bool, HashStateError> {
        if fence != self.fence {
            return Ok(false);
        }
        let mut data = self.data.clone();
        let Some(record) = data.records.iter_mut().find(|record| record.id == id) else {
            return Ok(false);
        };
        if !record.enabled || record.generation != generation {
            return Ok(false);
        }
        record.status = HashStatus::Stale;
        record.bump_generation();
        record.clear_stale_provenance();
        self.commit(data)?;
        Ok(true)
    }

    fn mark_without_result(
        &mut self,
        id: &str,
        generation: u64,
        fence: u64,
        status: HashStatus,
    ) -> Result<bool, HashStateError> {
        if fence != self.fence {
            return Ok(false);
        }
        let mut data = self.data.clone();
        let Some(record) = data.records.iter_mut().find(|record| record.id == id) else {
            return Ok(false);
        };
        if !record.enabled
            || record.status != HashStatus::Pending
            || record.generation != generation
        {
            return Ok(false);
        }
        record.status = status;
        record.bump_generation();
        record.clear_digest();
        self.commit(data)?;
        Ok(true)
    }

    /// Remove a completed result only when the exact persisted identity is
    /// still current and no lifecycle change raced the verification.
    pub(crate) fn invalidate_completed(
        &mut self,
        expected: &CompletedHash,
        fence: u64,
    ) -> Result<bool, HashStateError> {
        if fence != self.fence {
            return Ok(false);
        }
        let mut data = self.data.clone();
        let Some(record) = data
            .records
            .iter_mut()
            .find(|record| record.id == expected.id)
        else {
            return Ok(false);
        };
        let same = record.status == HashStatus::Complete
            && record.generation == expected.generation
            && record.namespace.as_deref() == Some(expected.namespace.as_str())
            && record.added_at == Some(expected.added_at)
            && record.fingerprint.as_deref() == Some(expected.fingerprint.as_str())
            && record.file_size == Some(expected.size)
            && record.mtime_sec == Some(expected.mtime_sec)
            && record.mtime_nsec == Some(expected.mtime_nsec)
            && record.ctime_sec == Some(expected.ctime_sec)
            && record.ctime_nsec == Some(expected.ctime_nsec)
            && record.file_dev == Some(expected.file_dev)
            && record.file_ino == Some(expected.file_ino);
        if !same {
            return Ok(false);
        }
        record.status = HashStatus::Stale;
        record.bump_generation();
        record.clear_stale_provenance();
        self.commit(data)?;
        Ok(true)
    }

    fn commit(&mut self, data: HashStateFile) -> Result<(), HashStateError> {
        if !data.validate() {
            return Err(HashStateError);
        }
        if let Some(path) = self.path.as_ref() {
            let bytes = serde_json::to_vec(&data).map_err(|_| HashStateError)?;
            atomic_write(path, &bytes).map_err(|_| HashStateError)?;
        }
        self.data = data;
        Ok(())
    }
}

/// Result of authenticating the managed state directory and loading (or
/// creating) the random per-owned-queue namespace.
pub(crate) struct NamespaceState {
    pub namespace: Option<String>,
}

/// Read or atomically create the stable queue namespace. A malformed,
/// unreadable, symlinked, or non-regular namespace file fails closed with no
/// namespace rather than guessing or overwriting metadata.
pub(crate) fn prepare_namespace(directory: &Path) -> NamespaceState {
    if crate::managed_state::ManagedStateDir::prepare(directory).is_err() {
        return NamespaceState { namespace: None };
    }
    let path = directory.join(HASH_NAMESPACE_FILE);
    match std::fs::symlink_metadata(&path) {
        Ok(meta) => {
            if !meta.file_type().is_file() || meta.len() > MAX_NAMESPACE_FILE_BYTES as u64 {
                return NamespaceState { namespace: None };
            }
            let Ok(bytes) = std::fs::read(&path) else {
                return NamespaceState { namespace: None };
            };
            let Ok(text) = std::str::from_utf8(&bytes) else {
                return NamespaceState { namespace: None };
            };
            let value = text.trim_end_matches(['\r', '\n']);
            if valid_namespace(value) {
                NamespaceState {
                    namespace: Some(value.to_owned()),
                }
            } else {
                NamespaceState { namespace: None }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let Some(namespace) = random_namespace() else {
                return NamespaceState { namespace: None };
            };
            if atomic_write(&path, namespace.as_bytes()).is_err() {
                return NamespaceState { namespace: None };
            }
            NamespaceState {
                namespace: Some(namespace),
            }
        }
        Err(_) => NamespaceState { namespace: None },
    }
}

fn random_namespace() -> Option<String> {
    let mut raw = [0u8; HASH_NAMESPACE_BYTES];
    getrandom::fill(&mut raw).ok()?;
    let mut out = String::with_capacity(HASH_NAMESPACE_HEX_LEN);
    for byte in raw {
        out.push_str(&format!("{byte:02x}"));
    }
    Some(out)
}

fn load_state(path: &Path) -> Option<HashStateFile> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(_) => return None,
    };
    let mut data: HashStateFile = serde_json::from_slice(&bytes).ok()?;
    if !data.validate() {
        migrate_legacy_results(&mut data)?;
    }
    data.validate().then_some(data)
}

/// Older completed results lack change-time metadata and therefore cannot be
/// served under the stricter identity check. Fail them closed as `stale`
/// while preserving pending intents and the app-wide default.
fn migrate_legacy_results(data: &mut HashStateFile) -> Option<()> {
    if data.version != HASH_STATE_VERSION || data.records.len() > MAX_HASH_RECORDS {
        return None;
    }
    for record in &mut data.records {
        if record.status == HashStatus::Complete
            && (record.result_version != Some(HASH_RESULT_VERSION)
                || record.ctime_sec.is_none()
                || record.ctime_nsec.is_none())
        {
            record.status = HashStatus::Stale;
            record.bump_generation();
            record.clear_stale_provenance();
        }
    }
    Some(())
}

fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::sync::atomic::{AtomicU64, Ordering};

    static WRITE_COUNTER: AtomicU64 = AtomicU64::new(0);
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "state path has no parent")
    })?;

    for _ in 0..16 {
        let sequence = WRITE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let temp = parent.join(format!(
            ".automatic-hash-{}-{sequence}.tmp",
            std::process::id()
        ));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = match options.open(&temp) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        };
        let result = (|| {
            file.write_all(bytes)?;
            file.sync_all()?;
            drop(file);
            std::fs::rename(&temp, path)?;
            #[cfg(target_os = "linux")]
            {
                let directory = std::fs::File::open(parent)?;
                directory.sync_all()?;
            }
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&temp);
        }
        return result;
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "temporary state path is busy",
    ))
}

impl std::fmt::Debug for HashStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HashStateError")
    }
}

impl std::fmt::Display for HashStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("automatic hash state is unavailable")
    }
}

impl std::error::Error for HashStateError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    const NS: &str = "00112233445566778899aabbccddeeff";
    const NS2: &str = "ffeeddccbbaa99887766554433221100";
    const TEST_URL: &str = "https://example.test/file.bin";

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "surge-hash-state-{tag}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
                .expect("private temp dir");
        }
        dir
    }

    fn remove_all_best_effort(path: &Path) {
        if path.symlink_metadata().is_ok() {
            if path.is_dir() && !path.is_symlink() {
                let _ = std::fs::remove_dir_all(path);
            } else {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    fn state_file(root: &Path) -> PathBuf {
        root.join("automatic-hash.json")
    }

    #[cfg(any(target_os = "linux", target_os = "windows"))]
    fn namespace_dir(tag: &str) -> PathBuf {
        let parent = temp_dir(tag);
        let dir = parent.join("state");
        crate::managed_state::ManagedStateDir::prepare(&dir).expect("prepare private state dir");
        dir
    }

    fn intent(id: &str, enabled: bool) -> HashIntent {
        HashIntent {
            id: id.to_owned(),
            enabled,
        }
    }

    fn fingerprint(id: &str, added_at: i64) -> String {
        queue_fingerprint(id, added_at, TEST_URL)
    }

    fn provenance(namespace: &str, id: &str, added_at: i64) -> HashProvenance {
        HashProvenance {
            namespace: namespace.to_owned(),
            added_at,
            fingerprint: fingerprint(id, added_at),
        }
    }

    fn item(id: &str, added_at: i64) -> QueueItemIdentity {
        QueueItemIdentity {
            id: id.to_owned(),
            added_at,
            fingerprint: fingerprint(id, added_at),
        }
    }

    fn bind(store: &mut HashStore, id: &str, namespace: &str, added_at: i64) {
        store
            .record_intents_with(
                &[intent(id, true)],
                &[(id.to_owned(), provenance(namespace, id, added_at))],
            )
            .expect("record bound intent");
    }

    fn completed(added_at: i64) -> HashCompleted {
        HashCompleted {
            added_at,
            md5: "900150983cd24fb0d6963f7d28e17f72".into(),
            sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
            size: 3,
            mtime_sec: 1,
            mtime_nsec: 2,
            ctime_sec: 3,
            ctime_nsec: 4,
            file_dev: 10,
            file_ino: 20,
        }
    }

    fn candidate(store: &HashStore, namespace: &str, id: &str) -> HashCandidate {
        store
            .candidates(namespace)
            .into_iter()
            .find(|candidate| candidate.id == id)
            .expect("candidate exists")
    }

    fn ids(candidates: &[HashCandidate]) -> Vec<&str> {
        candidates
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect()
    }

    fn complete(store: &mut HashStore, id: &str, namespace: &str, added_at: i64) -> u64 {
        let candidate = candidate(store, namespace, id);
        let fence = candidate.fence;
        assert!(store
            .mark_complete(id, candidate.generation, fence, completed(added_at))
            .expect("complete"));
        fence
    }

    #[test]
    fn default_is_false_versioned_and_persistent() {
        let root = temp_dir("default");
        let path = state_file(&root);
        let mut store = HashStore::load(Some(path.clone()));
        assert!(!store.default_enabled());
        store.set_default_enabled(true).expect("persist default");
        let reloaded = HashStore::load(Some(path.clone()));
        assert!(reloaded.default_enabled());
        let json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).expect("read state")).expect("json");
        assert_eq!(json["version"], HASH_STATE_VERSION);
        assert_eq!(json["defaultEnabled"], true);
        remove_all_best_effort(&root);
    }

    #[test]
    fn malformed_state_fails_safe_to_false_and_empty() {
        let root = temp_dir("malformed");
        let path = state_file(&root);
        std::fs::write(&path, b"{not json").expect("write malformed");
        let mut store = HashStore::load(Some(path));
        assert!(!store.default_enabled());
        assert!(store.candidates(NS).is_empty());
        assert!(store.visible(NS).is_empty());
        store.set_default_enabled(true).expect("recover atomically");
        assert!(HashStore::load(Some(state_file(&root))).default_enabled());
        remove_all_best_effort(&root);
    }

    #[test]
    fn intents_persist_with_deduplicated_ids_and_last_value_wins() {
        let root = temp_dir("intents");
        let path = state_file(&root);
        let mut store = HashStore::load(Some(path.clone()));
        store
            .record_intents_with(
                &[
                    intent("d-1", true),
                    intent("d-1", false),
                    intent("d-2", true),
                ],
                &[
                    ("d-1".into(), provenance(NS, "d-1", 1)),
                    ("d-2".into(), provenance(NS, "d-2", 2)),
                ],
            )
            .expect("record intents");
        let assert_state = |store: &HashStore| {
            let visible = store.visible(NS);
            assert_eq!(visible.len(), 2);
            assert_eq!(
                visible.iter().find(|item| item.id == "d-1").unwrap().state,
                "disabled"
            );
            assert_eq!(
                visible.iter().find(|item| item.id == "d-2").unwrap().state,
                "pending"
            );
        };
        assert_state(&store);
        let reloaded = HashStore::load(Some(path));
        assert_state(&reloaded);
        remove_all_best_effort(&root);
    }

    #[test]
    fn invalid_ids_are_rejected_without_changing_state() {
        let root = temp_dir("invalid-id");
        let path = state_file(&root);
        let mut store = HashStore::load(Some(path));
        for id in ["", "bad\nid", &"x".repeat(MAX_HASH_ID_BYTES + 1)] {
            assert!(store.record_intents(&[intent(id, true)]).is_err());
        }
        assert!(store.candidates(NS).is_empty());
        remove_all_best_effort(&root);
    }

    #[test]
    fn records_are_bounded_and_results_are_associated_with_the_namespace() {
        let root = temp_dir("bounded-results");
        let mut store = HashStore::load(None);
        for index in 0..MAX_HASH_RECORDS + 8 {
            let id = format!("d-{index}");
            bind(&mut store, &id, NS, index as i64);
        }
        let visible = store.visible(NS);
        assert_eq!(visible.len(), MAX_HASH_RECORDS);
        assert_eq!(visible.first().unwrap().id, "d-8");

        complete(&mut store, "d-8", NS, 8);
        assert!(
            !store
                .candidates(NS)
                .iter()
                .any(|candidate| candidate.id == "d-8"),
            "a completed record for this queue must not be re-hashed"
        );
        let current = store.visible(NS);
        let row = current.iter().find(|item| item.id == "d-8").unwrap();
        assert_eq!(row.state, "complete");
        assert_eq!(row.md5.as_deref(), Some("900150983cd24fb0d6963f7d28e17f72"));
        assert_eq!(row.result_version, Some(HASH_RESULT_VERSION));
        assert!(
            store.visible(NS2).iter().all(|item| item.id != "d-8"),
            "another namespace must never see this result"
        );
        remove_all_best_effort(&root);
    }

    #[test]
    fn failed_and_unavailable_results_persist_without_poisoning_later_saves() {
        let root = temp_dir("failed-persistence");
        let path = state_file(&root);
        let mut store = HashStore::load(Some(path.clone()));
        bind(&mut store, "failed", NS, 1);
        bind(&mut store, "unavailable", NS, 2);
        let failed = candidate(&store, NS, "failed");
        let unavailable = candidate(&store, NS, "unavailable");
        assert!(store
            .mark_failed("failed", failed.generation, failed.fence)
            .expect("failed"));
        assert!(store
            .mark_unavailable("unavailable", unavailable.generation, unavailable.fence)
            .expect("unavailable"));
        store
            .set_default_enabled(true)
            .expect("changing the default must still save");
        bind(&mut store, "another", NS, 3);

        let reloaded = HashStore::load(Some(path));
        assert!(reloaded.default_enabled(), "default must persist");
        let same = reloaded.candidates(NS);
        assert!(!same.iter().any(|candidate| candidate.id == "failed"));
        assert!(!same.iter().any(|candidate| candidate.id == "unavailable"));
        assert!(same.iter().any(|candidate| candidate.id == "another"));
        let states = reloaded.visible(NS);
        assert_eq!(
            states.iter().find(|row| row.id == "failed").unwrap().state,
            "failed"
        );
        assert_eq!(
            states
                .iter()
                .find(|row| row.id == "unavailable")
                .unwrap()
                .state,
            "unavailable"
        );
        remove_all_best_effort(&root);
    }

    #[test]
    fn failed_save_rolls_back_memory_and_leaves_the_store_usable() {
        let root = temp_dir("rollback");
        let path = state_file(&root);
        let mut store = HashStore::load(Some(path.clone()));
        bind(&mut store, "d-1", NS, 1);
        let candidate = candidate(&store, NS, "d-1");

        std::fs::remove_file(&path).expect("remove state file");
        std::fs::create_dir(&path).expect("replace state path with a directory");
        assert!(
            store
                .mark_complete("d-1", candidate.generation, candidate.fence, completed(1))
                .is_err(),
            "a failed persistence must be reported"
        );
        let row = store
            .visible(NS)
            .into_iter()
            .find(|row| row.id == "d-1")
            .expect("record remains");
        assert_eq!(row.state, "pending", "the failed update must roll back");
        assert!(row.md5.is_none());

        std::fs::remove_dir(&path).expect("remove blocking directory");
        store
            .set_default_enabled(true)
            .expect("the store must stay usable after a failed save");
        let reloaded = HashStore::load(Some(path));
        assert!(reloaded.default_enabled());
        assert!(
            reloaded
                .visible(NS)
                .iter()
                .any(|row| row.id == "d-1" && row.state == "pending"),
            "the rolled-back record must remain pending"
        );
        remove_all_best_effort(&root);
    }

    #[test]
    fn pending_resumes_once_after_restart_with_the_same_namespace_and_queue() {
        let root = temp_dir("restart-pending");
        let path = state_file(&root);
        let mut store = HashStore::load(Some(path.clone()));
        bind(&mut store, "d-1", NS, 10);
        drop(store);

        // App restart: a fresh store/worker must see the same owned queue.
        let mut restarted = HashStore::load(Some(path));
        let items = [item("d-1", 10)];
        let candidates = restarted
            .reconcile_and_candidates(NS, &items)
            .expect("reconcile after restart");
        assert_eq!(ids(&candidates), vec!["d-1"]);
        complete(&mut restarted, "d-1", NS, 10);
        drop(restarted);

        let mut after_completion = HashStore::load(Some(state_file(&root)));
        assert!(
            after_completion
                .reconcile_and_candidates(NS, &items)
                .expect("reconcile completed")
                .is_empty(),
            "completed work must not be hashed a second time"
        );
        assert_eq!(
            after_completion
                .visible(NS)
                .iter()
                .find(|row| row.id == "d-1")
                .unwrap()
                .state,
            "complete"
        );
        remove_all_best_effort(&root);
    }

    #[test]
    fn legacy_completed_results_without_ctime_become_stale_but_pending_survives() {
        let root = temp_dir("legacy-ctime");
        let path = state_file(&root);
        let mut store = HashStore::load(Some(path.clone()));
        bind(&mut store, "pending", NS, 10);
        bind(&mut store, "done", NS, 11);
        complete(&mut store, "done", NS, 11);
        drop(store);

        let mut json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).expect("read state")).expect("json");
        for record in json["records"].as_array_mut().expect("records") {
            if record["id"] == "done" {
                record["resultVersion"] = serde_json::json!(HASH_RESULT_VERSION - 1);
                record.as_object_mut().expect("object").remove("ctimeSec");
                record.as_object_mut().expect("object").remove("ctimeNsec");
            }
        }
        std::fs::write(&path, serde_json::to_vec(&json).expect("encode")).expect("write legacy");

        let reloaded = HashStore::load(Some(path));
        assert!(reloaded.completed_for(NS).is_empty());
        let rows = reloaded.visible(NS);
        assert_eq!(
            rows.iter().find(|row| row.id == "pending").unwrap().state,
            "pending",
            "pending intents must survive result migration"
        );
        assert_eq!(
            rows.iter().find(|row| row.id == "done").unwrap().state,
            "stale",
            "legacy results without change time must not be served"
        );
        remove_all_best_effort(&root);
    }

    #[test]
    fn completed_results_survive_restart_only_for_the_same_queue_identity() {
        let root = temp_dir("restart-complete");
        let path = state_file(&root);
        let mut store = HashStore::load(Some(path.clone()));
        bind(&mut store, "d-1", NS, 10);
        complete(&mut store, "d-1", NS, 10);
        drop(store);

        let mut restarted = HashStore::load(Some(path));
        let matching = [item("d-1", 10)];
        assert!(restarted
            .reconcile_and_candidates(NS, &matching)
            .expect("matching queue")
            .is_empty());
        assert_eq!(restarted.completed_for(NS).len(), 1);
        assert_eq!(
            restarted
                .visible(NS)
                .iter()
                .find(|row| row.id == "d-1")
                .unwrap()
                .state,
            "complete"
        );

        let changed_added_at = [item("d-1", 11)];
        restarted
            .reconcile_and_candidates(NS, &changed_added_at)
            .expect("changed queue identity");
        assert!(restarted.completed_for(NS).is_empty());
        assert_eq!(
            restarted
                .visible(NS)
                .iter()
                .find(|row| row.id == "d-1")
                .unwrap()
                .state,
            "stale"
        );
        remove_all_best_effort(&root);
    }

    #[test]
    fn different_namespace_or_reused_id_provenance_fails_closed_as_stale() {
        let root = temp_dir("namespace-reuse");
        let path = state_file(&root);
        let mut store = HashStore::load(Some(path.clone()));
        bind(&mut store, "d-1", NS, 10);
        assert!(store
            .reconcile_and_candidates(NS2, &[item("d-1", 10)])
            .expect("other namespace")
            .is_empty());
        assert!(store.visible(NS2).iter().all(|row| row.id != "d-1"));
        assert_eq!(
            store
                .visible(NS)
                .iter()
                .find(|row| row.id == "d-1")
                .unwrap()
                .state,
            "stale"
        );

        let mut reused = HashStore::load(Some(path));
        bind(&mut reused, "d-2", NS, 20);
        assert!(reused
            .reconcile_and_candidates(NS, &[item("d-2", 21)])
            .expect("reused id")
            .is_empty());
        assert_eq!(
            reused
                .visible(NS)
                .iter()
                .find(|row| row.id == "d-2")
                .unwrap()
                .state,
            "stale"
        );
        remove_all_best_effort(&root);
    }

    #[test]
    fn old_lifecycle_fence_cannot_publish_after_disconnect_reconnect() {
        let mut store = HashStore::load(None);
        bind(&mut store, "d-1", NS, 10);
        let old = candidate(&store, NS, "d-1");
        let old_fence = old.fence;

        // Disconnect/reconnect increments only the transient fence.
        store.fence_in_flight();
        assert_ne!(store.fence(), old_fence);
        assert!(!store
            .mark_complete("d-1", old.generation, old_fence, completed(10))
            .expect("old fence is rejected"));
        assert!(!store
            .mark_failed("d-1", old.generation, old_fence)
            .expect("old fence cannot fail the new lifecycle"));
        assert!(!store
            .mark_unavailable("d-1", old.generation, old_fence)
            .expect("old fence cannot mark unavailable"));
        assert!(!store
            .mark_stale("d-1", old.generation, old_fence)
            .expect("old fence cannot mark stale"));

        let fresh = store
            .reconcile_and_candidates(NS, &[item("d-1", 10)])
            .expect("reconcile new lifecycle");
        assert_eq!(ids(&fresh), vec!["d-1"]);
        assert_ne!(fresh[0].fence, old_fence);
        assert_eq!(fresh[0].generation, old.generation);
        assert!(store
            .mark_complete("d-1", fresh[0].generation, fresh[0].fence, completed(10))
            .expect("fresh lifecycle publishes once"));
    }

    #[test]
    fn disabling_in_flight_work_drops_every_terminal_update() {
        let mut store = HashStore::load(None);
        bind(&mut store, "d-1", NS, 10);
        let stale = candidate(&store, NS, "d-1");

        store
            .record_intents(&[intent("d-1", false)])
            .expect("disable");
        assert!(!store
            .mark_complete("d-1", stale.generation, stale.fence, completed(10))
            .expect("complete is fenced"));
        assert!(!store
            .mark_failed("d-1", stale.generation, stale.fence)
            .expect("failed is fenced"));
        assert!(!store
            .mark_unavailable("d-1", stale.generation, stale.fence)
            .expect("unavailable is fenced"));
        assert!(!store
            .mark_stale("d-1", stale.generation, stale.fence)
            .expect("stale is fenced"));
        let row = store
            .visible(NS)
            .into_iter()
            .find(|row| row.id == "d-1")
            .unwrap();
        assert_eq!(row.state, "disabled");
        assert!(row.md5.is_none());

        bind(&mut store, "d-1", NS, 10);
        let fresh = candidate(&store, NS, "d-1");
        assert_ne!(fresh.generation, stale.generation);
        assert!(!store
            .mark_complete("d-1", stale.generation, fresh.fence, completed(10))
            .expect("old generation stays fenced"));
        assert!(store
            .mark_complete("d-1", fresh.generation, fresh.fence, completed(10))
            .expect("fresh generation completes"));
        assert_eq!(
            store
                .visible(NS)
                .into_iter()
                .find(|row| row.id == "d-1")
                .unwrap()
                .state,
            "complete"
        );
    }

    #[test]
    fn generation_change_invalidates_an_in_flight_candidate_for_every_terminal_state() {
        for terminal in ["complete", "failed", "unavailable", "stale"] {
            let mut store = HashStore::load(None);
            bind(&mut store, "d-1", NS, 10);
            let old = candidate(&store, NS, "d-1");
            bind(&mut store, "d-1", NS, 11);
            let new = candidate(&store, NS, "d-1");
            assert_ne!(old.generation, new.generation, "re-binding must bump");
            let fence = new.fence;
            match terminal {
                "complete" => assert!(!store
                    .mark_complete("d-1", old.generation, fence, completed(11))
                    .expect("fenced")),
                "failed" => assert!(!store
                    .mark_failed("d-1", old.generation, fence)
                    .expect("fenced")),
                "unavailable" => assert!(!store
                    .mark_unavailable("d-1", old.generation, fence)
                    .expect("fenced")),
                _ => assert!(!store
                    .mark_stale("d-1", old.generation, fence)
                    .expect("fenced")),
            }
            let row = store
                .visible(NS)
                .into_iter()
                .find(|row| row.id == "d-1")
                .unwrap();
            assert_eq!(row.state, "pending", "{terminal} must not apply");
            assert_eq!(candidate(&store, NS, "d-1").generation, new.generation);
        }
    }

    #[test]
    fn reconcile_cohort_ignores_records_added_or_modified_after_capture() {
        let mut store = HashStore::load(None);
        bind(&mut store, "old", NS, 10);
        let cohort = store.reconcile_cohort();
        assert!(!cohort.is_empty());

        // Both of these happen after the cohort was captured and are therefore
        // invisible to the older `/list` snapshot.
        bind(&mut store, "new", NS, 20);
        bind(&mut store, "old", NS, 11);

        let candidates = store
            .reconcile_cohort_and_candidates(NS, &[item("old", 10)], &cohort)
            .expect("reconcile captured cohort");
        assert_eq!(ids(&candidates), vec!["old", "new"]);
        let rows = store.visible(NS);
        for id in ["old", "new"] {
            assert_eq!(
                rows.iter().find(|row| row.id == id).unwrap().state,
                "pending",
                "{id} must not be invalidated by the older snapshot"
            );
        }
    }

    #[test]
    fn unbound_or_foreign_pending_intents_fail_closed_as_stale() {
        let mut store = HashStore::load(None);
        store
            .record_intents(&[intent("unbound", true)])
            .expect("record unbound");
        bind(&mut store, "foreign", NS2, 10);

        let candidates = store
            .reconcile_and_candidates(NS, &[item("foreign", 10)])
            .expect("reconcile");
        assert!(candidates.is_empty());
        let rows = store.visible(NS);
        assert_eq!(
            rows.iter().find(|row| row.id == "unbound").unwrap().state,
            "stale"
        );
        assert!(rows.iter().all(|row| row.id != "foreign"));
        assert_eq!(
            store
                .visible(NS2)
                .iter()
                .find(|row| row.id == "foreign")
                .unwrap()
                .state,
            "stale"
        );
    }

    #[test]
    fn reused_id_with_different_provenance_is_never_treated_as_the_same_download() {
        let mut store = HashStore::load(None);
        bind(&mut store, "d-1", NS, 10);
        let first = candidate(&store, NS, "d-1");
        assert_eq!(first.added_at, 10);

        bind(&mut store, "d-1", NS, 99);
        let second = candidate(&store, NS, "d-1");
        assert_eq!(second.added_at, 99);
        assert_ne!(first.generation, second.generation);
        assert!(!store
            .mark_complete("d-1", first.generation, second.fence, completed(10))
            .expect("old provenance is fenced"));
        assert!(store
            .mark_complete("d-1", second.generation, second.fence, completed(99))
            .expect("new provenance completes"));
    }

    #[test]
    fn completed_results_can_only_be_invalidated_by_the_exact_identity_and_fence() {
        let mut store = HashStore::load(None);
        bind(&mut store, "d-1", NS, 10);
        let fence = complete(&mut store, "d-1", NS, 10);
        let expected = store.completed_for(NS).remove(0);

        let mut replacement = expected.clone();
        replacement.added_at = 11;
        assert!(!store
            .invalidate_completed(&replacement, fence)
            .expect("mismatched provenance is ignored"));
        assert_eq!(
            store
                .visible(NS)
                .into_iter()
                .find(|row| row.id == "d-1")
                .unwrap()
                .state,
            "complete"
        );

        store.fence_in_flight();
        assert!(!store
            .invalidate_completed(&expected, fence)
            .expect("old lifecycle fence cannot invalidate"));
        assert_eq!(store.completed_for(NS).len(), 1);

        let current_fence = store.fence();
        assert!(store
            .invalidate_completed(&expected, current_fence)
            .expect("exact identity invalidates"));
        let row = store
            .visible(NS)
            .into_iter()
            .find(|row| row.id == "d-1")
            .unwrap();
        assert_eq!(row.state, "stale");
        assert!(row.md5.is_none());
        assert!(!store
            .invalidate_completed(&expected, current_fence)
            .expect("already invalidated"));
    }

    #[test]
    #[cfg(any(target_os = "linux", target_os = "windows"))]
    fn namespace_is_created_atomically_and_reused() {
        let root = namespace_dir("namespace-create");
        let first = prepare_namespace(&root);
        let Some(namespace) = first.namespace.clone() else {
            panic!("namespace must be created");
        };
        assert!(valid_namespace(&namespace));
        let path = root.join(HASH_NAMESPACE_FILE);
        let metadata = std::fs::symlink_metadata(&path).expect("namespace file");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        }
        let second = prepare_namespace(&root);
        assert_eq!(second.namespace.as_deref(), Some(namespace.as_str()));
        remove_all_best_effort(&root);
    }

    #[test]
    #[cfg(any(target_os = "linux", target_os = "windows"))]
    fn malformed_or_missing_namespace_invalidates_bound_records_but_keeps_default() {
        let root = namespace_dir("namespace-malformed");
        let path = state_file(&root);
        let mut store = HashStore::load(Some(path.clone()));
        store.set_default_enabled(true).expect("default");
        bind(&mut store, "d-1", NS, 10);
        std::fs::write(root.join(HASH_NAMESPACE_FILE), b"not-a-namespace")
            .expect("malformed namespace");

        let namespace = prepare_namespace(&root);
        assert!(namespace.namespace.is_none());
        let mut reloaded = HashStore::load(Some(path));
        assert!(reloaded.default_enabled(), "default is independent");
        reloaded
            .invalidate_unprovable(namespace.namespace.as_deref())
            .expect("fail closed");
        assert!(reloaded.candidates(NS).is_empty());
        assert!(
            reloaded.visible(NS).iter().all(|row| row.id != "d-1"),
            "unprovable namespace records must stay hidden"
        );
        remove_all_best_effort(&root);
    }

    #[test]
    #[cfg(any(target_os = "linux", target_os = "windows"))]
    fn missing_namespace_file_generates_new_namespace_and_stales_old_queue_records() {
        let root = namespace_dir("namespace-missing");
        let path = state_file(&root);
        let mut store = HashStore::load(Some(path.clone()));
        store.set_default_enabled(true).expect("default");
        bind(&mut store, "d-1", NS, 10);
        drop(store);

        // Crash/removal between namespace and state save boundaries: the old
        // records cannot be proven to belong to the newly generated queue.
        let namespace = prepare_namespace(&root);
        let Some(new_namespace) = namespace.namespace else {
            panic!("a new namespace is created for a missing file");
        };
        assert_ne!(new_namespace, NS);
        let mut reloaded = HashStore::load(Some(path));
        assert!(reloaded.default_enabled(), "default is independent");
        reloaded
            .invalidate_unprovable(Some(&new_namespace))
            .expect("fail closed");
        assert!(reloaded.candidates(&new_namespace).is_empty());
        assert!(
            reloaded
                .visible(&new_namespace)
                .iter()
                .all(|row| row.id != "d-1"),
            "the old queue record must never resurrect"
        );
        remove_all_best_effort(&root);
    }

    #[test]
    #[cfg(any(target_os = "linux", target_os = "windows"))]
    fn missing_state_file_with_existing_namespace_starts_empty_without_guessing() {
        let root = namespace_dir("state-missing");
        let namespace = prepare_namespace(&root);
        assert!(namespace.namespace.is_some());
        let mut store = HashStore::load(Some(state_file(&root)));
        assert!(!store.default_enabled());
        assert!(store
            .reconcile_and_candidates(namespace.namespace.as_deref().unwrap(), &[item("d-1", 10)])
            .expect("reconcile")
            .is_empty());
        assert!(store
            .visible(namespace.namespace.as_deref().unwrap())
            .is_empty());
        remove_all_best_effort(&root);
    }
}
