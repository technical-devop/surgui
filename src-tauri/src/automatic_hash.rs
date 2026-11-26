//! Completion-gated automatic hashing worker.
//!
//! The worker only ever sees daemon-provided download ids from persisted
//! intents. It refreshes the authenticated `/list`, requires a unique fully
//! finalized record with no running hook, hashes the daemon-reported file
//! through [`crate::file_hash`], revalidates the same record and connection,
//! and only then publishes both digests. A single worker per manager is
//! enforced by the caller; this module is also usable in isolation in tests.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::file_hash;
use crate::hash_state::HashCompleted;
use crate::surge_client::{ConnectError, Download, SurgeClient};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompletionDecision {
    Ready,
    Pending,
    Failed,
    Unavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HashWorkError {
    Daemon,
    Internal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum HashOneOutcome {
    Completed(HashCompleted),
    Pending,
    Failed,
    Unavailable,
    Stale,
    Cancelled,
}

pub(crate) fn completion_decision(download: &Download, hook_applies: bool) -> CompletionDecision {
    match download.status.as_str() {
        "completed" => {}
        "error" => return CompletionDecision::Failed,
        _ => return CompletionDecision::Pending,
    }
    if download.moving || download.processing {
        return CompletionDecision::Pending;
    }
    if download.file_missing {
        return CompletionDecision::Unavailable;
    }
    match download.phase.as_deref() {
        None | Some("") | Some("completed") => {}
        Some(_) => return CompletionDecision::Pending,
    }
    if hook_applies && !download.processed {
        return CompletionDecision::Pending;
    }
    if download
        .dest_path
        .as_deref()
        .is_none_or(|path| path.is_empty())
    {
        return CompletionDecision::Unavailable;
    }
    if download.downloaded < 0 || download.total_size < 0 {
        return CompletionDecision::Unavailable;
    }
    if download.total_size > 0 && download.downloaded != download.total_size {
        return CompletionDecision::Unavailable;
    }
    CompletionDecision::Ready
}

fn unique_download<'a>(downloads: &'a [Download], id: &str) -> Result<Option<&'a Download>, ()> {
    let mut matches = downloads.iter().filter(|download| download.id == id);
    match (matches.next(), matches.next()) {
        (Some(download), None) => Ok(Some(download)),
        (None, _) => Ok(None),
        (Some(_), Some(_)) => Err(()),
    }
}

fn decision_outcome(decision: CompletionDecision) -> Option<HashOneOutcome> {
    match decision {
        CompletionDecision::Ready => None,
        CompletionDecision::Pending => Some(HashOneOutcome::Pending),
        CompletionDecision::Failed => Some(HashOneOutcome::Failed),
        CompletionDecision::Unavailable => Some(HashOneOutcome::Unavailable),
    }
}

/// Apply the same completion and hook rules used before hashing. Settings are
/// fetched only when the non-hook checks pass; a malformed or failed settings
/// response surfaces as [`HashWorkError::Daemon`] and nothing is published.
pub(crate) async fn completion_state(
    client: &SurgeClient,
    download: &Download,
) -> Result<CompletionDecision, HashWorkError> {
    let preliminary = completion_decision(download, false);
    if preliminary != CompletionDecision::Ready {
        return Ok(preliminary);
    }
    let hook_applies = if download.processed {
        true
    } else {
        client
            .post_download_hook_applies(&download.filename, download.category_override.as_deref())
            .await
            .map_err(map_connect_error)?
    };
    Ok(completion_decision(download, hook_applies))
}

pub(crate) async fn hash_one<F>(
    client: &SurgeClient,
    id: &str,
    expected_added_at: i64,
    expected_fingerprint: &str,
    cancel: &Arc<AtomicBool>,
    mut should_continue: F,
) -> Result<HashOneOutcome, HashWorkError>
where
    F: FnMut() -> bool,
{
    if cancel.load(Ordering::SeqCst) || !should_continue() {
        return Ok(HashOneOutcome::Cancelled);
    }
    let first_list = client.refresh().await.map_err(map_connect_error)?;
    let first = match unique_download(&first_list, id) {
        Ok(Some(download)) => download.clone(),
        Ok(None) => return Ok(HashOneOutcome::Stale),
        Err(()) => return Ok(HashOneOutcome::Unavailable),
    };
    // The persisted provenance must match the daemon's stable per-download
    // value and queue fingerprint. A reused id is never hashed.
    if first.added_at != expected_added_at
        || crate::hash_state::queue_fingerprint(&first.id, first.added_at, &first.url)
            != expected_fingerprint
    {
        return Ok(HashOneOutcome::Stale);
    }

    if let Some(outcome) = decision_outcome(completion_state(client, &first).await?) {
        return Ok(outcome);
    }

    let Some(dest_path) = first.dest_path.clone() else {
        return Ok(HashOneOutcome::Unavailable);
    };
    let expected_size = first.downloaded as u64;
    let added_at = first.added_at;
    let fingerprint = crate::hash_state::queue_fingerprint(&first.id, first.added_at, &first.url);
    let total_size = first.total_size;
    let download_path = dest_path.clone();
    let cancel_for_hash = Arc::clone(cancel);
    let digests = match tokio::task::spawn_blocking(move || {
        file_hash::hash_file(&download_path, Some(expected_size), &cancel_for_hash)
    })
    .await
    {
        Ok(Ok(digests)) => digests,
        Ok(Err(_)) => {
            if cancel.load(Ordering::SeqCst) || !should_continue() {
                return Ok(HashOneOutcome::Cancelled);
            }
            return Ok(HashOneOutcome::Unavailable);
        }
        Err(_) => return Err(HashWorkError::Internal),
    };

    if cancel.load(Ordering::SeqCst) || !should_continue() {
        return Ok(HashOneOutcome::Cancelled);
    }
    let second_list = client.refresh().await.map_err(map_connect_error)?;
    let second = match unique_download(&second_list, id) {
        Ok(Some(download)) => download,
        Ok(None) => return Ok(HashOneOutcome::Stale),
        Err(()) => return Ok(HashOneOutcome::Unavailable),
    };
    if second.added_at != added_at
        || second.added_at != expected_added_at
        || crate::hash_state::queue_fingerprint(&second.id, second.added_at, &second.url)
            != fingerprint
        || second.dest_path.as_deref() != Some(dest_path.as_str())
    {
        return Ok(HashOneOutcome::Stale);
    }
    if second.downloaded != first.downloaded || second.total_size != total_size {
        return Ok(HashOneOutcome::Unavailable);
    }
    // Re-check the hook configuration after the read. A malformed settings
    // response fails closed through `HashWorkError::Daemon` and nothing is
    // published. A hook that became applicable while hashing keeps the record
    // pending until the daemon reports it completed.
    if let Some(outcome) = decision_outcome(completion_state(client, second).await?) {
        return Ok(outcome);
    }
    if cancel.load(Ordering::SeqCst) || !should_continue() {
        return Ok(HashOneOutcome::Cancelled);
    }
    Ok(HashOneOutcome::Completed(HashCompleted {
        added_at,
        md5: digests.md5,
        sha256: digests.sha256,
        size: digests.size,
        mtime_sec: digests.mtime_sec,
        mtime_nsec: digests.mtime_nsec,
        ctime_sec: digests.ctime_sec,
        ctime_nsec: digests.ctime_nsec,
        file_dev: digests.file_dev,
        file_ino: digests.file_ino,
    }))
}

fn map_connect_error(_error: ConnectError) -> HashWorkError {
    HashWorkError::Daemon
}

#[cfg(test)]
mod tests {
    use super::*;

    fn download() -> Download {
        Download {
            id: "d-1".into(),
            url: "https://example.test/file.bin".into(),
            filename: "file.bin".into(),
            total_size: 3,
            downloaded: 3,
            progress: 100.0,
            speed: 0.0,
            status: "completed".into(),
            eta: 0,
            moving: false,
            file_missing: false,
            phase: None,
            category_override: None,
            processing: false,
            processed: false,
            hook_error: None,
            connections: 0,
            added_at: 10,
            time_taken: 1,
            avg_speed: 1.0,
            dest_path: Some("/tmp/file.bin".into()),
            error: None,
            rate_limit: None,
            rate_limit_set: None,
        }
    }

    #[test]
    fn completion_matrix_blocks_every_non_final_state() {
        assert_eq!(
            completion_decision(&download(), false),
            CompletionDecision::Ready
        );

        let mut value = download();
        value.status = "downloading".into();
        assert_eq!(
            completion_decision(&value, false),
            CompletionDecision::Pending
        );

        let mut value = download();
        value.phase = Some("finalizing".into());
        assert_eq!(
            completion_decision(&value, false),
            CompletionDecision::Pending
        );

        let mut value = download();
        value.phase = Some("verifying".into());
        assert_eq!(
            completion_decision(&value, false),
            CompletionDecision::Pending
        );

        let mut value = download();
        value.moving = true;
        assert_eq!(
            completion_decision(&value, false),
            CompletionDecision::Pending
        );

        let mut value = download();
        value.processing = true;
        assert_eq!(
            completion_decision(&value, false),
            CompletionDecision::Pending
        );

        let mut value = download();
        value.file_missing = true;
        assert_eq!(
            completion_decision(&value, false),
            CompletionDecision::Unavailable
        );

        let mut value = download();
        value.status = "error".into();
        assert_eq!(
            completion_decision(&value, false),
            CompletionDecision::Failed
        );

        let mut value = download();
        value.dest_path = None;
        assert_eq!(
            completion_decision(&value, false),
            CompletionDecision::Unavailable
        );

        let mut value = download();
        value.total_size = 4;
        assert_eq!(
            completion_decision(&value, false),
            CompletionDecision::Unavailable
        );
    }

    #[test]
    fn hooks_must_finish_before_ready() {
        let mut value = download();
        value.processing = true;
        assert_eq!(
            completion_decision(&value, true),
            CompletionDecision::Pending
        );

        let mut value = download();
        value.processed = false;
        assert_eq!(
            completion_decision(&value, true),
            CompletionDecision::Pending
        );
        value.processed = true;
        assert_eq!(completion_decision(&value, true), CompletionDecision::Ready);

        let mut value = download();
        value.processed = false;
        assert_eq!(
            completion_decision(&value, false),
            CompletionDecision::Ready
        );
    }

    #[test]
    fn completed_with_finalizing_phase_is_not_ready() {
        let mut value = download();
        value.status = "completed".into();
        value.phase = Some("finalizing".into());
        assert_eq!(
            completion_decision(&value, false),
            CompletionDecision::Pending
        );
    }
}
