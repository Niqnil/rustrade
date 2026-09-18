//! Export-job lifecycle tests for the London Strategic Edge vault.
//!
//! Every response here is **synthetic**, shaped from measurements of the live API. No provider
//! data is committed — the provider prohibits redistribution
//! (<https://londonstrategicedge.com/terms>), so `wiremock` is the only in-repo option.
//!
//! The measurements these encode, and which a change to `export.rs` must not regress:
//!
//! - Submit answers `202`, not `200`.
//! - The job identifier is `job_id` on submit and `id` on status.
//! - A `429` from an export endpoint means the allowance is exhausted, not a per-minute rate
//!   limit, and carries no `Retry-After`.
//! - The artifact download honours `Range` with a `206`, and the job record carries a `sha256`.
//! - A rejected submit costs an export, so anything checkable is rejected before it is sent.
//!
//! Run with: `cargo test --test lse_export --features lse`

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use rustrade_data::exchange::lse::error::LseError;
use rustrade_data::exchange::lse::export::{
    LseExportJobStatus, LseExportRange, LseExportRequest, LseExportStatus, LseExportTimeframe,
};
use rustrade_data::exchange::lse::market::LseDataset;
use rustrade_data::exchange::lse::vault::LseVaultClient;
use rustrade_data::subscription::candle::CandleInterval;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;
use wiremock::matchers::{body_json_string, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// The in-progress filename `download_export` uses, mirrored here rather than exported.
///
/// It is scoped to the **job**, not to the destination alone, which is what makes "a leftover
/// in-progress file is a prefix of the artifact being fetched" an invariant of the name. Several
/// tests below depend on that scoping either holding or being visibly broken, so they compute the
/// path the same way the implementation does.
fn part_path(destination: &Path, job_id: &str) -> PathBuf {
    let mut name = destination.as_os_str().to_owned();
    name.push(format!(".{job_id}.part"));
    PathBuf::from(name)
}

/// A destination the guard under test must reject before the filesystem is ever touched.
///
/// These tests assert that a *client-side* check fires, not that a path is unwritable, so any name
/// that is never created will do. Portable where a `/dev/null/...` sentinel is not.
fn unwritten_destination(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("never-written.parquet")
}

fn client(server: &MockServer) -> LseVaultClient {
    LseVaultClient::new("test-key")
        .unwrap()
        .with_base_url(format!("{}/vault", server.uri()))
        .with_pace(Duration::ZERO)
}

fn range() -> LseExportRange {
    LseExportRange::new("2026-07-01".parse().unwrap(), "2026-07-02".parse().unwrap()).unwrap()
}

fn tick_request() -> LseExportRequest {
    LseExportRequest::new(LseDataset::Fx, "EUR/USD", LseExportTimeframe::Tick, range()).unwrap()
}

/// The measured submit response: `202`, `job_id`, `status: queued`.
fn submit_body(job_id: &str) -> String {
    format!(
        r#"{{"job_id":"{job_id}","status":"queued","dataset":"fx","symbol":"EUR/USD",
            "timeframe":"tick","start":"2026-07-01","end":"2026-07-02","format":"parquet",
            "est_bytes":7111419512,"poll":"/export/{job_id}"}}"#
    )
}

/// The measured ready-job record, including the `sha256` this integration verifies against.
fn ready_body(job_id: &str, payload: &[u8]) -> String {
    let digest = hex::encode(Sha256::digest(payload));
    format!(
        r#"{{"id":"{job_id}","status":"ready","dataset":"fx","table_name":"ticks_fx",
            "symbol":"EUR/USD","start":"2026-07-01","end":"2026-07-02","format":"parquet",
            "rows":64958,"bytes":{},"sha256":"{digest}","error":null,
            "created_at":"2026-07-28T02:06:49.072387+00:00",
            "updated_at":"2026-07-28T02:07:44.862841+00:00",
            "expires_at":"2026-07-30T02:07:44.862806+00:00","timeframe":"tick",
            "download_url":"/export/{job_id}/download"}}"#,
        payload.len()
    )
}

/// A `ready` job reporting neither `bytes` nor `sha256`.
///
/// Both fields are independent `Option`s, and every measured `ready` job carried both — but the
/// provider makes no guarantee, and the download path deliberately accepts an artifact it cannot
/// fully verify rather than refusing one the provider calls ready. These are the tests for what
/// that concession costs.
fn job_without_integrity_metadata(job_id: &str) -> LseExportJobStatus {
    serde_json::from_str(&format!(
        r#"{{"id":"{job_id}","status":"ready","symbol":"EUR/USD","format":"parquet"}}"#
    ))
    .unwrap()
}

/// A `ready` job reporting `sha256` but not `bytes`.
///
/// This is the only combination that reaches a **resume** at all. With `bytes` present, a `.part`
/// holding the whole artifact is declared complete before any request is sent; with *neither* field
/// present there is nothing to verify against, so `download_export` ignores the `.part` and transfers
/// in full rather than renaming bytes it never fetched. Only "digest, no length" both resumes and
/// leaves the completeness question to the server — which is what the `206` and `416` tests below
/// exercise.
fn job_with_sha256_only(job_id: &str, payload: &[u8]) -> LseExportJobStatus {
    serde_json::from_str(&format!(
        r#"{{"id":"{job_id}","status":"ready","symbol":"EUR/USD","format":"parquet",
            "sha256":"{}"}}"#,
        hex::encode(Sha256::digest(payload))
    ))
    .unwrap()
}

#[tokio::test]
async fn submit_accepts_the_202_the_provider_actually_returns() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/vault/export"))
        .respond_with(ResponseTemplate::new(202).set_body_string(submit_body("abc123")))
        .mount(&server)
        .await;

    let job = client(&server)
        .submit_export(&tick_request())
        .await
        .unwrap();

    assert_eq!(job.job_id, "abc123");
    assert_eq!(job.status, LseExportStatus::Queued);
}

#[tokio::test]
async fn submit_sends_the_measured_payload_shape() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/vault/export"))
        .and(header("x-api-key", "test-key"))
        .and(body_json_string(
            r#"{"dataset":"fx","symbol":"EUR/USD","timeframe":"tick","start":"2026-07-01","end":"2026-07-02","format":"parquet"}"#,
        ))
        .respond_with(ResponseTemplate::new(202).set_body_string(submit_body("abc123")))
        .mount(&server)
        .await;

    let job = client(&server)
        .submit_export(&tick_request())
        .await
        .unwrap();

    // The mock matches on the exact body, so a wrong payload would 404 — but pinning the decoded
    // job id costs nothing and makes the response half of the round trip an assertion too.
    assert_eq!(job.job_id, "abc123");
}

#[tokio::test]
async fn the_job_id_is_read_from_job_id_on_submit_and_id_on_status() {
    // The provider renames the field between the two responses. One shared struct silently fails
    // to deserialise, which is why these are two types.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/vault/export"))
        .respond_with(ResponseTemplate::new(202).set_body_string(submit_body("renamed")))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/vault/export/renamed"))
        .respond_with(ResponseTemplate::new(200).set_body_string(ready_body("renamed", b"data")))
        .mount(&server)
        .await;

    let client = client(&server);
    let job = client.submit_export(&tick_request()).await.unwrap();
    let status = client.export_status(&job.job_id).await.unwrap();

    assert_eq!(job.job_id, status.id);
}

#[tokio::test]
async fn an_exhausted_allowance_is_quota_exceeded_not_rate_limited() {
    // The measured rejection: 429, single-encoded detail, NO Retry-After. It is a different
    // condition from a per-minute rate limit, and only this one carries an allowance position.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/vault/export"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_string(r#"{"detail":"too many export requests; try again shortly"}"#),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/vault/usage"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"bytes_used_month":1,"bytes_cap_month":2,"bytes_used_week":1,"bytes_cap_week":2,
                "exports_this_hour":5,"exports_cap_hour":5,"historical_data_months":-1,
                "calls_per_minute":200,"max_rows_per_request":5000,"vault_concurrency":2}"#,
        ))
        .mount(&server)
        .await;

    let error = client(&server)
        .submit_export(&tick_request())
        .await
        .unwrap_err();

    let LseError::QuotaExceeded {
        status: Some(status),
    } = error
    else {
        panic!("expected QuotaExceeded carrying an allowance position, got {error:?}");
    };
    assert_eq!(status.exports_this_hour, 5);
    assert_eq!(status.exports_cap_hour, 5);
}

#[tokio::test]
async fn a_429_whose_usage_lookup_fails_is_still_quota_exceeded_without_a_position() {
    // No fabricated allowance position: if the follow-up call fails there is nothing truthful to put
    // in `status`, so it stays `None`. The *variant* must not change with it — a caller pacing itself
    // off `QuotaExceeded` has to see every exhausted allowance, including the ones where the usage
    // lookup happened to fail too, or it silently misses half of them.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/vault/export"))
        .respond_with(ResponseTemplate::new(429))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/vault/usage"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    assert!(matches!(
        client(&server).submit_export(&tick_request()).await,
        Err(LseError::QuotaExceeded { status: None })
    ));
}

#[tokio::test]
async fn await_export_returns_the_ready_job() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1"))
        .respond_with(ResponseTemplate::new(200).set_body_string(ready_body("job1", b"payload")))
        .mount(&server)
        .await;

    let status = client(&server)
        .await_export("job1", Duration::ZERO, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(status.status, LseExportStatus::Ready);
    assert_eq!(status.rows, Some(64958));
    assert_eq!(status.table_name.as_deref(), Some("ticks_fx"));
    assert!(status.expires_at.is_some());
}

#[tokio::test]
async fn a_failed_job_is_terminal_and_carries_the_providers_diagnostic() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"id":"job1","status":"failed","error":"upstream table unavailable"}"#,
        ))
        .mount(&server)
        .await;

    let error = client(&server)
        .await_export("job1", Duration::ZERO, Duration::from_secs(5))
        .await
        .unwrap_err();

    assert!(matches!(error, LseError::ExportFailed { .. }));
    assert!(error.to_string().contains("upstream table unavailable"));
}

#[tokio::test]
async fn a_job_that_never_readies_times_out_without_cancelling_it() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"id":"job1","status":"queued"}"#),
        )
        .mount(&server)
        .await;

    let error = client(&server)
        .await_export("job1", Duration::ZERO, Duration::ZERO)
        .await
        .unwrap_err();

    assert!(matches!(error, LseError::ExportTimeout { .. }));
    // The message must steer the caller to re-poll rather than re-export, which costs allowance.
    assert!(error.to_string().contains("poll it again"));
}

/// `timeout` must bound the call, not `poll_interval`. Sleeping the interval in full before
/// re-checking would make a long interval paired with a short timeout block for the interval — so
/// this pairs a 30s interval with a 50ms timeout and asserts the call returns in well under either.
#[tokio::test]
async fn a_short_timeout_is_not_extended_by_a_long_poll_interval() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"id":"job1","status":"queued"}"#),
        )
        .mount(&server)
        .await;

    let started = std::time::Instant::now();
    let error = client(&server)
        .await_export("job1", Duration::from_secs(30), Duration::from_millis(50))
        .await
        .unwrap_err();
    let elapsed = started.elapsed();

    assert!(matches!(error, LseError::ExportTimeout { .. }));
    // A wide margin: the point is to separate "capped at the deadline" from "slept 30s first",
    // not to pin the exact wake-up.
    assert!(
        elapsed < Duration::from_secs(5),
        "await_export took {elapsed:?}, so the sleep ran past the deadline instead of being cut short"
    );
}

/// `Duration::MAX` is a plausible "no timeout" sentinel, and `Instant + Duration` panics on
/// overflow. The deadline must saturate instead — computed before the first status request, so a
/// job that is already `ready` exercises it without waiting.
#[tokio::test]
async fn a_timeout_that_would_overflow_the_clock_does_not_panic() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"id":"job1","status":"ready"}"#),
        )
        .mount(&server)
        .await;

    let status = client(&server)
        .await_export("job1", Duration::ZERO, Duration::MAX)
        .await
        .unwrap();

    assert_eq!(status.status, LseExportStatus::Ready);
}

#[tokio::test]
async fn an_unknown_status_is_preserved_rather_than_guessed_at() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"id":"job1","status":"materialising"}"#),
        )
        .mount(&server)
        .await;

    let status = client(&server).export_status("job1").await.unwrap();

    assert_eq!(
        status.status,
        LseExportStatus::Other("materialising".to_owned())
    );
    assert!(!status.status.is_terminal());
}

#[tokio::test]
async fn download_verifies_the_sha256_and_renames_atomically() {
    let payload = b"synthetic parquet bytes";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.to_vec()))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let client = client(&server);
    let job = serde_json::from_str(&ready_body("job1", payload)).unwrap();

    let export = client
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap();

    assert_eq!(export.path(), destination);
    assert_eq!(export.dataset(), LseDataset::Fx);
    assert_eq!(export.exchange_id(), LseDataset::Fx.exchange_id());
    assert_eq!(std::fs::read(&destination).unwrap(), payload);
    // The in-progress file is gone, so nothing is left to confuse a later resume.
    assert!(!part_path(&destination, "job1").exists());
}

#[tokio::test]
async fn a_truncated_download_keeps_the_partial_file_and_resumes_from_it() {
    // The common failure, not the rare one: the connection drops mid-transfer. The `.part` is
    // shorter than the artifact, so those bytes are a real prefix -- KEPT, and the next call resumes
    // from them with a `Range` request instead of re-transferring everything. For a multi-gigabyte
    // artifact that is the difference between finishing and starting over, which is why the
    // retain/discard decision keys on *which* check failed rather than on the weaker "did this call
    // append anything?".
    let payload = b"the real payload";
    let server = MockServer::start().await;
    // Registered first so it wins the resume request; the catch-all below answers the initial one.
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .and(header("range", "bytes=9-"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 9-15/16")
                .set_body_bytes(payload[9..].to_vec()),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload[..9].to_vec()))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let part = part_path(&destination, "job1");
    let job: LseExportJobStatus = serde_json::from_str(&ready_body("job1", payload)).unwrap();
    let client = client(&server);

    let error = client
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        LseError::IntegrityMismatch {
            discarded: false,
            ..
        }
    ));
    // An incomplete artifact must never appear at the destination path.
    assert!(!destination.exists());
    // Kept, holding exactly the prefix that arrived.
    assert_eq!(std::fs::read(&part).unwrap(), &payload[..9]);

    // ...which is what lets the re-call finish rather than start over.
    client
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap();

    assert_eq!(std::fs::read(&destination).unwrap(), payload);
    assert!(!part.exists());
}

#[tokio::test]
async fn a_partial_file_from_a_different_job_is_never_resumed_onto() {
    // Two exports landing on one destination is ordinary usage -- re-exporting a range after the
    // provider corrects it, say. Sharing a single `.part` across them let job B resume onto job A's
    // bytes: the filename alone could not tell one job's prefix from another's, so the result was
    // neither artifact and cost a billed download to discover. Scoping the name to the job makes
    // that unrepresentable -- the leftover here is simply not this job's, so it is neither resumed
    // onto nor deleted.
    let payload = b"the real payload";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.to_vec()))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let foreign = part_path(&destination, "an-earlier-job");
    let leftovers = b"leftovers from an earlier job that used this same destination";
    std::fs::write(&foreign, leftovers).unwrap();

    let job: LseExportJobStatus = serde_json::from_str(&ready_body("job1", payload)).unwrap();

    client(&server)
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap();

    // Downloaded in full and verified, with no `Range` request sent.
    assert_eq!(std::fs::read(&destination).unwrap(), payload);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(!requests[0].headers.contains_key("range"));
    // The other job's in-progress file is untouched: it may still be being resumed by its own call.
    assert_eq!(std::fs::read(&foreign).unwrap(), leftovers);
}

#[tokio::test]
async fn an_oversized_partial_file_for_this_job_is_discarded_rather_than_failing_forever() {
    // The same job's `.part` holding more bytes than the artifact -- a crashed run that re-appended
    // a buffer, or a truncated `bytes` correction from the provider. It cannot be a prefix of
    // anything, so it is corrupt rather than incomplete. Worse, its length makes the file look
    // complete, so no transfer is attempted and it fails verification unfetched. Keeping it would
    // take exactly this branch on every retry, and the documented "re-call to resume" would never
    // converge.
    let payload = b"the real payload";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.to_vec()))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let part = part_path(&destination, "job1");
    std::fs::write(
        &part,
        b"more bytes than this job's artifact could ever hold",
    )
    .unwrap();

    let job: LseExportJobStatus = serde_json::from_str(&ready_body("job1", payload)).unwrap();
    let client = client(&server);

    let error = client
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        LseError::IntegrityMismatch {
            discarded: true,
            ..
        }
    ));
    assert!(!destination.exists());
    // Discarded, not kept: that is what lets the retry below make progress.
    assert!(!part.exists());
    // Nothing was requested, because the oversized file looked complete.
    assert!(server.received_requests().await.unwrap().is_empty());

    // The same call now succeeds instead of failing identically forever.
    client
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap();

    assert_eq!(std::fs::read(&destination).unwrap(), payload);
}

#[tokio::test]
async fn an_interrupted_download_resumes_from_the_partial_file() {
    // The provider advertises `Accept-Ranges: bytes` and answers `206`, so resume is real. The
    // hash must cover the prefix that was never re-fetched, or verification is meaningless.
    let payload = b"0123456789abcdef";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .and(header("range", "bytes=10-"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 10-15/16")
                .set_body_bytes(payload[10..].to_vec()),
        )
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    std::fs::write(part_path(&destination, "job1"), &payload[..10]).unwrap();

    let job = serde_json::from_str(&ready_body("job1", payload)).unwrap();
    client(&server)
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap();

    assert_eq!(std::fs::read(&destination).unwrap(), payload);
}

#[tokio::test]
async fn a_server_that_ignores_range_restarts_the_download_instead_of_appending() {
    // A server is entitled to answer a `Range` request with `200` and the whole artifact. Appending
    // that to the existing prefix would double it, so the transfer must restart from zero -- hasher
    // included, or the digest would cover the duplicated bytes and reject a correct download.
    let payload = b"0123456789abcdef";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .and(header("range", "bytes=10-"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.to_vec()))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let part = part_path(&destination, "job1");
    std::fs::write(&part, &payload[..10]).unwrap();

    let job = serde_json::from_str(&ready_body("job1", payload)).unwrap();
    client(&server)
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap();

    // Exactly the artifact -- not the prefix twice over.
    assert_eq!(std::fs::read(&destination).unwrap(), payload);
    assert!(!part.exists());
}

#[tokio::test]
async fn a_206_resuming_from_the_wrong_offset_is_rejected_before_it_reaches_the_file() {
    // The digest would catch this eventually, but only after appending the misplaced bytes and
    // re-transferring the whole artifact to find out. The `Content-Range` says what actually went
    // wrong, at the point it went wrong, and leaves the prefix resumable.
    let payload = b"0123456789abcdef";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .and(header("range", "bytes=10-"))
        .respond_with(
            // Asked for byte 10, answered from byte 4.
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 4-15/16")
                .set_body_bytes(payload[4..].to_vec()),
        )
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let part = part_path(&destination, "job1");
    std::fs::write(&part, &payload[..10]).unwrap();

    let error = client(&server)
        .download_export(
            &job_with_sha256_only("job1", payload),
            &destination,
            &tick_request(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, LseError::Api { status: 206, .. }));
    assert!(!destination.exists());
    // The prefix is untouched, so a later correct `206` still resumes from it.
    assert_eq!(std::fs::read(&part).unwrap(), &payload[..10]);
}

#[tokio::test]
async fn a_206_with_no_usable_content_range_is_rejected_rather_than_assumed_to_resume() {
    // The other two arms of the same seam check. RFC 9110 §15.3.7 requires `Content-Range` on a
    // `206`, so absent or unparseable is a protocol violation -- and treating either as benign would
    // append bytes at an offset the server never claimed, leaving a file that is neither the artifact
    // nor a resumable prefix of it.
    let payload = b"0123456789abcdef";

    for content_range in [None, Some("bytes abc-15/16")] {
        let server = MockServer::start().await;
        let mut response = ResponseTemplate::new(206).set_body_bytes(payload[10..].to_vec());
        if let Some(value) = content_range {
            response = response.insert_header("content-range", value);
        }
        Mock::given(method("GET"))
            .and(path("/vault/export/job1/download"))
            .and(header("range", "bytes=10-"))
            .respond_with(response)
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let destination = dir.path().join("out.parquet");
        let part = part_path(&destination, "job1");
        std::fs::write(&part, &payload[..10]).unwrap();

        let error = client(&server)
            .download_export(
                &job_with_sha256_only("job1", payload),
                &destination,
                &tick_request(),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, LseError::Api { status: 206, .. }),
            "Content-Range {content_range:?} should be rejected, got {error:?}"
        );
        assert!(!destination.exists());
        // Kept, not discarded: nothing was appended, and the prefix is still a real one that a
        // correct `206` can resume from.
        assert_eq!(std::fs::read(&part).unwrap(), &payload[..10]);
    }
}

#[tokio::test]
async fn a_416_on_an_already_complete_part_file_converges_instead_of_failing_forever() {
    // A run interrupted between the final write and the rename leaves a `.part` holding the whole
    // artifact. Without `bytes` on the job there is no way to know that up front, so the resume is
    // attempted and earns a spec-compliant `416`. Treating that as an error would make every retry
    // fail identically, contradicting the documented "re-calling resumes" -- and the digest, which is
    // present here, confirms the file really is the artifact before it is renamed.
    let payload = b"0123456789abcdef";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .and(header("range", "bytes=16-"))
        .respond_with(ResponseTemplate::new(416))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let part = part_path(&destination, "job1");
    std::fs::write(&part, payload).unwrap();

    let export = client(&server)
        .download_export(
            &job_with_sha256_only("job1", payload),
            &destination,
            &tick_request(),
        )
        .await
        .unwrap();

    assert_eq!(export.path(), destination);
    assert_eq!(std::fs::read(&destination).unwrap(), payload);
    assert!(!part.exists());
}

#[tokio::test]
async fn a_416_on_a_part_file_that_fails_verification_discards_it_and_converges() {
    // The other half of the `416` story. This job's own `.part` can reach the artifact's length
    // holding the wrong bytes — a disk error, or an earlier resume that appended at a bad offset —
    // so the resume earns a `416` and then fails the digest. The file still has to go: with the
    // server refusing to serve anything more, keeping it would earn the same `416` and fail the same
    // hash on every retry.
    //
    // Reaching this needs `sha256` without `bytes`. With `bytes` present the length check would
    // have declared the file complete before any request, taking the no-transfer path instead — so
    // this combination is the only door to a discard decision made *after* talking to the server.
    let payload = b"0123456789abcdef";
    // Same length as the artifact, so the resume offset lands exactly on its end and earns a `416`.
    let stale = b"corrupted-bytes!";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .and(header("range", "bytes=16-"))
        .respond_with(ResponseTemplate::new(416))
        .mount(&server)
        .await;
    // The retry after the discard starts from scratch, so it carries no `Range` and falls through
    // to this one.
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.to_vec()))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let part = part_path(&destination, "job1");
    std::fs::write(&part, stale).unwrap();

    let job = job_with_sha256_only("job1", payload);
    let client = client(&server);

    let error = client
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        LseError::IntegrityMismatch {
            discarded: true,
            ..
        }
    ));
    assert!(!destination.exists());
    // Discarded, not kept: that is what lets the retry below make progress.
    assert!(!part.exists());
    // Unlike the oversized-`.part` discard, this one did reach the server — which is why the warning
    // it logs must not claim the file went unfetched.
    assert_eq!(server.received_requests().await.unwrap().len(), 1);

    // The same call now succeeds instead of failing identically forever.
    client
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap();

    assert_eq!(std::fs::read(&destination).unwrap(), payload);
    assert!(!part.exists());
}

#[tokio::test]
async fn a_fresh_transfer_of_the_right_length_at_the_wrong_digest_is_discarded_not_kept() {
    // The case a `416` is never reached for. The job reports `bytes`, no `.part` exists, and the
    // server answers `200` with the right *number* of bytes and the wrong content — a provider
    // regenerating the artifact between the status poll and the download, or any transport that
    // preserves length but not content.
    //
    // The length check passes, so only the digest fails, and the transfer is over: the artifact is
    // exactly as long as it will ever be and no `Range` request can add to it. Keeping the `.part`
    // would leave a corrupt file at the artifact's true length, indistinguishable at rest from a
    // legitimate one, for a caller that treats the hard `Err` as terminal and never retries.
    let expected = b"0123456789abcdef";
    // Same length, different bytes.
    let served = b"corrupted-bytes!";
    assert_eq!(expected.len(), served.len());

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(served.to_vec()))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let part = part_path(&destination, "job1");
    let job = serde_json::from_str(&ready_body("job1", expected)).unwrap();

    let error = client(&server)
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap_err();

    assert!(
        matches!(
            error,
            LseError::IntegrityMismatch {
                discarded: true,
                ..
            }
        ),
        "a complete-but-corrupt artifact is not resumable: {error:?}"
    );
    // Fails safe either way — the corrupt bytes never reach the destination.
    assert!(!destination.exists());
    assert!(
        !part.exists(),
        "the corrupt .part must not survive at the artifact's true length"
    );
}

#[tokio::test]
async fn a_job_with_no_integrity_metadata_ignores_a_partial_file_and_transfers_in_full() {
    // With neither `bytes` nor `sha256` there is nothing to verify the result against, so resuming
    // would rename a file into place having fetched only part of it and checked none of it — the
    // `.part` would be accepted on the strength of its name alone. A download spends no export
    // allowance (the five-per-hour limit is on submits), so restarting costs bandwidth and nothing
    // else, and what lands at the destination is at least something this call fetched end to end.
    let payload = b"0123456789abcdef";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.to_vec()))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let part = part_path(&destination, "job1");
    std::fs::write(&part, &payload[..10]).unwrap();

    client(&server)
        .download_export(
            &job_without_integrity_metadata("job1"),
            &destination,
            &tick_request(),
        )
        .await
        .unwrap();

    // The whole artifact, exactly once -- the prefix was overwritten, not appended to.
    assert_eq!(std::fs::read(&destination).unwrap(), payload);
    assert!(!part.exists());
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(!requests[0].headers.contains_key("range"));
}

#[tokio::test]
async fn a_job_describing_a_different_request_is_rejected_before_a_byte_is_fetched() {
    // This provider silently substitutes defaults for parameters it does not recognise, so a request
    // it partly ignored still readies a job -- covering another symbol, range or resolution. The
    // echoed fields are the only client-side evidence of that, and downloading anyway would attribute
    // one instrument's or one range's data to another with nothing downstream to catch it.
    let cases = [
        (r#""dataset":"stocks""#, "dataset", "stocks"),
        (r#""symbol":"GBP/USD""#, "symbol", "GBP/USD"),
        (r#""timeframe":"1d""#, "timeframe", "1d"),
        (r#""start":"2026-06-01""#, "start", "2026-06-01"),
        (r#""end":"2026-08-01""#, "end", "2026-08-01"),
    ];

    let dir = tempfile::tempdir().unwrap();

    for (echo, field, reported) in cases {
        let server = MockServer::start().await;
        let job: LseExportJobStatus = serde_json::from_str(&format!(
            r#"{{"id":"job1","status":"ready","format":"parquet",{echo}}}"#
        ))
        .unwrap();

        let error = client(&server)
            .download_export(&job, unwritten_destination(&dir), &tick_request())
            .await
            .unwrap_err();

        let LseError::ExportJobMismatch {
            field: got_field,
            reported: got_reported,
            ..
        } = error
        else {
            panic!("expected ExportJobMismatch for {echo}, got {error:?}");
        };
        assert_eq!(got_field, field);
        assert_eq!(got_reported, reported);
        // Rejected before any request, and before the destination's parent directory is created.
        assert!(server.received_requests().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn a_job_that_echoes_nothing_back_is_downloaded_rather_than_refused() {
    // Absence is not disagreement. Every echoed field is an independent `Option`, and refusing to
    // download over metadata the provider never promised would forbid a correct download with no way
    // around it -- the same concession the integrity checks make.
    let payload = b"0123456789abcdef";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.to_vec()))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let job: LseExportJobStatus =
        serde_json::from_str(r#"{"id":"job1","status":"ready","format":"parquet"}"#).unwrap();

    client(&server)
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap();

    assert_eq!(std::fs::read(&destination).unwrap(), payload);
}

#[tokio::test]
async fn downloading_a_job_that_is_not_ready_is_rejected_before_a_request_is_sent() {
    let server = MockServer::start().await;
    let job = serde_json::from_str(r#"{"id":"job1","status":"queued"}"#).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let destination = unwritten_destination(&dir);

    let error = client(&server)
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap_err();

    assert!(matches!(error, LseError::InvalidInput { .. }));
    // Nothing was requested: the guard is client-side.
    assert!(server.received_requests().await.unwrap().is_empty());
    // ...and nothing was created either, not even the parent directory.
    assert!(!destination.exists());
}

// ── pre-flight validation: every one of these would otherwise cost a real export ──

#[test]
fn a_candle_export_naming_all_is_rejected_because_it_silently_returns_nothing() {
    // Measured: `symbol: "all"` returns 202 -> ready -> a valid Parquet file with the full schema
    // and ZERO rows, with no error, and consumes an export.
    let error = LseExportRequest::new(
        LseDataset::Etf,
        "all",
        LseExportTimeframe::Candle(CandleInterval::Day1),
        range(),
    )
    .unwrap_err();

    assert!(matches!(error, LseError::InvalidInput { .. }));
    assert!(error.to_string().contains("EMPTY"));
}

#[test]
fn a_tick_export_naming_all_is_rejected_too_because_it_fails_the_same_way() {
    // Measured on the tick path as well: 202 -> ready -> zero rows. Combined with a missing symbol
    // being a hard 400, no spelling reaches more than one symbol, so every artifact is
    // single-symbol and this literal is never the right request on either path.
    let error = LseExportRequest::new(LseDataset::Fx, "all", LseExportTimeframe::Tick, range())
        .unwrap_err();

    assert!(matches!(error, LseError::InvalidInput { .. }));
    assert!(error.to_string().contains("EMPTY"));
}

#[test]
fn every_casing_but_the_uppercase_ticker_is_rejected() {
    // `ALL` exactly is Allstate's real, priced symbol (verified live against the provider's price
    // endpoint), so it is the one spelling that must get through -- a blanket case-insensitive guard
    // would forbid a correct export with no escape hatch, which is the failure mode such a rule is
    // meant to avoid. Every other casing is nobody's symbol: the provider publishes symbols
    // uppercase, and title case is exactly what a spreadsheet produces. Each one costs a billed
    // export to discover it matched nothing, so the guard covers them rather than only `"all"`.
    for symbol in ["All", "aLL", "aLl", "ALl"] {
        let error = LseExportRequest::new(
            LseDataset::Stocks,
            symbol,
            LseExportTimeframe::Candle(CandleInterval::Day1),
            range(),
        )
        .unwrap_err();

        assert!(
            matches!(error, LseError::InvalidInput { .. }),
            "{symbol:?} should be rejected"
        );
        // The rejection names the offending spelling, and points at the one that is a real ticker.
        assert!(error.to_string().contains(symbol));
        assert!(error.to_string().contains("\"ALL\""));
    }

    assert!(
        LseExportRequest::new(
            LseDataset::Stocks,
            "ALL",
            LseExportTimeframe::Candle(CandleInterval::Day1),
            range(),
        )
        .is_ok()
    );
}

#[test]
fn a_candle_export_of_a_tick_only_dataset_is_rejected() {
    // These serve candles over REST but are tick-only on the export path.
    for dataset in [
        LseDataset::Volatility,
        LseDataset::InterestRates,
        LseDataset::CurrencyIndex,
    ] {
        let error = LseExportRequest::new(
            dataset,
            "VIX/USD",
            LseExportTimeframe::Candle(CandleInterval::Day1),
            range(),
        )
        .unwrap_err();

        assert!(
            matches!(error, LseError::InvalidInput { .. }),
            "{dataset:?} should be rejected"
        );
        assert!(error.to_string().contains("tick-only"));
    }

    // The same datasets are exportable as ticks.
    assert!(
        LseExportRequest::new(
            LseDataset::Volatility,
            "VIX/USD",
            LseExportTimeframe::Tick,
            range()
        )
        .is_ok()
    );
}

#[test]
fn a_resolution_the_provider_does_not_serve_is_rejected_before_it_is_billed() {
    let error = LseExportRequest::new(
        LseDataset::Etf,
        "SPY",
        LseExportTimeframe::Candle(CandleInterval::Hour2),
        range(),
    )
    .unwrap_err();

    assert!(matches!(error, LseError::UnsupportedInterval { .. }));
}

#[test]
fn a_blank_symbol_is_rejected_rather_than_earning_a_400() {
    // Measured: omitting `symbol` returns `400 invalid or missing symbol` -- and still consumes an
    // export.
    for symbol in ["", "   "] {
        assert!(matches!(
            LseExportRequest::new(LseDataset::Fx, symbol, LseExportTimeframe::Tick, range()),
            Err(LseError::InvalidInput { .. })
        ));
    }
}

#[test]
fn an_inverted_or_empty_range_is_rejected() {
    let day = "2026-07-01".parse().unwrap();
    let earlier = "2026-06-01".parse().unwrap();

    assert!(matches!(
        LseExportRange::new(day, day),
        Err(LseError::InvalidInput { .. })
    ));
    assert!(matches!(
        LseExportRange::new(day, earlier),
        Err(LseError::InvalidInput { .. })
    ));
}

#[test]
fn a_range_starting_more_than_a_day_after_today_is_wholly_in_the_future() {
    let today: chrono::NaiveDate = "2026-07-01".parse().unwrap();
    let day_after = |date: chrono::NaiveDate, days| date + chrono::Days::new(days);

    // A fat-fingered year is the failure this exists for; it misses by hundreds of days.
    let next_year = LseExportRange::new(day_after(today, 365), day_after(today, 366)).unwrap();
    assert!(next_year.is_wholly_after(today));

    // The documented day of slack: a caller at UTC+14 can legitimately mean UTC's tomorrow by
    // "today", so the boundary sits at today + 1, not at today.
    let tomorrow = LseExportRange::new(day_after(today, 1), day_after(today, 2)).unwrap();
    assert!(!tomorrow.is_wholly_after(today));

    let day_after_tomorrow = LseExportRange::new(day_after(today, 2), day_after(today, 3)).unwrap();
    assert!(day_after_tomorrow.is_wholly_after(today));

    // "Wholly" is the operative word: a range that merely ENDS in the future still holds data.
    let straddling = LseExportRange::new(today, day_after(today, 30)).unwrap();
    assert!(!straddling.is_wholly_after(today));

    let past = LseExportRange::new("2026-06-01".parse().unwrap(), today).unwrap();
    assert!(!past.is_wholly_after(today));
}

/// The check exists to stop an export being spent, so the assertion that matters is that the
/// request never left the process.
#[tokio::test]
async fn a_wholly_future_range_is_rejected_before_an_export_is_spent() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/vault/export"))
        .respond_with(
            ResponseTemplate::new(202).set_body_raw(submit_body("job1"), "application/json"),
        )
        .mount(&server)
        .await;

    // Relative to the same clock `submit_export` reads, so this cannot rot into a past range.
    let start = chrono::Utc::now().date_naive() + chrono::Days::new(400);
    let request = LseExportRequest::new(
        LseDataset::Fx,
        "EUR/USD",
        LseExportTimeframe::Tick,
        LseExportRange::new(start, start + chrono::Days::new(1)).unwrap(),
    )
    .unwrap();

    let error = client(&server).submit_export(&request).await.unwrap_err();

    assert!(matches!(error, LseError::InvalidInput { .. }));
    assert!(error.to_string().contains("EMPTY"));
    assert!(
        server.received_requests().await.unwrap().is_empty(),
        "the rejection must happen client-side: a submitted request costs an export"
    );
}

#[test]
fn the_uppercase_all_hatch_is_scoped_to_the_equity_datasets() {
    // "Matches nothing" is a property of the (dataset, symbol) PAIR: on `Fx` a symbol is
    // pair-shaped and on the synthetic classes it is a contract slug, so no casing of `ALL` names
    // anything there -- and letting it through spends a billed export to learn that.
    for dataset in [LseDataset::Fx, LseDataset::Volatility] {
        let error =
            LseExportRequest::new(dataset, "ALL", LseExportTimeframe::Tick, range()).unwrap_err();

        assert!(
            matches!(error, LseError::InvalidInput { .. }),
            "{dataset:?} should reject \"ALL\""
        );
        assert!(error.to_string().contains("EMPTY"));
        // The Allstate hint must not appear on a dataset carrying no such listing.
        assert!(
            !error.to_string().contains("Allstate"),
            "{dataset:?} must not send the reader after a listing it does not carry"
        );
    }

    // Both equity classes keep the hatch -- `Etf` as well as `Stocks`, since they share one venue
    // and one symbology.
    for dataset in [LseDataset::Stocks, LseDataset::Etf] {
        assert!(
            LseExportRequest::new(dataset, "ALL", LseExportTimeframe::Tick, range()).is_ok(),
            "{dataset:?} must admit Allstate's real ticker"
        );
    }
}

// -----------------------------------------------------------------------------------------------
// Credential containment
// -----------------------------------------------------------------------------------------------

/// The download endpoint is the one that genuinely invites a redirect — a multi-gigabyte artifact
/// is exactly the shape that hands off to object storage — and it carries the same client-wide
/// `x-api-key` every other call does. reqwest strips only `Authorization`, `Cookie`, `Cookie2`,
/// `Proxy-Authorization` and `WWW-Authenticate` across hosts, so under the default
/// `Policy::limited(10)` that key would ride a `302 Location: https://<bucket>/…` straight to the
/// bucket host. `Policy::none()` surfaces the 3xx unfollowed instead; the assertion that matters is
/// that the redirect target is never contacted, which holds regardless of reqwest's stripping list.
#[tokio::test]
async fn a_download_does_not_follow_a_redirect_off_host() {
    let bucket = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"artifact".to_vec()))
        .mount(&bucket)
        .await;

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .respond_with(ResponseTemplate::new(302).insert_header(
            "location",
            format!("{}/bucket/job1.parquet", bucket.uri()).as_str(),
        ))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let job: LseExportJobStatus = serde_json::from_str(&ready_body("job1", b"artifact")).unwrap();

    let error = client(&server)
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap_err();

    assert!(
        matches!(error, LseError::Api { status, .. } if (300..400).contains(&status)),
        "expected an unfollowed 3xx as LseError::Api, got {error:?}"
    );
    assert!(
        bucket.received_requests().await.unwrap().is_empty(),
        "the client must not follow a redirect off-host: the x-api-key would ride along"
    );
    // Nothing was written, so no `.part` is left claiming progress against this job.
    assert!(!part_path(&destination, "job1").exists());
}
