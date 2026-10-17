//! The requisition lifecycle over real Postgres: the approval gate on
//! opening, and the hold / resume / close / cancel verbs that move a
//! requisition's write-protected status.
//!
//! Hermetic like `hire_flow.rs`: a private scratch database per test, the
//! minimal inline DDL the verbs touch, and a SKIP (not a failure) when no
//! database is reachable. Set `DATABASE_URL` to run it for real.

use std::sync::Arc;

use async_trait::async_trait;
use backbone_recruitment::application::service::recruitment_approvals_port::{
    OfferFiling, RecruitmentFilingPort, RecruitmentSeamError, RecruitmentVerdict,
    RequisitionFiling,
};
use backbone_recruitment::application::service::requisition_lifecycle::{
    RequisitionError, RequisitionLifecycleService,
};
use sqlx::PgPool;
use uuid::Uuid;

/// A private scratch database for the calling test, or `None` to skip.
async fn connect(suffix: &str) -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/backbone_hr".into());
    let (prefix, _) = url.trim_end_matches('/').rsplit_once('/')?;
    let admin = match PgPool::connect(&format!("{prefix}/postgres")).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("skip requisition_lifecycle: could not reach `{prefix}/postgres` ({e}); set DATABASE_URL to run");
            return None;
        }
    };
    let scratch = format!("recruitment_requisition_lifecycle_{suffix}");
    let _ = sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{scratch}" WITH (FORCE)"#))
        .execute(&admin)
        .await;
    sqlx::query(&format!(r#"CREATE DATABASE "{scratch}""#))
        .execute(&admin)
        .await
        .ok()?;
    admin.close().await;
    let pool = PgPool::connect(&format!("{prefix}/{scratch}")).await.ok()?;
    for ddl in [
        "CREATE SCHEMA recruitment",
        "CREATE TYPE requisition_status AS ENUM ('draft','open','on_hold','closed','cancelled')",
        r#"CREATE TABLE recruitment.job_requisitions (
               id UUID PRIMARY KEY,
               title TEXT NOT NULL DEFAULT 'Payroll analyst',
               headcount INTEGER NOT NULL DEFAULT 1,
               employment_type TEXT,
               status requisition_status NOT NULL DEFAULT 'draft',
               approval_request_id UUID,
               opened_by UUID NOT NULL,
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
    ] {
        sqlx::query(ddl).execute(&pool).await.ok()?;
    }
    Some(pool)
}

async fn requisition(pool: &PgPool, status: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO recruitment.job_requisitions (id, status, opened_by) \
         VALUES ($1, $2::requisition_status, $3)",
    )
    .bind(id)
    .bind(status)
    .bind(Uuid::new_v4())
    .execute(pool)
    .await
    .unwrap();
    id
}

async fn status_of(pool: &PgPool, id: Uuid) -> String {
    sqlx::query_scalar("SELECT status::text FROM recruitment.job_requisitions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// A wired engine that files every request and answers one fixed verdict.
struct Engine(RecruitmentVerdict);

#[async_trait]
impl RecruitmentFilingPort for Engine {
    async fn file_requisition(&self, _: &RequisitionFiling) -> Result<Uuid, RecruitmentSeamError> {
        Ok(Uuid::new_v4())
    }
    async fn file_offer(&self, _: &OfferFiling) -> Result<Uuid, RecruitmentSeamError> {
        Ok(Uuid::new_v4())
    }
    async fn status(&self, _: Uuid) -> Result<RecruitmentVerdict, RecruitmentSeamError> {
        Ok(self.0)
    }
}

fn wired(pool: &PgPool, verdict: RecruitmentVerdict) -> RequisitionLifecycleService {
    let svc = RequisitionLifecycleService::new(pool.clone());
    svc.set_approvals(Arc::new(Engine(verdict)));
    svc
}

/// With the engine wired, a draft that was never filed cannot be opened
/// directly: the open verb files it first, and the verdict decides.
#[tokio::test]
async fn a_wired_engine_refuses_to_open_an_unfiled_draft() {
    let Some(pool) = connect("unfiled").await else { return };
    let svc = wired(&pool, RecruitmentVerdict::Approved);
    let draft = requisition(&pool, "draft").await;

    let refused = svc.confirm_open(draft).await;
    assert!(
        matches!(refused, Err(RequisitionError::ApprovalNotFiled)),
        "an unfiled draft must not open past a wired engine, got {refused:?}"
    );
    assert_eq!(status_of(&pool, draft).await, "draft");

    // Filed and granted, it opens.
    assert!(svc.file_open(draft).await.unwrap().is_some());
    assert!(svc.confirm_open(draft).await.unwrap());
    assert_eq!(status_of(&pool, draft).await, "open");
}

/// A filed draft whose approval is still pending stays a draft.
#[tokio::test]
async fn a_pending_approval_keeps_the_draft_closed() {
    let Some(pool) = connect("pending").await else { return };
    let svc = wired(&pool, RecruitmentVerdict::Pending);
    let draft = requisition(&pool, "draft").await;
    assert!(svc.file_open(draft).await.unwrap().is_some());
    assert!(matches!(svc.confirm_open(draft).await, Err(RequisitionError::ApprovalNotGranted)));
    assert_eq!(status_of(&pool, draft).await, "draft");
}

/// Without an engine the draft opens directly, as before.
#[tokio::test]
async fn an_unwired_deployment_opens_directly() {
    let Some(pool) = connect("unwired").await else { return };
    let svc = RequisitionLifecycleService::new(pool.clone());
    let draft = requisition(&pool, "draft").await;
    assert!(svc.confirm_open(draft).await.unwrap());
    assert_eq!(status_of(&pool, draft).await, "open");
}

/// Hold, resume, close: each moves from the states it names, repeating a
/// verb changes nothing, and a final state refuses every move.
#[tokio::test]
async fn hold_resume_and_close_follow_the_machine() {
    let Some(pool) = connect("machine").await else { return };
    let svc = RequisitionLifecycleService::new(pool.clone());
    let req = requisition(&pool, "open").await;

    assert!(svc.hold(req).await.unwrap());
    assert_eq!(status_of(&pool, req).await, "on_hold");
    assert!(!svc.hold(req).await.unwrap(), "holding again is a no-op");

    assert!(svc.resume(req).await.unwrap());
    assert_eq!(status_of(&pool, req).await, "open");

    assert!(svc.hold(req).await.unwrap());
    assert!(svc.close(req).await.unwrap(), "a held requisition closes");
    assert_eq!(status_of(&pool, req).await, "closed");
    assert!(!svc.close(req).await.unwrap(), "closing again is a no-op");

    for refused in [svc.resume(req).await, svc.hold(req).await, svc.cancel(req).await] {
        assert!(
            matches!(refused, Err(RequisitionError::InvalidTransition { .. })),
            "a closed requisition stays closed, got {refused:?}"
        );
    }
    assert_eq!(status_of(&pool, req).await, "closed");

    let draft = requisition(&pool, "draft").await;
    assert!(matches!(svc.hold(draft).await, Err(RequisitionError::InvalidTransition { .. })));
    assert!(matches!(svc.close(draft).await, Err(RequisitionError::InvalidTransition { .. })));

    assert!(matches!(
        svc.hold(Uuid::new_v4()).await,
        Err(RequisitionError::NotFound(_))
    ));
}

/// Cancel withdraws a draft, an open or a held requisition, and nothing
/// moves it afterwards.
#[tokio::test]
async fn cancel_withdraws_anything_not_yet_closed() {
    let Some(pool) = connect("cancel").await else { return };
    let svc = RequisitionLifecycleService::new(pool.clone());
    for start in ["draft", "open", "on_hold"] {
        let req = requisition(&pool, start).await;
        assert!(svc.cancel(req).await.unwrap(), "cancel from {start}");
        assert_eq!(status_of(&pool, req).await, "cancelled");
        assert!(!svc.cancel(req).await.unwrap(), "cancelling again is a no-op");
        assert!(matches!(svc.resume(req).await, Err(RequisitionError::InvalidTransition { .. })));
        assert!(matches!(svc.close(req).await, Err(RequisitionError::InvalidTransition { .. })));
    }
}
