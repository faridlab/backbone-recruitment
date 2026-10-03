//! The full-requisition refusals at the early doors, and the offer's agreed
//! start date reaching the hire event — over real Postgres.
//!
//! 1. `create_application` on a requisition whose openings are all filled
//!    refuses with the typed `no_open_headcount`, and admits while one is left;
//! 2. `create_draft` (the offer) refuses on a full requisition unless the
//!    application already sits in a hired stage (it holds one of the filled
//!    openings itself);
//! 3. an approved `extend` refuses on a full requisition BEFORE the offer
//!    moves (it stays a draft, no letter, no `offered_at`);
//! 4. `extend` records the promised first day on the offer, and `hire`
//!    stages a `recruitment.hired` payload whose `join_date` is that day.
//!
//! Hermetic like `hire_flow.rs`: a private scratch database per test, the
//! minimal inline DDL the verbs touch (no tenancy column — org scoping is
//! composition-installed), and a SKIP (not a failure) when no database is
//! reachable. Set `DATABASE_URL` to run it for real.

use std::sync::Arc;

use async_trait::async_trait;
use backbone_orm::org_scope::{with_org_request_scope, OrgScope};
use backbone_outbox::outbox;
use backbone_recruitment::application::service::recruitment_approvals_port::{
    OfferFiling, RecruitmentFilingPort, RecruitmentSeamError, RecruitmentVerdict,
    RequisitionFiling,
};
use backbone_recruitment::application::service::{
    ApplicationError, ExtendOptions, JobApplicationWriteService, JobOfferWriteService,
    NewJobApplication, NewJobOffer, NoOpenings, OfferError,
};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// A private scratch database for the calling test, or `None` to skip.
async fn connect(suffix: &str) -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/backbone_hr".into());
    let (prefix, _) = url.trim_end_matches('/').rsplit_once('/')?;
    let admin = match PgPool::connect(&format!("{prefix}/postgres")).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("skip vacancy_and_start_date: could not reach `{prefix}/postgres` ({e}); set DATABASE_URL to run");
            return None;
        }
    };
    let scratch = format!("recruitment_vacancy_test_{suffix}");
    let _ = sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{scratch}" WITH (FORCE)"#))
        .execute(&admin)
        .await;
    sqlx::query(&format!(r#"CREATE DATABASE "{scratch}""#))
        .execute(&admin)
        .await
        .ok()?;
    admin.close().await;
    match PgPool::connect(&format!("{prefix}/{scratch}")).await {
        Ok(p) => Some(p),
        Err(e) => {
            eprintln!("skip vacancy_and_start_date: could not connect to scratch db ({e})");
            None
        }
    }
}

async fn setup(pool: &PgPool) -> sqlx::Result<()> {
    let _ = sqlx::query(
        "CREATE TYPE offer_status AS ENUM ('draft','extended','accepted','declined','withdrawn')",
    )
    .execute(pool)
    .await;
    sqlx::query("CREATE SCHEMA IF NOT EXISTS recruitment")
        .execute(pool)
        .await?;
    for ddl in [
        r#"CREATE TABLE recruitment.candidates (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               first_name TEXT NOT NULL,
               last_name TEXT,
               email TEXT,
               phone TEXT
           )"#,
        r#"CREATE TABLE recruitment.job_requisitions (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               department_id UUID,
               position_id UUID,
               title TEXT NOT NULL DEFAULT 'Payroll analyst',
               headcount INTEGER NOT NULL,
               filled_headcount INTEGER NOT NULL DEFAULT 0,
               status TEXT NOT NULL DEFAULT 'open',
               opened_by UUID
           )"#,
        r#"CREATE TABLE recruitment.recruitment_stages (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               name TEXT NOT NULL,
               sequence INTEGER NOT NULL,
               is_hired BOOLEAN NOT NULL DEFAULT FALSE
           )"#,
        r#"CREATE TABLE recruitment.job_applications (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               candidate_id UUID NOT NULL,
               requisition_id UUID NOT NULL,
               stage_id UUID NOT NULL REFERENCES recruitment.recruitment_stages(id),
               last_stage_id UUID,
               stage_updated_at TIMESTAMPTZ,
               applied_at TIMESTAMPTZ,
               date_closed TIMESTAMPTZ,
               refused_at TIMESTAMPTZ,
               refuse_reason TEXT,
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
        r#"CREATE TABLE recruitment.job_offers (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               approval_request_id UUID,
               application_id UUID NOT NULL,
               proposed_salary NUMERIC,
               employment_type TEXT,
               letter_template_id UUID,
               status offer_status NOT NULL DEFAULT 'draft',
               offered_at TIMESTAMPTZ,
               accepted_at TIMESTAMPTZ,
               start_date DATE,
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
    ] {
        sqlx::query(ddl).execute(pool).await?;
    }
    outbox::migrate(pool, "recruitment")
        .await
        .expect("outbox migrate recruitment");
    Ok(())
}

/// The pipeline: an entry stage and a hired stage.
struct Pipeline {
    entry: Uuid,
    hired: Uuid,
}

async fn pipeline(pool: &PgPool) -> Pipeline {
    let entry = sqlx::query_scalar(
        "INSERT INTO recruitment.recruitment_stages (name, sequence) VALUES ('New', 10) RETURNING id",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    let hired = sqlx::query_scalar(
        "INSERT INTO recruitment.recruitment_stages (name, sequence, is_hired) VALUES ('Hired', 90, TRUE) RETURNING id",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    Pipeline { entry, hired }
}

async fn candidate(pool: &PgPool, first: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO recruitment.candidates (first_name, last_name, email, phone)
         VALUES ($1, 'Kusuma', $2, '+62811000111') RETURNING id",
    )
    .bind(first)
    .bind(format!("{}@example.com", first.to_lowercase()))
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn requisition(pool: &PgPool, headcount: i32, filled: i32) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO recruitment.job_requisitions (headcount, filled_headcount, department_id, position_id)
         VALUES ($1, $2, gen_random_uuid(), gen_random_uuid()) RETURNING id",
    )
    .bind(headcount)
    .bind(filled)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn application(pool: &PgPool, candidate_id: Uuid, requisition_id: Uuid, stage_id: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO recruitment.job_applications (candidate_id, requisition_id, stage_id)
         VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(candidate_id)
    .bind(requisition_id)
    .bind(stage_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// A draft offer already linked to an approval request the stub engine
/// says is approved.
async fn approved_draft_offer(pool: &PgPool, application_id: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO recruitment.job_offers (application_id, proposed_salary, employment_type, approval_request_id)
         VALUES ($1, 9000000, 'permanent', gen_random_uuid()) RETURNING id",
    )
    .bind(application_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// The approvals engine stand-in: every request is approved; filings are
/// not taken (the unwired answer keeps `create_draft` on the direct lane).
struct ApprovesEverything;

#[async_trait]
impl RecruitmentFilingPort for ApprovesEverything {
    async fn file_requisition(&self, _: &RequisitionFiling) -> Result<Uuid, RecruitmentSeamError> {
        Err(RecruitmentSeamError::Unwired)
    }
    async fn file_offer(&self, _: &OfferFiling) -> Result<Uuid, RecruitmentSeamError> {
        Err(RecruitmentSeamError::Unwired)
    }
    async fn status(&self, _: Uuid) -> Result<RecruitmentVerdict, RecruitmentSeamError> {
        Ok(RecruitmentVerdict::Approved)
    }
}

fn offers(pool: &PgPool) -> JobOfferWriteService {
    let svc = JobOfferWriteService::new(pool.clone());
    svc.set_approvals(Arc::new(ApprovesEverything));
    svc
}

#[tokio::test]
async fn a_full_requisition_refuses_a_new_application() -> Result<(), Box<dyn std::error::Error>> {
    let Some(pool) = connect("application").await else { return Ok(()) };
    setup(&pool).await?;
    let _ = pipeline(&pool).await;
    let svc = JobApplicationWriteService::new(pool.clone());

    let full = requisition(&pool, 2, 2).await;
    let who = candidate(&pool, "Rina").await;
    let err = svc
        .create_application(NewJobApplication { candidate_id: who, requisition_id: full })
        .await
        .expect_err("2 of 2 filled must refuse the application");
    match err {
        ApplicationError::NoOpenHeadcount(NoOpenings { requisition_id, headcount, filled }) => {
            assert_eq!((requisition_id, headcount, filled), (full, 2, 2));
        }
        other => panic!("expected no_open_headcount, got {other:?}"),
    }
    let rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM recruitment.job_applications WHERE requisition_id = $1",
    )
    .bind(full)
    .fetch_one(&pool)
    .await?;
    assert_eq!(rows, 0, "the refusal wrote nothing");

    let with_room = requisition(&pool, 2, 1).await;
    svc.create_application(NewJobApplication { candidate_id: who, requisition_id: with_room })
        .await
        .expect("a requisition with an opening left takes the application");
    Ok(())
}

#[tokio::test]
async fn a_full_requisition_refuses_an_offer_unless_the_application_holds_a_seat(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(pool) = connect("offer_draft").await else { return Ok(()) };
    setup(&pool).await?;
    let stages = pipeline(&pool).await;
    let svc = offers(&pool);
    let full = requisition(&pool, 2, 2).await;

    // Still in the entry stage: no seat of its own on a full requisition.
    let waiting = application(&pool, candidate(&pool, "Rina").await, full, stages.entry).await;
    let err = svc
        .create_draft(NewJobOffer {
            application_id: waiting,
            proposed_salary: Some(Decimal::new(9_000_000, 0)),
            employment_type: Some("permanent".into()),
            letter_template_id: None,
        })
        .await
        .expect_err("a full requisition refuses the offer");
    assert!(matches!(err, OfferError::NoOpenHeadcount(_)), "got {err:?}");
    assert_eq!(err.code(), "no_open_headcount");
    assert_eq!(err.http_status(), 409);

    // Already hired: it is one of the two filled openings — admitted.
    let seated = application(&pool, candidate(&pool, "Kirana").await, full, stages.hired).await;
    svc.create_draft(NewJobOffer {
        application_id: seated,
        proposed_salary: None,
        employment_type: None,
        letter_template_id: None,
    })
    .await
    .expect("the application holding a filled opening may still be offered");
    Ok(())
}

#[tokio::test]
async fn an_approved_offer_is_not_extended_on_a_full_requisition(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(pool) = connect("offer_extend").await else { return Ok(()) };
    setup(&pool).await?;
    let stages = pipeline(&pool).await;
    let svc = offers(&pool);

    // The requisition filled up after the offer was drafted and approved.
    let req = requisition(&pool, 2, 2).await;
    let app = application(&pool, candidate(&pool, "Rina").await, req, stages.entry).await;
    let offer = approved_draft_offer(&pool, app).await;

    let err = svc
        .extend(offer, ExtendOptions { start_date: NaiveDate::from_ymd_opt(2026, 11, 2), company_name: None })
        .await
        .expect_err("a full requisition refuses the extension");
    assert!(matches!(err, OfferError::NoOpenHeadcount(_)), "got {err:?}");

    let row = sqlx::query("SELECT status::text AS s, offered_at, start_date FROM recruitment.job_offers WHERE id = $1")
        .bind(offer)
        .fetch_one(&pool)
        .await?;
    assert_eq!(row.get::<String, _>("s"), "draft", "the offer did not move");
    assert!(row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("offered_at").is_none());
    assert!(row.get::<Option<NaiveDate>, _>("start_date").is_none());
    Ok(())
}

#[tokio::test]
async fn the_hire_joins_on_the_start_date_the_offer_was_extended_with(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(pool) = connect("start_date").await else { return Ok(()) };
    setup(&pool).await?;
    let stages = pipeline(&pool).await;
    let svc = offers(&pool);

    let req = requisition(&pool, 2, 1).await;
    let app = application(&pool, candidate(&pool, "Rina").await, req, stages.entry).await;
    let offer = approved_draft_offer(&pool, app).await;
    let first_day = NaiveDate::from_ymd_opt(2026, 11, 2).unwrap();

    assert!(svc
        .extend(offer, ExtendOptions { start_date: Some(first_day), company_name: None })
        .await?);
    let stored: Option<NaiveDate> =
        sqlx::query_scalar("SELECT start_date FROM recruitment.job_offers WHERE id = $1")
            .bind(offer)
            .fetch_one(&pool)
            .await?;
    assert_eq!(stored, Some(first_day), "extend recorded the promised first day");

    // The pipeline hires the application (consumes the last opening), then
    // the offer is hired.
    JobApplicationWriteService::new(pool.clone())
        .move_stage(app, stages.hired)
        .await?;
    let company_id = Uuid::new_v4();
    let event_id = with_org_request_scope(&pool, OrgScope::for_company_unit(company_id), async {
        svc.hire(offer).await
    })
    .await?
    .expect("fresh hire")
    .expect("a fresh hire stages an event");

    let payload: serde_json::Value =
        sqlx::query_scalar("SELECT payload FROM recruitment.outbox_events WHERE id = $1")
            .bind(event_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(payload["join_date"], "2026-11-02", "join date = the agreed first day: {payload}");
    assert_eq!(payload["start_date"], "2026-11-02");
    assert_eq!(payload["proposed_salary"], "9000000");
    assert_eq!(payload["employment_type"], "permanent");
    assert_eq!(payload["first_name"], "Rina");
    assert_eq!(payload["email"], "rina@example.com");
    assert_eq!(payload["requisition_id"], req.to_string());
    assert_eq!(payload["application_id"], app.to_string());
    assert_eq!(payload["position_title"], "Payroll analyst");
    Ok(())
}
