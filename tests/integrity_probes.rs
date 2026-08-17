//! Integrity probes — route-level. The guarded composition locks stateful
//! mutation behind verbs, the stage-driven invariants hold (vacancy coupling,
//! sticky refusal, hired-stage hire guard, fail-closed seams), and the
//! company fence holds cross-tenant.
//!
//! Every request runs behind the REAL `company_auth` middleware with a minted
//! HS256 token — the same mounting a composing service uses in production
//! (the family probe-suite harness pattern). The DB runs the strict fence
//! (RLS ENABLE+FORCE on every recruitment table) — but this suite connects
//! as the DB owner (a superuser, whom RLS can never bind; the fence
//! migration says the app connects as a non-superuser). So verbs carry their
//! company predicate in SQL (belt-and-braces — cross-tenant is a 404 even
//! here), raw assertion SQL runs inside `company_scope::with_company_scope`,
//! and the FENCE itself is pinned under `SET ROLE` to a plain non-superuser
//! (the composing-app posture): unbound sees zero rows, bound sees exactly
//! its company's rows.
//!
//! The module's outbound seams are default-UNWIRED (the family posture), so
//! route-level probes assert the fail-closed contract; the wired-success
//! paths (letter send, interviewer activity) run at the SERVICE level on a
//! separately-constructed service with a fake sink — exactly the shape a
//! composing service uses to wire the real adapters.
//!
//! Skill ids are validated against `learning.skills` — the test DB carries a
//! minimal hermetic `learning.skills` table (cross-module read, no Cargo
//! edge), the same fixture shape the lifecycle flow tests use for their
//! cross-schema inputs.
//!
//! DB: DATABASE_URL wins, else the module's local test DB
//! (`backbone_recruitment_test` on the metaphora dev postgres, migrated).
//! Fresh random company ids per test so parallel runs never collide.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::middleware::from_fn_with_state;
use backbone_auth::company::{company_auth, CompanyVerifier};
use backbone_recruitment::application::service::{
    ActivityAck, ActivityCommand, ActivityRejected, ActivitySink, ExtendOptions,
    InterviewWriteService, JobApplicationWriteService, JobOfferWriteService, LetterAck,
    LetterMessage, LetterRejected, NewInterview, NewJobApplication, NewJobOffer, OfferLetterSink,
};
use backbone_recruitment::RecruitmentModule;
use sqlx::{Acquire, PgPool};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"recruitment-integrity-probe-secret";
/// A fixed interview slot — deterministic, far in the future, RFC3339.
const WHEN: &str = "2027-01-15T09:00:00Z";

async fn pool() -> PgPool {
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://serpa:serpa_dev_password@127.0.0.1:5432/backbone_recruitment_test".into()
    });
    let pool = PgPool::connect(&url).await.unwrap();

    // The outbox DDL is framework-owned (`outbox::migrate`), not a module
    // migration — install it once, guarded: parallel tests racing the
    // CREATE SCHEMA inside migrate can trip a unique violation on
    // pg_namespace (the hazard the hire-flow suite documented first).
    static OUTBOX_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    let lock = OUTBOX_LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
    let _guard = lock.lock().await;
    backbone_outbox::outbox::migrate(&pool, "recruitment")
        .await
        .expect("outbox migrate recruitment");

    pool
}

fn token_for(company: Uuid) -> String {
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize
        + 3600;
    let claims = serde_json::json!({"sub": "integrity-probe", "company_id": company, "exp": exp});
    jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}

/// Status + body — probes must pin the stable error code, not just the status.
/// `token: None` sends no Authorization header at all (the unauth leg).
async fn req_full_opt(
    app: axum::Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: String,
) -> (StatusCode, String) {
    let app = app.route_layer(from_fn_with_state(
        CompanyVerifier::hs256(SECRET),
        company_auth,
    ));
    let mut b = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    let r = b.body(Body::from(body)).unwrap();
    let resp = app.oneshot(r).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

async fn req_full(
    app: axum::Router,
    method: &str,
    uri: &str,
    token: &str,
    body: String,
) -> (StatusCode, String) {
    req_full_opt(app, method, uri, Some(token), body).await
}

async fn req(app: axum::Router, method: &str, uri: &str, token: &str, body: String) -> StatusCode {
    req_full(app, method, uri, token, body).await.0
}

/// Scoped scalar read for assertions — binds `app.company_id` the way the
/// request scope does so the FORCE-fenced tables answer under RLS (an
/// unbound connection sees 0 rows by design).
async fn scoped_one<T>(pool: &PgPool, company: Uuid, sql: String) -> T
where
    T: Send + Unpin + for<'r> sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    backbone_orm::company_scope::with_company_scope(Some(company), async move {
        sqlx::query_scalar::<_, T>(&sql).fetch_one(pool).await.unwrap()
    })
    .await
}

/// Seed one company's world: three stages (first / mid / hired — the hired
/// one flagged), a candidate, an OPEN requisition (headcount 1), and a skill
/// in the learning fixture. Returns the ids a probe drives.
struct World {
    company: Uuid,
    #[allow(dead_code)]
    first_stage: Uuid,
    mid_stage: Uuid,
    hired_stage: Uuid,
    candidate: Uuid,
    requisition: Uuid,
    skill: Uuid,
}

#[allow(clippy::redundant_field_names)]
async fn seed_world(pool: &PgPool) -> World {
    use backbone_orm::company_scope;

    let company = Uuid::new_v4();
    let first_stage = Uuid::new_v4();
    let mid_stage = Uuid::new_v4();
    let hired_stage = Uuid::new_v4();
    let candidate = Uuid::new_v4();
    let requisition = Uuid::new_v4();
    let skill = Uuid::new_v4();

    company_scope::with_company_scope(Some(company), async {
        for (id, name, seq, hired) in [
            (first_stage, "Applied", 10, false),
            (mid_stage, "Interview", 50, false),
            (hired_stage, "Hired", 90, true),
        ] {
            sqlx::query(
                r#"INSERT INTO recruitment.recruitment_stages
                       (id, company_id, name, sequence, is_hired, folded, metadata)
                   VALUES ($1, $2, $3, $4, $5, FALSE, '{}'::jsonb)"#,
            )
            .bind(id)
            .bind(company)
            .bind(name)
            .bind(seq)
            .bind(hired)
            .execute(pool)
            .await
            .unwrap();
        }
        sqlx::query(
            r#"INSERT INTO recruitment.candidates
                   (id, company_id, first_name, last_name, email, metadata)
               VALUES ($1, $2, 'Ada', 'Lovelace', 'ada@example.com', '{}'::jsonb)"#,
        )
        .bind(candidate)
        .bind(company)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            r#"INSERT INTO recruitment.job_requisitions
                   (id, company_id, title, headcount, filled_headcount, status, opened_by, metadata)
               VALUES ($1, $2, 'Staff Accountant', 1, 0, 'open', $3, '{}'::jsonb)"#,
        )
        .bind(requisition)
        .bind(company)
        .bind(Uuid::new_v4())
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO learning.skills (id, company_id, name) VALUES ($1, $2, 'Bookkeeping')",
        )
        .bind(skill)
        .bind(company)
        .execute(pool)
        .await
        .unwrap();
    })
    .await;

    World { company, first_stage, mid_stage, hired_stage, candidate, requisition, skill }
}

/// Create an application through the real route (the only create path).
async fn create_application(
    app: axum::Router,
    t: &str,
    candidate: Uuid,
    requisition: Uuid,
) -> Uuid {
    let body = format!(r#"{{"candidate_id":"{candidate}","requisition_id":"{requisition}"}}"#);
    let (status, out) = req_full(app, "POST", "/applications", t, body).await;
    assert_eq!(status, StatusCode::CREATED, "application create: {out}");
    serde_json::from_str::<serde_json::Value>(&out).unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

// ─── Vacancy coupling: enter hired fills, leave releases, no headcount = 409 ────

#[tokio::test]
async fn move_stage_couples_the_requisition_vacancy() {
    let pool = pool().await;
    let w = seed_world(&pool).await;
    let m = RecruitmentModule::builder().with_database(pool.clone()).build().unwrap();
    let app = m.guarded_routes();
    let t = token_for(w.company);

    let application = create_application(app.clone(), &t, w.candidate, w.requisition).await;

    // Fresh application sits in the company's first stage, closing nothing.
    assert_eq!(
        scoped_one::<i32>(&pool, w.company,
            format!("SELECT filled_headcount FROM recruitment.job_requisitions WHERE id = '{}'", w.requisition)).await,
        0
    );

    // first → mid: no vacancy effect (neither stage is hired).
    let (s, out) = req_full(app.clone(), "POST", &format!("/applications/{application}/stage"), &t,
        format!(r#"{{"to_stage_id":"{}"}}"#, w.mid_stage)).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(
        scoped_one::<i32>(&pool, w.company,
            format!("SELECT filled_headcount FROM recruitment.job_requisitions WHERE id = '{}'", w.requisition)).await,
        0
    );

    // mid → hired: consumes the last opening, stamps date_closed.
    let s = req(app.clone(), "POST", &format!("/applications/{application}/stage"), &t,
        format!(r#"{{"to_stage_id":"{}"}}"#, w.hired_stage)).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        scoped_one::<i32>(&pool, w.company,
            format!("SELECT filled_headcount FROM recruitment.job_requisitions WHERE id = '{}'", w.requisition)).await,
        1
    );
    let closed: Option<chrono::DateTime<chrono::Utc>> = scoped_one(&pool, w.company,
        format!("SELECT date_closed FROM recruitment.job_applications WHERE id = '{application}'")).await;
    assert!(closed.is_some(), "hired stage stamps date_closed");

    // The derived projection agrees: hired.
    let (status, body) = req_full(app.clone(), "GET", &format!("/applications/{application}/pipeline"), &t, String::new()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["status"],
        "hired"
    );

    // hired → mid again: releases the opening, clears date_closed.
    let (s, out) = req_full(app.clone(), "POST", &format!("/applications/{application}/stage"), &t,
        format!(r#"{{"to_stage_id":"{}"}}"#, w.mid_stage)).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(
        scoped_one::<i32>(&pool, w.company,
            format!("SELECT filled_headcount FROM recruitment.job_requisitions WHERE id = '{}'", w.requisition)).await,
        0
    );
    let closed: Option<chrono::DateTime<chrono::Utc>> = scoped_one(&pool, w.company,
        format!("SELECT date_closed FROM recruitment.job_applications WHERE id = '{application}'")).await;
    assert!(closed.is_none(), "leaving the hired stage reopens the application");
}

#[tokio::test]
async fn entering_hired_without_openings_is_a_409() {
    let pool = pool().await;
    let w = seed_world(&pool).await;
    let m = RecruitmentModule::builder().with_database(pool.clone()).build().unwrap();
    let app = m.guarded_routes();
    let t = token_for(w.company);

    // Fill the single opening with a first application.
    let a1 = create_application(app.clone(), &t, w.candidate, w.requisition).await;
    let s = req(app.clone(), "POST", &format!("/applications/{a1}/stage"), &t,
        format!(r#"{{"to_stage_id":"{}"}}"#, w.hired_stage)).await;
    assert_eq!(s, StatusCode::OK);

    // A second candidate's application cannot also enter the hired stage.
    let second_candidate = Uuid::new_v4();
    backbone_orm::company_scope::with_company_scope(Some(w.company), async {
        sqlx::query(
            r#"INSERT INTO recruitment.candidates (id, company_id, first_name, metadata)
               VALUES ($1, $2, 'Second', '{}'::jsonb)"#,
        )
        .bind(second_candidate)
        .bind(w.company)
        .execute(&pool)
        .await
        .unwrap();
    })
    .await;
    let a2 = create_application(app.clone(), &t, second_candidate, w.requisition).await;
    let (status, body) = req_full(app.clone(), "POST", &format!("/applications/{a2}/stage"), &t,
        format!(r#"{{"to_stage_id":"{}"}}"#, w.hired_stage)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(
        body.contains("no_open_headcount"),
        "stable machine code in the body: {body}"
    );
}

// ─── Refusal is sticky and releases a held opening ──────────────────────────────

#[tokio::test]
async fn refuse_is_sticky() {
    let pool = pool().await;
    let w = seed_world(&pool).await;
    let m = RecruitmentModule::builder().with_database(pool.clone()).build().unwrap();
    let app = m.guarded_routes();
    let t = token_for(w.company);

    let application = create_application(app.clone(), &t, w.candidate, w.requisition).await;

    // Refusing an ongoing application closes it.
    let s = req(app.clone(), "POST", &format!("/applications/{application}/refuse"), &t,
        r#"{"reason":"went with another candidate"}"#.to_string()).await;
    assert_eq!(s, StatusCode::OK);

    // Double refuse: already refused.
    let (status, body) = req_full(app.clone(), "POST", &format!("/applications/{application}/refuse"), &t,
        r#"{"reason":"again"}"#.to_string()).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body.contains("already_refused"));

    // Refused applications never move.
    let (status, body) = req_full(app.clone(), "POST", &format!("/applications/{application}/stage"), &t,
        format!(r#"{{"to_stage_id":"{}"}}"#, w.mid_stage)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body.contains("already_refused"));
}

#[tokio::test]
async fn refusing_a_hired_application_releases_the_opening() {
    let pool = pool().await;
    let w = seed_world(&pool).await;
    let m = RecruitmentModule::builder().with_database(pool.clone()).build().unwrap();
    let app = m.guarded_routes();
    let t = token_for(w.company);

    let application = create_application(app.clone(), &t, w.candidate, w.requisition).await;
    let s = req(app.clone(), "POST", &format!("/applications/{application}/stage"), &t,
        format!(r#"{{"to_stage_id":"{}"}}"#, w.hired_stage)).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        scoped_one::<i32>(&pool, w.company,
            format!("SELECT filled_headcount FROM recruitment.job_requisitions WHERE id = '{}'", w.requisition)).await,
        1
    );

    let s = req(app.clone(), "POST", &format!("/applications/{application}/refuse"), &t,
        r#"{"reason":"reference check failed"}"#.to_string()).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        scoped_one::<i32>(&pool, w.company,
            format!("SELECT filled_headcount FROM recruitment.job_requisitions WHERE id = '{}'", w.requisition)).await,
        0,
        "refusing a hired application gives the opening back"
    );
}

// ─── Hire guard: the pipeline decides who is hired, not the offer ───────────────

#[tokio::test]
async fn hire_requires_the_application_in_a_hired_stage() {
    let pool = pool().await;
    let w = seed_world(&pool).await;
    let m = RecruitmentModule::builder().with_database(pool.clone()).build().unwrap();
    let app = m.guarded_routes();
    let t = token_for(w.company);

    let application = create_application(app.clone(), &t, w.candidate, w.requisition).await;
    let (status, body) = req_full(app.clone(), "POST", "/offers", &t,
        format!(r#"{{"application_id":"{application}","proposed_salary":5000000,"employment_type":"permanent"}}"#)).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let offer: Uuid = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
        .as_str().unwrap().parse().unwrap();

    // draft → extended while the application is still mid-pipeline.
    let s = req(app.clone(), "POST", &format!("/offers/{offer}/extend"), &t, String::new()).await;
    assert_eq!(s, StatusCode::OK);

    // hire BEFORE the application reaches a hired stage: refused with the
    // stable application_not_hired code.
    let (status, body) = req_full(app.clone(), "POST", &format!("/offers/{offer}/hire"), &t, String::new()).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(body.contains("application_not_hired"));

    // The offer state did not move.
    assert_eq!(
        scoped_one::<String>(&pool, w.company,
            format!("SELECT status::text FROM recruitment.job_offers WHERE id = '{offer}'")).await,
        "extended"
    );

    // Move the application to the hired stage, then hire: accepted + exactly
    // one staged recruitment.hired event (atomic accept+emit).
    let s = req(app.clone(), "POST", &format!("/applications/{application}/stage"), &t,
        format!(r#"{{"to_stage_id":"{}"}}"#, w.hired_stage)).await;
    assert_eq!(s, StatusCode::OK);
    let (status, body) = req_full(app.clone(), "POST", &format!("/offers/{offer}/hire"), &t, String::new()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        scoped_one::<String>(&pool, w.company,
            format!("SELECT status::text FROM recruitment.job_offers WHERE id = '{offer}'")).await,
        "accepted"
    );
    // Count scoped to THIS offer's aggregate id: the probe connection is the
    // DB owner (superuser — RLS does not filter it) and the outbox table
    // carries rows from earlier runs, so a bare event_type count would span
    // companies and runs.
    let staged: i64 = scoped_one(&pool, w.company,
        format!("SELECT count(*) FROM recruitment.outbox_events \
                 WHERE event_type = 'recruitment.hired' AND aggregate_id = '{offer}'")).await;
    assert_eq!(staged, 1, "hire stages exactly one outbox event");
}

// ─── Skills: cross-module validation against learning.skills ────────────────────

#[tokio::test]
async fn requisition_skills_validate_against_learning() {
    let pool = pool().await;
    let w = seed_world(&pool).await;
    let m = RecruitmentModule::builder().with_database(pool.clone()).build().unwrap();
    let app = m.guarded_routes();
    let t = token_for(w.company);

    // An unknown (or cross-tenant) skill id is rejected wholesale — the
    // previous set stays intact.
    let unknown = Uuid::new_v4();
    let (status, body) = req_full(app.clone(), "POST", &format!("/requisitions/{}/skills", w.requisition), &t,
        format!(r#"{{"skills":[{{"skill_id":"{}","required_proficiency":"advanced"}},{{"skill_id":"{unknown}","required_proficiency":"novice"}}]}}"#, w.skill)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body.contains("unknown_skill"));

    // The known-only set applies.
    let (status, body) = req_full(app.clone(), "POST", &format!("/requisitions/{}/skills", w.requisition), &t,
        format!(r#"{{"skills":[{{"skill_id":"{}","required_proficiency":"advanced"}}]}}"#, w.skill)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = req_full(app.clone(), "GET", &format!("/requisitions/{}/skills", w.requisition), &t, String::new()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let listed = serde_json::from_str::<serde_json::Value>(&body).unwrap();
    assert_eq!(listed[0]["skill_name"], "Bookkeeping");
    assert_eq!(listed[0]["required_proficiency"], "advanced");
}

// ─── Letter seam: explicit template + unwired sink fails closed ─────────────────

async fn seed_template(pool: &PgPool, company: Uuid) -> Uuid {
    let template = Uuid::new_v4();
    backbone_orm::company_scope::with_company_scope(Some(company), async {
        sqlx::query(
            r#"INSERT INTO recruitment.offer_letter_templates
                   (id, company_id, name, subject, body, metadata)
               VALUES ($1, $2, 'Standard offer',
                       'Your offer — {{position_title}}',
                       'Dear {{candidate_first_name}}, welcome to {{company_name}}. Salary {{proposed_salary}}, start {{start_date}}.',
                       '{}'::jsonb)"#,
        )
        .bind(template)
        .bind(company)
        .execute(pool)
        .await
        .unwrap();
    })
    .await;
    template
}

#[tokio::test]
async fn extend_with_a_template_but_unwired_letter_fails_closed() {
    let pool = pool().await;
    let w = seed_world(&pool).await;
    let template = seed_template(&pool, w.company).await;
    let m = RecruitmentModule::builder().with_database(pool.clone()).build().unwrap();
    let app = m.guarded_routes();
    let t = token_for(w.company);

    let application = create_application(app.clone(), &t, w.candidate, w.requisition).await;
    let (status, body) = req_full(app.clone(), "POST", "/offers", &t,
        format!(r#"{{"application_id":"{application}","letter_template_id":"{template}"}}"#)).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let offer: Uuid = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
        .as_str().unwrap().parse().unwrap();

    // Template + unwired seam: 422 BEFORE the offer moves.
    let (status, body) = req_full(app.clone(), "POST", &format!("/offers/{offer}/extend"), &t,
        r#"{"company_name":"Acme"}"#.to_string()).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body.contains("letter_seam_unwired"));
    assert_eq!(
        scoped_one::<String>(&pool, w.company,
            format!("SELECT status::text FROM recruitment.job_offers WHERE id = '{offer}'")).await,
        "draft",
        "the offer stays draft and retryable"
    );

    // No template on the offer: extend succeeds with no letter involved.
    let (status, body) = req_full(app.clone(), "POST", "/offers", &t,
        format!(r#"{{"application_id":"{application}"}}"#)).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let plain_offer: Uuid = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
        .as_str().unwrap().parse().unwrap();
    let s = req(app.clone(), "POST", &format!("/offers/{plain_offer}/extend"), &t, String::new()).await;
    assert_eq!(s, StatusCode::OK);
}

/// A fake letter sink that records what it was asked to send.
#[derive(Default)]
struct FakeLetters {
    sent: Mutex<Vec<LetterMessage>>,
}
#[async_trait::async_trait]
impl OfferLetterSink for FakeLetters {
    async fn send(&self, msg: LetterMessage) -> Result<LetterAck, LetterRejected> {
        self.sent.lock().unwrap().push(msg);
        Ok(LetterAck { message_id: Uuid::new_v4() })
    }
}

#[tokio::test]
async fn extend_with_a_wired_sink_renders_and_sends_the_letter() {
    let pool = pool().await;
    let w = seed_world(&pool).await;
    let template = seed_template(&pool, w.company).await;

    // Service level with the seam wired — the composing-app shape.
    let sink = Arc::new(FakeLetters::default());
    let svc = JobOfferWriteService::with_letter_sink(pool.clone(), sink.clone());

    // Application straight into the hired stage via the application service.
    let apps = JobApplicationWriteService::new(pool.clone());
    let application = apps
        .create_application(NewJobApplication {
            company_id: w.company,
            candidate_id: w.candidate,
            requisition_id: w.requisition,
        })
        .await
        .unwrap();
    apps.move_stage(w.company, application, w.hired_stage).await.unwrap();

    let offer = svc
        .create_draft(NewJobOffer {
            company_id: w.company,
            application_id: application,
            proposed_salary: Some(rust_decimal::Decimal::new(5_000_000, 0)),
            employment_type: Some("permanent".into()),
            letter_template_id: Some(template),
        })
        .await
        .unwrap();

    svc.extend(
        w.company,
        offer,
        ExtendOptions {
            start_date: chrono::NaiveDate::from_ymd_opt(2026, 9, 1),
            company_name: Some("Acme".into()),
        },
    )
    .await
    .unwrap();

    let sent = sink.sent.lock().unwrap();
    assert_eq!(sent.len(), 1, "exactly one letter sent");
    assert_eq!(sent[0].to_email, "ada@example.com");
    assert_eq!(sent[0].subject, "Your offer — Staff Accountant");
    assert!(sent[0].body.contains("Dear Ada,"), "candidate token rendered: {}", sent[0].body);
    assert!(sent[0].body.contains("welcome to Acme."), "company token rendered");
    assert!(sent[0].body.contains("Salary 5000000"), "salary token rendered");
    assert!(sent[0].body.contains("start 2026-09-01"), "start-date token rendered");
}

// ─── Activity seam: schedule with a wired fake sink ─────────────────────────────

/// A fake activity sink that records its commands.
#[derive(Default)]
struct FakeActivities {
    cmds: Mutex<Vec<ActivityCommand>>,
}
#[async_trait::async_trait]
impl ActivitySink for FakeActivities {
    async fn schedule(&self, cmd: ActivityCommand) -> Result<ActivityAck, ActivityRejected> {
        self.cmds.lock().unwrap().push(cmd);
        Ok(ActivityAck { activity_id: Uuid::new_v4() })
    }
}

#[tokio::test]
async fn interview_schedule_notifies_through_the_wired_sink() {
    let pool = pool().await;
    let w = seed_world(&pool).await;

    let sink = Arc::new(FakeActivities::default());
    let svc = InterviewWriteService::with_activity_sink(pool.clone(), sink.clone());

    let apps = JobApplicationWriteService::new(pool.clone());
    let application = apps
        .create_application(NewJobApplication {
            company_id: w.company,
            candidate_id: w.candidate,
            requisition_id: w.requisition,
        })
        .await
        .unwrap();

    let when = chrono::DateTime::parse_from_rfc3339(WHEN).unwrap().with_timezone(&chrono::Utc);
    let interviewer = Uuid::new_v4();
    let notify_user = Uuid::new_v4();
    let interview = svc
        .schedule(NewInterview {
            company_id: w.company,
            application_id: application,
            interviewer_id: interviewer,
            scheduled_at: when,
            round: Some(1),
            interview_format: Some("video".into()),
            notify_user_id: Some(notify_user),
        })
        .await
        .unwrap();

    let cmds = sink.cmds.lock().unwrap();
    assert_eq!(cmds.len(), 1, "exactly one activity scheduled");
    assert_eq!(cmds[0].res_id, interview);
    assert_eq!(cmds[0].res_model, "interview");
    assert_eq!(cmds[0].user_id, notify_user, "the activity lands on the notified user");
    assert_eq!(cmds[0].deadline, Some(when.date_naive()), "deadline is the interview date");
    assert!(cmds[0].summary.contains("2027-01-15"), "summary names the date: {}", cmds[0].summary);
}

#[tokio::test]
async fn interview_schedule_unwired_notification_fails_closed() {
    let pool = pool().await;
    let w = seed_world(&pool).await;

    // Route level, default-unwired module: an explicit notify_user_id must 422
    // BEFORE the interview row exists.
    let m = RecruitmentModule::builder().with_database(pool.clone()).build().unwrap();
    let app = m.guarded_routes();
    let t = token_for(w.company);

    let apps = JobApplicationWriteService::new(pool.clone());
    let application = apps
        .create_application(NewJobApplication {
            company_id: w.company,
            candidate_id: w.candidate,
            requisition_id: w.requisition,
        })
        .await
        .unwrap();

    let (status, body) = req_full(app.clone(), "POST", "/interviews", &t,
        format!(r#"{{"application_id":"{application}","interviewer_id":"{}","scheduled_at":"{WHEN}","notify_user_id":"{}"}}"#,
            Uuid::new_v4(), Uuid::new_v4())).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body.contains("activity_seam_unwired"));

    // Silent scheduling (no notify_user_id) still works unwired.
    let (status, body) = req_full(app, "POST", "/interviews", &t,
        format!(r#"{{"application_id":"{application}","interviewer_id":"{}","scheduled_at":"{WHEN}"}}"#,
            Uuid::new_v4())).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
}

// ─── No stages configured: fail closed ──────────────────────────────────────────

#[tokio::test]
async fn application_create_without_stages_fails_closed() {
    let pool = pool().await;
    let company = Uuid::new_v4();
    let candidate = Uuid::new_v4();
    let requisition = Uuid::new_v4();

    backbone_orm::company_scope::with_company_scope(Some(company), async {
        sqlx::query(
            r#"INSERT INTO recruitment.candidates (id, company_id, first_name, metadata)
               VALUES ($1, $2, 'NoStages', '{}'::jsonb)"#,
        )
        .bind(candidate)
        .bind(company)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            r#"INSERT INTO recruitment.job_requisitions
                   (id, company_id, title, headcount, filled_headcount, status, opened_by, metadata)
               VALUES ($1, $2, 'Any', 1, 0, 'open', $3, '{}'::jsonb)"#,
        )
        .bind(requisition)
        .bind(company)
        .bind(Uuid::new_v4())
        .execute(&pool)
        .await
        .unwrap();
    })
    .await;

    let m = RecruitmentModule::builder().with_database(pool.clone()).build().unwrap();
    let t = token_for(company);
    let (status, body) = req_full(m.guarded_routes(), "POST", "/applications", &t,
        format!(r#"{{"candidate_id":"{candidate}","requisition_id":"{requisition}"}}"#)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body.contains("no_stages_configured"));
}

// ─── Cross-tenant: a verb against another company's row is a 404 ────────────────

#[tokio::test]
async fn cross_tenant_verbs_are_404_and_unauth_is_401() {
    let pool = pool().await;
    let w = seed_world(&pool).await;
    let other = seed_world(&pool).await; // a different company's whole world
    let m = RecruitmentModule::builder().with_database(pool.clone()).build().unwrap();
    let app = m.guarded_routes();

    // The OTHER company's token cannot move OUR application (and vice versa).
    let application = create_application(app.clone(), &token_for(w.company), w.candidate, w.requisition).await;
    let other_token = token_for(other.company);
    let (status, body) = req_full(app.clone(), "POST", &format!("/applications/{application}/stage"),
        other_token.as_str(),
        format!(r#"{{"to_stage_id":"{}"}}"#, other.hired_stage)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");

    // No token at all: the middleware rejects before any handler runs.
    let (status, _) = req_full_opt(app.clone(), "POST", &format!("/applications/{application}/refuse"),
        None, r#"{"reason":"x"}"#.to_string()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// ─── The fence itself: SET ROLE to a plain non-superuser ─────────────────────────

#[tokio::test]
async fn rls_fence_under_non_owner_role() {
    let pool = pool().await;
    let w = seed_world(&pool).await;
    let m = RecruitmentModule::builder().with_database(pool.clone()).build().unwrap();
    let app = m.guarded_routes();
    let t = token_for(w.company);

    let _application = create_application(app, &t, w.candidate, w.requisition).await;

    // This suite connects as the DB owner, whom RLS can never bind (superusers
    // bypass RLS even under FORCE). Run the probe the way production does:
    // SET ROLE to a plain role (the composing-app posture) on one connection.
    let mut conn = pool.acquire().await.unwrap();
    sqlx::query(
        r#"DO $$ BEGIN
               IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'recruitment_probe_rls') THEN
                   CREATE ROLE recruitment_probe_rls NOLOGIN;
               END IF;
           END $$"#,
    )
    .execute(&mut *conn)
    .await
    .unwrap();
    sqlx::query("GRANT USAGE ON SCHEMA recruitment TO recruitment_probe_rls")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("GRANT SELECT ON ALL TABLES IN SCHEMA recruitment TO recruitment_probe_rls")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("SET ROLE recruitment_probe_rls").execute(&mut *conn).await.unwrap();

    // Unbound (no tenant): zero rows by design — the fence default.
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM recruitment.job_applications")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(n, 0, "unbound non-superuser sees zero rows");

    // Bound to the company (request-scoped set_config, transaction-local like
    // the app): exactly its company's rows — this one application.
    let mut tx = conn.begin().await.unwrap();
    sqlx::query("SELECT set_config('app.company_id', $1, true)")
        .bind(w.company.to_string())
        .execute(&mut *tx)
        .await
        .unwrap();
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM recruitment.job_applications")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(n, 1, "bound connection sees exactly its company's rows");
    tx.rollback().await.unwrap();

    sqlx::query("RESET ROLE").execute(&mut *conn).await.unwrap();
}
