//! Integration test for the `recruitment.hired → employee` compound event (ADR-005).
//!
//! Proves the full producer→relay→consumer flow + idempotency, over real Postgres:
//!
//! 1. seeds candidate → requisition → application → offer(extended);
//! 2. calls the PRODUCER ([`JobOfferWriteService::hire`]) — in one tx the offer goes `accepted` AND a
//!    `recruitment.hired` row is staged in `recruitment.outbox_events`;
//! 3. runs the RELAY ([`backbone_outbox::relay::drain_once`]) — drains the outbox row and hands it to
//!    the CONSUMER ([`backbone_employee::application::RecruitmentHiredHandler`]) exactly as the composer's bus does;
//! 4. asserts the Employee + Employment were created in `employee.*` with the offer's data;
//! 5. replays the SAME event id (simulating a relay redelivery) and asserts NO second employee — the
//!    inbox dedup makes the effect exactly-once.
//!
//! The test is hermetic about schema: it builds the minimal DDL the flow touches inline (the producer's
//! SQL is schema-pinned to `recruitment.*`/`employee.*`, so the real module tables are exercised). It
//! SKIPS (not fails) when no DB is reachable, so `cargo test` stays green in any environment; set
//! `DATABASE_URL` to run it for real.

use backbone_messaging::{IntegrationEventEnvelope, IntegrationEventHandler};
use backbone_outbox::{inbox, outbox, relay, OutboxRecord};
use backbone_recruitment::application::service::JobOfferWriteService;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// Connect to a dedicated scratch database this test rebuilds from scratch,
/// or `None` to skip.
///
/// The flow's schema is hermetic (minimal inline DDL below), so the test must
/// NOT run against a database that already carries the full module migrations:
/// the `CREATE ... IF NOT EXISTS` guards would silently keep the real, stricter
/// tables (e.g. requisitions require a title) and the seeds would violate
/// them. Credentials/host come from `DATABASE_URL` (local dev default
/// otherwise); the database itself is always a private scratch DB.
async fn connect() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/backbone_hr".into());
    let (prefix, _) = url.trim_end_matches('/').rsplit_once('/')?;
    let admin = match PgPool::connect(&format!("{prefix}/postgres")).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("skip hire_flow: could not reach `{prefix}/postgres` ({e}); set DATABASE_URL to run");
            return None;
        }
    };
    let scratch = "recruitment_hire_flow_test";
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
            eprintln!("skip hire_flow: could not connect to scratch db ({e})");
            None
        }
    }
}

/// Serialize the tests in this binary against each other. Two hazards make
/// concurrency wrong here, and one guard fixes both: (1) `CREATE SCHEMA IF
/// NOT EXISTS` (also inside `outbox::migrate`) can still raise a unique
/// violation when two connections race the same schema name; (2) the setup
/// TRUNCATEs the shared seed tables, which would wipe another test's
/// mid-flight rows. Hold the returned guard for the WHOLE test body.
async fn setup_locked(
    pool: &PgPool,
) -> sqlx::Result<tokio::sync::MutexGuard<'static, ()>> {
    static SETUP_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> =
        std::sync::OnceLock::new();
    let lock: &'static _ = SETUP_LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
    let guard = lock.lock().await;
    setup(pool).await?;
    Ok(guard)
}

/// Build the minimal schema the flow exercises. Idempotent (CREATE ... IF NOT EXISTS), so it is safe to
/// run against a DB that already carries the full module migrations — the IF NOT EXISTS no-ops there.
async fn setup(pool: &PgPool) -> sqlx::Result<()> {
    // Enum types the producer/consumer SQL depends on (ignore "already exists").
    for stmt in [
        "CREATE TYPE offer_status AS ENUM ('draft','extended','accepted','declined','withdrawn')",
        "CREATE TYPE employment_status AS ENUM ('permanent','contract','probation','associate')",
        "CREATE TYPE employment_state AS ENUM ('active','inactive')",
    ] {
        let _ = sqlx::query(stmt).execute(pool).await;
    }

    let _ = sqlx::query("CREATE SCHEMA IF NOT EXISTS recruitment")
        .execute(pool)
        .await?;
    let _ = sqlx::query("CREATE SCHEMA IF NOT EXISTS employee")
        .execute(pool)
        .await?;

    // ── recruitment.* (the producer reads/writes these) ────────────────────────────────────────
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS recruitment.candidates (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               company_id UUID NOT NULL,
               first_name TEXT NOT NULL,
               last_name TEXT,
               email TEXT
           )"#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS recruitment.job_requisitions (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               company_id UUID NOT NULL,
               department_id UUID,
               position_id UUID,
               title TEXT,
               headcount INTEGER NOT NULL DEFAULT 1,
               filled_headcount INTEGER NOT NULL DEFAULT 0,
               status TEXT NOT NULL DEFAULT 'open'
           )"#,
    )
    .execute(pool)
    .await?;
    // The pipeline config: applications reference a stage row; the hire guard
    // reads its is_hired flag.
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS recruitment.recruitment_stages (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               company_id UUID NOT NULL,
               name TEXT NOT NULL,
               sequence INTEGER NOT NULL DEFAULT 10,
               is_hired BOOLEAN NOT NULL DEFAULT FALSE
           )"#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS recruitment.job_applications (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               company_id UUID NOT NULL,
               candidate_id UUID NOT NULL,
               requisition_id UUID NOT NULL,
               stage_id UUID NOT NULL REFERENCES recruitment.recruitment_stages(id),
               refused_at TIMESTAMPTZ
           )"#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS recruitment.job_offers (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               company_id UUID NOT NULL,
               application_id UUID NOT NULL,
               proposed_salary NUMERIC,
               employment_type TEXT,
               status offer_status NOT NULL DEFAULT 'draft',
               offered_at TIMESTAMPTZ,
               accepted_at TIMESTAMPTZ,
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
    )
    .execute(pool)
    .await?;

    // ── employee.* (the consumer writes these) ────────────────────────────────────────────────
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS employee.employees (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               company_id UUID NOT NULL,
               employee_number TEXT NOT NULL,
               first_name TEXT NOT NULL,
               last_name TEXT,
               email TEXT,
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS employee.employments (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               company_id UUID NOT NULL,
               employee_id UUID NOT NULL,
               employment_status employment_status NOT NULL DEFAULT 'permanent',
               join_date DATE NOT NULL,
               department_id UUID,
               position_id UUID,
               status employment_state NOT NULL DEFAULT 'active',
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
    )
    .execute(pool)
    .await?;

    // Outbox + inbox tables in both schemas (framework DDL).
    outbox::migrate(pool, "recruitment")
        .await
        .expect("outbox migrate recruitment");
    outbox::migrate(pool, "employee")
        .await
        .expect("outbox migrate employee");

    // Isolate this run from any prior data in the shared shapes.
    sqlx::query("TRUNCATE recruitment.job_offers, recruitment.job_applications, recruitment.candidates, recruitment.job_requisitions, recruitment.recruitment_stages")
        .execute(pool)
        .await?;
    sqlx::query("TRUNCATE employee.employments, employee.employees")
        .execute(pool)
        .await?;
    sqlx::query("TRUNCATE recruitment.outbox_events, employee.inbox_consumed")
        .execute(pool)
        .await?;
    Ok(())
}

/// Seed a hire-able offer chain (candidate → requisition → application → offer in `extended`) and
/// return the offer id + the seed data so assertions can reference it.
async fn seed_hireable_offer(pool: &PgPool) -> (Uuid, Uuid, Uuid, Uuid) {
    let company_id = Uuid::new_v4();
    let candidate_id: Uuid = sqlx::query(
        "INSERT INTO recruitment.candidates (company_id, first_name, last_name, email)
         VALUES ($1,'Ada','Lovelace','ada@example.com') RETURNING id",
    )
    .bind(company_id)
    .fetch_one(pool)
    .await
    .unwrap()
    .get("id");

    let department_id = Uuid::new_v4();
    let position_id = Uuid::new_v4();
    let requisition_id: Uuid = sqlx::query(
        "INSERT INTO recruitment.job_requisitions (company_id, department_id, position_id)
         VALUES ($1,$2,$3) RETURNING id",
    )
    .bind(company_id)
    .bind(department_id)
    .bind(position_id)
    .fetch_one(pool)
    .await
    .unwrap()
    .get("id");

    // A two-stage pipeline; the application sits in the is_hired one so the
    // producer's hired-stage guard passes.
    let hired_stage_id: Uuid = sqlx::query(
        "INSERT INTO recruitment.recruitment_stages (company_id, name, sequence, is_hired)
         VALUES ($1,'Hired',90,TRUE) RETURNING id",
    )
    .bind(company_id)
    .fetch_one(pool)
    .await
    .unwrap()
    .get("id");

    let application_id: Uuid = sqlx::query(
        "INSERT INTO recruitment.job_applications (company_id, candidate_id, requisition_id, stage_id)
         VALUES ($1,$2,$3,$4) RETURNING id",
    )
    .bind(company_id)
    .bind(candidate_id)
    .bind(requisition_id)
    .bind(hired_stage_id)
    .fetch_one(pool)
    .await
    .unwrap()
    .get("id");

    let offer_id: Uuid = sqlx::query(
        "INSERT INTO recruitment.job_offers (company_id, application_id, employment_type, proposed_salary, status)
         VALUES ($1,$2,'permanent',$3,'extended') RETURNING id",
    )
    .bind(company_id)
    .bind(application_id)
    .bind(Decimal::new(5_000_000, 0))
    .fetch_one(pool)
    .await
    .unwrap()
    .get("id");

    (company_id, offer_id, department_id, position_id)
}

#[tokio::test]
async fn hire_flow_creates_employee_and_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    let _guard = setup_locked(&pool).await?;

    let (company_id, offer_id, department_id, position_id) = seed_hireable_offer(&pool).await;

    // ── 1. PRODUCER: hire() marks the offer accepted + stages recruitment.hired, in one tx. ──────
    let svc = JobOfferWriteService::new(pool.clone());
    let event_id = svc
        .hire(company_id, offer_id)
        .await
        .expect("fresh hire")
        .expect("a fresh hire stages an event");

    // The state change committed.
    let offer_status: String =
        sqlx::query_scalar("SELECT status::text FROM recruitment.job_offers WHERE id=$1")
            .bind(offer_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(offer_status, "accepted", "offer is accepted after hire()");

    // The event is staged and pending.
    assert_eq!(
        outbox::pending_count(&pool, "recruitment").await?,
        1,
        "exactly one recruitment.hired row staged"
    );

    // ── 2. RELAY → CONSUMER: drain the outbox straight into the employee handler, exactly as the
    //     composer's bus wiring does (envelope id = outbox row id = consumer dedup key). ───────────
    let handler = std::sync::Arc::new(backbone_employee::application::RecruitmentHiredHandler::new(pool.clone()));
    let published = relay::drain_once(&pool, "recruitment", 10, |rec: OutboxRecord| {
        let handler = handler.clone();
        async move {
            let envelope = IntegrationEventEnvelope {
                id: rec.id.to_string(),
                event_type: rec.event_type.clone(),
                source_context: rec.aggregate_type.clone(),
                aggregate_id: rec.aggregate_id.clone(),
                occurred_at: rec.occurred_at,
                published_at: Utc::now(),
                version: rec.version as u32,
                correlation_id: rec.correlation_id.clone(),
                causation_id: rec.causation_id.clone(),
                payload: rec.payload.clone(),
            };
            handler
                .handle(envelope)
                .await
                .map_err(|e| backbone_outbox::OutboxError::Publish(format!("consumer: {e}")))
        }
    })
    .await?;
    assert_eq!(published, 1, "the relay drained + the consumer acked the event");
    assert_eq!(
        outbox::pending_count(&pool, "recruitment").await?,
        0,
        "outbox drained"
    );

    // ── 3. ASSERT: Employee + Employment created with the offer's data. ─────────────────────────
    let emp = sqlx::query(
        "SELECT id, employee_number, first_name, last_name, email
         FROM employee.employees WHERE company_id=$1",
    )
    .bind(company_id)
    .fetch_one(&pool)
    .await?;
    let employee_id: Uuid = emp.get("id");
    let employee_number: String = emp.get("employee_number");
    let first_name: String = emp.get("first_name");
    let last_name: Option<String> = emp.get("last_name");
    let email: Option<String> = emp.get("email");

    assert_eq!(first_name, "Ada");
    assert_eq!(last_name.as_deref(), Some("Lovelace"));
    assert_eq!(email.as_deref(), Some("ada@example.com"));
    assert_eq!(
        employee_number, format!("REC-{offer_id}"),
        "employee_number is derived deterministically from the offer (idempotency friendly)"
    );

    let emp_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM employee.employees WHERE company_id=$1",
    )
    .bind(company_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(emp_count, 1, "exactly one employee");

    let emt = sqlx::query(
        "SELECT employment_status::text AS s, position_id, department_id
         FROM employee.employments WHERE employee_id=$1",
    )
    .bind(employee_id)
    .fetch_one(&pool)
    .await?;
    let employment_status: String = emt.get("s");
    let got_position: Option<Uuid> = emt.get("position_id");
    let got_department: Option<Uuid> = emt.get("department_id");
    assert_eq!(employment_status, "permanent", "employment_type mapped onto the enum");
    assert_eq!(got_position, Some(position_id), "position carried from the requisition");
    assert_eq!(got_department, Some(department_id), "department carried from the requisition");

    // The consumer recorded the apply in its inbox.
    assert!(
        inbox::was_consumed(&pool, "employee", "recruitment.hired", event_id).await?,
        "inbox recorded the consumption"
    );

    // ── 4. IDEMPOTENCY: replay the SAME event id (as if the relay redelivered before marking). The
    //     inbox claim returns false → no second employee, no second employment. ────────────────────
    let replay = fetch_envelope(&pool, event_id).await?;
    let handler2 = backbone_employee::application::RecruitmentHiredHandler::new(pool.clone());
    handler2.handle(replay).await.expect("replay is Ok (a no-op)");

    let emp_count_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM employee.employees WHERE company_id=$1",
    )
    .bind(company_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(emp_count_after, 1, "replay did not create a second employee");

    let emt_count_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM employee.employments WHERE employee_id=$1",
    )
    .bind(employee_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(emt_count_after, 1, "replay did not create a second employment");

    Ok(())
}

#[tokio::test]
async fn hire_is_idempotent_at_the_producer_too() -> Result<(), Box<dyn std::error::Error>> {
    // Calling hire() twice on the same offer must NOT stage a second event: the offer's own status is
    // a producer-side idempotency guard (the consumer inbox is still the mandatory backstop).
    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    let _guard = setup_locked(&pool).await?;

    let (_company_id, offer_id, _, _) = seed_hireable_offer(&pool).await;
    let svc = JobOfferWriteService::new(pool.clone());

    let first = svc.hire(_company_id, offer_id).await?.expect("first hire stages an event");
    let second = svc.hire(_company_id, offer_id).await?;
    assert!(second.is_none(), "re-hire of an accepted offer stages no second event");

    assert_eq!(
        outbox::pending_count(&pool, "recruitment").await?,
        1,
        "still exactly one outbox row — the first event id {first}"
    );
    Ok(())
}

/// Re-fetch the staged outbox row and rebuild the envelope the relay would publish — a faithful
/// redelivery for the idempotency replay.
async fn fetch_envelope(pool: &PgPool, event_id: Uuid) -> sqlx::Result<IntegrationEventEnvelope> {
    let row = sqlx::query(
        "SELECT event_type, aggregate_type, aggregate_id, payload, occurred_at, version
         FROM recruitment.outbox_events WHERE id=$1",
    )
    .bind(event_id)
    .fetch_one(pool)
    .await?;
    Ok(IntegrationEventEnvelope {
        id: event_id.to_string(),
        event_type: row.get("event_type"),
        source_context: row.get("aggregate_type"),
        aggregate_id: row.get("aggregate_id"),
        occurred_at: row.get::<DateTime<Utc>, _>("occurred_at"),
        published_at: Utc::now(),
        version: row.get::<i32, _>("version") as u32,
        correlation_id: None,
        causation_id: None,
        payload: row.get("payload"),
    })
}
