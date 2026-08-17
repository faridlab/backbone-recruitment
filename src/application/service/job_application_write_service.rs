//! Stage-driven application transitions (hand-authored, user-owned).
//!
//! This service is the ONE door for moving an application through the hiring
//! pipeline. Every verb takes the caller's `company` explicitly and binds it
//! onto the transaction before any statement runs, so the whole path is
//! correct under the strict company fence (row-level security) — a scoped
//! non-owner session cannot smuggle a read or write past `WHERE company_id`.
//!
//! Two invariants live here and nowhere else:
//!
//! 1. **Vacancy coupling.** Entering a stage flagged `is_hired` consumes a
//!    requisition opening (`filled_headcount + 1`); leaving one releases it.
//!    Because moving *between* two hired stages both enters and leaves, the
//!    counter moves only on a real boundary crossing. The requisition row is
//!    locked in the same transaction as the move, so concurrent hires cannot
//!    over-fill the headcount.
//!
//! 2. **Refusal is sticky.** `refuse` stamps `refused_at` once and the
//!    application never moves again — a refused row is closed, and re-opening
//!    it would need a new application. Refusing an application that currently
//!    sits in a hired stage releases its opening (the same coupling, other
//!    door).
//!
//! There is deliberately no stored `status` column to maintain: "ongoing /
//! hired / refused" is derived (stage flags + refusal marks) on read —
//! [`JobApplicationWriteService::pipeline`] is that projection.

use backbone_orm::company_scope;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// Errors from the application write-service.
#[derive(Debug, thiserror::Error)]
pub enum ApplicationError {
    #[error("application {0} not found")]
    NotFound(Uuid),
    #[error("candidate {0} not found in company")]
    CandidateNotFound(Uuid),
    #[error("requisition {0} not found in company")]
    RequisitionNotFound(Uuid),
    #[error("requisition {0} is not open for applications")]
    RequisitionNotOpen(Uuid),
    #[error("stage {0} not found in company")]
    StageNotFound(Uuid),
    #[error("the company has no pipeline stages configured")]
    NoStagesConfigured,
    #[error("application {0} is already refused — refused applications cannot move")]
    AlreadyRefused(Uuid),
    #[error("requisition has no open headcount left")]
    NoOpenHeadcount,
    #[error("a database failure: {0}")]
    Db(#[from] sqlx::Error),
}

impl ApplicationError {
    /// Stable machine code for the HTTP surface.
    pub fn code(&self) -> &'static str {
        match self {
            ApplicationError::NotFound(_) => "application_not_found",
            ApplicationError::CandidateNotFound(_) => "candidate_not_found",
            ApplicationError::RequisitionNotFound(_) => "requisition_not_found",
            ApplicationError::RequisitionNotOpen(_) => "requisition_not_open",
            ApplicationError::StageNotFound(_) => "stage_not_found",
            ApplicationError::NoStagesConfigured => "no_stages_configured",
            ApplicationError::AlreadyRefused(_) => "already_refused",
            ApplicationError::NoOpenHeadcount => "no_open_headcount",
            ApplicationError::Db(_) => "internal_error",
        }
    }
    pub fn http_status(&self) -> u16 {
        match self {
            ApplicationError::NotFound(_)
            | ApplicationError::CandidateNotFound(_)
            | ApplicationError::RequisitionNotFound(_)
            | ApplicationError::StageNotFound(_) => 404,
            ApplicationError::RequisitionNotOpen(_)
            | ApplicationError::NoStagesConfigured
            | ApplicationError::AlreadyRefused(_) => 422,
            // Full pipeline: a business-rule refusal, not a bad request.
            ApplicationError::NoOpenHeadcount => 409,
            ApplicationError::Db(_) => 500,
        }
    }
}

/// Input for `create_application`.
#[derive(Debug, Clone)]
pub struct NewJobApplication {
    pub company_id: Uuid,
    pub candidate_id: Uuid,
    pub requisition_id: Uuid,
}

/// Read-side projection: where an application sits, derived rather than stored.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PipelineStatus {
    pub application_id: Uuid,
    pub stage_id: Uuid,
    pub stage_name: String,
    pub stage_sequence: i32,
    /// Derived bucket: `ongoing`, `hired`, or `refused`.
    pub status: &'static str,
    pub stage_updated_at: Option<DateTime<Utc>>,
    pub date_closed: Option<DateTime<Utc>>,
    pub refuse_reason: Option<String>,
    pub requisition_id: Uuid,
    pub requisition_headcount: i32,
    pub requisition_filled_headcount: i32,
}

/// The application write-service: create / move_stage / refuse + the derived
/// pipeline read.
pub struct JobApplicationWriteService {
    pool: PgPool,
}

impl JobApplicationWriteService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Create an application in the company's FIRST pipeline stage (lowest
    /// `sequence`). Stages are company configuration — there is no global
    /// default pipeline to fall back on, so a company with no stages fails
    /// closed with [`ApplicationError::NoStagesConfigured`].
    pub async fn create_application(
        &self,
        input: NewJobApplication,
    ) -> Result<Uuid, ApplicationError> {
        let company = input.company_id;
        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company).await?;

        // Candidate must exist in this company (fenced join — no cross-tenant ids).
        let candidate: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM recruitment.candidates WHERE id = $1 AND company_id = $2",
        )
        .bind(input.candidate_id)
        .bind(company)
        .fetch_optional(&mut *tx)
        .await?;
        if candidate.is_none() {
            tx.rollback().await?;
            return Err(ApplicationError::CandidateNotFound(input.candidate_id));
        }

        // Requisition must exist and be open for applications.
        let requisition_open: Option<String> = sqlx::query_scalar(
            "SELECT status::text FROM recruitment.job_requisitions WHERE id = $1 AND company_id = $2",
        )
        .bind(input.requisition_id)
        .bind(company)
        .fetch_optional(&mut *tx)
        .await?;
        match requisition_open.as_deref() {
            None => {
                tx.rollback().await?;
                return Err(ApplicationError::RequisitionNotFound(input.requisition_id));
            }
            Some("open") => {}
            Some(_) => {
                tx.rollback().await?;
                return Err(ApplicationError::RequisitionNotOpen(input.requisition_id));
            }
        }

        // Entry stage = the company's first by sequence. Fail-closed when the
        // company has configured no pipeline yet.
        let entry_stage: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM recruitment.recruitment_stages \
             WHERE company_id = $1 ORDER BY sequence ASC, id ASC LIMIT 1",
        )
        .bind(company)
        .fetch_optional(&mut *tx)
        .await?;
        let stage_id = match entry_stage {
            Some(s) => s,
            None => {
                tx.rollback().await?;
                return Err(ApplicationError::NoStagesConfigured);
            }
        };

        let id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO recruitment.job_applications
                   (id, company_id, candidate_id, requisition_id, stage_id,
                    stage_updated_at, applied_at, metadata)
               VALUES ($1, $2, $3, $4, $5, NOW(), NOW(),
                       '{"created_at":null,"updated_at":null,"deleted_at":null,
                         "created_by":null,"updated_by":null,"deleted_by":null}'::jsonb)"#,
        )
        .bind(id)
        .bind(company)
        .bind(input.candidate_id)
        .bind(input.requisition_id)
        .bind(stage_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(id)
    }

    /// Move an application to another stage — the single transition point of
    /// the pipeline.
    ///
    /// Idempotent: moving to the stage the application is already in is a
    /// no-op returning `Ok(false)`. Refused applications never move
    /// ([`ApplicationError::AlreadyRefused`]). Entering an `is_hired` stage
    /// requires the requisition to have openings left
    /// ([`ApplicationError::NoOpenHeadcount`]) and stamps `date_closed`;
    /// leaving one releases the opening and clears it.
    pub async fn move_stage(
        &self,
        company: Uuid,
        application_id: Uuid,
        to_stage_id: Uuid,
    ) -> Result<bool, ApplicationError> {
        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company).await?;

        // Lock the application AND its requisition together: the vacancy
        // counter and the move must not race a concurrent hire on the same
        // requisition. Stage rows are read unlocked (their flags are treated
        // as config; a stage edit racing a move re-validates on the next one).
        let row = sqlx::query(
            r#"SELECT a.stage_id                       AS stage_id,
                      (cur.is_hired)                    AS cur_is_hired,
                      (tgt.is_hired)                    AS tgt_is_hired,
                      a.refused_at                      AS refused_at,
                      r.id                              AS requisition_id,
                      r.headcount                       AS headcount,
                      r.filled_headcount                AS filled_headcount
                 FROM recruitment.job_applications a
                 JOIN recruitment.recruitment_stages cur ON cur.id = a.stage_id
                 JOIN recruitment.recruitment_stages tgt ON tgt.id = $2
                 JOIN recruitment.job_requisitions  r   ON r.id = a.requisition_id
                WHERE a.id = $1 AND a.company_id = $3
                FOR UPDATE OF a, r"#,
        )
        .bind(application_id)
        .bind(to_stage_id)
        .bind(company)
        .fetch_optional(&mut *tx)
        .await?;

        let row = match row {
            Some(r) => r,
            // Distinguish "no such application" from "no such target stage":
            // re-probe each separately for the right error.
            None => {
                let app: Option<Uuid> = sqlx::query_scalar(
                    "SELECT id FROM recruitment.job_applications WHERE id = $1 AND company_id = $2",
                )
                .bind(application_id)
                .bind(company)
                .fetch_optional(&mut *tx)
                .await?;
                tx.rollback().await?;
                if app.is_none() {
                    return Err(ApplicationError::NotFound(application_id));
                }
                return Err(ApplicationError::StageNotFound(to_stage_id));
            }
        };

        let current_stage: Uuid = row.try_get("stage_id")?;
        let cur_is_hired: bool = row.try_get("cur_is_hired")?;
        let tgt_is_hired: bool = row.try_get("tgt_is_hired")?;
        let refused_at: Option<DateTime<Utc>> = row.try_get("refused_at")?;
        let requisition_id: Uuid = row.try_get("requisition_id")?;
        let headcount: i32 = row.try_get("headcount")?;
        let filled_headcount: i32 = row.try_get("filled_headcount")?;

        if refused_at.is_some() {
            tx.rollback().await?;
            return Err(ApplicationError::AlreadyRefused(application_id));
        }
        if to_stage_id == current_stage {
            tx.rollback().await?;
            return Ok(false);
        }

        // Vacancy coupling — only on a real boundary crossing. Moving between
        // two hired stages neither consumes nor releases an opening.
        let enters_hired = tgt_is_hired && !cur_is_hired;
        let leaves_hired = cur_is_hired && !tgt_is_hired;
        if enters_hired {
            let remaining = headcount - filled_headcount;
            if remaining <= 0 {
                tx.rollback().await?;
                return Err(ApplicationError::NoOpenHeadcount);
            }
            sqlx::query(
                "UPDATE recruitment.job_requisitions \
                 SET filled_headcount = filled_headcount + 1 WHERE id = $1",
            )
            .bind(requisition_id)
            .execute(&mut *tx)
            .await?;
        } else if leaves_hired {
            sqlx::query(
                "UPDATE recruitment.job_requisitions \
                 SET filled_headcount = filled_headcount - 1 WHERE id = $1",
            )
            .bind(requisition_id)
            .execute(&mut *tx)
            .await?;
        }

        // The move itself. `date_closed` tracks "sits in a closing state":
        // set while in a hired stage, cleared on leaving it (refuse sets its
        // own close stamp and refuses cannot move).
        sqlx::query(
            r#"UPDATE recruitment.job_applications
                  SET last_stage_id = stage_id,
                      stage_id = $2,
                      stage_updated_at = NOW(),
                      date_closed = CASE WHEN $3 THEN NOW() ELSE NULL END
                WHERE id = $1"#,
        )
        .bind(application_id)
        .bind(to_stage_id)
        .bind(tgt_is_hired)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(true)
    }

    /// Refuse an application — sticky. Sets `refuse_reason` / `refused_at` /
    /// `date_closed` once; a second refusal is
    /// [`ApplicationError::AlreadyRefused`]. Refusing an application that
    /// currently sits in a hired stage releases the requisition opening it
    /// held (the vacancy coupling's other door).
    pub async fn refuse(
        &self,
        company: Uuid,
        application_id: Uuid,
        reason: Option<String>,
    ) -> Result<(), ApplicationError> {
        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company).await?;

        let row = sqlx::query(
            r#"SELECT a.refused_at AS refused_at,
                      (s.is_hired)  AS cur_is_hired,
                      r.id          AS requisition_id
                 FROM recruitment.job_applications a
                 JOIN recruitment.recruitment_stages s ON s.id = a.stage_id
                 JOIN recruitment.job_requisitions  r ON r.id = a.requisition_id
                WHERE a.id = $1 AND a.company_id = $2
                FOR UPDATE OF a, r"#,
        )
        .bind(application_id)
        .bind(company)
        .fetch_optional(&mut *tx)
        .await?;

        let row = match row {
            Some(r) => r,
            None => {
                tx.rollback().await?;
                return Err(ApplicationError::NotFound(application_id));
            }
        };

        let refused_at: Option<DateTime<Utc>> = row.try_get("refused_at")?;
        if refused_at.is_some() {
            tx.rollback().await?;
            return Err(ApplicationError::AlreadyRefused(application_id));
        }

        let cur_is_hired: bool = row.try_get("cur_is_hired")?;
        if cur_is_hired {
            let requisition_id: Uuid = row.try_get("requisition_id")?;
            sqlx::query(
                "UPDATE recruitment.job_requisitions \
                 SET filled_headcount = filled_headcount - 1 WHERE id = $1",
            )
            .bind(requisition_id)
            .execute(&mut *tx)
            .await?;
        }

        sqlx::query(
            r#"UPDATE recruitment.job_applications
                  SET refuse_reason = $2, refused_at = NOW(), date_closed = NOW()
                WHERE id = $1"#,
        )
        .bind(application_id)
        .bind(reason)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    /// The derived status projection for one application — "ongoing / hired /
    /// refused" computed from the stage flags and refusal marks instead of
    /// being stored. `None` when the id is unknown in this company.
    pub async fn pipeline(
        &self,
        company: Uuid,
        application_id: Uuid,
    ) -> Result<Option<PipelineStatus>, ApplicationError> {
        // Read inside a bound scope too: under the fence an unbound read
        // returns zero rows regardless of the WHERE clause.
        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company).await?;
        let row = sqlx::query(
            r#"SELECT a.id, a.stage_id, s.name AS stage_name, s.sequence AS stage_sequence,
                      (s.is_hired) AS is_hired, a.refused_at, a.stage_updated_at, a.date_closed,
                      a.refuse_reason, r.id AS requisition_id, r.headcount, r.filled_headcount
                 FROM recruitment.job_applications a
                 JOIN recruitment.recruitment_stages s ON s.id = a.stage_id
                 JOIN recruitment.job_requisitions  r ON r.id = a.requisition_id
                WHERE a.id = $1 AND a.company_id = $2"#,
        )
        .bind(application_id)
        .bind(company)
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit().await?;

        let row = match row {
            Some(r) => r,
            None => return Ok(None),
        };

        let refused: bool = row.try_get::<Option<DateTime<Utc>>, _>("refused_at")?.is_some();
        let is_hired: bool = row.try_get("is_hired")?;
        let status = if refused {
            "refused"
        } else if is_hired {
            "hired"
        } else {
            "ongoing"
        };

        Ok(Some(PipelineStatus {
            application_id: row.try_get("id")?,
            stage_id: row.try_get("stage_id")?,
            stage_name: row.try_get("stage_name")?,
            stage_sequence: row.try_get("stage_sequence")?,
            status,
            stage_updated_at: row.try_get("stage_updated_at")?,
            date_closed: row.try_get("date_closed")?,
            refuse_reason: row.try_get("refuse_reason")?,
            requisition_id: row.try_get("requisition_id")?,
            requisition_headcount: row.try_get("headcount")?,
            requisition_filled_headcount: row.try_get("filled_headcount")?,
        }))
    }
}
