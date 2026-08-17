//! Interview write-service (hand-authored, user-owned): schedule / complete /
//! cancel — plus the interviewer notification seam.
//!
//! Every verb takes the caller's `company` explicitly and binds it onto the
//! transaction before any statement runs, so the path is correct under the
//! strict company fence (row-level security).
//!
//! Scheduling can put an activity on the interviewer's plate through the
//! [`ActivitySink`] port. Activities belong to login USERS, not employees, and
//! identity lives outside this module — so the caller passes the already
//! resolved `notify_user_id`. A notification that was explicitly requested
//! plus an unwired sink fails closed BEFORE the interview row is written
//! (nothing silently unnotified); a wired adapter is called after commit (it
//! owns its own durability) and a failure there is surfaced, with the
//! interview left scheduled — the true state.

use std::sync::Arc;

use backbone_orm::company_scope;
use chrono::{DateTime, NaiveDate, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::activity_port::{ActivityCommand, ActivitySink, UnwiredActivitySink};

/// Errors from the interview write-service.
#[derive(Debug, thiserror::Error)]
pub enum InterviewError {
    #[error("interview {0} not found")]
    NotFound(Uuid),
    #[error("application {0} not found in company")]
    ApplicationNotFound(Uuid),
    #[error("application {0} was refused — no interviews may be scheduled")]
    ApplicationRefused(Uuid),
    #[error("interview {0} is not in a state that permits this verb (status: {1})")]
    NotTransitionable(Uuid, String),
    #[error("the activity seam is not wired — supply an ActivitySink to notify users")]
    ActivitySeamUnwired,
    /// The wired adapter failed after the interview was already scheduled.
    /// The interview EXISTS; only the notification failed.
    #[error("activity scheduling failed (interview is scheduled): {0}")]
    ActivityDelivery(String),
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

impl InterviewError {
    pub fn code(&self) -> &'static str {
        match self {
            InterviewError::NotFound(_) => "interview_not_found",
            InterviewError::ApplicationNotFound(_) => "application_not_found",
            InterviewError::ApplicationRefused(_) => "application_refused",
            InterviewError::NotTransitionable(..) => "interview_not_transitionable",
            InterviewError::ActivitySeamUnwired => "activity_seam_unwired",
            InterviewError::ActivityDelivery(_) => "activity_delivery_failed",
            InterviewError::Db(_) => "internal_error",
        }
    }
    pub fn http_status(&self) -> u16 {
        match self {
            InterviewError::NotFound(_) | InterviewError::ApplicationNotFound(_) => 404,
            InterviewError::ApplicationRefused(_)
            | InterviewError::NotTransitionable(..)
            | InterviewError::ActivitySeamUnwired => 422,
            InterviewError::ActivityDelivery(_) => 502,
            InterviewError::Db(_) => 500,
        }
    }
}

/// Input for `schedule`.
#[derive(Debug, Clone)]
pub struct NewInterview {
    pub company_id: Uuid,
    pub application_id: Uuid,
    pub interviewer_id: Uuid,
    pub scheduled_at: DateTime<Utc>,
    pub round: Option<i32>,
    pub interview_format: Option<String>,
    /// The interviewer's login user, when the caller wants an activity placed
    /// on their plate. `None` = schedule silently (no notification asked for).
    pub notify_user_id: Option<Uuid>,
}

/// The interview write-service.
pub struct InterviewWriteService {
    pool: PgPool,
    activities: Arc<dyn ActivitySink>,
}

impl InterviewWriteService {
    /// Unwired default — explicitly requested notifications fail closed.
    pub fn new(pool: PgPool) -> Self {
        Self { pool, activities: Arc::new(UnwiredActivitySink) }
    }

    /// Bind a real activity adapter (the host app's mail seam).
    pub fn with_activity_sink(pool: PgPool, sink: Arc<dyn ActivitySink>) -> Self {
        Self { pool, activities: sink }
    }

    /// Schedule an interview round for an ongoing application.
    pub async fn schedule(&self, input: NewInterview) -> Result<Uuid, InterviewError> {
        let company = input.company_id;
        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company).await?;

        // The application must exist in this company and still be ongoing.
        let row = sqlx::query(
            "SELECT refused_at FROM recruitment.job_applications \
             WHERE id = $1 AND company_id = $2",
        )
        .bind(input.application_id)
        .bind(company)
        .fetch_optional(&mut *tx)
        .await?;
        match row {
            None => {
                tx.rollback().await?;
                return Err(InterviewError::ApplicationNotFound(input.application_id));
            }
            Some(r) if r.try_get::<Option<DateTime<Utc>>, _>("refused_at")?.is_some() => {
                tx.rollback().await?;
                return Err(InterviewError::ApplicationRefused(input.application_id));
            }
            Some(_) => {}
        }

        // Notification seam: explicitly requested + no adapter fails closed
        // BEFORE the row is written.
        if input.notify_user_id.is_some() && !self.activities.is_wired() {
            tx.rollback().await?;
            return Err(InterviewError::ActivitySeamUnwired);
        }

        let id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO recruitment.interviews
                   (id, company_id, application_id, interviewer_id, scheduled_at,
                    round, interview_format, status, metadata)
               VALUES ($1, $2, $3, $4, $5, $6, $7, 'scheduled',
                       '{"created_at":null,"updated_at":null,"deleted_at":null,
                         "created_by":null,"updated_by":null,"deleted_by":null}'::jsonb)"#,
        )
        .bind(id)
        .bind(company)
        .bind(input.application_id)
        .bind(input.interviewer_id)
        .bind(input.scheduled_at)
        .bind(input.round)
        .bind(input.interview_format)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        // Notify after commit: the adapter owns its own durability. A failure
        // leaves the interview scheduled (true state) and is surfaced for a
        // notify-only retry.
        if let Some(user_id) = input.notify_user_id {
            let date: NaiveDate = input.scheduled_at.date_naive();
            self.activities
                .schedule(ActivityCommand {
                    res_model: "interview",
                    res_id: id,
                    summary: format!("Interview round scheduled for {date}"),
                    note: None,
                    deadline: Some(date),
                    user_id,
                })
                .await
                .map_err(|e| InterviewError::ActivityDelivery(e.message))?;
        }
        Ok(id)
    }

    /// Scheduled → completed, recording the outcome.
    pub async fn complete(
        &self,
        company: Uuid,
        interview_id: Uuid,
        rating: Option<i32>,
        feedback: Option<String>,
    ) -> Result<(), InterviewError> {
        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company).await?;

        let status: Option<String> = sqlx::query_scalar(
            "SELECT status::text FROM recruitment.interviews WHERE id = $1 AND company_id = $2 \
             FOR UPDATE",
        )
        .bind(interview_id)
        .bind(company)
        .fetch_optional(&mut *tx)
        .await?;
        match status.as_deref() {
            None => {
                tx.rollback().await?;
                return Err(InterviewError::NotFound(interview_id));
            }
            Some("scheduled") | Some("no_show") => {}
            Some(s) => {
                tx.rollback().await?;
                return Err(InterviewError::NotTransitionable(interview_id, s.to_string()));
            }
        }

        sqlx::query(
            "UPDATE recruitment.interviews \
             SET status = 'completed', rating = $2, feedback = $3 WHERE id = $1",
        )
        .bind(interview_id)
        .bind(rating)
        .bind(feedback)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    /// Scheduled → cancelled (a completed interview is history; it does not
    /// un-happen).
    pub async fn cancel(&self, company: Uuid, interview_id: Uuid) -> Result<(), InterviewError> {
        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company).await?;

        let status: Option<String> = sqlx::query_scalar(
            "SELECT status::text FROM recruitment.interviews WHERE id = $1 AND company_id = $2 \
             FOR UPDATE",
        )
        .bind(interview_id)
        .bind(company)
        .fetch_optional(&mut *tx)
        .await?;
        match status.as_deref() {
            None => {
                tx.rollback().await?;
                return Err(InterviewError::NotFound(interview_id));
            }
            Some("scheduled") => {}
            Some(s) => {
                tx.rollback().await?;
                return Err(InterviewError::NotTransitionable(interview_id, s.to_string()));
            }
        }

        sqlx::query("UPDATE recruitment.interviews SET status = 'cancelled' WHERE id = $1")
            .bind(interview_id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }
}
