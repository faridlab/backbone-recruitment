//! The requisition open lifecycle (#550): the engine-gated lane for moving
//! a hiring request from draft to open.
//!
//! Requisitions keep their generic write surface (low-invariant master
//! data); the OPEN transition is the one step with a gate on it — a
//! requisition carrying headcount and budget should not accept
//! applications until the approvals engine says so, when a policy exists.
//!
//! `open_requisition` files the draft into the engine (the `requisition`
//! resource type) when the port is wired and returns the request id; the
//! flip to open happens on `confirm_open` (the caller re-invokes, or the
//! compose-side settlement dispatcher drives it from the verdict). An
//! unwired port opens directly — the pre-engine behaviour, unchanged. A
//! wired port never lets an unfiled draft open: the open verb files it first,
//! and an engine with no policy for requisitions approves the filing outright.
//!
//! After opening, the status moves only through the verbs here:
//!
//! ```text
//! open    --hold-->   on_hold
//! on_hold --resume--> open
//! open | on_hold          --close-->  closed
//! draft | open | on_hold  --cancel--> cancelled
//! ```
//!
//! `closed` and `cancelled` are final. Each verb is idempotent: asking for the
//! state the requisition is already in changes nothing.

use std::sync::RwLock;
use uuid::Uuid;

pub struct RequisitionLifecycleService {
    pool: sqlx::PgPool,
    approvals: RwLock<
        std::sync::Arc<dyn super::recruitment_approvals_port::RecruitmentFilingPort>,
    >,
}

#[derive(Debug, thiserror::Error)]
pub enum RequisitionError {
    #[error("requisition {0} not found")]
    NotFound(Uuid),
    #[error("requisition {0} is not draft (status: {1})")]
    NotDraft(Uuid, String),
    #[error("the requisition's approval has not been granted by the engine")]
    ApprovalNotGranted,
    #[error("the requisition has not been filed for approval; ask to open it first")]
    ApprovalNotFiled,
    #[error("requisition {id} cannot {verb} while it is {status}")]
    InvalidTransition {
        id: Uuid,
        verb: &'static str,
        status: String,
    },
    #[error("approvals transport: {0}")]
    Transport(String),
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

impl RequisitionError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotFound(_) => "requisition_not_found",
            Self::NotDraft(..) => "requisition_not_draft",
            Self::ApprovalNotGranted => "approval_not_granted",
            Self::ApprovalNotFiled => "approval_not_filed",
            Self::InvalidTransition { .. } => "requisition_invalid_transition",
            Self::Transport(_) => "approvals_seam_error",
            Self::Db(_) => "internal_error",
        }
    }

    pub fn http_status(&self) -> u16 {
        match self {
            Self::NotFound(_) => 404,
            Self::NotDraft(..) => 409,
            Self::ApprovalNotGranted => 409,
            Self::ApprovalNotFiled => 409,
            Self::InvalidTransition { .. } => 409,
            Self::Transport(_) => 502,
            Self::Db(_) => 500,
        }
    }
}

impl From<super::recruitment_approvals_port::RecruitmentSeamError> for RequisitionError {
    fn from(e: super::recruitment_approvals_port::RecruitmentSeamError) -> Self {
        Self::Transport(e.to_string())
    }
}

impl RequisitionLifecycleService {
    /// The database this verb runs on: the composer's request pool when the
    /// tenant router installed one, else the composed pool.
    fn rpool(&self) -> sqlx::PgPool {
        crate::request_pool::current().unwrap_or_else(|| self.pool.clone())
    }

    pub fn new(pool: sqlx::PgPool) -> Self {
        Self {
            pool,
            approvals: RwLock::new(std::sync::Arc::new(
                super::recruitment_approvals_port::UnwiredRecruitmentApprovals,
            )),
        }
    }

    /// Wire the approvals port (the composing service's adapter).
    pub fn set_approvals(
        &self,
        port: std::sync::Arc<dyn super::recruitment_approvals_port::RecruitmentFilingPort>,
    ) {
        *self.approvals.write().expect("requisition approvals lock poisoned") = port;
    }

    /// File a draft requisition into the engine. Returns the approval
    /// request id when filed; `None` when the port is unwired (the caller
    /// may then open directly — the pre-engine behaviour).
    pub async fn file_open(
        &self,
        requisition_id: Uuid,
    ) -> Result<Option<Uuid>, RequisitionError> {
        let mut tx = self.rpool().begin().await?;
        if let Some(scope) = backbone_orm::org_scope::current_org_scope() {
            backbone_orm::org_scope::bind_org_scope_on(&mut tx, &scope).await?;
        }
        let row: Option<(String, String, i32, Option<String>, Uuid)> = sqlx::query_as(
            r#"SELECT status::text, title, headcount, employment_type, opened_by
                 FROM recruitment.job_requisitions
                WHERE id = $1 AND (metadata->>'deleted_at') IS NULL
                FOR UPDATE"#,
        )
        .bind(requisition_id)
        .fetch_optional(&mut *tx)
        .await?;
        let (status, title, headcount, employment_type, opened_by) = match row {
            Some(r) => r,
            None => return Err(RequisitionError::NotFound(requisition_id)),
        };
        if status != "draft" {
            tx.rollback().await?;
            return Err(RequisitionError::NotDraft(requisition_id, status));
        }
        tx.commit().await?;

        let port = self
            .approvals
            .read()
            .expect("requisition approvals lock poisoned")
            .clone();
        match port
            .file_requisition(&super::recruitment_approvals_port::RequisitionFiling {
                requisition_id,
                opened_by,
                title,
                headcount,
                employment_type,
            })
            .await
        {
            Ok(request_id) => {
                let mut tx = self.rpool().begin().await?;
                if let Some(scope) = backbone_orm::org_scope::current_org_scope() {
                    backbone_orm::org_scope::bind_org_scope_on(&mut tx, &scope).await?;
                }
                sqlx::query(
                    "UPDATE recruitment.job_requisitions SET approval_request_id = $2 \
                     WHERE id = $1",
                )
                .bind(requisition_id)
                .bind(request_id)
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
                Ok(Some(request_id))
            }
            Err(super::recruitment_approvals_port::RecruitmentSeamError::Unwired) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Flip a draft requisition to open — verdict-gated when a link exists
    /// (the same pull-based fail-closed posture every engine-gated verb
    /// uses). With the approvals port wired, a draft that was never filed is
    /// refused (`approval_not_filed`); unwired, it opens directly. Returns
    /// false when already open (idempotent).
    pub async fn confirm_open(&self, requisition_id: Uuid) -> Result<bool, RequisitionError> {
        let mut tx = self.rpool().begin().await?;
        if let Some(scope) = backbone_orm::org_scope::current_org_scope() {
            backbone_orm::org_scope::bind_org_scope_on(&mut tx, &scope).await?;
        }
        let row: Option<(String, Option<Uuid>)> = sqlx::query_as(
            r#"SELECT status::text, approval_request_id
                 FROM recruitment.job_requisitions
                WHERE id = $1 AND (metadata->>'deleted_at') IS NULL
                FOR UPDATE"#,
        )
        .bind(requisition_id)
        .fetch_optional(&mut *tx)
        .await?;
        let (status, link) = match row {
            Some(r) => r,
            None => return Err(RequisitionError::NotFound(requisition_id)),
        };
        if status == "open" {
            tx.rollback().await?;
            return Ok(false);
        }
        if status != "draft" {
            tx.rollback().await?;
            return Err(RequisitionError::NotDraft(requisition_id, status));
        }
        let port = self
            .approvals
            .read()
            .expect("requisition approvals lock poisoned")
            .clone();
        if link.is_none() && port.is_wired() {
            tx.rollback().await?;
            return Err(RequisitionError::ApprovalNotFiled);
        }
        if let Some(request_id) = link {
            if !matches!(
                port.status(request_id).await,
                Ok(super::recruitment_approvals_port::RecruitmentVerdict::Approved)
            ) {
                tx.rollback().await?;
                return Err(RequisitionError::ApprovalNotGranted);
            }
        }
        sqlx::query(
            "UPDATE recruitment.job_requisitions SET status = 'open' \
             WHERE id = $1 AND status = 'draft'",
        )
        .bind(requisition_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Pause hiring on an open requisition. Returns false when already on hold.
    pub async fn hold(&self, requisition_id: Uuid) -> Result<bool, RequisitionError> {
        self.move_status(requisition_id, "hold", &["open"], "on_hold").await
    }

    /// Resume hiring on a requisition on hold. Returns false when already open.
    pub async fn resume(&self, requisition_id: Uuid) -> Result<bool, RequisitionError> {
        self.move_status(requisition_id, "resume", &["on_hold"], "open").await
    }

    /// Close an open or held requisition: hiring for it is done. Returns false
    /// when already closed.
    pub async fn close(&self, requisition_id: Uuid) -> Result<bool, RequisitionError> {
        self.move_status(requisition_id, "close", &["open", "on_hold"], "closed").await
    }

    /// Withdraw a requisition that has not closed. Returns false when already
    /// cancelled.
    pub async fn cancel(&self, requisition_id: Uuid) -> Result<bool, RequisitionError> {
        self.move_status(requisition_id, "cancel", &["draft", "open", "on_hold"], "cancelled")
            .await
    }

    /// Move the status from one of `from` to `to` under a row lock. The
    /// target state itself is a no-op (`Ok(false)`); any other state refuses.
    async fn move_status(
        &self,
        requisition_id: Uuid,
        verb: &'static str,
        from: &[&str],
        to: &str,
    ) -> Result<bool, RequisitionError> {
        let mut tx = self.rpool().begin().await?;
        if let Some(scope) = backbone_orm::org_scope::current_org_scope() {
            backbone_orm::org_scope::bind_org_scope_on(&mut tx, &scope).await?;
        }
        let status: Option<String> = sqlx::query_scalar(
            r#"SELECT status::text
                 FROM recruitment.job_requisitions
                WHERE id = $1 AND (metadata->>'deleted_at') IS NULL
                FOR UPDATE"#,
        )
        .bind(requisition_id)
        .fetch_optional(&mut *tx)
        .await?;
        let status = match status {
            Some(s) => s,
            None => return Err(RequisitionError::NotFound(requisition_id)),
        };
        if status == to {
            tx.rollback().await?;
            return Ok(false);
        }
        if !from.contains(&status.as_str()) {
            tx.rollback().await?;
            return Err(RequisitionError::InvalidTransition { id: requisition_id, verb, status });
        }
        sqlx::query(
            "UPDATE recruitment.job_requisitions SET status = $2::requisition_status WHERE id = $1",
        )
        .bind(requisition_id)
        .bind(to)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }
}
