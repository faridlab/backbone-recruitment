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
//! unwired port opens directly — the pre-engine behaviour, unchanged.

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
            Self::Transport(_) => "approvals_seam_error",
            Self::Db(_) => "internal_error",
        }
    }

    pub fn http_status(&self) -> u16 {
        match self {
            Self::NotFound(_) => 404,
            Self::NotDraft(..) => 409,
            Self::ApprovalNotGranted => 409,
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
        let mut tx = self.pool.begin().await?;
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
                let mut tx = self.pool.begin().await?;
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
    /// uses). Returns false when already open (idempotent).
    pub async fn confirm_open(&self, requisition_id: Uuid) -> Result<bool, RequisitionError> {
        let mut tx = self.pool.begin().await?;
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
        if let Some(request_id) = link {
            let port = self
                .approvals
                .read()
                .expect("requisition approvals lock poisoned")
                .clone();
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
}
