//! The approvals seam for recruitment workflow gates (#550): the port a
//! requisition's opening and an offer's extension file through, and read
//! back — the same TR2 posture leave, overtime, corrections and promotions
//! use. The default is unwired: an unwired deployment keeps the direct
//! verbs exactly as they were.

use async_trait::async_trait;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use uuid::Uuid;

/// What a requisition filing carries: enough of the ask for an approver to
/// render a verdict row without another read.
#[derive(Debug, Clone)]
pub struct RequisitionFiling {
    pub requisition_id: Uuid,
    pub opened_by: Uuid,
    pub title: String,
    pub headcount: i32,
    pub employment_type: Option<String>,
}

/// What an offer filing carries.
#[derive(Debug, Clone)]
pub struct OfferFiling {
    pub offer_id: Uuid,
    pub application_id: Uuid,
    pub employee_id: Uuid,
    pub proposed_salary: Option<Decimal>,
    pub employment_type: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecruitmentVerdict {
    Pending,
    Approved,
    Rejected,
}

#[derive(Debug, thiserror::Error)]
pub enum RecruitmentSeamError {
    #[error("recruitment approvals seam is not wired")]
    Unwired,
    #[error("no approval request {0} is known to the engine")]
    UnknownApprovalRequest(Uuid),
    #[error("approvals transport: {0}")]
    Transport(String),
}

/// One port, two resource kinds (requisition, offer) — the engine key and
/// the verdict shape are shared; only the filing differs.
#[async_trait]
pub trait RecruitmentFilingPort: Send + Sync {
    async fn file_requisition(
        &self,
        filing: &RequisitionFiling,
    ) -> Result<Uuid, RecruitmentSeamError>;
    async fn file_offer(&self, filing: &OfferFiling) -> Result<Uuid, RecruitmentSeamError>;
    async fn status(&self, approval_request_id: Uuid)
        -> Result<RecruitmentVerdict, RecruitmentSeamError>;
}

/// The unwired default.
pub struct UnwiredRecruitmentApprovals;

#[async_trait]
impl RecruitmentFilingPort for UnwiredRecruitmentApprovals {
    async fn file_requisition(
        &self,
        _filing: &RequisitionFiling,
    ) -> Result<Uuid, RecruitmentSeamError> {
        Err(RecruitmentSeamError::Unwired)
    }
    async fn file_offer(&self, _filing: &OfferFiling) -> Result<Uuid, RecruitmentSeamError> {
        Err(RecruitmentSeamError::Unwired)
    }
    async fn status(
        &self,
        id: Uuid,
    ) -> Result<RecruitmentVerdict, RecruitmentSeamError> {
        Err(RecruitmentSeamError::UnknownApprovalRequest(id))
    }
}

/// Re-exported for the filing builders' date leg (kept for the caller's
/// convenience; the filings above carry no dates themselves).
pub type FilingDate = NaiveDate;
