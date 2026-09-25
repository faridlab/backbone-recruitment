//! Guarded route composition — the RECOMMENDED way to mount the recruitment
//! module (hand-authored, user-owned).
//!
//! The generated CRUD surface writes rows with no domain validation: a
//! generic update could set an offer `accepted` without staging the hire
//! handoff, or flip an application's `stage_id` without touching the
//! requisition's filled count. This composition closes that bypass:
//!
//! - every entity stays READABLE through the generated GET endpoints;
//! - low-invariant master data (candidates, requisitions, stages, offer
//!   letter templates, draft offers) also keeps the generic write endpoints;
//! - everything with a transition rule goes through a write-service verb:
//!   applications (create / move-stage / refuse), offers (extend / hire /
//!   decline / withdraw), interviews (schedule / complete / cancel),
//!   requisition skills (set / list).
//!
//! Every write handler extracts the [`OrgContext`] the composing service's
//! `org_auth` middleware inserts — the org identity comes from the verified
//! request, never the body. Extraction IS the gate: a request with no
//! verified org context never reaches a verb (standing 401). The verbs
//! themselves relay the ambient request scope onto their transactions, so a
//! deployment whose tenancy decorator fenced the tables does the actual
//! scoping at the row level.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use backbone_auth::org::OrgContext;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::application::service::{
    ApplicationError, ExtendOptions, InterviewError, InterviewWriteService,
    JobApplicationWriteService, JobOfferWriteService, NewInterview, NewJobApplication,
    NewJobOffer, OfferError, RequisitionSkillError, RequisitionSkillWriteService,
    SkillRequirement,
};
use crate::RecruitmentModule;

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: &'static str,
    message: String,
}
#[derive(Debug, Serialize)]
struct IdResponse {
    id: Uuid,
}
#[derive(Debug, Serialize)]
struct OkResponse {
    ok: bool,
}

fn status_of(code: u16) -> StatusCode {
    StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}

fn err_response(code: &'static str, status: u16, message: String) -> axum::response::Response {
    (status_of(status), Json(ErrorBody { error: code, message })).into_response()
}

// ── Applications ────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct CreateApplicationBody {
    candidate_id: Uuid,
    requisition_id: Uuid,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicApplyBody {
    requisition_id: Uuid,
    email: String,
    first_name: String,
    #[serde(default)]
    last_name: Option<String>,
    #[serde(default)]
    phone: Option<String>,
    /// Honeypot: a real applicant never fills it. Non-empty = spam.
    #[serde(default)]
    website: Option<String>,
}

/// The public intake (#559). Mounted on the module's guarded composer for
/// composition, but the HOST mounts this specific path bare (no auth): the
/// verb itself dedups the candidate by email and creates the application.
/// The honeypot field answers 202 whatever it holds — a bot learns nothing.
async fn public_apply(
    State(svc): State<Arc<JobApplicationWriteService>>,
    _org: Option<OrgContext>,
    Json(b): Json<PublicApplyBody>,
) -> axum::response::Response {
    if b.website.as_deref().unwrap_or("").trim() != "" {
        return (
            axum::http::StatusCode::ACCEPTED,
            Json(serde_json::json!({ "received": true })),
        )
            .into_response();
    }
    match svc
        .public_apply(
            &b.email,
            &b.first_name,
            b.last_name.as_deref(),
            b.phone.as_deref(),
            b.requisition_id,
        )
        .await
    {
        Ok((application_id, _candidate_id, created)) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "applicationId": application_id,
                "candidateCreated": created,
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": e.code(), "message": e.to_string() })),
        )
            .into_response(),
    }
}

async fn open_requisition(
    State(svc): State<Arc<crate::application::service::requisition_lifecycle::RequisitionLifecycleService>>,
    _org: OrgContext,
    Path(requisition_id): Path<Uuid>,
) -> axum::response::Response {
    match svc.file_open(requisition_id).await {
        Ok(Some(request_id)) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "approvalRequestId": request_id,
                "status": "pending",
            })),
        )
            .into_response(),
        // Unwired port: nothing to wait for — the caller confirms directly.
        Ok(None) => (
            StatusCode::OK,
            Json(serde_json::json!({ "status": "untracked" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::CONFLICT),
            Json(serde_json::json!({ "error": e.code(), "message": e.to_string() })),
        )
            .into_response(),
    }
}

async fn confirm_open_requisition(
    State(svc): State<Arc<crate::application::service::requisition_lifecycle::RequisitionLifecycleService>>,
    _org: OrgContext,
    Path(requisition_id): Path<Uuid>,
) -> axum::response::Response {
    match svc.confirm_open(requisition_id).await {
        Ok(true) => (
            StatusCode::OK,
            Json(serde_json::json!({ "status": "open" })),
        )
            .into_response(),
        Ok(false) => (
            StatusCode::OK,
            Json(serde_json::json!({ "status": "open", "already": true })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::CONFLICT),
            Json(serde_json::json!({ "error": e.code(), "message": e.to_string() })),
        )
            .into_response(),
    }
}

async fn create_application(
    State(svc): State<Arc<JobApplicationWriteService>>,
    _org: OrgContext,
    Json(b): Json<CreateApplicationBody>,
) -> axum::response::Response {
    match svc
        .create_application(NewJobApplication {
            candidate_id: b.candidate_id,
            requisition_id: b.requisition_id,
        })
        .await
    {
        Ok(id) => (StatusCode::CREATED, Json(IdResponse { id })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct MoveStageBody {
    to_stage_id: Uuid,
}

async fn move_stage(
    State(svc): State<Arc<JobApplicationWriteService>>,
    _org: OrgContext,
    Path(application_id): Path<Uuid>,
    Json(b): Json<MoveStageBody>,
) -> axum::response::Response {
    match svc.move_stage(application_id, b.to_stage_id).await {
        Ok(moved) => (StatusCode::OK, Json(OkResponse { ok: moved })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct RefuseBody {
    #[serde(default)]
    reason: Option<String>,
}

async fn refuse_application(
    State(svc): State<Arc<JobApplicationWriteService>>,
    _org: OrgContext,
    Path(application_id): Path<Uuid>,
    Json(b): Json<RefuseBody>,
) -> axum::response::Response {
    match svc.refuse(application_id, b.reason).await {
        Ok(()) => (StatusCode::OK, Json(OkResponse { ok: true })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

async fn application_pipeline(
    State(svc): State<Arc<JobApplicationWriteService>>,
    _org: OrgContext,
    Path(application_id): Path<Uuid>,
) -> axum::response::Response {
    match svc.pipeline(application_id).await {
        Ok(Some(p)) => (StatusCode::OK, Json(p)).into_response(),
        Ok(None) => err_response(
            ApplicationError::NotFound(application_id).code(),
            404,
            format!("application {application_id} not found"),
        ),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

// ── Offers ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize)]
struct CreateOfferBody {
    application_id: Option<Uuid>,
    #[serde(default)]
    proposed_salary: Option<rust_decimal::Decimal>,
    #[serde(default)]
    employment_type: Option<String>,
    #[serde(default)]
    letter_template_id: Option<Uuid>,
}

async fn create_offer(
    State(svc): State<Arc<JobOfferWriteService>>,
    _org: OrgContext,
    Json(b): Json<CreateOfferBody>,
) -> axum::response::Response {
    let application_id = match b.application_id {
        Some(id) => id,
        None => {
            return err_response("bad_request", 400, "applicationId is required".to_string())
        }
    };
    match svc
        .create_draft(NewJobOffer {
            application_id,
            proposed_salary: b.proposed_salary,
            employment_type: b.employment_type,
            letter_template_id: b.letter_template_id,
        })
        .await
    {
        Ok(id) => (StatusCode::CREATED, Json(IdResponse { id })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

#[derive(Debug, Default, Deserialize)]
struct ExtendBody {
    #[serde(default)]
    start_date: Option<NaiveDate>,
    #[serde(default)]
    company_name: Option<String>,
}

async fn extend_offer(
    State(svc): State<Arc<JobOfferWriteService>>,
    _org: OrgContext,
    Path(offer_id): Path<Uuid>,
    body: Option<Json<ExtendBody>>,
) -> axum::response::Response {
    let b = body.map(|Json(b)| b).unwrap_or_default();
    match svc
        .extend(
            offer_id,
            ExtendOptions { start_date: b.start_date, company_name: b.company_name },
        )
        .await
    {
        Ok(moved) => (StatusCode::OK, Json(OkResponse { ok: moved })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

async fn hire_offer(
    State(svc): State<Arc<JobOfferWriteService>>,
    _org: OrgContext,
    Path(offer_id): Path<Uuid>,
) -> axum::response::Response {
    match svc.hire(offer_id).await {
        Ok(Some(event_id)) => (StatusCode::OK, Json(IdResponse { id: event_id })).into_response(),
        // Idempotent no-op: the offer was already accepted; no second event.
        Ok(None) => (StatusCode::OK, Json(OkResponse { ok: false })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

async fn decline_offer(
    State(svc): State<Arc<JobOfferWriteService>>,
    _org: OrgContext,
    Path(offer_id): Path<Uuid>,
) -> axum::response::Response {
    match svc.decline(offer_id).await {
        Ok(()) => (StatusCode::OK, Json(OkResponse { ok: true })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

async fn withdraw_offer(
    State(svc): State<Arc<JobOfferWriteService>>,
    _org: OrgContext,
    Path(offer_id): Path<Uuid>,
) -> axum::response::Response {
    match svc.withdraw(offer_id).await {
        Ok(()) => (StatusCode::OK, Json(OkResponse { ok: true })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

// ── Interviews ──────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct ScheduleInterviewBody {
    application_id: Uuid,
    interviewer_id: Uuid,
    scheduled_at: DateTime<Utc>,
    #[serde(default)]
    round: Option<i32>,
    #[serde(default)]
    interview_format: Option<String>,
    /// The interviewer's login user, when the caller wants an activity on
    /// their plate. Omit to schedule silently.
    #[serde(default)]
    notify_user_id: Option<Uuid>,
}

async fn schedule_interview(
    State(svc): State<Arc<InterviewWriteService>>,
    _org: OrgContext,
    Json(b): Json<ScheduleInterviewBody>,
) -> axum::response::Response {
    match svc
        .schedule(NewInterview {
            application_id: b.application_id,
            interviewer_id: b.interviewer_id,
            scheduled_at: b.scheduled_at,
            round: b.round,
            interview_format: b.interview_format,
            notify_user_id: b.notify_user_id,
        })
        .await
    {
        Ok(id) => (StatusCode::CREATED, Json(IdResponse { id })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

#[derive(Debug, Default, Deserialize)]
struct CompleteInterviewBody {
    #[serde(default)]
    rating: Option<i32>,
    #[serde(default)]
    feedback: Option<String>,
}

async fn complete_interview(
    State(svc): State<Arc<InterviewWriteService>>,
    _org: OrgContext,
    Path(interview_id): Path<Uuid>,
    body: Option<Json<CompleteInterviewBody>>,
) -> axum::response::Response {
    let b = body.map(|Json(b)| b).unwrap_or_default();
    match svc.complete(interview_id, b.rating, b.feedback).await {
        Ok(()) => (StatusCode::OK, Json(OkResponse { ok: true })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

async fn cancel_interview(
    State(svc): State<Arc<InterviewWriteService>>,
    _org: OrgContext,
    Path(interview_id): Path<Uuid>,
) -> axum::response::Response {
    match svc.cancel(interview_id).await {
        Ok(()) => (StatusCode::OK, Json(OkResponse { ok: true })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

// ── Requisition skills ──────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct SetSkillsBody {
    skills: Vec<SkillLine>,
}

#[derive(Debug, Deserialize)]
struct SkillLine {
    skill_id: Uuid,
    required_proficiency: String,
}

async fn set_requisition_skills(
    State(svc): State<Arc<RequisitionSkillWriteService>>,
    _org: OrgContext,
    Path(requisition_id): Path<Uuid>,
    Json(b): Json<SetSkillsBody>,
) -> axum::response::Response {
    let skills = b
        .skills
        .into_iter()
        .map(|s| SkillRequirement {
            skill_id: s.skill_id,
            required_proficiency: s.required_proficiency,
        })
        .collect();
    match svc.set_skills(requisition_id, skills).await {
        Ok(()) => (StatusCode::OK, Json(OkResponse { ok: true })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

#[derive(Debug, Serialize)]
struct SkillLineOut {
    skill_id: Uuid,
    skill_name: Option<String>,
    required_proficiency: String,
}

async fn list_requisition_skills(
    State(svc): State<Arc<RequisitionSkillWriteService>>,
    _org: OrgContext,
    Path(requisition_id): Path<Uuid>,
) -> axum::response::Response {
    match svc.list_skills(requisition_id).await {
        Ok(rows) => (
            StatusCode::OK,
            Json(rows
                .into_iter()
                .map(|(skill_id, skill_name, required_proficiency)| SkillLineOut {
                    skill_id,
                    skill_name,
                    required_proficiency,
                })
                .collect::<Vec<_>>()),
        )
            .into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

// ── Composition ─────────────────────────────────────────────────────────────────

/// The verb routes (state-machine writes). Combined with read-only CRUD for
/// every entity; generic writes stay unmounted for the stateful entities.
/// Each service gets its own state-typed router; merging normalizes them.
fn create_recruitment_verb_routes(
    applications: Arc<JobApplicationWriteService>,
    offers: Arc<JobOfferWriteService>,
    interviews: Arc<InterviewWriteService>,
    skills: Arc<RequisitionSkillWriteService>,
    requisition_gate_svc: Arc<
        crate::application::service::requisition_lifecycle::RequisitionLifecycleService,
    >,
) -> Router {
    let requisition_gate = Router::new()
        .route("/requisitions/:id/open", post(open_requisition))
        .route("/requisitions/:id/confirm-open", post(confirm_open_requisition))
        .with_state(requisition_gate_svc);

    let applications = Router::new()
        .route("/applications", post(create_application))
        .route("/public/applications", post(public_apply))
        .route("/applications/:id/stage", post(move_stage))
        .route("/applications/:id/refuse", post(refuse_application))
        .route("/applications/:id/pipeline", get(application_pipeline))
        .with_state(applications);

    let offers = Router::new()
        .route("/offers", post(create_offer))
        .route("/offers/:id/extend", post(extend_offer))
        .route("/offers/:id/hire", post(hire_offer))
        .route("/offers/:id/decline", post(decline_offer))
        .route("/offers/:id/withdraw", post(withdraw_offer))
        .with_state(offers);

    let interviews = Router::new()
        .route("/interviews", post(schedule_interview))
        .route("/interviews/:id/complete", post(complete_interview))
        .route("/interviews/:id/cancel", post(cancel_interview))
        .with_state(interviews);

    let skills = Router::new()
        .route(
            "/requisitions/:id/skills",
            post(set_requisition_skills).get(list_requisition_skills),
        )
        .with_state(skills);

    Router::new()
        .merge(applications)
        .merge(offers)
        .merge(interviews)
        .merge(skills)
        .merge(requisition_gate)
}

/// Mount the recruitment module with write paths locked to validated verbs.
///
/// = read-only CRUD for every entity
/// + generic writes for low-invariant master data (candidates, requisitions,
///   stages, letter templates)
/// + the state-machine verbs (applications / offers / interviews / skills) —
///   offers and interviews have NO generic write surface at all, so no path
///   can set a lifecycle state directly and sidestep a verb's side effects
///   (the hire outbox emit, the vacancy coupling).
/// **Prefer this over `RecruitmentModule::all_crud_routes()` for any real
/// deployment.**
pub fn create_guarded_recruitment_routes(m: &RecruitmentModule) -> Router {
    use crate::presentation::http::{
        create_candidate_write_routes, create_job_requisition_write_routes,
        create_offer_letter_template_write_routes, create_recruitment_stage_write_routes,
    };

    Router::new()
        // Safe base: GET-only for all eight entities.
        .merge(m.readonly_routes())
        // Master data keeps generic writes (no cross-entity invariants).
        .merge(create_candidate_write_routes(m.candidate_service.clone()))
        .merge(create_job_requisition_write_routes(m.job_requisition_service.clone()))
        .merge(create_recruitment_stage_write_routes(m.recruitment_stage_service.clone()))
        .merge(create_offer_letter_template_write_routes(m.offer_letter_template_service.clone()))
        // State machines + the validated create paths.
        .merge(create_recruitment_verb_routes(
            m.job_application_write_service.clone(),
            m.job_offer_write_service.clone(),
            m.interview_write_service.clone(),
            m.requisition_skill_write_service.clone(),
            m.requisition_lifecycle.clone(),
        ))
}

// Keep the error types referenced even if a handler path is compiled out.
#[allow(dead_code)]
fn _error_types_referenced(
    _: ApplicationError,
    _: OfferError,
    _: InterviewError,
    _: RequisitionSkillError,
) {
}
