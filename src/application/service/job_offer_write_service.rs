//! Offer write-service (hand-authored, user-owned): extend / hire / decline /
//! withdraw — the offer half of the recruitment → employee handoff.
//!
//! Every verb that opens its own transaction relays the ambient request scope
//! (when one is bound) onto that transaction before any statement runs, so a
//! deployment whose composing service installed the tenancy decorator's
//! row-level fence sees only the caller's org-unit rows. Unfenced deployments
//! have no ambient scope and the relay is skipped entirely — the module stays
//! posture-agnostic. The one exception that still NEEDS a tenant: the
//! durable `recruitment.hired` outbox record is a still-company-keyed
//! framework surface, so [`JobOfferWriteService::hire`] takes the owning
//! tenant from the ambient org scope's legacy company id and fails closed
//! when the request carries none — a composition fault, not a caller error.
//!
//! [`JobOfferWriteService::hire`] is the producer side of the hire handoff:
//! in a SINGLE database transaction it (1) marks the `JobOffer` accepted
//! (`extended` → `accepted`, stamps `accepted_at`), (2) assembles the
//! new-employee fields from the offer + application + candidate + requisition,
//! and (3) stages a [`HIRED_EVENT_TYPE`] row into `recruitment.outbox_events`
//! via the framework's [`backbone_outbox::outbox::stage`]. That in-tx write is
//! the load-bearing invariant: the offer-accept and the event-emit commit
//! atomically, so there is never an "accepted with no handoff started" window.
//!
//! Two doors into "hired" stay coherent: `move_stage` owns the requisition's
//! vacancy count (an application enters an `is_hired` stage there), while
//! `hire` owns the employee handoff and REFUSES to run unless the linked
//! application already sits in an `is_hired` stage — you cannot hire someone
//! the pipeline has not hired.
//!
//! The offer verbs refuse on a full requisition too: `create_draft` and
//! `extend` answer [`OfferError::NoOpenHeadcount`] when the requisition has
//! no openings left (`headcount - filled_headcount <= 0`) — unless the
//! application already sits in a hired stage and so holds one of the filled
//! openings itself. The candidate is never offered, or told "yes", for a seat
//! that does not exist; the rule lives in [`super::requisition_vacancy`].
//!
//! `extend` records the promised first working day (`start_date`) on the
//! offer, and `hire` hands it on as the new employee's join date in the
//! `recruitment.hired` payload (see [`hired_event_payload`]).
//!
//! `extend` optionally sends the offer letter: when the offer references a
//! letter template, the body is rendered from the candidate/offer context and
//! handed to the [`OfferLetterSink`] port. An unwired sink plus an explicit
//! template fails closed BEFORE the offer changes state (nothing silently
//! unsent); a wired adapter is called after commit (its transport owns its own
//! durability) and a delivery failure is surfaced to the caller.
//!
//! This is a user-owned custom file — it is NEVER regenerated, so it is safe
//! to edit freely.

use std::sync::Arc;

use backbone_orm::org_scope;
use backbone_outbox::{outbox, OutboxRecord};
use chrono::{NaiveDate, Utc};
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::letter_port::{LetterMessage, OfferLetterSink, UnwiredOfferLetterSink};
use super::offer_letter_render::{format_date_long, format_salary_idr, render};
use super::requisition_vacancy::{require_opening, NoOpenings};

/// The `event_type` stamped on every hire outbox row. The employee consumer
/// subscribes to exactly this pattern (`"recruitment.hired"`).
pub const HIRED_EVENT_TYPE: &str = "recruitment.hired";

/// Errors from the offer write-service.
#[derive(Debug, thiserror::Error)]
pub enum OfferError {
    /// No `JobOffer` exists for the given id visible to the caller.
    #[error("offer {0} not found")]
    NotFound(Uuid),
    /// The operation refused on the offer's shape (no template, wrong state).
    #[error("{0}")]
    InvalidState(&'static str),
    /// `create_draft` on an application that does not exist.
    #[error("application {0} not found")]
    ApplicationNotFound(Uuid),
    /// The offer exists but is not in a state that permits this verb.
    #[error("offer {offer_id} is not extensible (status: {status})")]
    NotExtensible { offer_id: Uuid, status: String },
    /// `hire` on an application whose stage is not flagged `is_hired`.
    #[error("application {application_id} is not in a hired stage — move it there first")]
    ApplicationNotHired { application_id: Uuid },
    /// The approvals engine has not granted the offer's extension.
    #[error("the offer's approval has not been granted by the engine")]
    ApprovalNotGranted,
    /// `extend` on an application that was already refused.
    #[error("application {0} was refused — no offer may be extended")]
    ApplicationRefused(Uuid),
    /// `extend` on an application whose requisition is not open.
    #[error("requisition {0} is not open")]
    RequisitionNotOpen(Uuid),
    /// `create_draft` / `extend` on a requisition with every opening filled.
    #[error("{0}")]
    NoOpenHeadcount(NoOpenings),
    /// A letter was requested (template set) but no adapter is wired.
    #[error("the letter seam is not wired — supply an OfferLetterSink to send letters")]
    LetterSeamUnwired,
    /// The wired adapter accepted the offer change but failed delivery.
    /// The offer IS extended; only the letter failed — retry just the send.
    #[error("letter delivery failed (offer is extended): {0}")]
    LetterDelivery(String),
    /// The hire handoff must name the owning tenant for the still-company-keyed
    /// outbox record, but the request carries no org scope whose legacy company
    /// could name it. This is a composition fault — the service must be mounted
    /// under a scope-resolving auth middleware — not a caller error, so it
    /// fails loud instead of guessing.
    #[error("no org scope bound: {0}")]
    OrgScopeRequired(&'static str),
    /// A database failure.
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    /// An outbox staging failure.
    #[error("outbox error: {0}")]
    Outbox(#[from] backbone_outbox::OutboxError),
}

impl OfferError {
    /// Stable machine code for the HTTP surface.
    pub fn code(&self) -> &'static str {
        match self {
            OfferError::InvalidState(_) => "offer_not_extensible",
            OfferError::ApprovalNotGranted => "approval_not_granted",
            OfferError::NotFound(_) => "offer_not_found",
            OfferError::ApplicationNotFound(_) => "application_not_found",
            OfferError::NotExtensible { .. } => "offer_not_extensible",
            OfferError::ApplicationNotHired { .. } => "application_not_hired",
            OfferError::ApplicationRefused(_) => "application_refused",
            OfferError::RequisitionNotOpen(_) => "requisition_not_open",
            OfferError::NoOpenHeadcount(_) => "no_open_headcount",
            OfferError::LetterSeamUnwired => "letter_seam_unwired",
            OfferError::LetterDelivery(_) => "letter_delivery_failed",
            OfferError::OrgScopeRequired(_) => "org_scope_required",
            OfferError::Db(_) => "internal_error",
            OfferError::Outbox(_) => "outbox_error",
        }
    }
    pub fn http_status(&self) -> u16 {
        match self {
            OfferError::InvalidState(_) => 409,
            OfferError::ApprovalNotGranted => 409,
            OfferError::NotFound(_) | OfferError::ApplicationNotFound(_) => 404,
            OfferError::ApplicationNotHired { .. } => 409,
            OfferError::NotExtensible { .. }
            | OfferError::ApplicationRefused(_)
            | OfferError::RequisitionNotOpen(_) => 422,
            OfferError::LetterSeamUnwired => 422,
            // Full requisition: a business-rule refusal, the same status the
            // stage move answers with.
            OfferError::NoOpenHeadcount(_) => 409,
            OfferError::LetterDelivery(_) => 502,
            // A composition fault, not a caller error.
            OfferError::OrgScopeRequired(_) => 500,
            OfferError::Db(_) | OfferError::Outbox(_) => 500,
        }
    }
}

/// Input for `create_draft` — the only way an offer row comes to exist.
#[derive(Debug, Clone)]
pub struct NewJobOffer {
    pub application_id: Uuid,
    pub proposed_salary: Option<Decimal>,
    pub employment_type: Option<String>,
    /// Letter template to render when the offer is extended (optional — no
    /// template, no letter).
    pub letter_template_id: Option<Uuid>,
}

/// Optional context for the rendered offer letter. Every field is optional
/// and an omitted field is NEVER invented: its template token falls back to
/// the template's own wording (`{{start_date|as agreed}}`) or stays visible.
#[derive(Debug, Clone, Default)]
pub struct ExtendOptions {
    /// The promised first working day. Omitted: no default to today — the
    /// letter renders the template's fallback wording for the token.
    pub start_date: Option<NaiveDate>,
    /// Company display name for the salutation. Not owned by this module, so
    /// the caller supplies it; omitted → the `{{company_name}}` token stays
    /// visible in the letter.
    pub company_name: Option<String>,
}

/// Letter variables, shared by `preview_letter` and `extend` so the two
/// doors always render the identical letter. Formal-document values carry
/// their written form (`Rp 12.000.000`, `27 September 2026`); the machine
/// forms stay reachable as `proposed_salary_raw` / `start_date_iso`.
/// Optional facts are OMITTED when unknown so the template's fallback
/// wording applies — never a silent empty string, never an invented date.
fn letter_vars(
    candidate_first_name: String,
    position_title: String,
    proposed_salary: Option<Decimal>,
    company_name: Option<String>,
    start_date: Option<NaiveDate>,
) -> serde_json::Value {
    let mut vars = serde_json::Map::new();
    vars.insert(
        "candidate_first_name".to_string(),
        serde_json::json!(candidate_first_name),
    );
    vars.insert(
        "position_title".to_string(),
        serde_json::json!(position_title),
    );
    if let Some(salary) = proposed_salary {
        vars.insert(
            "proposed_salary".to_string(),
            serde_json::json!(format_salary_idr(salary)),
        );
        vars.insert(
            "proposed_salary_raw".to_string(),
            serde_json::json!(salary.to_string()),
        );
    }
    if let Some(name) = company_name {
        vars.insert("company_name".to_string(), serde_json::json!(name));
    }
    if let Some(date) = start_date {
        vars.insert(
            "start_date".to_string(),
            serde_json::json!(format_date_long(date)),
        );
        vars.insert(
            "start_date_iso".to_string(),
            serde_json::json!(date.to_string()),
        );
    }
    serde_json::Value::Object(vars)
}

/// Everything the hire hands to the employee side, read inside the hire
/// transaction from the offer, its application, the candidate and the
/// requisition the application answered.
#[derive(Debug, Clone)]
pub struct HiredFacts {
    pub offer_id: Uuid,
    /// The owning tenant (the ambient org scope's legacy company id).
    pub company_id: Uuid,
    pub application_id: Uuid,
    pub requisition_id: Option<Uuid>,
    pub candidate_id: Option<Uuid>,
    pub first_name: String,
    pub last_name: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub employment_type: Option<String>,
    pub proposed_salary: Option<Decimal>,
    pub position_id: Option<Uuid>,
    pub department_id: Option<Uuid>,
    /// The requisition's role title.
    pub position_title: Option<String>,
    /// The promised first working day recorded on the offer at extend.
    pub start_date: Option<NaiveDate>,
}

/// The `recruitment.hired` payload — the contract the employee-side
/// consumers read. `accepted_on` is the day the hire runs.
///
/// Dates are ISO `YYYY-MM-DD` strings, the salary a decimal string, ids
/// UUID strings; an unknown fact is `null`, never invented.
///
/// - `join_date` (always set): the first working day — the offer's agreed
///   `start_date` when one was recorded at extend, else `accepted_on`.
/// - `start_date` (nullable): the agreed first day exactly as recorded on
///   the offer, so a consumer can tell an agreed date from the fallback.
/// - `accepted_on`: the day the offer was accepted (the hire ran).
pub fn hired_event_payload(f: &HiredFacts, accepted_on: NaiveDate) -> serde_json::Value {
    let join_date = f.start_date.unwrap_or(accepted_on);
    serde_json::json!({
        // Identity — the consumer dedups on the envelope id and keys the
        // employee off the offer, so the payload is replay safe.
        "offer_id": f.offer_id,
        "company_id": f.company_id,
        "application_id": f.application_id,
        "requisition_id": f.requisition_id,
        "candidate_id": f.candidate_id,
        // The person.
        "first_name": f.first_name,
        "last_name": f.last_name,
        "email": f.email,
        "phone": f.phone,
        // Offer terms.
        "employment_type": f.employment_type,
        "proposed_salary": f.proposed_salary.map(|d| d.to_string()),
        // Org placement from the requisition the application answered.
        "position_id": f.position_id,
        "department_id": f.department_id,
        "position_title": f.position_title,
        // Dates.
        "join_date": join_date.to_string(),
        "start_date": f.start_date.map(|d| d.to_string()),
        "accepted_on": accepted_on.to_string(),
    })
}

/// The offer write-service: the one door for offer state transitions and the
/// hire-handoff producer.
pub struct JobOfferWriteService {
    pool: PgPool,
    letters: Arc<dyn OfferLetterSink>,
    approvals: std::sync::RwLock<
        std::sync::Arc<dyn super::recruitment_approvals_port::RecruitmentFilingPort>,
    >,
}

impl JobOfferWriteService {
    /// The database this verb runs on: the composer's request pool when the
    /// tenant router installed one, else the composed pool.
    fn rpool(&self) -> sqlx::PgPool {
        crate::request_pool::current().unwrap_or_else(|| self.pool.clone())
    }

    /// Unwired default — letters explicitly requested will fail closed.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            letters: Arc::new(UnwiredOfferLetterSink),
            approvals: std::sync::RwLock::new(std::sync::Arc::new(
                super::recruitment_approvals_port::UnwiredRecruitmentApprovals,
            )),
        }
    }

    /// Bind a real letter adapter (the host app's mail seam).
    pub fn with_letter_sink(pool: PgPool, sink: Arc<dyn OfferLetterSink>) -> Self {
        Self {
            pool,
            letters: sink,
            approvals: std::sync::RwLock::new(std::sync::Arc::new(
                super::recruitment_approvals_port::UnwiredRecruitmentApprovals,
            )),
        }
    }

    /// Wire the approvals port (the composing service's adapter).
    pub fn set_approvals(
        &self,
        port: std::sync::Arc<dyn super::recruitment_approvals_port::RecruitmentFilingPort>,
    ) {
        *self
            .approvals
            .write()
            .expect("offer approvals lock poisoned") = port;
    }

    /// Create an offer in `draft` for an ongoing application. Offers only
    /// come to exist here — the generic CRUD write surface stays unmounted for
    /// offers precisely so no path can set `status` directly and sidestep
    /// [`JobOfferWriteService::hire`]'s atomic accept+emit.
    pub async fn create_draft(&self, input: NewJobOffer) -> Result<Uuid, OfferError> {
        let mut tx = self.rpool().begin().await?;
        // Relay the ambient request scope, when one is bound, onto this transaction:
        // a decorated deployment's row fence reads it; an unfenced one skips this.
        if let Some(scope) = org_scope::current_org_scope() {
            org_scope::bind_org_scope_on(&mut *tx, &scope).await?;
        }

        // The application with its requisition's counters and whether it
        // already holds an opening (sits in a hired stage).
        let row = sqlx::query(
            r#"SELECT a.refused_at                    AS refused_at,
                      r.id                            AS requisition_id,
                      r.headcount                     AS headcount,
                      r.filled_headcount              AS filled_headcount,
                      COALESCE(s.is_hired, FALSE)     AS holds_opening
                 FROM recruitment.job_applications a
            LEFT JOIN recruitment.job_requisitions r  ON r.id = a.requisition_id
            LEFT JOIN recruitment.recruitment_stages s ON s.id = a.stage_id
                WHERE a.id = $1"#,
        )
        .bind(input.application_id)
        .fetch_optional(&mut *tx)
        .await?;
        let row = match row {
            None => {
                tx.rollback().await?;
                return Err(OfferError::ApplicationNotFound(input.application_id));
            }
            Some(r) => r,
        };
        if row
            .try_get::<Option<chrono::DateTime<Utc>>, _>("refused_at")?
            .is_some()
        {
            tx.rollback().await?;
            return Err(OfferError::ApplicationRefused(input.application_id));
        }
        // A full requisition refuses the draft before it is filed for
        // approval — nobody approves, or tells a candidate about, a seat
        // that does not exist.
        if let (Some(requisition_id), Some(headcount), Some(filled)) = (
            row.try_get::<Option<Uuid>, _>("requisition_id")?,
            row.try_get::<Option<i32>, _>("headcount")?,
            row.try_get::<Option<i32>, _>("filled_headcount")?,
        ) {
            let holds_opening: bool = row.try_get("holds_opening")?;
            if let Err(full) = require_opening(requisition_id, headcount, filled, holds_opening) {
                tx.rollback().await?;
                return Err(OfferError::NoOpenHeadcount(full));
            }
        }

        let id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO recruitment.job_offers
                   (id, application_id, proposed_salary, employment_type,
                    letter_template_id, status, metadata)
               VALUES ($1, $2, $3, $4, $5, 'draft',
                       '{"created_at":null,"updated_at":null,"deleted_at":null,
                         "created_by":null,"updated_by":null,"deleted_by":null}'::jsonb)"#,
        )
        .bind(id)
        .bind(input.application_id)
        .bind(input.proposed_salary)
        .bind(&input.employment_type)
        .bind(input.letter_template_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        // File into the engine when wired (#550): the extension waits for
        // the verdict. An unwired deployment keeps the direct verb.
        // The filing facts read on a SCOPE-BOUND transaction — a raw pool
        // read runs unfenced under the decorator's RLS and the just-written
        // offer is invisible, silently skipping the filing.
        let mut facts_tx = self.rpool().begin().await?;
        if let Some(scope) = org_scope::current_org_scope() {
            org_scope::bind_org_scope_on(&mut facts_tx, &scope).await?;
        }
        let linked: Option<(Uuid, Uuid, Option<Decimal>, Option<String>)> =
            sqlx::query_as::<_, (Uuid, Uuid, Option<Decimal>, Option<String>)>(
                r#"SELECT a.id, COALESCE(r.opened_by, a.candidate_id),
                      o.proposed_salary, o.employment_type
                 FROM recruitment.job_offers o
                 JOIN recruitment.job_applications a ON a.id = o.application_id
            LEFT JOIN recruitment.job_requisitions r ON r.id = a.requisition_id
                WHERE o.id = $1"#,
            )
            .bind(id)
            .fetch_optional(&mut *facts_tx)
            .await?;
        facts_tx.commit().await?;
        if let Some((application_id, filer, proposed_salary, employment_type)) = linked {
            let port = self
                .approvals
                .read()
                .expect("offer approvals lock poisoned")
                .clone();
            match port
                .file_offer(&super::recruitment_approvals_port::OfferFiling {
                    offer_id: id,
                    application_id,
                    employee_id: filer,
                    proposed_salary,
                    employment_type,
                })
                .await
            {
                Ok(request_id) => {
                    let mut tx = self.rpool().begin().await?;
                    if let Some(scope) = org_scope::current_org_scope() {
                        org_scope::bind_org_scope_on(&mut tx, &scope).await?;
                    }
                    sqlx::query(
                        "UPDATE recruitment.job_offers SET approval_request_id = $2 WHERE id = $1",
                    )
                    .bind(id)
                    .bind(request_id)
                    .execute(&mut *tx)
                    .await?;
                    tx.commit().await?;
                }
                Err(super::recruitment_approvals_port::RecruitmentSeamError::Unwired) => {}
                // A WIRED port that fails takes the offer with it: a row
                // committed with no approval request behind it is half-made
                // (the extend gate refuses it, the screens show a draft
                // nothing can advance). Compensating delete — same pool,
                // same scope.
                Err(_) => {
                    let mut tx = self.rpool().begin().await?;
                    if let Some(scope) = org_scope::current_org_scope() {
                        org_scope::bind_org_scope_on(&mut tx, &scope).await?;
                    }
                    sqlx::query("DELETE FROM recruitment.job_offers WHERE id = $1")
                        .bind(id)
                        .execute(&mut *tx)
                        .await?;
                    tx.commit().await?;
                    return Err(OfferError::ApprovalNotGranted);
                }
            }
        }
        Ok(id)
    }

    /// Draft → extended. Idempotent: an already-extended offer is a no-op
    /// (`Ok(false)`). Guards: the linked application is not refused and its
    /// requisition is open. When the offer references a letter template the
    /// letter is rendered and sent through the [`OfferLetterSink`].
    /// Render the offer letter WITHOUT extending (#610): the same
    /// template, vars and renderer the extend verb uses, returned for the
    /// caller to preview. Refuses when the offer carries no template or
    /// the template is gone; never sends anything.
    pub async fn preview_letter(
        &self,
        offer_id: Uuid,
        company_name: Option<String>,
        start_date: Option<chrono::NaiveDate>,
    ) -> Result<(String, String), OfferError> {
        let mut tx = self.rpool().begin().await?;
        if let Some(scope) = org_scope::current_org_scope() {
            org_scope::bind_org_scope_on(&mut tx, &scope).await?;
        }
        let row = sqlx::query(
            r#"SELECT o.letter_template_id, o.proposed_salary, o.start_date,
                      c.first_name AS candidate_first_name,
                      r.title AS position_title
                 FROM recruitment.job_offers o
                 JOIN recruitment.job_applications a ON a.id = o.application_id
                 JOIN recruitment.candidates c      ON c.id = a.candidate_id
            LEFT JOIN recruitment.job_requisitions r ON r.id = a.requisition_id
                WHERE o.id = $1"#,
        )
        .bind(offer_id)
        .fetch_optional(&mut *tx)
        .await?;
        use sqlx::Row;
        let row = match row {
            Some(r) => r,
            None => {
                tx.rollback().await?;
                return Err(OfferError::NotFound(offer_id));
            }
        };
        let tid: Option<Uuid> = row.try_get("letter_template_id")?;
        let Some(tid) = tid else {
            tx.rollback().await?;
            return Err(OfferError::InvalidState(
                "the offer carries no letter template — nothing to preview",
            ));
        };
        let template_row = sqlx::query(
            "SELECT subject, body FROM recruitment.offer_letter_templates WHERE id = $1",
        )
        .bind(tid)
        .fetch_one(&mut *tx)
        .await?;
        let subject: String = template_row.try_get("subject")?;
        let body: String = template_row.try_get("body")?;
        let candidate_first_name: String = row.try_get("candidate_first_name")?;
        let position_title: String = row.try_get("position_title")?;
        let proposed_salary: Option<Decimal> = row.try_get("proposed_salary")?;
        // An explicit date previews that date; otherwise the one already on
        // the offer (if any) — the letter extend would send.
        let start_date = start_date.or(row.try_get::<Option<NaiveDate>, _>("start_date")?);
        tx.commit().await?;
        let vars = letter_vars(
            candidate_first_name,
            position_title,
            proposed_salary,
            company_name,
            start_date,
        );
        Ok((render(&subject, &vars), render(&body, &vars)))
    }

    pub async fn extend(&self, offer_id: Uuid, opts: ExtendOptions) -> Result<bool, OfferError> {
        let mut tx = self.rpool().begin().await?;
        // Relay the ambient request scope, when one is bound, onto this transaction:
        // a decorated deployment's row fence reads it; an unfenced one skips this.
        if let Some(scope) = org_scope::current_org_scope() {
            org_scope::bind_org_scope_on(&mut *tx, &scope).await?;
        }

        // `status::text` — the column is a Postgres enum (`offer_status`);
        // sqlx will not decode an enum column straight to a Rust `String`,
        // so cast here and compare strings below.
        // The engine-gated lane (#550): an offer linked into the approvals
        // engine extends ONLY on the engine's Approved verdict — read the
        // link first; unwired/unknown fail closed like every other gate.
        // fetch_optional → Option<Option<Uuid>> (row may be absent, column
        // may be NULL); the double flatten lands on Option<Uuid>.
        let linked: Option<uuid::Uuid> = sqlx::query_scalar::<_, Option<uuid::Uuid>>(
            "SELECT approval_request_id FROM recruitment.job_offers WHERE id = $1",
        )
        .bind(offer_id)
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
        // (outer: row present; inner: column non-null) — and the link MUST
        // exist: an offer with NO approval request behind it (a failed or
        // skipped filing) refuses here, fail-closed, instead of extending
        // on row state alone.
        let Some(request_id) = linked else {
            tx.rollback().await?;
            return Err(OfferError::ApprovalNotGranted);
        };
        {
            let port = self
                .approvals
                .read()
                .expect("offer approvals lock poisoned")
                .clone();
            if !matches!(
                port.status(request_id).await,
                Ok(super::recruitment_approvals_port::RecruitmentVerdict::Approved)
            ) {
                tx.rollback().await?;
                return Err(OfferError::ApprovalNotGranted);
            }
        }

        let row = sqlx::query(
            r#"SELECT o.application_id, o.proposed_salary, o.letter_template_id,
                      o.status::text AS status, o.start_date AS start_date,
                      c.first_name AS candidate_first_name, c.email AS candidate_email,
                      r.id AS requisition_id, r.title AS position_title,
                      r.status::text AS requisition_status,
                      r.headcount AS headcount, r.filled_headcount AS filled_headcount,
                      COALESCE(s.is_hired, FALSE) AS holds_opening,
                      a.refused_at AS refused_at
                 FROM recruitment.job_offers o
                 JOIN recruitment.job_applications a ON a.id = o.application_id
                 JOIN recruitment.candidates c      ON c.id = a.candidate_id
                 JOIN recruitment.job_requisitions r ON r.id = a.requisition_id
            LEFT JOIN recruitment.recruitment_stages s ON s.id = a.stage_id
                WHERE o.id = $1
                FOR UPDATE OF o"#,
        )
        .bind(offer_id)
        .fetch_optional(&mut *tx)
        .await?;

        let row = match row {
            Some(r) => r,
            None => {
                tx.rollback().await?;
                return Err(OfferError::NotFound(offer_id));
            }
        };

        let status: String = row.try_get("status")?;
        if status == "extended" {
            tx.rollback().await?;
            return Ok(false);
        }
        if status != "draft" {
            tx.rollback().await?;
            return Err(OfferError::NotExtensible { offer_id, status });
        }
        if row
            .try_get::<Option<chrono::DateTime<Utc>>, _>("refused_at")?
            .is_some()
        {
            tx.rollback().await?;
            return Err(OfferError::ApplicationRefused(
                row.try_get("application_id")?,
            ));
        }
        if row.try_get::<String, _>("requisition_status")? != "open" {
            let req: Uuid = row.try_get("requisition_id")?;
            tx.rollback().await?;
            return Err(OfferError::RequisitionNotOpen(req));
        }
        // The requisition may have filled up since the draft was approved:
        // refuse BEFORE the letter goes out, not at the move to hired.
        if let Err(full) = require_opening(
            row.try_get("requisition_id")?,
            row.try_get("headcount")?,
            row.try_get("filled_headcount")?,
            row.try_get("holds_opening")?,
        ) {
            tx.rollback().await?;
            return Err(OfferError::NoOpenHeadcount(full));
        }
        // The promised first day: the one given now, else the one already on
        // the offer. It goes into the letter AND onto the offer, so the hire
        // can hand it on as the join date.
        let start_date: Option<NaiveDate> = opts
            .start_date
            .or(row.try_get::<Option<NaiveDate>, _>("start_date")?);

        // Letter seam: an explicit template plus no adapter fails closed
        // BEFORE the offer moves — nothing is silently unsent.
        let template_id: Option<Uuid> = row.try_get("letter_template_id")?;
        let mut letter = None;
        if let Some(tid) = template_id {
            if !self.letters.is_wired() {
                tx.rollback().await?;
                return Err(OfferError::LetterSeamUnwired);
            }
            let template_row = sqlx::query(
                "SELECT subject, body FROM recruitment.offer_letter_templates WHERE id = $1",
            )
            .bind(tid)
            .fetch_one(&mut *tx)
            .await?;
            let subject: String = template_row.try_get("subject")?;
            let body: String = template_row.try_get("body")?;
            let candidate_first_name: String = row.try_get("candidate_first_name")?;
            let position_title: String = row.try_get("position_title")?;
            let proposed_salary: Option<Decimal> = row.try_get("proposed_salary")?;
            let vars = letter_vars(
                candidate_first_name,
                position_title,
                proposed_salary,
                opts.company_name,
                start_date,
            );
            letter = Some(LetterMessage {
                to_email: row
                    .try_get::<Option<String>, _>("candidate_email")?
                    .unwrap_or_default(),
                subject: render(&subject, &vars),
                body: render(&body, &vars),
                res_model: "job_offer",
                res_id: offer_id,
            });
        }

        sqlx::query(
            "UPDATE recruitment.job_offers \
                SET status = 'extended', offered_at = NOW(), start_date = $2 \
              WHERE id = $1",
        )
        .bind(offer_id)
        .bind(start_date)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        // Send after commit: the adapter owns its own durability and cannot
        // join this transaction. A delivery failure leaves the offer extended
        // (true state) and is surfaced for a send-only retry.
        if let Some(msg) = letter {
            if msg.to_email.is_empty() {
                return Err(OfferError::LetterDelivery(
                    "candidate has no email address".to_string(),
                ));
            }
            self.letters
                .send(msg)
                .await
                .map_err(|e| OfferError::LetterDelivery(e.message))?;
        }
        Ok(true)
    }

    /// Mark the offer accepted and stage a `recruitment.hired` outbox event —
    /// atomically.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(event_id))` on a fresh hire. `event_id` is the outbox row's
    ///   id — the end-to-end dedup key (it becomes the bus envelope id, which
    ///   the consumer's inbox keys on).
    /// - `Ok(None)` if the offer was already `accepted`. The producer is
    ///   idempotent on the offer's own state: re-calling `hire` on an accepted
    ///   offer stages NO second event, so the consumer never sees a duplicate
    ///   from this path. (Consumer-side inbox dedup is the mandatory backstop
    ///   regardless — it catches relay redelivery, which the producer cannot
    ///   see.)
    ///
    /// Only an `extended` offer may be hired; any other non-accepted status is
    /// an [`OfferError::NotExtensible`]. The linked application must sit in a
    /// stage flagged `is_hired` ([`OfferError::ApplicationNotHired`] otherwise)
    /// — the pipeline, not the offer, decides who is hired.
    ///
    /// The staged record must name the owning tenant (the outbox is a
    /// still-company-keyed framework surface), so the tenant comes from the
    /// ambient org scope's legacy company id; no bound scope is
    /// [`OfferError::OrgScopeRequired`].
    pub async fn hire(&self, offer_id: Uuid) -> Result<Option<Uuid>, OfferError> {
        let mut tx = self.rpool().begin().await?;
        // Relay the ambient request scope, when one is bound, onto this transaction:
        // a decorated deployment's row fence reads it; an unfenced one skips this.
        if let Some(scope) = org_scope::current_org_scope() {
            org_scope::bind_org_scope_on(&mut *tx, &scope).await?;
        }

        // The durable event path must name the owning tenant: the outbox is a
        // still-company-keyed surface (ADR-0011). Take it from the ambient org
        // scope the composing service resolved — never guess — and fail closed
        // when absent.
        let scope = org_scope::current_org_scope();
        let owning_company = scope.as_ref().and_then(|s| s.legacy_company_id()).ok_or(
            OfferError::OrgScopeRequired(
                "hiring an offer stages an outbox event that must carry the owning tenant",
            ),
        )?;

        // Lock the offer row for the duration of the state change + the outbox
        // stage, so a concurrent hire cannot race a second accept.
        let row = sqlx::query(
            r#"SELECT o.application_id, o.proposed_salary, o.employment_type,
                      o.status::text AS status, o.start_date AS start_date
                 FROM recruitment.job_offers o
                WHERE o.id = $1
                FOR UPDATE OF o"#,
        )
        .bind(offer_id)
        .fetch_optional(&mut *tx)
        .await?;

        let row = match row {
            Some(r) => r,
            None => {
                tx.rollback().await?;
                return Err(OfferError::NotFound(offer_id));
            }
        };

        let application_id: Uuid = row.try_get("application_id")?;
        let proposed_salary: Option<Decimal> = row.try_get("proposed_salary")?;
        let employment_type: Option<String> = row.try_get("employment_type")?;
        let start_date: Option<NaiveDate> = row.try_get("start_date")?;
        let status: String = row.try_get("status")?;

        if status == "accepted" {
            // Producer-side idempotency: an already-accepted offer does not
            // emit a second event.
            tx.rollback().await?;
            return Ok(None);
        }
        if status != "extended" {
            tx.rollback().await?;
            return Err(OfferError::NotExtensible { offer_id, status });
        }

        // 1. Apply the state change.
        sqlx::query(
            r#"UPDATE recruitment.job_offers
               SET status = 'accepted', accepted_at = NOW()
               WHERE id = $1"#,
        )
        .bind(offer_id)
        .execute(&mut *tx)
        .await?;

        // 2. Assemble the new-employee payload from application → candidate
        //    (+ requisition), and enforce the hired-stage guard: the
        //    application must sit in a stage flagged is_hired.
        let joined = sqlx::query(
            r#"SELECT c.first_name    AS first_name,
                      c.last_name      AS last_name,
                      c.email          AS email,
                      c.phone          AS phone,
                      a.candidate_id   AS candidate_id,
                      a.requisition_id AS requisition_id,
                      r.position_id    AS position_id,
                      r.department_id  AS department_id,
                      r.title          AS position_title,
                      (s.is_hired)     AS application_is_hired
                 FROM recruitment.job_applications a
                 JOIN recruitment.candidates c       ON c.id = a.candidate_id
                 JOIN recruitment.recruitment_stages s ON s.id = a.stage_id
            LEFT JOIN recruitment.job_requisitions r  ON r.id = a.requisition_id
                WHERE a.id = $1"#,
        )
        .bind(application_id)
        .fetch_one(&mut *tx)
        .await?;

        if !joined.try_get::<bool, _>("application_is_hired")? {
            tx.rollback().await?;
            return Err(OfferError::ApplicationNotHired { application_id });
        }

        let facts = HiredFacts {
            offer_id,
            company_id: owning_company,
            application_id,
            requisition_id: joined.try_get("requisition_id")?,
            candidate_id: joined.try_get("candidate_id")?,
            first_name: joined.try_get("first_name")?,
            last_name: joined.try_get("last_name")?,
            email: joined.try_get("email")?,
            phone: joined.try_get("phone")?,
            employment_type,
            proposed_salary,
            position_id: joined.try_get("position_id")?,
            department_id: joined.try_get("department_id")?,
            position_title: joined.try_get("position_title")?,
            start_date,
        };
        let payload = hired_event_payload(&facts, Utc::now().date_naive());

        // 3. Stage the outbox event IN THE SAME TX as the state change. The
        //    outbox row's `id` is the end-to-end dedup key (the relay
        //    preserves it as the bus envelope id, which the consumer's inbox
        //    keys on). `outbox::stage` is idempotent on the id (ON CONFLICT
        //    DO NOTHING).
        let event_id = Uuid::new_v4();
        let rec = OutboxRecord::new(
            HIRED_EVENT_TYPE,
            "JobOffer",
            offer_id.to_string(),
            owning_company,
            payload,
            Utc::now(),
        )
        .with_id(event_id);
        outbox::stage(&mut *tx, "recruitment", &rec).await?;

        tx.commit().await?;
        Ok(Some(event_id))
    }

    /// Extended → declined (the candidate turned it down).
    pub async fn decline(&self, offer_id: Uuid) -> Result<(), OfferError> {
        self.cas(offer_id, "declined", &["extended"]).await
    }

    /// Draft/extended → withdrawn (the hiring organization pulled it back).
    pub async fn withdraw(&self, offer_id: Uuid) -> Result<(), OfferError> {
        self.cas(offer_id, "withdrawn", &["draft", "extended"])
            .await
    }

    /// Shared compare-and-set transition: only `from` states may move to `to`.
    async fn cas(
        &self,
        offer_id: Uuid,
        to: &'static str,
        from: &[&'static str],
    ) -> Result<(), OfferError> {
        let mut tx = self.rpool().begin().await?;
        // Relay the ambient request scope, when one is bound, onto this transaction:
        // a decorated deployment's row fence reads it; an unfenced one skips this.
        if let Some(scope) = org_scope::current_org_scope() {
            org_scope::bind_org_scope_on(&mut *tx, &scope).await?;
        }

        let status: Option<String> = sqlx::query_scalar(
            "SELECT status::text FROM recruitment.job_offers WHERE id = $1 FOR UPDATE",
        )
        .bind(offer_id)
        .fetch_optional(&mut *tx)
        .await?;

        let status = match status {
            Some(s) => s,
            None => {
                tx.rollback().await?;
                return Err(OfferError::NotFound(offer_id));
            }
        };
        if !from.contains(&status.as_str()) {
            tx.rollback().await?;
            return Err(OfferError::NotExtensible { offer_id, status });
        }

        sqlx::query(&format!(
            "UPDATE recruitment.job_offers SET status = '{to}' WHERE id = $1"
        ))
        .bind(offer_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(start_date: Option<NaiveDate>) -> HiredFacts {
        HiredFacts {
            offer_id: Uuid::from_u128(1),
            company_id: Uuid::from_u128(2),
            application_id: Uuid::from_u128(3),
            requisition_id: Some(Uuid::from_u128(4)),
            candidate_id: Some(Uuid::from_u128(5)),
            first_name: "Rina".to_string(),
            last_name: Some("Kusuma".to_string()),
            email: Some("rina@example.com".to_string()),
            phone: Some("+62811000111".to_string()),
            employment_type: Some("permanent".to_string()),
            proposed_salary: Some(Decimal::new(9_000_000, 0)),
            position_id: Some(Uuid::from_u128(6)),
            department_id: Some(Uuid::from_u128(7)),
            position_title: Some("Payroll analyst".to_string()),
            start_date,
        }
    }

    fn day(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn the_hire_joins_on_the_first_day_the_offer_promised() {
        let p = hired_event_payload(&facts(Some(day(2026, 11, 2))), day(2026, 10, 2));
        assert_eq!(p["join_date"], "2026-11-02", "join date is the offer's start date, not the hire day");
        assert_eq!(p["start_date"], "2026-11-02");
        assert_eq!(p["accepted_on"], "2026-10-02");
    }

    #[test]
    fn without_an_agreed_start_date_the_hire_joins_on_the_day_of_acceptance() {
        let p = hired_event_payload(&facts(None), day(2026, 10, 2));
        assert_eq!(p["join_date"], "2026-10-02");
        assert!(p["start_date"].is_null(), "no invented start date");
        assert_eq!(p["accepted_on"], "2026-10-02");
    }

    #[test]
    fn the_payload_carries_the_offer_terms_candidate_and_placement() {
        let p = hired_event_payload(&facts(Some(day(2026, 11, 2))), day(2026, 10, 2));
        assert_eq!(p["offer_id"], Uuid::from_u128(1).to_string());
        assert_eq!(p["company_id"], Uuid::from_u128(2).to_string());
        assert_eq!(p["application_id"], Uuid::from_u128(3).to_string());
        assert_eq!(p["requisition_id"], Uuid::from_u128(4).to_string());
        assert_eq!(p["candidate_id"], Uuid::from_u128(5).to_string());
        assert_eq!(p["first_name"], "Rina");
        assert_eq!(p["last_name"], "Kusuma");
        assert_eq!(p["email"], "rina@example.com");
        assert_eq!(p["phone"], "+62811000111");
        assert_eq!(p["employment_type"], "permanent");
        assert_eq!(p["proposed_salary"], "9000000", "salary travels as a decimal string");
        assert_eq!(p["position_id"], Uuid::from_u128(6).to_string());
        assert_eq!(p["department_id"], Uuid::from_u128(7).to_string());
        assert_eq!(p["position_title"], "Payroll analyst");
    }

    #[test]
    fn the_payload_key_set_is_pinned() {
        // The consumers read these keys by name; adding one is fine, but a
        // rename or a drop must fail here first.
        let p = hired_event_payload(&facts(None), day(2026, 10, 2));
        let mut keys: Vec<&str> = p.as_object().unwrap().keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "accepted_on",
                "application_id",
                "candidate_id",
                "company_id",
                "department_id",
                "email",
                "employment_type",
                "first_name",
                "join_date",
                "last_name",
                "offer_id",
                "phone",
                "position_id",
                "position_title",
                "proposed_salary",
                "requisition_id",
                "start_date",
            ]
        );
    }

    #[test]
    fn a_full_requisition_refusal_is_a_typed_409() {
        let e = OfferError::NoOpenHeadcount(NoOpenings {
            requisition_id: Uuid::from_u128(4),
            headcount: 2,
            filled: 2,
        });
        assert_eq!(e.code(), "no_open_headcount");
        assert_eq!(e.http_status(), 409);
        assert!(e.to_string().starts_with("this requisition has no openings left: 2 of 2"));
    }
}
