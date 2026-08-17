//! Offer write-service (hand-authored, user-owned): extend / hire / decline /
//! withdraw — the offer half of the recruitment → employee handoff.
//!
//! Every verb takes the caller's `company` explicitly and binds it onto the
//! transaction before any statement runs, so the whole path is correct under
//! the strict company fence (row-level security): a scoped non-owner session
//! cannot read or write rows outside its company, and a caller that forgets
//! the scope fails closed (no rows) instead of leaking.
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

use backbone_orm::company_scope;
use backbone_outbox::{outbox, OutboxRecord};
use chrono::{NaiveDate, Utc};
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::letter_port::{LetterMessage, OfferLetterSink, UnwiredOfferLetterSink};
use super::offer_letter_render::render;

/// The `event_type` stamped on every hire outbox row. The employee consumer
/// subscribes to exactly this pattern (`"recruitment.hired"`).
pub const HIRED_EVENT_TYPE: &str = "recruitment.hired";

/// Errors from the offer write-service.
#[derive(Debug, thiserror::Error)]
pub enum OfferError {
    /// No `JobOffer` exists for the given id in the caller's company.
    #[error("offer {0} not found")]
    NotFound(Uuid),
    /// `create_draft` on an application that does not exist in the company.
    #[error("application {0} not found in company")]
    ApplicationNotFound(Uuid),
    /// The offer exists but is not in a state that permits this verb.
    #[error("offer {offer_id} is not extensible (status: {status})")]
    NotExtensible { offer_id: Uuid, status: String },
    /// `hire` on an application whose stage is not flagged `is_hired`.
    #[error("application {application_id} is not in a hired stage — move it there first")]
    ApplicationNotHired { application_id: Uuid },
    /// `extend` on an application that was already refused.
    #[error("application {0} was refused — no offer may be extended")]
    ApplicationRefused(Uuid),
    /// `extend` on an application whose requisition is not open.
    #[error("requisition {0} is not open")]
    RequisitionNotOpen(Uuid),
    /// A letter was requested (template set) but no adapter is wired.
    #[error("the letter seam is not wired — supply an OfferLetterSink to send letters")]
    LetterSeamUnwired,
    /// The wired adapter accepted the offer change but failed delivery.
    /// The offer IS extended; only the letter failed — retry just the send.
    #[error("letter delivery failed (offer is extended): {0}")]
    LetterDelivery(String),
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
            OfferError::NotFound(_) => "offer_not_found",
            OfferError::ApplicationNotFound(_) => "application_not_found",
            OfferError::NotExtensible { .. } => "offer_not_extensible",
            OfferError::ApplicationNotHired { .. } => "application_not_hired",
            OfferError::ApplicationRefused(_) => "application_refused",
            OfferError::RequisitionNotOpen(_) => "requisition_not_open",
            OfferError::LetterSeamUnwired => "letter_seam_unwired",
            OfferError::LetterDelivery(_) => "letter_delivery_failed",
            OfferError::Db(_) => "internal_error",
            OfferError::Outbox(_) => "outbox_error",
        }
    }
    pub fn http_status(&self) -> u16 {
        match self {
            OfferError::NotFound(_) | OfferError::ApplicationNotFound(_) => 404,
            OfferError::ApplicationNotHired { .. } => 409,
            OfferError::NotExtensible { .. }
            | OfferError::ApplicationRefused(_)
            | OfferError::RequisitionNotOpen(_) => 422,
            OfferError::LetterSeamUnwired => 422,
            OfferError::LetterDelivery(_) => 502,
            OfferError::Db(_) | OfferError::Outbox(_) => 500,
        }
    }
}

/// Input for `create_draft` — the only way an offer row comes to exist.
#[derive(Debug, Clone)]
pub struct NewJobOffer {
    pub company_id: Uuid,
    pub application_id: Uuid,
    pub proposed_salary: Option<Decimal>,
    pub employment_type: Option<String>,
    /// Letter template to render when the offer is extended (optional — no
    /// template, no letter).
    pub letter_template_id: Option<Uuid>,
}

/// Optional context for the rendered offer letter. Everything has a sensible
/// default (start date = today; company name omitted) so a plain `Default`
/// still renders a usable letter.
#[derive(Debug, Clone, Default)]
pub struct ExtendOptions {
    /// The promised first working day. Defaults to today.
    pub start_date: Option<NaiveDate>,
    /// Company display name for the salutation. Not owned by this module, so
    /// the caller supplies it; omitted → the `{{company_name}}` token stays
    /// visible in the letter.
    pub company_name: Option<String>,
}

/// The offer write-service: the one door for offer state transitions and the
/// hire-handoff producer.
pub struct JobOfferWriteService {
    pool: PgPool,
    letters: Arc<dyn OfferLetterSink>,
}

impl JobOfferWriteService {
    /// Unwired default — letters explicitly requested will fail closed.
    pub fn new(pool: PgPool) -> Self {
        Self { pool, letters: Arc::new(UnwiredOfferLetterSink) }
    }

    /// Bind a real letter adapter (the host app's mail seam).
    pub fn with_letter_sink(pool: PgPool, sink: Arc<dyn OfferLetterSink>) -> Self {
        Self { pool, letters: sink }
    }

    /// Create an offer in `draft` for an ongoing application. Offers only
    /// come to exist here — the generic CRUD write surface stays unmounted for
    /// offers precisely so no path can set `status` directly and sidestep
    /// [`JobOfferWriteService::hire`]'s atomic accept+emit.
    pub async fn create_draft(&self, input: NewJobOffer) -> Result<Uuid, OfferError> {
        let company = input.company_id;
        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company).await?;

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
                return Err(OfferError::ApplicationNotFound(input.application_id));
            }
            Some(r) if r.try_get::<Option<chrono::DateTime<Utc>>, _>("refused_at")?.is_some() => {
                tx.rollback().await?;
                return Err(OfferError::ApplicationRefused(input.application_id));
            }
            Some(_) => {}
        }

        let id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO recruitment.job_offers
                   (id, company_id, application_id, proposed_salary, employment_type,
                    letter_template_id, status, metadata)
               VALUES ($1, $2, $3, $4, $5, $6, 'draft',
                       '{"created_at":null,"updated_at":null,"deleted_at":null,
                         "created_by":null,"updated_by":null,"deleted_by":null}'::jsonb)"#,
        )
        .bind(id)
        .bind(company)
        .bind(input.application_id)
        .bind(input.proposed_salary)
        .bind(&input.employment_type)
        .bind(input.letter_template_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(id)
    }

    /// Draft → extended. Idempotent: an already-extended offer is a no-op
    /// (`Ok(false)`). Guards: the linked application is not refused and its
    /// requisition is open. When the offer references a letter template the
    /// letter is rendered and sent through the [`OfferLetterSink`].
    pub async fn extend(
        &self,
        company: Uuid,
        offer_id: Uuid,
        opts: ExtendOptions,
    ) -> Result<bool, OfferError> {
        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company).await?;

        // `status::text` — the column is a Postgres enum (`offer_status`);
        // sqlx will not decode an enum column straight to a Rust `String`,
        // so cast here and compare strings below.
        let row = sqlx::query(
            r#"SELECT o.application_id, o.proposed_salary, o.letter_template_id,
                      o.status::text AS status,
                      c.first_name AS candidate_first_name, c.email AS candidate_email,
                      r.id AS requisition_id, r.title AS position_title,
                      r.status::text AS requisition_status,
                      a.refused_at AS refused_at
                 FROM recruitment.job_offers o
                 JOIN recruitment.job_applications a ON a.id = o.application_id
                 JOIN recruitment.candidates c      ON c.id = a.candidate_id
                 JOIN recruitment.job_requisitions r ON r.id = a.requisition_id
                WHERE o.id = $1 AND o.company_id = $2
                FOR UPDATE OF o"#,
        )
        .bind(offer_id)
        .bind(company)
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
        if row.try_get::<Option<chrono::DateTime<Utc>>, _>("refused_at")?.is_some() {
            tx.rollback().await?;
            return Err(OfferError::ApplicationRefused(row.try_get("application_id")?));
        }
        if row.try_get::<String, _>("requisition_status")? != "open" {
            let req: Uuid = row.try_get("requisition_id")?;
            tx.rollback().await?;
            return Err(OfferError::RequisitionNotOpen(req));
        }

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
                "SELECT subject, body FROM recruitment.offer_letter_templates \
                 WHERE id = $1 AND company_id = $2",
            )
            .bind(tid)
            .bind(company)
            .fetch_one(&mut *tx)
            .await?;
            let subject: String = template_row.try_get("subject")?;
            let body: String = template_row.try_get("body")?;
            let candidate_first_name: String = row.try_get("candidate_first_name")?;
            let position_title: String = row.try_get("position_title")?;
            let proposed_salary: Option<Decimal> = row.try_get("proposed_salary")?;
            let vars = serde_json::json!({
                "candidate_first_name": candidate_first_name,
                "position_title": position_title,
                "proposed_salary": proposed_salary.map(|d| d.to_string()),
                "company_name": opts.company_name,
                "start_date": opts.start_date.unwrap_or_else(|| Utc::now().date_naive()).to_string(),
            });
            letter = Some(LetterMessage {
                to_email: row.try_get::<Option<String>, _>("candidate_email")?
                    .unwrap_or_default(),
                subject: render(&subject, &vars),
                body: render(&body, &vars),
                res_model: "job_offer",
                res_id: offer_id,
            });
        }

        sqlx::query(
            "UPDATE recruitment.job_offers SET status = 'extended', offered_at = NOW() WHERE id = $1",
        )
        .bind(offer_id)
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
            self.letters.send(msg).await.map_err(|e| OfferError::LetterDelivery(e.message))?;
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
    pub async fn hire(&self, company: Uuid, offer_id: Uuid) -> Result<Option<Uuid>, OfferError> {
        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company).await?;

        // Lock the offer row for the duration of the state change + the outbox
        // stage, so a concurrent hire cannot race a second accept.
        let row = sqlx::query(
            r#"SELECT o.company_id, o.application_id, o.proposed_salary, o.employment_type,
                      o.status::text AS status
                 FROM recruitment.job_offers o
                WHERE o.id = $1 AND o.company_id = $2
                FOR UPDATE OF o"#,
        )
        .bind(offer_id)
        .bind(company)
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

        let first_name: String = joined.try_get("first_name")?;
        let last_name: Option<String> = joined.try_get("last_name")?;
        let email: Option<String> = joined.try_get("email")?;
        let position_id: Option<Uuid> = joined.try_get("position_id")?;
        let department_id: Option<Uuid> = joined.try_get("department_id")?;

        let payload = serde_json::json!({
            // Identity — the consumer dedups on `offer_id` (via the envelope
            // id) and keys the employee_number off it, so this payload is
            // round-trip safe across a replay.
            "offer_id": offer_id,
            "company_id": company,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            // Offer terms.
            "employment_type": employment_type,
            "proposed_salary": proposed_salary.map(|d| d.to_string()),
            // Org placement (from the requisition the application answered;
            // nullable).
            "position_id": position_id,
            "department_id": department_id,
            // join_date = the day the offer was accepted (hire effective
            // today; onboarding/lifecycle may adjust it later). ISO date
            // string — the consumer parses it into a NaiveDate.
            "join_date": Utc::now().date_naive().to_string(),
        });

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
            company,
            payload,
            Utc::now(),
        )
        .with_id(event_id);
        outbox::stage(&mut *tx, "recruitment", &rec).await?;

        tx.commit().await?;
        Ok(Some(event_id))
    }

    /// Extended → declined (the candidate turned it down).
    pub async fn decline(&self, company: Uuid, offer_id: Uuid) -> Result<(), OfferError> {
        self.cas(company, offer_id, "declined", &["extended"]).await
    }

    /// Draft/extended → withdrawn (the company pulled it back).
    pub async fn withdraw(&self, company: Uuid, offer_id: Uuid) -> Result<(), OfferError> {
        self.cas(company, offer_id, "withdrawn", &["draft", "extended"]).await
    }

    /// Shared compare-and-set transition: only `from` states may move to `to`.
    async fn cas(
        &self,
        company: Uuid,
        offer_id: Uuid,
        to: &'static str,
        from: &[&'static str],
    ) -> Result<(), OfferError> {
        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company).await?;

        let status: Option<String> = sqlx::query_scalar(
            "SELECT status::text FROM recruitment.job_offers WHERE id = $1 AND company_id = $2 \
             FOR UPDATE",
        )
        .bind(offer_id)
        .bind(company)
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
