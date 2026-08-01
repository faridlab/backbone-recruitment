//! Custom write-service — the recruitment→employee hire handoff (ADR-005 compound events).
//!
//! This is the PRODUCER side of the first compound event. [`JobOfferWriteService::hire`] is the one
//! verb with a cross-module side effect, and it stages that side effect the transactional-outbox way:
//! in a SINGLE database transaction it (1) marks the `JobOffer` accepted (`extended` → `accepted`,
//! stamps `accepted_at`), (2) assembles the new-employee fields from the offer + application +
//! candidate + requisition, and (3) stages a [`HIRED_EVENT_TYPE`] row into `recruitment.outbox_events`
//! via the framework's [`backbone_outbox::outbox::stage`].
//!
//! That in-tx write is the load-bearing invariant: the offer-accept and the event-emit commit
//! atomically, so there is never an "accepted with no handoff started" window (nor a handoff for a
//! rolled-back accept). The relay (in backbone-hr-app) drains the row onto the integration bus; the
//! employee consumer applies it idempotently (inbox dedup on the event id, which the relay preserves
//! end-to-end as the bus envelope id).
//!
//! This is a user-owned custom file — it is NEVER regenerated, so it is safe to edit freely.

use backbone_outbox::{outbox, OutboxRecord};
use chrono::Utc;
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// The `event_type` stamped on every hire outbox row. The employee consumer subscribes to exactly this
/// pattern (`"recruitment.hired"`).
pub const HIRED_EVENT_TYPE: &str = "recruitment.hired";

/// Errors from the hire write-service.
#[derive(Debug, thiserror::Error)]
pub enum HireError {
    /// No `JobOffer` exists for the given id.
    #[error("offer {0} not found")]
    NotFound(Uuid),
    /// The offer exists but is not in a state that may be accepted (only `extended` may be hired;
    /// `accepted` is a no-op, anything else is a domain violation).
    #[error("offer {offer_id} is not extensible (status: {status})")]
    NotExtensible { offer_id: Uuid, status: String },
    /// A database failure.
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    /// An outbox staging failure.
    #[error("outbox error: {0}")]
    Outbox(#[from] backbone_outbox::OutboxError),
}

/// The recruitment write-service that owns the offer→accepted transition + the outbox emit.
///
/// Construct with [`JobOfferWriteService::new`], or via `RecruitmentModule::job_offer_write_service`
/// when composed. This is a thin custom service — it does NOT replace the CRUD `JobOfferService`; it
/// adds the one compound-write verb that has a cross-module side effect (the hire handoff).
pub struct JobOfferWriteService {
    pool: PgPool,
}

impl JobOfferWriteService {
    /// Create a new write-service bound to the given pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Mark the offer accepted and stage a `recruitment.hired` outbox event — atomically.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(event_id))` on a fresh hire. `event_id` is the outbox row's id — the end-to-end
    ///   dedup key (it becomes the bus envelope id, which the consumer's inbox keys on).
    /// - `Ok(None)` if the offer was already `accepted`. The producer is idempotent on the offer's own
    ///   state: re-calling `hire` on an accepted offer stages NO second event, so the consumer never
    ///   sees a duplicate from this path. (Consumer-side inbox dedup is the mandatory backstop
    ///   regardless — it catches relay redelivery, which the producer cannot see.)
    ///
    /// Only an `extended` offer may be hired; any other non-accepted status is a [`HireError::NotExtensible`].
    pub async fn hire(&self, offer_id: Uuid) -> Result<Option<Uuid>, HireError> {
        let mut tx = self.pool.begin().await?;

        // Lock the offer row for the duration of the state change + the outbox stage, so a concurrent
        // hire cannot race a second accept. `status::text` — the column is a Postgres enum
        // (`offer_status`); sqlx will not decode an enum column straight to a Rust `String`, so cast it
        // here and compare the strings below (avoids importing the OfferStatus enum into this service).
        let row = sqlx::query(
            r#"SELECT company_id, application_id, proposed_salary, employment_type, status::text AS status
               FROM recruitment.job_offers
               WHERE id = $1
               FOR UPDATE"#,
        )
        .bind(offer_id)
        .fetch_optional(&mut *tx)
        .await?;

        let row = match row {
            Some(r) => r,
            None => {
                tx.rollback().await?;
                return Err(HireError::NotFound(offer_id));
            }
        };

        let company_id: Uuid = row.try_get("company_id")?;
        let application_id: Uuid = row.try_get("application_id")?;
        let proposed_salary: Option<Decimal> = row.try_get("proposed_salary")?;
        let employment_type: Option<String> = row.try_get("employment_type")?;
        let status: String = row.try_get("status")?;

        if status == "accepted" {
            // Producer-side idempotency: an already-accepted offer does not emit a second event.
            tx.rollback().await?;
            return Ok(None);
        }
        if status != "extended" {
            tx.rollback().await?;
            return Err(HireError::NotExtensible { offer_id, status });
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

        // 2. Assemble the new-employee payload from application → candidate (+ requisition).
        let joined = sqlx::query(
            r#"SELECT c.first_name    AS first_name,
                      c.last_name      AS last_name,
                      c.email          AS email,
                      r.position_id    AS position_id,
                      r.department_id  AS department_id
                 FROM recruitment.job_applications a
                 JOIN recruitment.candidates c       ON c.id = a.candidate_id
            LEFT JOIN recruitment.job_requisitions r  ON r.id = a.requisition_id
                WHERE a.id = $1"#,
        )
        .bind(application_id)
        .fetch_one(&mut *tx)
        .await?;

        let first_name: String = joined.try_get("first_name")?;
        let last_name: Option<String> = joined.try_get("last_name")?;
        let email: Option<String> = joined.try_get("email")?;
        let position_id: Option<Uuid> = joined.try_get("position_id")?;
        let department_id: Option<Uuid> = joined.try_get("department_id")?;

        let payload = serde_json::json!({
            // Identity — the consumer dedups on `offer_id` (via the envelope id) and keys the
            // employee_number off it, so this payload is round-trip safe across a replay.
            "offer_id": offer_id,
            "company_id": company_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            // Offer terms.
            "employment_type": employment_type,
            "proposed_salary": proposed_salary.map(|d| d.to_string()),
            // Org placement (from the requisition the application answered; nullable).
            "position_id": position_id,
            "department_id": department_id,
            // join_date = the day the offer was accepted (hire effective today; onboarding/lifecycle
            // may adjust it later). ISO date string — the consumer parses it into a NaiveDate.
            "join_date": Utc::now().date_naive().to_string(),
        });

        // 3. Stage the outbox event IN THE SAME TX as the state change. The outbox row's `id` is the
        //    end-to-end dedup key (the relay preserves it as the bus envelope id, which the consumer's
        //    inbox keys on). `outbox::stage` is idempotent on the id (ON CONFLICT DO NOTHING).
        let event_id = Uuid::new_v4();
        let rec = OutboxRecord::new(
            HIRED_EVENT_TYPE,
            "JobOffer",
            offer_id.to_string(),
            company_id,
            payload,
            Utc::now(),
        )
        .with_id(event_id);
        outbox::stage(&mut *tx, "recruitment", &rec).await?;

        tx.commit().await?;
        Ok(Some(event_id))
    }
}
