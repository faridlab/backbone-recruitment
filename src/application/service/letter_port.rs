//! Outbound offer-letter port (hand-authored, user-owned).
//!
//! Extending an offer should put the letter in the candidate's inbox. Sending
//! mail is the mail module's job; recruitment only owns the letter's CONTENT
//! (template + rendered body). To keep the domain→domain Cargo edge at zero,
//! this module declares the PORT and the host app supplies an ADAPTER over
//! the mail module's send surface at composition time.
//!
//! The default is [`UnwiredOfferLetterSink`]: extending an offer WITHOUT a
//! letter template succeeds (no letter was asked for); extending one WITH a
//! template fails closed with the stable `letter_seam_unwired` code — an
//! explicit letter request must never silently send nothing.

use uuid::Uuid;

/// A rendered letter, ready for the adapter's transport.
#[derive(Debug, Clone)]
pub struct LetterMessage {
    /// Candidate's email address.
    pub to_email: String,
    /// Rendered subject line.
    pub subject: String,
    /// Rendered body.
    pub body: String,
    /// The business object the letter is about (`"job_offer"`).
    pub res_model: &'static str,
    /// Id of that object — lets the mail thread group by offer.
    pub res_id: Uuid,
}

/// Acknowledgement: the message was accepted for delivery.
#[derive(Debug, Clone)]
pub struct LetterAck {
    /// Transport-side id (mail message id), for tracing.
    pub message_id: Uuid,
}

/// Why a letter was rejected by the adapter.
#[derive(Debug, thiserror::Error)]
#[error("{code}: {message}")]
pub struct LetterRejected {
    /// Stable machine code (`letter_seam_unwired` for the default sink).
    pub code: String,
    pub message: String,
}

/// The port. Implemented by the host app over its real mail service;
/// [`UnwiredOfferLetterSink`] is the fail-closed default.
#[async_trait::async_trait]
pub trait OfferLetterSink: Send + Sync {
    async fn send(&self, msg: LetterMessage) -> Result<LetterAck, LetterRejected>;

    /// Whether a real adapter sits behind this port. Lets a verb that was
    /// explicitly asked to send a letter refuse BEFORE mutating any row
    /// (fail-closed pre-check) instead of discovering the missing seam after
    /// the write already committed.
    fn is_wired(&self) -> bool {
        true
    }
}

/// The default sink: nothing is wired. A letter explicitly requested via a
/// template fails loudly with `letter_seam_unwired`.
pub struct UnwiredOfferLetterSink;

#[async_trait::async_trait]
impl OfferLetterSink for UnwiredOfferLetterSink {
    fn is_wired(&self) -> bool {
        false
    }

    async fn send(&self, _msg: LetterMessage) -> Result<LetterAck, LetterRejected> {
        Err(LetterRejected {
            code: "letter_seam_unwired".to_string(),
            message: "the letter seam is not wired — supply an OfferLetterSink to send letters"
                .to_string(),
        })
    }
}
