//! The requisition vacancy rule (hand-authored, user-owned): one place that
//! says whether a requisition still has an opening for the next candidate.
//!
//! A requisition's openings are never stored: they are always
//! `headcount - filled_headcount` (the schema keeps the original ask
//! unmutated so the plan stays auditable). `filled_headcount` moves only in
//! the stage engine (`JobApplicationWriteService::move_stage` / `refuse`),
//! which stays the hard invariant under a row lock. The checks here are the
//! EARLIER doors — taking an application, drafting an offer, extending it —
//! refusing as soon as nothing is left, so a candidate is never put forward,
//! offered or told "yes" for a seat that does not exist.
//!
//! A full requisition deliberately keeps its status. `status` is hand-set by
//! the open/close verbs, and openings come back on their own when a hired
//! application leaves the hired stage or is refused (filled − 1). An
//! automatic close would strand that returned opening: the only way back to
//! `open` is the approvals-gated draft → open lane, which a closed
//! requisition cannot take. "Full" is therefore read off the two counters,
//! which every requisition read already carries.

use uuid::Uuid;

/// Openings a requisition has left: `headcount - filled_headcount`. Zero or
/// less means full (a headcount lowered below the filled count reads as
/// negative, which is just as full).
pub fn openings_left(headcount: i32, filled_headcount: i32) -> i32 {
    headcount - filled_headcount
}

/// The refusal a full requisition answers with — shared by the application
/// and offer write-services so both say the same thing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "this requisition has no openings left: {filled} of {headcount} are filled. \
     Raise its headcount to take on another candidate"
)]
pub struct NoOpenings {
    pub requisition_id: Uuid,
    pub headcount: i32,
    pub filled: i32,
}

/// Admit the next step for a candidate on this requisition, or refuse with
/// [`NoOpenings`] when nothing is left.
///
/// `holds_opening` is true when the candidate's application already sits in
/// a hired stage: that application is one of the filled openings, so it is
/// never refused for the requisition being full — refusing it would block an
/// offer for the very person the seat was counted for.
pub fn require_opening(
    requisition_id: Uuid,
    headcount: i32,
    filled_headcount: i32,
    holds_opening: bool,
) -> Result<(), NoOpenings> {
    if holds_opening || openings_left(headcount, filled_headcount) > 0 {
        return Ok(());
    }
    Err(NoOpenings {
        requisition_id,
        headcount,
        filled: filled_headcount,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req() -> Uuid {
        Uuid::from_u128(0x706)
    }

    #[test]
    fn openings_are_headcount_minus_filled() {
        assert_eq!(openings_left(3, 1), 2);
        assert_eq!(openings_left(2, 2), 0);
        assert_eq!(openings_left(1, 2), -1);
    }

    #[test]
    fn a_full_requisition_refuses_the_next_candidate() {
        let err = require_opening(req(), 2, 2, false).expect_err("2 of 2 filled must refuse");
        assert_eq!(
            err,
            NoOpenings {
                requisition_id: req(),
                headcount: 2,
                filled: 2
            }
        );
    }

    #[test]
    fn an_overfilled_requisition_refuses_too() {
        // headcount lowered below what is already filled
        assert!(require_opening(req(), 1, 2, false).is_err());
    }

    #[test]
    fn a_requisition_with_an_opening_admits() {
        assert_eq!(require_opening(req(), 2, 1, false), Ok(()));
    }

    #[test]
    fn an_application_already_holding_an_opening_is_admitted_when_full() {
        assert_eq!(require_opening(req(), 2, 2, true), Ok(()));
    }

    #[test]
    fn the_refusal_reads_as_a_sentence_with_the_counts() {
        let msg = NoOpenings {
            requisition_id: req(),
            headcount: 2,
            filled: 2,
        }
        .to_string();
        assert_eq!(
            msg,
            "this requisition has no openings left: 2 of 2 are filled. \
             Raise its headcount to take on another candidate"
        );
    }
}
